/*
 * Copyright 2026 The Dapr Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dapr.workflows.client;

import io.dapr.durabletask.DurableTaskClient;
import io.dapr.durabletask.DurableTaskGrpcClientBuilder;
import io.dapr.durabletask.implementation.protobuf.OrchestratorService;
import io.dapr.durabletask.implementation.protobuf.TaskHubSidecarServiceGrpc;
import io.dapr.workflows.Workflow;
import io.dapr.workflows.WorkflowContext;
import io.dapr.workflows.WorkflowStub;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that {@link DaprWorkflowClient} reports whether the sidecar staged an operation. It runs the real durable
 * task gRPC client against an in-process fake sidecar that sets {@code staged} on its responses.
 */
class DaprWorkflowClientStagedTest {

  private static final String INSTANCE_ID = "testInstanceId";
  private static final String NEW_INSTANCE_ID = "newInstanceId";
  private static final String APP_ID = "targetApp";

  public static class TestWorkflow implements Workflow {
    @Override
    public WorkflowStub create() {
      return WorkflowContext::getInstanceId;
    }
  }

  private Server server;
  private DaprWorkflowClient client;

  // The value of staged the fake sidecar puts on every response.
  private volatile boolean staged;
  // When true, the fake sidecar rejects a start as a duplicate instance ID.
  private volatile boolean rejectStart;
  // The target app ID of each request the fake sidecar received, or "" when there was no router.
  private final List<String> targetAppIds = new CopyOnWriteArrayList<>();
  private final List<String> scheduledNames = new CopyOnWriteArrayList<>();
  private final List<OrchestratorService.RerunWorkflowFromEventRequest> rerunRequests = new CopyOnWriteArrayList<>();

  @BeforeEach
  void setUp() throws Exception {
    String serverName = InProcessServerBuilder.generateName();
    server = InProcessServerBuilder.forName(serverName)
        .directExecutor()
        .addService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
          @Override
          public void startInstance(
              OrchestratorService.CreateInstanceRequest request,
              StreamObserver<OrchestratorService.CreateInstanceResponse> responseObserver) {
            if (rejectStart) {
              responseObserver.onError(Status.ALREADY_EXISTS.asRuntimeException());
              return;
            }
            scheduledNames.add(request.getName());
            targetAppIds.add(request.getRouter().getTargetAppID());
            responseObserver.onNext(OrchestratorService.CreateInstanceResponse.newBuilder()
                .setInstanceId(request.getInstanceId())
                .setStaged(staged)
                .build());
            responseObserver.onCompleted();
          }

          @Override
          public void raiseEvent(
              OrchestratorService.RaiseEventRequest request,
              StreamObserver<OrchestratorService.RaiseEventResponse> responseObserver) {
            targetAppIds.add(request.getRouter().getTargetAppID());
            responseObserver.onNext(OrchestratorService.RaiseEventResponse.newBuilder().setStaged(staged).build());
            responseObserver.onCompleted();
          }

          @Override
          public void terminateInstance(
              OrchestratorService.TerminateRequest request,
              StreamObserver<OrchestratorService.TerminateResponse> responseObserver) {
            targetAppIds.add(request.getRouter().getTargetAppID());
            responseObserver.onNext(OrchestratorService.TerminateResponse.newBuilder().setStaged(staged).build());
            responseObserver.onCompleted();
          }

          @Override
          public void suspendInstance(
              OrchestratorService.SuspendRequest request,
              StreamObserver<OrchestratorService.SuspendResponse> responseObserver) {
            targetAppIds.add(request.getRouter().getTargetAppID());
            responseObserver.onNext(OrchestratorService.SuspendResponse.newBuilder().setStaged(staged).build());
            responseObserver.onCompleted();
          }

          @Override
          public void resumeInstance(
              OrchestratorService.ResumeRequest request,
              StreamObserver<OrchestratorService.ResumeResponse> responseObserver) {
            targetAppIds.add(request.getRouter().getTargetAppID());
            responseObserver.onNext(OrchestratorService.ResumeResponse.newBuilder().setStaged(staged).build());
            responseObserver.onCompleted();
          }

          @Override
          public void rerunWorkflowFromEvent(
              OrchestratorService.RerunWorkflowFromEventRequest request,
              StreamObserver<OrchestratorService.RerunWorkflowFromEventResponse> responseObserver) {
            rerunRequests.add(request);
            responseObserver.onNext(OrchestratorService.RerunWorkflowFromEventResponse.newBuilder()
                .setNewInstanceID(request.getNewInstanceID())
                .setStaged(staged)
                .build());
            responseObserver.onCompleted();
          }
        })
        .build()
        .start();

    ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
    DurableTaskClient innerClient = new DurableTaskGrpcClientBuilder().grpcChannel(channel).build();
    client = newClient(innerClient, channel);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (client != null) {
      client.close();
    }
    if (server != null) {
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void scheduleNewWorkflowWithResultReportsStaged(boolean staged) {
    this.staged = staged;

    NewWorkflowResult byName = client.scheduleNewWorkflowWithResult("TestWorkflow",
        new NewWorkflowOptions().setInstanceId(INSTANCE_ID));
    NewWorkflowResult byClass = client.scheduleNewWorkflowWithResult(TestWorkflow.class,
        new NewWorkflowOptions().setInstanceId(INSTANCE_ID).setAppId(APP_ID));

    assertEquals(INSTANCE_ID, byName.getInstanceId());
    assertEquals(staged, byName.isStaged());
    assertEquals(INSTANCE_ID, byClass.getInstanceId());
    assertEquals(staged, byClass.isStaged());
    assertEquals(List.of("TestWorkflow", TestWorkflow.class.getCanonicalName()), scheduledNames);
    assertEquals(List.of("", APP_ID), targetAppIds);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void raiseEventWithResultReportsStaged(boolean staged) {
    this.staged = staged;

    assertEquals(staged, client.raiseEventWithResult(INSTANCE_ID, "event", "payload").isStaged());
    assertEquals(staged, client.raiseEventWithResult(INSTANCE_ID, "event", "payload", APP_ID).isStaged());
    assertEquals(List.of("", APP_ID), targetAppIds);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void terminateWorkflowWithResultReportsStaged(boolean staged) {
    this.staged = staged;

    assertEquals(staged, client.terminateWorkflowWithResult(INSTANCE_ID, "output").isStaged());
    assertEquals(staged, client.terminateWorkflowWithResult(INSTANCE_ID, "output", APP_ID).isStaged());
    assertEquals(List.of("", APP_ID), targetAppIds);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void suspendWorkflowWithResultReportsStaged(boolean staged) {
    this.staged = staged;

    assertEquals(staged, client.suspendWorkflowWithResult(INSTANCE_ID, "reason").isStaged());
    assertEquals(staged, client.suspendWorkflowWithResult(INSTANCE_ID, "reason", APP_ID).isStaged());
    assertEquals(List.of("", APP_ID), targetAppIds);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void resumeWorkflowWithResultReportsStaged(boolean staged) {
    this.staged = staged;

    assertEquals(staged, client.resumeWorkflowWithResult(INSTANCE_ID, "reason").isStaged());
    assertEquals(staged, client.resumeWorkflowWithResult(INSTANCE_ID, "reason", APP_ID).isStaged());
    assertEquals(List.of("", APP_ID), targetAppIds);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void rerunWorkflowFromEventWithResultReportsStaged(boolean staged) {
    this.staged = staged;

    NewWorkflowResult withoutOptions = client.rerunWorkflowFromEventWithResult(INSTANCE_ID, 2, null);
    NewWorkflowResult withOptions = client.rerunWorkflowFromEventWithResult(INSTANCE_ID, 3,
        new RerunWorkflowFromEventOptions().setNewInstanceId(NEW_INSTANCE_ID).setInput("in").setOverwriteInput(true));

    assertEquals(staged, withoutOptions.isStaged());
    assertEquals(NEW_INSTANCE_ID, withOptions.getInstanceId());
    assertEquals(staged, withOptions.isStaged());

    assertEquals(2, rerunRequests.size());
    assertEquals(2, rerunRequests.get(0).getEventID());
    assertFalse(rerunRequests.get(0).getOverwriteInput());
    assertEquals(3, rerunRequests.get(1).getEventID());
    assertTrue(rerunRequests.get(1).getOverwriteInput());
    assertEquals("\"in\"", rerunRequests.get(1).getInput().getValue());
  }

  @Test
  void rerunWorkflowFromEventWithResultValidatesArguments() {
    assertThrows(IllegalArgumentException.class, () -> client.rerunWorkflowFromEventWithResult("", 1, null));
    assertThrows(IllegalArgumentException.class, () -> client.rerunWorkflowFromEventWithResult(INSTANCE_ID, -1, null));
    assertThrows(IllegalArgumentException.class, () -> client.rerunWorkflowFromEventWithResult(INSTANCE_ID, 1,
        new RerunWorkflowFromEventOptions().setInput("in")));
    assertTrue(rerunRequests.isEmpty());
  }

  @Test
  void scheduleNewWorkflowWithResultThrowsWhenInstanceAlreadyExists() {
    rejectStart = true;

    NewWorkflowOptions options = new NewWorkflowOptions().setInstanceId(INSTANCE_ID);
    WorkflowInstanceAlreadyExistsException e = assertThrows(WorkflowInstanceAlreadyExistsException.class,
        () -> client.scheduleNewWorkflowWithResult("TestWorkflow", options));
    assertEquals(INSTANCE_ID, e.getInstanceId());
  }

  @SuppressWarnings("unchecked")
  private static DaprWorkflowClient newClient(DurableTaskClient innerClient, ManagedChannel channel) throws Exception {
    for (Constructor<?> c : DaprWorkflowClient.class.getDeclaredConstructors()) {
      if (c.getParameterCount() == 2 && c.getParameterTypes()[0] == DurableTaskClient.class) {
        c.setAccessible(true);
        return ((Constructor<DaprWorkflowClient>) c).newInstance(innerClient, channel);
      }
    }
    throw new IllegalStateException("DaprWorkflowClient(DurableTaskClient, ManagedChannel) not found");
  }
}
