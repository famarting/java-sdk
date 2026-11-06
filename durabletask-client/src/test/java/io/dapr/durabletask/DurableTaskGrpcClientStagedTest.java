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

package io.dapr.durabletask;

import io.dapr.durabletask.implementation.protobuf.HistoryEvents.HistoryEvent;
import io.dapr.durabletask.implementation.protobuf.OrchestratorService;
import io.dapr.durabletask.implementation.protobuf.TaskHubSidecarServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests that the client reports whether the sidecar staged an operation. A sidecar with no workflow worker
 * connected can accept an operation, save it, and run it once a worker connects; it then sets {@code staged} on the
 * response.
 */
class DurableTaskGrpcClientStagedTest {

  private static final String INSTANCE_ID = "testInstanceId";
  private static final String NEW_INSTANCE_ID = "newInstanceId";
  private static final String APP_ID = "targetApp";

  private Server server;
  private ManagedChannel channel;
  private DurableTaskClient client;

  // The value of staged the fake sidecar puts on every response.
  private volatile boolean staged;

  private final AtomicReference<OrchestratorService.RaiseEventRequest> raiseEventRequest = new AtomicReference<>();
  private final AtomicReference<OrchestratorService.TerminateRequest> terminateRequest = new AtomicReference<>();
  private final AtomicReference<OrchestratorService.SuspendRequest> suspendRequest = new AtomicReference<>();
  private final AtomicReference<OrchestratorService.ResumeRequest> resumeRequest = new AtomicReference<>();

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
            raiseEventRequest.set(request);
            responseObserver.onNext(OrchestratorService.RaiseEventResponse.newBuilder().setStaged(staged).build());
            responseObserver.onCompleted();
          }

          @Override
          public void terminateInstance(
              OrchestratorService.TerminateRequest request,
              StreamObserver<OrchestratorService.TerminateResponse> responseObserver) {
            terminateRequest.set(request);
            responseObserver.onNext(OrchestratorService.TerminateResponse.newBuilder().setStaged(staged).build());
            responseObserver.onCompleted();
          }

          @Override
          public void suspendInstance(
              OrchestratorService.SuspendRequest request,
              StreamObserver<OrchestratorService.SuspendResponse> responseObserver) {
            suspendRequest.set(request);
            responseObserver.onNext(OrchestratorService.SuspendResponse.newBuilder().setStaged(staged).build());
            responseObserver.onCompleted();
          }

          @Override
          public void resumeInstance(
              OrchestratorService.ResumeRequest request,
              StreamObserver<OrchestratorService.ResumeResponse> responseObserver) {
            resumeRequest.set(request);
            responseObserver.onNext(OrchestratorService.ResumeResponse.newBuilder().setStaged(staged).build());
            responseObserver.onCompleted();
          }

          @Override
          public void rerunWorkflowFromEvent(
              OrchestratorService.RerunWorkflowFromEventRequest request,
              StreamObserver<OrchestratorService.RerunWorkflowFromEventResponse> responseObserver) {
            responseObserver.onNext(OrchestratorService.RerunWorkflowFromEventResponse.newBuilder()
                .setNewInstanceID(request.getNewInstanceID())
                .setStaged(staged)
                .build());
            responseObserver.onCompleted();
          }
        })
        .build()
        .start();

    channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
    client = new DurableTaskGrpcClientBuilder().grpcChannel(channel).build();
  }

  @AfterEach
  void tearDown() throws Exception {
    if (client != null) {
      client.close();
    }
    if (channel != null) {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
    if (server != null) {
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void scheduleNewOrchestrationInstanceWithResultReportsStaged(boolean staged) {
    this.staged = staged;

    NewOrchestrationInstanceResult result = client.scheduleNewOrchestrationInstanceWithResult(
        "TestOrchestration", new NewOrchestrationInstanceOptions().setInstanceId(INSTANCE_ID));

    assertEquals(INSTANCE_ID, result.getInstanceId());
    assertEquals(staged, result.isStaged());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void raiseEventWithResultReportsStaged(boolean staged) {
    this.staged = staged;

    OrchestrationOperationResult result = client.raiseEventWithResult(INSTANCE_ID, "event", "payload", APP_ID);

    assertEquals(staged, result.isStaged());
    assertEquals(INSTANCE_ID, raiseEventRequest.get().getInstanceId());
    assertEquals(APP_ID, raiseEventRequest.get().getRouter().getTargetAppID());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void terminateWithResultReportsStaged(boolean staged) {
    this.staged = staged;

    OrchestrationOperationResult result = client.terminateWithResult(INSTANCE_ID, "output", APP_ID);

    assertEquals(staged, result.isStaged());
    assertEquals(INSTANCE_ID, terminateRequest.get().getInstanceId());
    assertEquals(APP_ID, terminateRequest.get().getRouter().getTargetAppID());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void suspendInstanceWithResultReportsStaged(boolean staged) {
    this.staged = staged;

    OrchestrationOperationResult result = client.suspendInstanceWithResult(INSTANCE_ID, "reason", APP_ID);

    assertEquals(staged, result.isStaged());
    assertEquals(INSTANCE_ID, suspendRequest.get().getInstanceId());
    assertEquals(APP_ID, suspendRequest.get().getRouter().getTargetAppID());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void resumeInstanceWithResultReportsStaged(boolean staged) {
    this.staged = staged;

    OrchestrationOperationResult result = client.resumeInstanceWithResult(INSTANCE_ID, "reason", APP_ID);

    assertEquals(staged, result.isStaged());
    assertEquals(INSTANCE_ID, resumeRequest.get().getInstanceId());
    assertEquals(APP_ID, resumeRequest.get().getRouter().getTargetAppID());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void rerunWorkflowFromEventWithResultReportsStaged(boolean staged) {
    this.staged = staged;

    NewOrchestrationInstanceResult result = client.rerunWorkflowFromEventWithResult(
        INSTANCE_ID, 3, NEW_INSTANCE_ID, null, false);

    assertEquals(NEW_INSTANCE_ID, result.getInstanceId());
    assertEquals(staged, result.isStaged());
  }

  @Test
  void existingMethodsStillWorkWhenTheSidecarStages() {
    this.staged = true;

    assertEquals(INSTANCE_ID, client.scheduleNewOrchestrationInstance("TestOrchestration", null, INSTANCE_ID));
    assertEquals(NEW_INSTANCE_ID, client.rerunWorkflowFromEvent(INSTANCE_ID, 3, NEW_INSTANCE_ID, null, false));

    client.raiseEvent(INSTANCE_ID, "event", "payload");
    client.terminate(INSTANCE_ID, "output");
    client.suspendInstance(INSTANCE_ID, "reason");
    client.resumeInstance(INSTANCE_ID, "reason");

    assertEquals(INSTANCE_ID, raiseEventRequest.get().getInstanceId());
    assertFalse(raiseEventRequest.get().hasRouter());
    assertEquals(INSTANCE_ID, terminateRequest.get().getInstanceId());
    assertEquals(INSTANCE_ID, suspendRequest.get().getInstanceId());
    assertEquals(INSTANCE_ID, resumeRequest.get().getInstanceId());
  }

  @Test
  void defaultImplementationsCallTheExistingMethodsAndReportNotStaged() {
    RecordingClient recording = new RecordingClient();

    NewOrchestrationInstanceResult scheduled = recording.scheduleNewOrchestrationInstanceWithResult(
        "TestOrchestration", new NewOrchestrationInstanceOptions().setInstanceId(INSTANCE_ID));
    assertEquals(INSTANCE_ID, scheduled.getInstanceId());
    assertFalse(scheduled.isStaged());

    assertFalse(recording.raiseEventWithResult(INSTANCE_ID, "event", "payload", null).isStaged());
    assertFalse(recording.terminateWithResult(INSTANCE_ID, "output", null).isStaged());
    assertFalse(recording.suspendInstanceWithResult(INSTANCE_ID, "reason", null).isStaged());
    assertFalse(recording.resumeInstanceWithResult(INSTANCE_ID, "reason", null).isStaged());

    NewOrchestrationInstanceResult rerun = recording.rerunWorkflowFromEventWithResult(
        INSTANCE_ID, 3, NEW_INSTANCE_ID, null, false);
    assertEquals(NEW_INSTANCE_ID, rerun.getInstanceId());
    assertFalse(rerun.isStaged());

    assertEquals(List.of("schedule", "raiseEvent", "terminate", "suspend", "resume", "rerun"), recording.calls);

    // The defaults keep the cross-app check of the existing methods.
    assertThrows(UnsupportedOperationException.class,
        () -> recording.raiseEventWithResult(INSTANCE_ID, "event", null, APP_ID));
  }

  /**
   * A client that only implements the existing abstract methods, to check the defaults of the new ones.
   */
  private static final class RecordingClient extends DurableTaskClient {
    private final List<String> calls = new ArrayList<>();

    @Override
    public String scheduleNewOrchestrationInstance(String orchestratorName, NewOrchestrationInstanceOptions options) {
      calls.add("schedule");
      return options.getInstanceId();
    }

    @Override
    public void raiseEvent(String instanceId, String eventName, Object eventPayload) {
      calls.add("raiseEvent");
    }

    @Override
    public void terminate(String instanceId, Object output) {
      calls.add("terminate");
    }

    @Override
    public void suspendInstance(String instanceId, String reason) {
      calls.add("suspend");
    }

    @Override
    public void resumeInstance(String instanceId, String reason) {
      calls.add("resume");
    }

    @Override
    public String rerunWorkflowFromEvent(String sourceInstanceId, int eventId, String newInstanceId, Object input,
                                         boolean overwriteInput) {
      calls.add("rerun");
      return newInstanceId;
    }

    @Override
    public OrchestrationMetadata getInstanceMetadata(String instanceId, boolean getInputsAndOutputs) {
      throw new UnsupportedOperationException();
    }

    @Override
    public OrchestrationMetadata waitForInstanceStart(String instanceId, Duration timeout,
                                                      boolean getInputsAndOutputs) {
      throw new UnsupportedOperationException();
    }

    @Override
    public OrchestrationMetadata waitForInstanceCompletion(String instanceId, Duration timeout,
                                                           boolean getInputsAndOutputs) {
      throw new UnsupportedOperationException();
    }

    @Override
    public PurgeResult purgeInstance(String instanceId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public PurgeResult purgeInstances(PurgeInstanceCriteria purgeInstanceCriteria) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String restartInstance(String instanceId, boolean restartWithNewInstanceId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public OrchestratorService.ListInstanceIDsResponse listInstanceIds(String continuationToken, Integer pageSize) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<HistoryEvent> getInstanceHistory(String instanceId) {
      throw new UnsupportedOperationException();
    }
  }
}
