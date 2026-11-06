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
limitations under the License.
*/

package io.dapr.workflows.client;

/**
 * The result of an operation that creates a new workflow instance: scheduling it, or rerunning a workflow from an
 * event.
 *
 * <p>A Dapr sidecar with no workflow worker connected can accept an operation, save it, and run it once a worker
 * connects. Such an operation is "staged". {@link #isStaged()} tells it apart from an operation that ran at once.
 */
public final class NewWorkflowResult {

  private final String instanceId;
  private final boolean staged;

  /**
   * Creates a new result.
   *
   * @param instanceId the ID of the new workflow instance
   * @param staged     true if the sidecar staged the operation
   */
  public NewWorkflowResult(String instanceId, boolean staged) {
    this.instanceId = instanceId;
    this.staged = staged;
  }

  /**
   * Gets the ID of the new workflow instance.
   *
   * @return the ID of the new workflow instance
   */
  public String getInstanceId() {
    return this.instanceId;
  }

  /**
   * Tells whether the sidecar staged the operation.
   *
   * @return true if no workflow worker was connected and the sidecar saved the operation to run once one connects;
   *     false if the operation ran at once, or the sidecar does not stage operations
   */
  public boolean isStaged() {
    return this.staged;
  }

  @Override
  public String toString() {
    return "NewWorkflowResult{instanceId=" + this.instanceId + ", staged=" + this.staged + "}";
  }
}
