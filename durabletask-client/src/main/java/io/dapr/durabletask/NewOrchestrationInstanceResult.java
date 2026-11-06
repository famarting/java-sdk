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

package io.dapr.durabletask;

/**
 * The result of an operation that creates a new orchestration instance: scheduling it, or rerunning a workflow from
 * an event.
 *
 * <p>A sidecar with no workflow worker connected can accept an operation, save it, and run it once a worker connects.
 * Such an operation is "staged". {@link #isStaged()} tells it apart from an operation that ran at once.</p>
 */
public final class NewOrchestrationInstanceResult {

  private final String instanceId;
  private final boolean staged;

  /**
   * Creates a new result.
   *
   * @param instanceId the ID of the new orchestration instance
   * @param staged     <code>true</code> if the sidecar staged the operation
   */
  public NewOrchestrationInstanceResult(String instanceId, boolean staged) {
    this.instanceId = instanceId;
    this.staged = staged;
  }

  /**
   * Gets the ID of the new orchestration instance.
   *
   * @return the ID of the new orchestration instance
   */
  public String getInstanceId() {
    return this.instanceId;
  }

  /**
   * Tells whether the sidecar staged the operation.
   *
   * @return <code>true</code> if no workflow worker was connected and the sidecar saved the operation to run once one
   *     connects; <code>false</code> if the operation ran at once, or the sidecar does not stage operations
   */
  public boolean isStaged() {
    return this.staged;
  }

  @Override
  public String toString() {
    return "NewOrchestrationInstanceResult{instanceId=" + this.instanceId + ", staged=" + this.staged + "}";
  }
}
