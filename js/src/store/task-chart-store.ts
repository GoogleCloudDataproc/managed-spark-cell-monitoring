/**
 * @license
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import type { NotebookStore } from './notebook';

export class TaskChartStore {
  jobDataX: Array<number> = [];
  jobDataY: Array<number> = [];
  jobDataText: Array<string> = [];
  executorDataX: Array<number> = [];
  executorDataY: Array<number> = [];
  taskDataX: Array<number> = [];
  taskDataY: Array<number> = [];
  numActiveTasks = 0;

  constructor(private notebookStore: NotebookStore) {}
  reset() {
    this.jobDataX = [];
    this.jobDataY = [];
    this.jobDataText = [];
    this.executorDataX = [];
    this.executorDataY = [];
    this.taskDataX = [];
    this.taskDataY = [];
    this.numActiveTasks = 0;
  }

  addExecutorData(time: number, numCores: number) {
    this.executorDataX.push(time);
    this.executorDataY.push(numCores);
  }

  /**
   * Returns `time` clamped so the series never goes backwards.
   *
   * The chart is drawn as a step line in insertion order. Timestamps come from
   * the Spark driver (job/stage events) and, with an older listener JAR, from
   * the browser clock (periodic task samples). Clock skew, delivery latency
   * and the 250ms sampling timer can therefore deliver a point whose time is
   * earlier than the previous one; plotting it as-is folds the line back on
   * itself. Clamping to the last plotted time keeps the step semantics ("state
   * changed no earlier than the previous point") and keeps the task and
   * executor series aligned index-for-index.
   */
  private monotonic(time: number | string | null | undefined): number {
    const last = this.taskDataX[this.taskDataX.length - 1];
    // `new Date(null)` is the epoch, not an invalid date, so a missing value
    // has to be rejected before parsing or it would plot at 1970.
    const t = time == null ? Number.NaN : new Date(time).getTime();
    if (!Number.isFinite(t)) {
      // A missing or unparsable timestamp must not poison the series; reuse
      // the last plotted time (or "now" for the very first point).
      return last ?? Date.now();
    }
    return last === undefined ? t : Math.max(t, last);
  }

  addTaskData(time: number, numTasks: number) {
    const t = this.monotonic(time);
    this.taskDataX.push(t);
    this.taskDataY.push(numTasks);
    this.addExecutorData(t, this.notebookStore.numTotalCores || 0);
  }

  onSparkJobStart(data: any) {
    const submissionTimestamp = this.monotonic(data.submissionTime);
    this.jobDataX.push(submissionTimestamp);
    this.jobDataY.push(0);
    this.jobDataText.push(`Job ${data.jobId} started`);

    this.addTaskData(submissionTimestamp, 0);
  }

  onSparkJobEnd(data: any) {
    const completionTime = this.monotonic(data.completionTime || Date.now());
    this.jobDataX.push(completionTime);
    this.jobDataY.push(0);
    this.jobDataText.push(`Job ${data.jobId} ended`);

    this.addTaskData(completionTime, 0);
  }

  onSparkStageActive(time: number, numActiveTasks: number) {
    this.numActiveTasks = numActiveTasks;
    this.addTaskData(time, numActiveTasks);
  }
}
