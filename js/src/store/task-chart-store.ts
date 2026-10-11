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

  /**
   * Ensures the active-task line ends at zero once nothing is running any
   * more. A lost stage-completion event can leave the last plotted point above
   * zero; this draws the missing drop at `time` (never earlier than the last
   * plotted point, so the series stays monotonic).
   */
  closeOut(time: number) {
    const last = this.taskDataY.length - 1;
    if (last < 0 || this.taskDataY[last] === 0) {
      return;
    }
    const lastTime = this.taskDataX[last];
    const t = new Date(time).getTime();
    this.addTaskData(Number.isFinite(t) ? Math.max(t, lastTime) : lastTime, 0);
  }

  addExecutorData(time: number, numCores: number) {
    this.executorDataX.push(time);
    this.executorDataY.push(numCores);
  }

  addTaskData(time: number, numTasks: number) {
    this.taskDataX.push(new Date(time).getTime());
    this.taskDataY.push(numTasks);
    this.addExecutorData(
      new Date(time).getTime(),
      this.notebookStore.numTotalCores || 0
    );
  }

  onSparkJobStart(data: any) {
    const submissionTimestamp = new Date(data.submissionTime).getTime();
    this.jobDataX.push(submissionTimestamp);
    this.jobDataY.push(0);
    this.jobDataText.push(`Job ${data.jobId} started`);

    this.addTaskData(submissionTimestamp, 0);
  }

  onSparkJobEnd(data: any) {
    const completionTime = new Date(
      data.completionTime || Date.now()
    ).getTime();
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

