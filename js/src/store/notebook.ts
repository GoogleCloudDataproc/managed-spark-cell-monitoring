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

import { makeAutoObservable } from 'mobx';
import { SparkStage } from './spark-stage';
import { SparkJob } from './spark-job';
import { Cell } from './cell';

/**
 * Task counters of a job, summed over its stages. Skipped stages count as
 * fully completed: Spark reuses their output without re-running them.
 */
type TaskTotals = {
  numTasks: number;
  numActiveTasks: number;
  numCompletedTasks: number;
  numFailedTasks: number;
};

export class NotebookStore {
  // Optional fields are initialised explicitly so that MobX sees them as
  // own properties and makes them observable (with an ES2020 target,
  // `field?: T` without an initialiser does not create the property).
  numExecutors: number | undefined = undefined;
  numTotalCores: number | undefined = undefined;
  applicationId: string | undefined = undefined;
  applicationAttemptId: string | undefined = undefined;
  viewUrl: string | undefined = undefined;
  uniqueId = 'default-key';
  hideAllDisplays = false;

  cells: { [cellId: string]: Cell } = {};
  jobs: { [jobId: string]: SparkJob } = {};
  // Stages are bookkeeping only: they feed the task totals of their job.
  stages: { [stageId: string]: SparkStage } = {};

  constructor(public notebookPanelId: string) {
    makeAutoObservable(this);
    if (notebookPanelId && notebookPanelId !== 'default-key') {
      this.uniqueId = notebookPanelId;
    }
  }

  setViewUrl(url?: string) {
    this.viewUrl = url || undefined;
  }

  resetNotebook() {
    Object.values(this.cells).forEach(cell => cell.reset());
  }

  resetCell(cellId:string) {
    const cell = this.cells[cellId];
    if (cell) {
      cell.reset();
    } else {
      console.error("Cell not found to reset");
    }
  }

  toggleHideAllDisplays() {
    this.hideAllDisplays = !this.hideAllDisplays;
  }

  onSparkApplicationStart(data: any) {
    this.applicationId = data.appId;
    this.applicationAttemptId = data.appAttemptId;
    this.uniqueId = `app${this.applicationId}-attempt${this.applicationAttemptId}`;
  }

  private deleteCellData(cellId: string) {
    const cell = this.cells[cellId];
    if (cell) {
      cell.uniqueJobIds.forEach(uniqueJobId => {
        const job = this.jobs[uniqueJobId];
        if (job) {
          job.uniqueStageIds.forEach(uniqueStageId => {
            const stage = this.stages[uniqueStageId];
            if (stage) {
              delete this.stages[uniqueStageId];
            }
          });
          delete this.jobs[uniqueJobId];
        }
      });
      delete this.cells[cellId];
    }
  }

  onCellRemoved(cellId: string) {
    this.deleteCellData(cellId);
  }

  onCellExecutedAgain(cellId: string) {
    this.deleteCellData(cellId);
    this.cells[cellId] = new Cell(cellId, this);
  }

  private sumStageTasks(job: SparkJob): TaskTotals {
    const totals: TaskTotals = {
      numTasks: 0,
      numActiveTasks: 0,
      numCompletedTasks: 0,
      numFailedTasks: 0,
    };
    job.uniqueStageIds.forEach((uniqueStageId) => {
      const stage = this.stages[uniqueStageId];
      if (!stage) {
        return;
      }
      totals.numTasks += stage.numTasks || 0;
      totals.numActiveTasks += stage.numActiveTasks || 0;
      totals.numFailedTasks += stage.numFailedTasks || 0;
      totals.numCompletedTasks +=
        stage.status === 'SKIPPED' ? stage.numTasks || 0 : stage.numCompletedTasks || 0;
    });
    return totals;
  }

  /** Recomputes a job's task counters from its stages. */
  private recomputeJobTasks(job: SparkJob) {
    const totals = this.sumStageTasks(job);
    job.numTasks = totals.numTasks;
    job.numActiveTasks = totals.numActiveTasks;
    job.numCompletedTasks = totals.numCompletedTasks;
    job.numFailedTasks = totals.numFailedTasks;
  }

  onSparkJobStart(cellId: string, data: any) {
    const uniqueJobId = `${this.uniqueId}-job-${data.jobId}`;
    const existingJob = this.jobs[uniqueJobId];
    if (existingJob && existingJob.uniqueStageIds.length > 0) {
      if (!this.cells[cellId]) {
        this.cells[cellId] = new Cell(cellId, this);
      }
      if (!this.cells[cellId].uniqueJobIds.includes(uniqueJobId)) {
        this.cells[cellId].uniqueJobIds.push(uniqueJobId);
      }
      return;
    }

    // These values are set here as previous messages may
    // be missed if reconnecting from a browser reload. Only valid numbers
    // are applied so a payload without them cannot clear known counts.
    if (typeof data.totalCores === 'number') {
      this.numTotalCores = Math.max(0, data.totalCores);
    }
    if (typeof data.numExecutors === 'number') {
      this.numExecutors = Math.max(0, data.numExecutors);
    }

    const job = existingJob || new SparkJob();
    job.uniqueId = uniqueJobId;
    job.jobId = data.jobId;
    if (!job.endTime) {
      job.status = data.status;
    }
    job.cellId = cellId;
    const jobName = String(data.name).split(' at ')[0];
    job.name = jobName;
    job.startTime = new Date(data.submissionTime);
    job.numTasks = data.numTasks;

    data.stageIds.forEach((stageId: string) => {
      const uniqueStageId = `${this.uniqueId}-stage-${stageId}`;
      let stage = this.stages[uniqueStageId];
      if (!stage) {
        stage = new SparkStage();
        stage.uniqueId = uniqueStageId;
        stage.stageId = String(stageId);
        stage.status = job.endTime ? 'SKIPPED' : 'PENDING';
        this.stages[uniqueStageId] = stage;
      }
      stage.uniqueJobId = job.uniqueId;
      if (data.stageInfos && data.stageInfos[stageId]) {
        stage.numTasks = data.stageInfos[stageId].numTasks;
        stage.name = data.stageInfos[stageId].name;
      }
      if (!job.uniqueStageIds.includes(uniqueStageId)) {
        job.uniqueStageIds.push(uniqueStageId);
      }
    });
    job.uniqueStageIds.sort((a, b) =>
      a.localeCompare(b, undefined, { numeric: true })
    );

    if (job.name === 'null' && data.stageIds.length > 0) {
      const lastStageId = Math.max(...data.stageIds.map(Number));
      job.name =
        this.stages[`${this.uniqueId}-stage-${lastStageId}`]?.name || 'Job';
    }

    // Re-aggregate task counts from stages in case stage updates arrived
    // before jobStart. Keep the jobStart estimate when the stages carry no
    // task counts yet.
    const totals = this.sumStageTasks(job);
    if (totals.numTasks > 0) {
      job.numTasks = totals.numTasks;
      job.numActiveTasks = job.endTime ? 0 : totals.numActiveTasks;
      job.numCompletedTasks = totals.numCompletedTasks;
      job.numFailedTasks = totals.numFailedTasks;
    }

    if (!this.cells[cellId]) {
      this.cells[cellId] = new Cell(cellId, this);
    }
    if (!this.cells[cellId].uniqueJobIds.includes(job.uniqueId)) {
      this.cells[cellId].uniqueJobIds.push(job.uniqueId);
    }
    job.cell = this.cells[cellId];
    job.cell.taskChartStore.onSparkJobStart(data);
    this.jobs[job.uniqueId] = job;
  }

  onSparkJobEnd(data: any) {
    const uniqueId = `${this.uniqueId}-job-${data.jobId}`;
    let job = this.jobs[uniqueId];
    if (!job) {
      job = new SparkJob();
      job.uniqueId = uniqueId;
      job.jobId = data.jobId;
      job.name = `Job ${data.jobId}`;
      job.startTime = new Date(data.completionTime || Date.now());
      this.jobs[uniqueId] = job;
    }
    if (job.endTime) {
      return;
    }
    job.status = data.status;
    job.endTime = new Date(data.completionTime);
    job.uniqueStageIds.forEach(uniqueStageId => {
      if (this.stages[uniqueStageId]?.status === 'PENDING') {
        this.stages[uniqueStageId].status = 'SKIPPED';
        // Skipped stages should remain at 0 completed tasks so their bar stays
        // grey
        this.stages[uniqueStageId].numCompletedTasks = 0;
      }
    });

    // Re-aggregate Job stats to ensure skipped stages contribute to Job
    // progress
    this.recomputeJobTasks(job);

    job.cell?.taskChartStore.onSparkJobEnd(data);
  }

  onSparkStageSubmitted(data: any) {
    const uniqueStageId = `${this.uniqueId}-stage-${data.stageId}`;
    if (!this.stages[uniqueStageId]) {
      this.stages[uniqueStageId] = new SparkStage();
      this.stages[uniqueStageId].uniqueId = uniqueStageId;
    }
    const stage = this.stages[uniqueStageId];
    if (stage.status === 'COMPLETED' || stage.status === 'SKIPPED') {
      return;
    }
    stage.stageId = String(data.stageId);
    stage.status = 'RUNNING';
    stage.numTasks = data.numTasks;
  }

  onSparkStageCompleted(data: any) {
    const uniqueStageId = `${this.uniqueId}-stage-${data.stageId}`;
    let stage = this.stages[uniqueStageId];
    if (!stage) {
      stage = new SparkStage();
      stage.uniqueId = uniqueStageId;
      stage.stageId = String(data.stageId);
      this.stages[uniqueStageId] = stage;
    }
    if (stage.status === 'COMPLETED') {
      return;
    }
    stage.status = data.status;
    stage.numActiveTasks = 0;
    stage.numTasks = data.numTasks;
    stage.numCompletedTasks =
        data.status === 'SKIPPED' ? 0 : data.numCompletedTasks;
    stage.numFailedTasks = data.numFailedTasks;

    const job = this.jobs[stage.uniqueJobId];
    if (job) {
      this.recomputeJobTasks(job);

      // Fix the Cliff: forcefully plot the final active tasks count (should
      // be 0 for this stage)
      const time = data.completionTime || Date.now();
      job.cell?.taskChartStore.onSparkStageActive(time, job.numActiveTasks);
    }
  }

  onSparkExecutorAdded(data: any) {
    if (typeof data.totalCores === 'number') {
      this.numTotalCores = Math.max(0, data.totalCores);
    }
    if (typeof data.numExecutors === 'number') {
      this.numExecutors = Math.max(0, data.numExecutors);
    } else {
      this.numExecutors = (this.numExecutors ?? 0) + 1;
    }
  }

  onSparkExecutorRemoved(data: any) {
    if (typeof data.totalCores === 'number') {
      this.numTotalCores = Math.max(0, data.totalCores);
    }
    if (typeof data.numExecutors === 'number') {
      this.numExecutors = Math.max(0, data.numExecutors);
    } else {
      this.numExecutors = Math.max(0, (this.numExecutors ?? 0) - 1);
    }
  }

  // Periodic stage updates
  onSparkStageActive(data: any) {
    const uniqueStageId = `${this.uniqueId}-stage-${data.stageId}`;
    const stage = this.stages[uniqueStageId];
    if (stage && stage.status === 'RUNNING') {
      stage.numActiveTasks = data.numActiveTasks;
      stage.numCompletedTasks = data.numCompletedTasks;
      stage.numFailedTasks = data.numFailedTasks;

      const job = this.jobs[stage.uniqueJobId];
      if (job) {
        this.recomputeJobTasks(job);

        const time = data.time || data.timestamp || Date.now();
        job.cell?.taskChartStore.onSparkStageActive(time, job.numActiveTasks);
      }
    }
  }
}
