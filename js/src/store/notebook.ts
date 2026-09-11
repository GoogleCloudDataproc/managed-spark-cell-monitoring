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

export class NotebookStore {
  numExecutors?: number;
  numTotalCores?: number;
  applicationName?: string;
  applicationId?: string;
  applicationAttemptId?: string;
  uniqueId = 'default-key';
  hideAllDisplays = false;

  cells: { [cellId: string]: Cell } = {};
  jobs: { [jobId: string]: SparkJob } = {};
  stages: { [stageId: string]: SparkStage } = {};

  constructor(public notebookPanelId: string) {
    makeAutoObservable(this);
    if (notebookPanelId && notebookPanelId !== 'default-key') {
      this.uniqueId = notebookPanelId;
    }
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
    this.applicationName = data.appName;
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

  onSparkJobStart(cellId: string, data: any) {
    const uniqueJobId = `${this.uniqueId}-job-${data.jobId}`;
    if (this.jobs[uniqueJobId]) {
      if (!this.cells[cellId]) {
        this.cells[cellId] = new Cell(cellId, this);
      }
      if (!this.cells[cellId].uniqueJobIds.includes(uniqueJobId)) {
        this.cells[cellId].uniqueJobIds.push(uniqueJobId);
      }
      return;
    }

    // These values are set here as previous messages may
    // be missed if reconnecting from a browser reload.
    this.numTotalCores = data.totalCores;
    this.numExecutors = data.numExecutors;

    const job = new SparkJob(this);
    job.uniqueId = uniqueJobId;
    job.jobId = data.jobId;
    job.status = data.status;
    job.cellId = cellId;
    const jobName = String(data.name).split(' at ')[0];
    job.name = jobName;
    job.startTime = new Date(data.submissionTime);
    job.stageIds = data.stageIds;
    job.numStages = data.stageIds.length;
    job.numTasks = data.numTasks;

    data.stageIds.forEach((stageId: string) => {
      const uniqueStageId = `${this.uniqueId}-stage-${stageId}`;
      let stage = this.stages[uniqueStageId];
      if (!stage) {
        stage = new SparkStage();
        stage.status = 'PENDING';
        this.stages[uniqueStageId] = stage;
      }
      stage.uniqueJobId = job.uniqueId;
      if (data.stageInfos && data.stageInfos[stageId]) {
        stage.numTasks = data.stageInfos[stageId].numTasks;
        stage.name = data.stageInfos[stageId].name;
      }
      job.uniqueStageIds.push(uniqueStageId);
    });
    job.uniqueStageIds.sort((a, b) =>
      a.localeCompare(b, undefined, { numeric: true })
    );

    if (job.name === 'null' && data.stageIds.length > 0) {
      const lastStageId = Math.max(...data.stageIds.map(Number));
      job.name =
        this.stages[`${this.uniqueId}-stage-${lastStageId}`]?.name || 'Job';
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
      job = new SparkJob(this);
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
    job.numActiveTasks = 0;
    job.numCompletedTasks = 0;
    job.numFailedTasks = 0;
    job.numTasks = 0;
    job.uniqueStageIds.forEach(uniqueStageId => {
      const s = this.stages[uniqueStageId];
      if (s) {
        job.numActiveTasks += s.numActiveTasks || 0;
        job.numFailedTasks += s.numFailedTasks || 0;
        job.numTasks += s.numTasks || 0;
        if (s.status === 'SKIPPED') {
          job.numCompletedTasks += s.numTasks || 0;
        } else {
          job.numCompletedTasks += s.numCompletedTasks || 0;
        }
      }
    });

    job.cell?.taskChartStore.onSparkJobEnd(data);
  }

  onSparkStageSubmitted(cellId: string, data: any) {
    const submissionTime =
      data.submissionTime === -1 ? new Date() : new Date(data.submissionTime);
    const uniqueStageId = `${this.uniqueId}-stage-${data.stageId}`;
    if (!this.stages[uniqueStageId]) {
      this.stages[uniqueStageId] = new SparkStage();
      this.stages[uniqueStageId].uniqueId = uniqueStageId;
    }
    const stage = this.stages[uniqueStageId];
    if (stage.status === 'COMPLETED' || stage.status === 'SKIPPED') {
      return;
    }
    stage.cellId = cellId;
    stage.stageId = data.stageId;
    stage.status = 'RUNNING';
    stage.name = String(data.name);
    stage.submissionTime = submissionTime;
    stage.numTasks = data.numTasks;
  }

  onSparkStageCompleted(data: any) {
    const uniqueStageId = `${this.uniqueId}-stage-${data.stageId}`;
    let stage = this.stages[uniqueStageId];
    if (!stage) {
      stage = new SparkStage();
      stage.uniqueId = uniqueStageId;
      stage.stageId = data.stageId;
      stage.name = data.name || `Stage ${data.stageId}`;
      this.stages[uniqueStageId] = stage;
    }
    if (stage.status === 'COMPLETED' && stage.completionTime) {
      return;
    }
    stage.status = data.status;
    stage.completionTime = new Date(data.completionTime);
    stage.submissionTime = new Date(data.submissionTime);
    stage.numActiveTasks = 0;
    stage.numTasks = data.numTasks;
    stage.numCompletedTasks =
        data.status === 'SKIPPED' ? 0 : data.numCompletedTasks;
    stage.numFailedTasks = data.numFailedTasks;

    const job = this.jobs[stage.uniqueJobId];
    if (job) {
      job.numActiveTasks = 0;
      job.numCompletedTasks = 0;
      job.numFailedTasks = 0;
      job.numTasks = 0;

      // Update active/completed/failed tasks number (scan all job stages tasks stats)
      job.uniqueStageIds.forEach((uniqueStageId) => {
        const s = this.stages[uniqueStageId];
        if (s) {
          job.numActiveTasks += s.numActiveTasks || 0;
          job.numFailedTasks += s.numFailedTasks || 0;
          job.numTasks += s.numTasks || 0;
          if (s.status === 'SKIPPED') {
            job.numCompletedTasks += s.numTasks || 0;
          } else {
            job.numCompletedTasks += s.numCompletedTasks || 0;
          }
        }
      });

      // Fix the Cliff: forcefully plot the final active tasks count (should
      // be 0 for this stage)
      const time = data.completionTime || Date.now();
      job.cell?.taskChartStore.onSparkStageActive(time, job.numActiveTasks);
    }
  }

  onSparkExecutorAdded(data: any) {
    this.numTotalCores = data.totalCores;
    if (!this.numExecutors) {
      this.numExecutors = 0;
    }
    this.numExecutors += 1;
  }

  onSparkExecutorRemoved(data: any) {
    this.numTotalCores = data.totalCores;
    if (!this.numExecutors) {
      this.numExecutors = 0;
    }
    this.numExecutors -= 1;
  }

  onSparkTaskStart(data: any) {
    const uniqueStageId = `${this.uniqueId}-stage-${data.stageId}`;
    const stage = this.stages[uniqueStageId];
    if (stage) {
      const uniqueJobId = stage.uniqueJobId;
      const job = this.jobs[uniqueJobId];
      if (job) {
        job.cell?.taskChartStore.onSparkTaskStart(data);
      }
    }
  }

  onSparkTaskEnd(data: any) {
    const uniqueStageId = `${this.uniqueId}-stage-${data.stageId}`;
    const stage = this.stages[uniqueStageId];
    if (stage) {
      const uniqueJobId = stage.uniqueJobId;
      const job = this.jobs[uniqueJobId];
      if (job) {
        job.cell?.taskChartStore.onSparkTaskEnd(data);
      }
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
        job.numActiveTasks = 0;
        job.numCompletedTasks = 0;
        job.numFailedTasks = 0;
        job.numTasks = 0;

        // Update active/completed/failed tasks number (scan all job stages tasks stats)
        job.uniqueStageIds.forEach(uStageId => {
          const s = this.stages[uStageId];
          if (s) {
            job.numActiveTasks += s.numActiveTasks || 0;
            job.numFailedTasks += s.numFailedTasks || 0;
            job.numTasks += s.numTasks || 0;
            if (s.status === 'SKIPPED') {
              job.numCompletedTasks += s.numTasks || 0;
            } else {
              job.numCompletedTasks += s.numCompletedTasks || 0;
            }
          }
        });

        const time = data.time || data.timestamp || Date.now();
        job.cell?.taskChartStore.onSparkStageActive(time, job.numActiveTasks);
      }
    }
  }
}
