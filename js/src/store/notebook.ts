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
import { JobStatusType, SparkJob } from './spark-job';
import { Cell } from './cell';
import type { OperationJobMarker, RunningTasksSnapshot } from './task-chart-store';

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

const TERMINAL_OP_STATES = new Set(['SUCCEEDED', 'FAILED', 'CANCELLED']);

function normalizeOperationState(raw: unknown): string {
  const s = String(raw || '').toUpperCase();
  return s.startsWith('OPERATION_STATE_')
    ? s.slice('OPERATION_STATE_'.length)
    : s;
}

function normalizeJobStatus(raw: unknown): JobStatusType {
  const s = String(raw || '').toUpperCase();
  const stripped = s.startsWith('JOB_STATUS_')
    ? s.slice('JOB_STATUS_'.length)
    : s;
  switch (stripped) {
    case 'SUCCEEDED':
    case 'COMPLETED':
      return 'COMPLETED';
    case 'FAILED':
      return 'FAILED';
    case 'CANCELLED':
    case 'CANCELED':
      return 'CANCELLED';
    default:
      return 'RUNNING';
  }
}

function normalizeStageStatus(raw: unknown): SparkStage['status'] {
  const s = String(raw || '').toUpperCase();
  const stripped = s.startsWith('STAGE_STATUS_')
    ? s.slice('STAGE_STATUS_'.length)
    : s;
  switch (stripped) {
    case 'PENDING':
      return 'PENDING';
    case 'RUNNING':
      return 'RUNNING';
    case 'COMPLETED':
      return 'COMPLETED';
    case 'FAILED':
      return 'FAILED';
    case 'SKIPPED':
      return 'SKIPPED';
    default:
      return 'Unknown';
  }
}

function terminalOutcomeForOpState(opState: string): JobStatusType {
  if (opState === 'FAILED') {
    return 'FAILED';
  }
  if (opState === 'CANCELLED') {
    return 'CANCELLED';
  }
  return 'COMPLETED';
}

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
  // Stages feed the task totals of their job; their timing metadata is kept
  // for a future timeline view (no current view renders stages).
  stages: { [stageId: string]: SparkStage } = {};
  operationVersions: { [operationId: string]: number } = {};
  operationFinalized: { [operationId: string]: boolean } = {};

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
      const opIds = new Set<string>();
      cell.uniqueJobIds.forEach(uniqueJobId => {
        const job = this.jobs[uniqueJobId];
        if (job) {
          if (job.operationId) {
            opIds.add(job.operationId);
          }
          job.uniqueStageIds.forEach(uniqueStageId => {
            const stage = this.stages[uniqueStageId];
            if (stage) {
              delete this.stages[uniqueStageId];
            }
          });
          delete this.jobs[uniqueJobId];
        }
      });
      opIds.forEach((opId) => {
        delete this.operationVersions[opId];
        delete this.operationFinalized[opId];
      });
      delete this.cells[cellId];
    }
  }

  onCellRemoved(cellId: string) {
    this.deleteCellData(cellId);
  }

  /**
   * Called after the kernel's `final_state` batch for `cellId` has been
   * applied. The batch itself closes jobs and stages through the regular
   * handlers; this only tidies the task chart so its line ends at zero when
   * the cell has nothing running any more. Jobs the kernel did not report as
   * ended are left untouched on purpose: the kernel is the source of truth.
   */
  finalizeCell(cellId: string) {
    const cell = this.cells[cellId];
    if (!cell || cell.numActiveJobs > 0) {
      return;
    }
    let latestEnd = Number.NEGATIVE_INFINITY;
    cell.uniqueJobIds.forEach((uniqueJobId) => {
      const end = this.jobs[uniqueJobId]?.endTime?.getTime();
      if (typeof end === 'number' && Number.isFinite(end)) {
        latestEnd = Math.max(latestEnd, end);
      }
    });
    // closeOut() falls back to the last plotted point when latestEnd is not
    // finite (no job with an end time), so no wall-clock fallback is needed.
    cell.taskChartStore.closeOut(latestEnd);
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
    stage.submissionTime =
      data.submissionTime === -1 ? new Date() : new Date(data.submissionTime);
    stage.numTasks = data.numTasks;

    // Job totals are stored values, so refresh them explicitly: the submitted
    // task count can differ from the jobStart estimate (e.g. adaptive
    // execution) and would otherwise stay stale until the next stage update.
    const job = this.jobs[stage.uniqueJobId];
    if (job) {
      this.recomputeJobTasks(job);
    }
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
    stage.completionTime = new Date(data.completionTime);
    stage.submissionTime = new Date(data.submissionTime);
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

  /**
   * Applies an authoritative Spark Connect `GetStatus` `CellMonitorTelemetry`
   * snapshot for one operation in `cellId`. Idempotent per `(operation_id,
   * version)` and handles multi-operation cells as well as terminal snapshots
   * with `final_snapshot === false`.
   */
  onOperationTelemetry(cellId: string, data: any) {
    if (!data || !data.operation_id) {
      return;
    }
    const opId = String(data.operation_id);
    const rawOpState = normalizeOperationState(data.operation_state);
    const execOutcome = data.execution_outcome
      ? normalizeOperationState(data.execution_outcome)
      : '';
    const finalSnapshot = Boolean(data.final_snapshot);
    const effectiveOpState = TERMINAL_OP_STATES.has(rawOpState)
      ? rawOpState
      : TERMINAL_OP_STATES.has(execOutcome)
        ? execOutcome
        : finalSnapshot
          ? 'SUCCEEDED'
          : rawOpState;
    const isTerminal = TERMINAL_OP_STATES.has(effectiveOpState);
    const version =
      typeof data.version === 'number' && Number.isFinite(data.version)
        ? data.version
        : 0;
    const prevVersion = this.operationVersions[opId];

    if (prevVersion !== undefined && version < prevVersion) {
      return;
    }

    if (!this.cells[cellId]) {
      this.cells[cellId] = new Cell(cellId, this);
    }
    const cell = this.cells[cellId];

    const isUnchanged =
      Boolean(data.unchanged) ||
      (prevVersion !== undefined && version > 0 && version === prevVersion);

    if (version > 0 || prevVersion === undefined) {
      this.operationVersions[opId] = version;
    }

    if (isUnchanged) {
      if (isTerminal && !this.operationFinalized[opId]) {
        this.reconcileTerminalOperation(
          cell,
          opId,
          effectiveOpState,
          finalSnapshot,
          data.closed_at,
        );
      }
      return;
    }

    // Deduplicate stage entries by stage_id, keeping highest attempt_id.
    const latestStagesById = new Map<number, any>();
    const rawStages = Array.isArray(data.stages) ? data.stages : [];
    for (const s of rawStages) {
      if (s == null || s.stage_id == null) {
        continue;
      }
      const sid = Number(s.stage_id);
      const prev = latestStagesById.get(sid);
      const attempt = typeof s.attempt_id === 'number' ? s.attempt_id : 0;
      const prevAttempt =
        prev && typeof prev.attempt_id === 'number' ? prev.attempt_id : -1;
      if (!prev || attempt >= prevAttempt) {
        latestStagesById.set(sid, s);
      }
    }

    latestStagesById.forEach((s, sid) => {
      const uniqueStageId = `${this.uniqueId}-op-${opId}-stage-${sid}`;
      let stage = this.stages[uniqueStageId];
      if (!stage) {
        stage = new SparkStage();
        stage.uniqueId = uniqueStageId;
        stage.stageId = String(sid);
        this.stages[uniqueStageId] = stage;
      }
      const stStatus = normalizeStageStatus(s.status);
      stage.status = stStatus;
      stage.name = s.name ? String(s.name) : `Stage ${sid}`;
      stage.numTasks = Math.max(0, Number(s.num_tasks) || 0);
      const isStageEnded =
        stStatus === 'COMPLETED' ||
        stStatus === 'FAILED' ||
        stStatus === 'SKIPPED';
      stage.numActiveTasks = isStageEnded
        ? 0
        : Math.max(0, Number(s.num_active_tasks) || 0);
      stage.numCompletedTasks =
        stStatus === 'SKIPPED'
          ? 0
          : Math.max(0, Number(s.num_completed_tasks) || 0);
      stage.numFailedTasks = Math.max(0, Number(s.num_failed_tasks) || 0);
      const stageSubMs = s.submission_time_ms ?? s.submission_time;
      const stageEndMs = s.completion_time_ms ?? s.completion_time;
      if (typeof stageSubMs === 'number' && stageSubMs > 0) {
        stage.submissionTime = new Date(stageSubMs);
      }
      if (typeof stageEndMs === 'number' && stageEndMs > 0) {
        stage.completionTime = new Date(stageEndMs);
      }
    });

    const rawJobs = Array.isArray(data.jobs) ? data.jobs : [];
    for (const j of rawJobs) {
      if (j == null || j.job_id == null) {
        continue;
      }
      const jid = Number(j.job_id);
      const uniqueJobId = `${this.uniqueId}-job-${jid}`;
      let job = this.jobs[uniqueJobId];
      if (!job) {
        job = new SparkJob();
        job.uniqueId = uniqueJobId;
        job.jobId = String(jid);
        job.startTime = new Date(0);
        this.jobs[uniqueJobId] = job;
      }
      job.operationId = opId;
      job.cellId = cellId;
      job.cell = cell;

      const jobStatus = normalizeJobStatus(j.status);
      job.status = jobStatus;

      const stageIds: number[] = Array.isArray(j.stage_ids)
        ? j.stage_ids.map(Number)
        : [];

      for (const sid of stageIds) {
        const uniqueStageId = `${this.uniqueId}-op-${opId}-stage-${sid}`;
        let stage = this.stages[uniqueStageId];
        if (!stage) {
          stage = new SparkStage();
          stage.uniqueId = uniqueStageId;
          stage.stageId = String(sid);
          stage.status = jobStatus === 'RUNNING' ? 'PENDING' : 'SKIPPED';
          stage.numTasks = 0;
          this.stages[uniqueStageId] = stage;
        }
        stage.uniqueJobId = uniqueJobId;
        if (!job.uniqueStageIds.includes(uniqueStageId)) {
          job.uniqueStageIds.push(uniqueStageId);
        }
      }
      job.uniqueStageIds.sort((a, b) =>
        a.localeCompare(b, undefined, { numeric: true }),
      );

      const rawName = j.name != null ? String(j.name) : '';
      const callSiteName = rawName ? rawName.split(' at ')[0] : '';
      if (callSiteName && callSiteName !== 'null') {
        job.name = callSiteName;
      } else {
        job.name = `Job ${jid}`;
      }

      const jobSubMs = j.submission_time_ms ?? j.submission_time;
      const jobEndMs = j.completion_time_ms ?? j.completion_time;
      if (typeof jobSubMs === 'number' && jobSubMs > 0) {
        job.startTime = new Date(jobSubMs);
      }
      if (jobStatus !== 'RUNNING') {
        if (typeof jobEndMs === 'number' && jobEndMs > 0) {
          job.endTime = new Date(jobEndMs);
        }
      } else {
        job.endTime = undefined;
      }

      if (typeof j.num_tasks === 'number') {
        job.hasServerTaskCounts = true;
        job.numTasks = Math.max(0, Number(j.num_tasks) || 0);
        job.numCompletedTasks = Math.max(
          0,
          Number(j.num_completed_tasks) || 0,
        );
        job.numActiveTasks =
          jobStatus !== 'RUNNING'
            ? 0
            : Math.max(0, Number(j.num_active_tasks) || 0);
        job.numFailedTasks = Math.max(0, Number(j.num_failed_tasks) || 0);
      } else {
        this.recomputeJobTasks(job);
        if (jobStatus !== 'RUNNING') {
          job.numActiveTasks = 0;
        }
      }

      if (!cell.uniqueJobIds.includes(uniqueJobId)) {
        cell.uniqueJobIds.push(uniqueJobId);
      }
    }

    const hasRunningTasksPayload =
      data.running_tasks != null && typeof data.running_tasks === 'object';
    const runningTasksPayload = hasRunningTasksPayload
      ? (data.running_tasks as RunningTasksSnapshot)
      : undefined;

    if (isTerminal) {
      this.reconcileTerminalOperation(
        cell,
        opId,
        effectiveOpState,
        finalSnapshot,
        data.closed_at,
        runningTasksPayload,
        hasRunningTasksPayload,
      );
    } else {
      const markers = this.collectOperationJobMarkers(cell, opId);
      cell.taskChartStore.updateOperationSeries(
        opId,
        runningTasksPayload,
        markers,
        { replaceSeries: hasRunningTasksPayload },
      );
    }
  }

  private collectOperationJobMarkers(
    cell: Cell,
    opId: string,
  ): OperationJobMarker[] {
    const markers: OperationJobMarker[] = [];
    for (const uniqueJobId of cell.uniqueJobIds) {
      const job = this.jobs[uniqueJobId];
      if (!job || job.operationId !== opId) {
        continue;
      }
      const startMs = job.startTime?.getTime();
      if (typeof startMs === 'number' && startMs > 0) {
        markers.push({ time: startMs, text: `Job ${job.jobId} started` });
      }
      const endMs = job.endTime?.getTime();
      if (typeof endMs === 'number' && endMs > 0) {
        markers.push({ time: endMs, text: `Job ${job.jobId} ended` });
      }
    }
    return markers;
  }

  private reconcileTerminalOperation(
    cell: Cell,
    opId: string,
    opState: string,
    finalSnapshot: boolean,
    closedAt?: number,
    runningTasksPayload?: RunningTasksSnapshot,
    replaceSeries = false,
  ) {
    let closeTime =
      typeof closedAt === 'number' && closedAt > 0 ? closedAt : Number.NaN;

    const considerTime = (ms: number | undefined) => {
      if (typeof ms === 'number' && Number.isFinite(ms) && ms > 0) {
        closeTime = Number.isFinite(closeTime) ? Math.max(closeTime, ms) : ms;
      }
    };

    for (const uniqueJobId of cell.uniqueJobIds) {
      const job = this.jobs[uniqueJobId];
      if (!job || job.operationId !== opId) {
        continue;
      }
      considerTime(job.endTime?.getTime());
      considerTime(job.startTime?.getTime());
      for (const uniqueStageId of job.uniqueStageIds) {
        const stage = this.stages[uniqueStageId];
        if (stage) {
          considerTime(stage.completionTime?.getTime());
          considerTime(stage.submissionTime?.getTime());
        }
      }
    }
    if (runningTasksPayload) {
      const slotWidth = Number(runningTasksPayload.slot_width_ms);
      const startTime = Number(
        runningTasksPayload.start_time_ms ??
          (runningTasksPayload as unknown as { start_time?: number })
            .start_time,
      );
      const count = Array.isArray(runningTasksPayload.values)
        ? runningTasksPayload.values.length
        : 0;
      if (
        Number.isFinite(slotWidth) &&
        slotWidth > 0 &&
        Number.isFinite(startTime) &&
        count > 0
      ) {
        considerTime(startTime + count * slotWidth);
      }
    } else if (!replaceSeries) {
      considerTime(cell.taskChartStore.getOperationMaxTime(opId));
    }
    if (!Number.isFinite(closeTime)) {
      closeTime = Date.now();
    }

    if (!finalSnapshot) {
      const outcome = terminalOutcomeForOpState(opState);
      for (const uniqueJobId of cell.uniqueJobIds) {
        const job = this.jobs[uniqueJobId];
        if (!job || job.operationId !== opId) {
          continue;
        }
        for (const uniqueStageId of job.uniqueStageIds) {
          const stage = this.stages[uniqueStageId];
          if (!stage) {
            continue;
          }
          if (stage.status === 'PENDING') {
            stage.status = 'SKIPPED';
            stage.numActiveTasks = 0;
            stage.numCompletedTasks = 0;
          } else if (stage.status === 'RUNNING') {
            stage.numActiveTasks = 0;
            if (outcome === 'COMPLETED') {
              stage.status = 'COMPLETED';
              stage.numCompletedTasks = stage.numTasks || 0;
            } else {
              // Per cell_monitor.proto StageStatus, a running stage whose job
              // fails or is cancelled ends as STAGE_STATUS_FAILED.
              stage.status = 'FAILED';
            }
            if (!stage.completionTime) {
              stage.completionTime = new Date(closeTime);
            }
          }
        }
        if (job.status === 'RUNNING') {
          job.status = outcome;
          job.endTime = new Date(closeTime);
          if (job.hasServerTaskCounts && outcome === 'COMPLETED') {
            job.numCompletedTasks = job.numTasks;
          }
        }
        if (!job.hasServerTaskCounts) {
          this.recomputeJobTasks(job);
        }
        job.numActiveTasks = 0;
      }
    }

    const markers = this.collectOperationJobMarkers(cell, opId);
    cell.taskChartStore.updateOperationSeries(
      opId,
      runningTasksPayload,
      markers,
      {
        replaceSeries,
        closeOutAt: closeTime,
      },
    );
    this.operationFinalized[opId] = true;
  }
}
