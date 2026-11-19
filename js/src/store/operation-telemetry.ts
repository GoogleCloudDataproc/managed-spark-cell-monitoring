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

import { Cell } from './cell';
import type { NotebookStore } from './notebook';
import { JobStatusType, SparkJob } from './spark-job';
import { SparkStage } from './spark-stage';
import type { OperationJobMarker, RunningTasksSnapshot } from './task-chart-store';

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

function collectOperationJobMarkers(
  store: NotebookStore,
  cell: Cell,
  opId: string,
): OperationJobMarker[] {
  const markers: OperationJobMarker[] = [];
  for (const uniqueJobId of cell.uniqueJobIds) {
    const job = store.jobs[uniqueJobId];
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

function applyStagesSnapshot(
  store: NotebookStore,
  opId: string,
  rawStages: any[],
) {
  // Deduplicate stage entries by stage_id, keeping highest attempt_id.
  const latestStagesById = new Map<number, any>();
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
    const uniqueStageId = `${store.uniqueId}-op-${opId}-stage-${sid}`;
    let stage = store.stages[uniqueStageId];
    if (!stage) {
      stage = new SparkStage();
      stage.uniqueId = uniqueStageId;
      stage.stageId = String(sid);
      store.stages[uniqueStageId] = stage;
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
}

function applyJobsSnapshot(
  store: NotebookStore,
  cell: Cell,
  cellId: string,
  opId: string,
  rawJobs: any[],
) {
  for (const j of rawJobs) {
    if (j == null || j.job_id == null) {
      continue;
    }
    const jid = Number(j.job_id);
    const uniqueJobId = `${store.uniqueId}-op-${opId}-job-${jid}`;
    let job = store.jobs[uniqueJobId];
    if (!job) {
      job = new SparkJob();
      job.uniqueId = uniqueJobId;
      job.jobId = String(jid);
      job.startTime = new Date(0);
      store.jobs[uniqueJobId] = job;
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
      const uniqueStageId = `${store.uniqueId}-op-${opId}-stage-${sid}`;
      let stage = store.stages[uniqueStageId];
      if (!stage) {
        stage = new SparkStage();
        stage.uniqueId = uniqueStageId;
        stage.stageId = String(sid);
        stage.status = jobStatus === 'RUNNING' ? 'PENDING' : 'SKIPPED';
        stage.numTasks = 0;
        store.stages[uniqueStageId] = stage;
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
    job.name = callSiteName && callSiteName !== 'null' ? callSiteName : `Job ${jid}`;

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
      job.numCompletedTasks = Math.max(0, Number(j.num_completed_tasks) || 0);
      job.numActiveTasks =
        jobStatus !== 'RUNNING'
          ? 0
          : Math.max(0, Number(j.num_active_tasks) || 0);
      job.numFailedTasks = Math.max(0, Number(j.num_failed_tasks) || 0);
    } else {
      store.recomputeJobTasks(job);
      if (jobStatus !== 'RUNNING') {
        job.numActiveTasks = 0;
      }
    }

    if (!cell.uniqueJobIds.includes(uniqueJobId)) {
      cell.uniqueJobIds.push(uniqueJobId);
    }
  }
}

function reconcileTerminalOperation(
  store: NotebookStore,
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
    const job = store.jobs[uniqueJobId];
    if (!job || job.operationId !== opId) {
      continue;
    }
    considerTime(job.endTime?.getTime());
    considerTime(job.startTime?.getTime());
    for (const uniqueStageId of job.uniqueStageIds) {
      const stage = store.stages[uniqueStageId];
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
        (runningTasksPayload as unknown as { start_time?: number }).start_time,
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
      const job = store.jobs[uniqueJobId];
      if (!job || job.operationId !== opId) {
        continue;
      }
      for (const uniqueStageId of job.uniqueStageIds) {
        const stage = store.stages[uniqueStageId];
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
        store.recomputeJobTasks(job);
      }
      job.numActiveTasks = 0;
    }
  }

  const markers = collectOperationJobMarkers(store, cell, opId);
  cell.taskChartStore.updateOperationSeries(
    opId,
    runningTasksPayload,
    markers,
    {
      replaceSeries,
      closeOutAt: closeTime,
    },
  );
  store.operationFinalized[opId] = true;
}

/**
 * Applies an authoritative Spark Connect `GetStatus` `CellMonitorTelemetry`
 * snapshot for one operation in `cellId`. Idempotent per `(operation_id,
 * version)` and handles multi-operation cells as well as terminal snapshots
 * with `final_snapshot === false`.
 */
export function applyOperationTelemetry(
  store: NotebookStore,
  cellId: string,
  data: any,
) {
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
  const prevVersion = store.operationVersions[opId];

  if (prevVersion !== undefined && version < prevVersion) {
    return;
  }

  if (!store.cells[cellId]) {
    store.cells[cellId] = new Cell(cellId, store);
  }
  const cell = store.cells[cellId];
  store.operationCellIds[opId] = cellId;

  const isUnchanged =
    Boolean(data.unchanged) ||
    (prevVersion !== undefined && version > 0 && version === prevVersion);

  if (version > 0 || prevVersion === undefined) {
    store.operationVersions[opId] = version;
  }

  if (isUnchanged) {
    if (isTerminal && !store.operationFinalized[opId]) {
      reconcileTerminalOperation(
        store,
        cell,
        opId,
        effectiveOpState,
        finalSnapshot,
        data.closed_at,
      );
    }
    return;
  }

  applyStagesSnapshot(
    store,
    opId,
    Array.isArray(data.stages) ? data.stages : [],
  );
  applyJobsSnapshot(
    store,
    cell,
    cellId,
    opId,
    Array.isArray(data.jobs) ? data.jobs : [],
  );

  const hasRunningTasksPayload =
    data.running_tasks != null && typeof data.running_tasks === 'object';
  const runningTasksPayload = hasRunningTasksPayload
    ? (data.running_tasks as RunningTasksSnapshot)
    : undefined;

  if (isTerminal) {
    reconcileTerminalOperation(
      store,
      cell,
      opId,
      effectiveOpState,
      finalSnapshot,
      data.closed_at,
      runningTasksPayload,
      hasRunningTasksPayload,
    );
  } else {
    const markers = collectOperationJobMarkers(store, cell, opId);
    cell.taskChartStore.updateOperationSeries(
      opId,
      runningTasksPayload,
      markers,
      { replaceSeries: hasRunningTasksPayload },
    );
  }
}
