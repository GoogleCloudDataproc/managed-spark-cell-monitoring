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

import anywidgetEntry from '../anywidget-entry';
import { store } from '../store';
import { NotebookStore } from '../store/notebook';

const CELL = 'cell-connect-1';

describe('NotebookStore.onOperationTelemetry (PS 8 schema)', () => {
  let nb: NotebookStore;

  beforeEach(() => {
    nb = new NotebookStore('nb-connect');
  });

  it('normalizes Job <job_id>, server task counters (even with evicted stages), and RunningTasks slots', () => {
    nb.onOperationTelemetry(CELL, {
      operation_id: 'op-1',
      operation_state: 'OPERATION_STATE_RUNNING',
      version: 1,
      unchanged: false,
      final_snapshot: false,
      jobs: [
        {
          job_id: 10,
          status: 'JOB_STATUS_RUNNING',
          submission_time_ms: 1000,
          num_tasks: 12,
          num_completed_tasks: 9,
          num_active_tasks: 3,
          num_failed_tasks: 0,
          stage_ids: [1, 2],
        },
      ],
      stages: [
        {
          stage_id: 1,
          attempt_id: 0,
          num_tasks: 8,
          num_active_tasks: 0,
          num_completed_tasks: 2,
          num_failed_tasks: 6,
          parent_ids: [],
          status: 'STAGE_STATUS_FAILED',
          submission_time_ms: 1000,
          completion_time_ms: 1200,
        },
        {
          stage_id: 1,
          attempt_id: 1,
          num_tasks: 8,
          num_active_tasks: 3,
          num_completed_tasks: 5,
          num_failed_tasks: 0,
          parent_ids: [],
          status: 'STAGE_STATUS_RUNNING',
          submission_time_ms: 1250,
        },
        {
          stage_id: 2,
          attempt_id: 0,
          num_tasks: 4,
          num_active_tasks: 0,
          num_completed_tasks: 0,
          num_failed_tasks: 0,
          parent_ids: [1],
          status: 'STAGE_STATUS_SKIPPED',
        },
      ],
      running_tasks: {
        slot_width_ms: 250,
        start_time_ms: 1000,
        values: [2, 3, 3],
      },
    });

    const cell = nb.cells[CELL];
    expect(cell).toBeDefined();
    expect(cell.numActiveJobs).toBe(1);

    const job = nb.jobs['nb-connect-job-10'];
    expect(job.name).toBe('Job 10');
    expect(job.status).toBe('RUNNING');
    expect(job.numTasks).toBe(12);
    expect(job.numCompletedTasks).toBe(9);
    expect(job.numActiveTasks).toBe(3);
    expect(job.numFailedTasks).toBe(0);

    // Slot 0=[1000..1250)->2, Slot 1..2=[1250..1750)->3 (extended to end of slot 2 while live)
    expect(cell.taskChartStore.taskDataX).toEqual([1000, 1250, 1750]);
    expect(cell.taskChartStore.taskDataY).toEqual([2, 3, 3]);

    // Terminal final snapshot evicts stage 2 from stages, yet job counters stay exact at 12/12
    nb.onOperationTelemetry(CELL, {
      operation_id: 'op-1',
      operation_state: 'OPERATION_STATE_SUCCEEDED',
      execution_outcome: 'SUCCEEDED',
      version: 2,
      unchanged: false,
      final_snapshot: true,
      jobs: [
        {
          job_id: 10,
          status: 'JOB_STATUS_SUCCEEDED',
          submission_time_ms: 1000,
          completion_time_ms: 1750,
          num_tasks: 12,
          num_completed_tasks: 12,
          num_active_tasks: 0,
          num_failed_tasks: 0,
          stage_ids: [1, 2],
        },
      ],
      stages: [
        {
          stage_id: 1,
          attempt_id: 1,
          num_tasks: 8,
          num_active_tasks: 0,
          num_completed_tasks: 8,
          num_failed_tasks: 0,
          parent_ids: [],
          status: 'STAGE_STATUS_COMPLETED',
          submission_time_ms: 1250,
          completion_time_ms: 1740,
        },
      ],
      running_tasks: {
        slot_width_ms: 250,
        start_time_ms: 1000,
        values: [2, 3, 3, 0],
      },
    });

    expect(job.status).toBe('COMPLETED');
    expect(job.endTime?.getTime()).toBe(1750);
    expect(job.numCompletedTasks).toBe(12);
    expect(job.numActiveTasks).toBe(0);
    expect(cell.numActiveJobs).toBe(0);
    expect(cell.numCompletedJobs).toBe(1);
    expect(cell.taskChartStore.taskDataX).toEqual([1000, 1250, 1750]);
    expect(cell.taskChartStore.taskDataY).toEqual([2, 3, 0]);
  });

  it('ignores stale versions and handles unchanged terminal snapshots when OperationStatus is still RUNNING', () => {
    nb.onOperationTelemetry(CELL, {
      operation_id: 'op-1',
      operation_state: 'OPERATION_STATE_RUNNING',
      version: 5,
      unchanged: false,
      final_snapshot: false,
      jobs: [
        {
          job_id: 1,
          status: 'JOB_STATUS_SUCCEEDED',
          submission_time_ms: 1000,
          completion_time_ms: 1400,
          num_tasks: 4,
          num_completed_tasks: 4,
          num_active_tasks: 0,
          num_failed_tasks: 0,
          stage_ids: [10],
        },
      ],
      stages: [
        {
          stage_id: 10,
          attempt_id: 0,
          num_tasks: 4,
          num_active_tasks: 0,
          num_completed_tasks: 4,
          num_failed_tasks: 0,
          parent_ids: [],
          status: 'STAGE_STATUS_COMPLETED',
          submission_time_ms: 1000,
          completion_time_ms: 1400,
        },
      ],
      running_tasks: {
        slot_width_ms: 250,
        start_time_ms: 1000,
        values: [4],
      },
    });

    // Older version 3 must be ignored even if it claims FAILED
    nb.onOperationTelemetry(CELL, {
      operation_id: 'op-1',
      operation_state: 'OPERATION_STATE_FAILED',
      version: 3,
      unchanged: false,
      final_snapshot: true,
      jobs: [
        {
          job_id: 1,
          status: 'JOB_STATUS_FAILED',
          submission_time_ms: 1000,
          completion_time_ms: 1400,
          num_tasks: 4,
          num_completed_tasks: 0,
          num_active_tasks: 0,
          num_failed_tasks: 4,
          stage_ids: [10],
        },
      ],
    });
    expect(nb.jobs['nb-connect-job-1'].status).toBe('COMPLETED');

    // ExecutePlan returned SUCCEEDED while GetStatus OperationStatus.state is still TERMINATING
    nb.onOperationTelemetry(CELL, {
      operation_id: 'op-1',
      operation_state: 'OPERATION_STATE_TERMINATING',
      execution_outcome: 'SUCCEEDED',
      version: 5,
      unchanged: true,
      final_snapshot: true,
    });

    const chart = nb.cells[CELL].taskChartStore;
    expect(chart.taskDataY[chart.taskDataY.length - 1]).toBe(0);
    // Closed out at driver-clock job end (1400), stripping the live synthetic 1250 hold point
    expect(chart.taskDataX).toEqual([1000, 1400]);
  });

  it('merges RunningTasks across concurrent operations and handles slot-width compaction', () => {
    // op-a runs from 1000ms with 4 tasks across 2 slots, then drops to 0 at 1500ms
    nb.onOperationTelemetry(CELL, {
      operation_id: 'op-a',
      operation_state: 'OPERATION_STATE_RUNNING',
      version: 1,
      jobs: [
        {
          job_id: 1,
          status: 'JOB_STATUS_RUNNING',
          submission_time_ms: 1000,
          num_tasks: 4,
          num_completed_tasks: 0,
          num_active_tasks: 4,
          num_failed_tasks: 0,
          stage_ids: [],
        },
      ],
      running_tasks: {
        slot_width_ms: 250,
        start_time_ms: 1000,
        values: [4, 4, 0],
      },
    });

    // op-b starts at 1250ms with 3 tasks across 2 slots [1250..1750), peaking concurrency at 4 + 3 = 7
    nb.onOperationTelemetry(CELL, {
      operation_id: 'op-b',
      operation_state: 'OPERATION_STATE_RUNNING',
      version: 1,
      jobs: [
        {
          job_id: 2,
          status: 'JOB_STATUS_RUNNING',
          submission_time_ms: 1250,
          num_tasks: 3,
          num_completed_tasks: 0,
          num_active_tasks: 3,
          num_failed_tasks: 0,
          stage_ids: [],
        },
      ],
      running_tasks: {
        slot_width_ms: 250,
        start_time_ms: 1250,
        values: [3, 3],
      },
    });

    const chart = nb.cells[CELL].taskChartStore;
    expect(chart.taskDataX).toEqual([1000, 1250, 1500, 1750]);
    expect(chart.taskDataY).toEqual([4, 7, 3, 3]);

    // Server doubles op-a slot_width_ms from 250 -> 500 on compaction
    nb.onOperationTelemetry(CELL, {
      operation_id: 'op-a',
      operation_state: 'OPERATION_STATE_SUCCEEDED',
      version: 2,
      final_snapshot: true,
      jobs: [
        {
          job_id: 1,
          status: 'JOB_STATUS_SUCCEEDED',
          submission_time_ms: 1000,
          completion_time_ms: 1500,
          num_tasks: 4,
          num_completed_tasks: 4,
          num_active_tasks: 0,
          num_failed_tasks: 0,
          stage_ids: [],
        },
      ],
      running_tasks: {
        slot_width_ms: 500,
        start_time_ms: 1000,
        values: [4, 0],
      },
    });

    expect(chart.taskDataX).toEqual([1000, 1250, 1500, 1750]);
    expect(chart.taskDataY).toEqual([4, 7, 3, 3]);
  });

  it('reconciles RUNNING jobs/stages and PENDING stages when final_snapshot is false after wait_for_final', () => {
    nb.onOperationTelemetry(CELL, {
      operation_id: 'op-cancel',
      operation_state: 'OPERATION_STATE_RUNNING',
      version: 1,
      final_snapshot: false,
      jobs: [
        {
          job_id: 7,
          status: 'JOB_STATUS_RUNNING',
          submission_time_ms: 2000,
          num_tasks: 20,
          num_completed_tasks: 4,
          num_active_tasks: 6,
          num_failed_tasks: 0,
          stage_ids: [70, 71],
        },
      ],
      stages: [
        {
          stage_id: 70,
          attempt_id: 0,
          num_tasks: 10,
          num_active_tasks: 6,
          num_completed_tasks: 4,
          num_failed_tasks: 0,
          parent_ids: [],
          status: 'STAGE_STATUS_RUNNING',
          submission_time_ms: 2000,
        },
        {
          stage_id: 71,
          attempt_id: 0,
          num_tasks: 10,
          num_active_tasks: 0,
          num_completed_tasks: 0,
          num_failed_tasks: 0,
          parent_ids: [70],
          status: 'STAGE_STATUS_PENDING',
        },
      ],
      running_tasks: {
        slot_width_ms: 250,
        start_time_ms: 2000,
        values: [6],
      },
    });

    // ExecutePlan ended CANCELLED while OperationStatus.state is still RUNNING and final_snapshot=false
    nb.onOperationTelemetry(CELL, {
      operation_id: 'op-cancel',
      operation_state: 'OPERATION_STATE_RUNNING',
      execution_outcome: 'CANCELLED',
      version: 1,
      unchanged: true,
      final_snapshot: false,
    });

    const cell = nb.cells[CELL];
    const job = nb.jobs['nb-connect-job-7'];
    const stage70 = nb.stages['nb-connect-op-op-cancel-stage-70'];
    const stage71 = nb.stages['nb-connect-op-op-cancel-stage-71'];

    expect(job.status).toBe('CANCELLED');
    // Completed tasks stay 4/20 (NOT inflated to 14/20 by PENDING stage 71 becoming SKIPPED!)
    expect(job.numCompletedTasks).toBe(4);
    expect(job.numActiveTasks).toBe(0);
    expect(stage70.status).toBe('FAILED');
    expect(stage70.numActiveTasks).toBe(0);
    expect(stage71.status).toBe('SKIPPED');
    expect(cell.numActiveJobs).toBe(0);
    expect(cell.numCancelledJobs).toBe(1);
    expect(cell.taskChartStore.numActiveTasks).toBe(0);
    expect(cell.taskChartStore.taskDataX).toEqual([2000, 2250]);
    expect(cell.taskChartStore.taskDataY).toEqual([6, 0]);
  });
});

describe('anywidget-entry Spark Connect snapshot messaging', () => {
  it('emits widget_ready on initialize and routes operation_telemetry + replay_snapshots', () => {
    const handlers: Record<string, Array<(arg?: any) => void>> = {};
    const sent: any[] = [];
    const attrs: Record<string, any> = {
      run_id: 'run-sc-1',
      session_id: 'sess-sc-1',
      spark_ui_url:
        'https://console.cloud.google.com/dataproc/interactive/us-central1/s1',
      cell_finished: false,
    };

    const mockModel = {
      get: (k: string) => attrs[k],
      on: (ev: string, cb: (arg?: any) => void) => {
        handlers[ev] = handlers[ev] || [];
        handlers[ev].push(cb);
      },
      off: (ev: string, cb: (arg?: any) => void) => {
        handlers[ev] = (handlers[ev] || []).filter((fn) => fn !== cb);
      },
      send: (msg: any) => sent.push(msg),
    };

    const cleanup = anywidgetEntry.initialize({ model: mockModel });
    expect(sent).toEqual([{ type: 'widget_ready' }]);

    const fireCustom = (msg: any) => {
      for (const cb of handlers['msg:custom'] || []) {
        cb(msg);
      }
    };

    fireCustom({
      type: 'replay_snapshots',
      snapshots: [
        {
          operation_id: 'op-replay-1',
          operation_state: 'OPERATION_STATE_RUNNING',
          version: 1,
          jobs: [
            {
              job_id: 42,
              status: 'JOB_STATUS_RUNNING',
              submission_time_ms: 1000,
              num_tasks: 4,
              num_completed_tasks: 1,
              num_active_tasks: 3,
              num_failed_tasks: 0,
              stage_ids: [],
            },
          ],
        },
      ],
    });

    const nb = store.notebooks['sess-sc-1'];
    expect(nb).toBeDefined();
    expect(nb.viewUrl).toBe(
      'https://console.cloud.google.com/dataproc/interactive/us-central1/s1',
    );
    expect(nb.jobs['sess-sc-1-job-42'].status).toBe('RUNNING');
    expect(nb.jobs['sess-sc-1-job-42'].name).toBe('Job 42');

    fireCustom({
      type: 'operation_telemetry',
      operation_id: 'op-replay-1',
      operation_state: 'OPERATION_STATE_SUCCEEDED',
      version: 2,
      final_snapshot: true,
      jobs: [
        {
          job_id: 42,
          status: 'JOB_STATUS_SUCCEEDED',
          submission_time_ms: 1000,
          completion_time_ms: 1300,
          num_tasks: 4,
          num_completed_tasks: 4,
          num_active_tasks: 0,
          num_failed_tasks: 0,
          stage_ids: [],
        },
      ],
    });
    expect(nb.jobs['sess-sc-1-job-42'].status).toBe('COMPLETED');

    cleanup();
    expect(sent[sent.length - 1]).toEqual({ type: 'widget_unmount' });
    delete store.notebooks['sess-sc-1'];
  });
});
