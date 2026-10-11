/**
 * @license
 * Copyright 2026 Google LLC
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
import { applyFinalState } from '../anywidget-entry';
import { Cell } from '../store/cell';
import { NotebookStore } from '../store/notebook';

const CELL = 'cell-1';

function jobStart(jobId: number, stageIds: number[], submissionTime: number) {
  return {
    msgtype: 'sparkJobStart',
    jobId,
    name: `collect at <console>:${jobId}`,
    stageIds,
    stageInfos: Object.fromEntries(stageIds.map((s) => [s, { numTasks: 10, name: `s${s}` }])),
    numTasks: 10 * stageIds.length,
    status: 'RUNNING',
    submissionTime,
  };
}

function stageSubmitted(stageId: number, submissionTime: number) {
  return { msgtype: 'sparkStageSubmitted', stageId, numTasks: 10, submissionTime };
}

function stageActive(stageId: number, active: number, completed: number, time: number) {
  return {
    msgtype: 'sparkStageActive',
    stageId,
    numActiveTasks: active,
    numCompletedTasks: completed,
    numFailedTasks: 0,
    time,
  };
}

function stageCompleted(stageId: number, submissionTime: number, completionTime: number) {
  return {
    msgtype: 'sparkStageCompleted',
    stageId,
    status: 'COMPLETED',
    numTasks: 10,
    numCompletedTasks: 10,
    numFailedTasks: 0,
    submissionTime,
    completionTime,
  };
}

function jobEnd(jobId: number, completionTime: number, status = 'COMPLETED') {
  return { msgtype: 'sparkJobEnd', jobId, status, completionTime };
}

describe('applyFinalState', () => {
  let nbStore: NotebookStore;
  let cell: Cell;
  let seq = 0;
  const live = (data: any) => {
    seq += 1;
    switch (data.msgtype) {
      case 'sparkJobStart':
        nbStore.onSparkJobStart(CELL, data);
        break;
      case 'sparkJobEnd':
        nbStore.onSparkJobEnd(data);
        break;
      case 'sparkStageSubmitted':
        nbStore.onSparkStageSubmitted(data);
        break;
      case 'sparkStageCompleted':
        nbStore.onSparkStageCompleted(data);
        break;
      case 'sparkStageActive':
        nbStore.onSparkStageActive(data);
        break;
    }
    return { sequence: seq, data };
  };

  beforeEach(() => {
    nbStore = new NotebookStore('nb');
    cell = new Cell(CELL, nbStore);
    nbStore.cells[CELL] = cell;
    seq = 0;
  });

  it('closes out a job whose end events were lost in transit', () => {
    // Live: job 1 starts, its stage runs... then the transport drops the
    // stage completion and the job end.
    const s1 = live(jobStart(1, [10], 1000));
    live(stageSubmitted(10, 1000));
    live(stageActive(10, 4, 6, 1500));
    const lostStage = { sequence: 4, data: stageCompleted(10, 1000, 2000) };
    const lostEnd = { sequence: 5, data: jobEnd(1, 2100) };

    const job = nbStore.jobs['nb-job-1'];
    expect(job.status).toBe('RUNNING');
    expect(cell.numActiveJobs).toBe(1);
    expect(job.numActiveTasks).toBe(4);

    cell.setCellFinished(true);
    applyFinalState([lostEnd, lostStage, s1], nbStore, CELL); // out of order on purpose

    expect(job.status).toBe('COMPLETED');
    expect(job.endTime?.getTime()).toBe(2100);
    expect(cell.numActiveJobs).toBe(0);
    expect(job.numActiveTasks).toBe(0);
    expect(job.numCompletedTasks).toBe(10);
    const stage = nbStore.stages['nb-stage-10'];
    expect(stage.status).toBe('COMPLETED');
    expect(stage.completionTime?.getTime()).toBe(2000);
    // The chart's last point is the "cliff" drop to 0.
    const y = cell.taskChartStore.taskDataY;
    expect(y[y.length - 1]).toBe(0);
  });

  it('is a no-op for jobs and stages that already completed live', () => {
    const events = [
      live(jobStart(1, [10], 1000)),
      live(stageSubmitted(10, 1000)),
      live(stageCompleted(10, 1000, 2000)),
      live(jobEnd(1, 2100)),
    ];
    const job = nbStore.jobs['nb-job-1'];
    const before = {
      status: job.status,
      end: job.endTime?.getTime(),
      points: cell.taskChartStore.taskDataX.length,
      markers: cell.taskChartStore.jobDataX.length,
    };

    applyFinalState(events, nbStore, CELL);

    expect(job.status).toBe(before.status);
    expect(job.endTime?.getTime()).toBe(before.end);
    expect(cell.taskChartStore.taskDataX.length).toBe(before.points);
    expect(cell.taskChartStore.jobDataX.length).toBe(before.markers);
  });

  it('creates a job that was entirely missed, from its retained start and end', () => {
    applyFinalState(
      [
        { sequence: 2, data: jobEnd(3, 5000, 'FAILED') },
        { sequence: 1, data: jobStart(3, [30], 4000) },
      ],
      nbStore,
      CELL,
    );
    const job = nbStore.jobs['nb-job-3'];
    expect(job).toBeDefined();
    expect(job.name).toBe('collect');
    expect(job.startTime.getTime()).toBe(4000);
    expect(job.status).toBe('FAILED');
    expect(cell.uniqueJobIds).toContain('nb-job-3');
    expect(cell.numActiveJobs).toBe(0);
  });

  it('leaves jobs the kernel did not report as ended untouched', () => {
    live(jobStart(1, [10], 1000));
    live(jobStart(2, [20], 1100));
    applyFinalState([{ sequence: 9, data: jobEnd(1, 2000) }], nbStore, CELL);
    expect(nbStore.jobs['nb-job-1'].status).toBe('COMPLETED');
    expect(nbStore.jobs['nb-job-2'].status).toBe('RUNNING');
    expect(cell.numActiveJobs).toBe(1);
  });

  it('drops the chart line to zero when the last live point was above zero', () => {
    live(jobStart(1, [10], 1000));
    live(stageSubmitted(10, 1000));
    live(stageActive(10, 7, 3, 1500)); // last live point: 7 active
    // Only the job end survives in the kernel map (stage completion lost
    // before the kernel saw it, e.g. listener restart) — the chart must still
    // not be left hanging at 7.
    applyFinalState([{ sequence: 9, data: jobEnd(1, 2000) }], nbStore, CELL);
    const x = cell.taskChartStore.taskDataX;
    const y = cell.taskChartStore.taskDataY;
    expect(y[y.length - 1]).toBe(0);
    expect(x[x.length - 1]).toBeGreaterThanOrEqual(1500);
  });

  it('only runs the close-out on the last chunk of a split batch', () => {
    live(jobStart(1, [10], 1000));
    live(stageSubmitted(10, 1000));
    live(stageActive(10, 7, 3, 1500));
    live(jobStart(2, [20], 1600));
    live(stageSubmitted(20, 1600));
    live(stageActive(20, 5, 0, 1700)); // last live point: 5 active
    const closeOut = jest.spyOn(cell.taskChartStore, 'closeOut');

    // Chunk 1 of 2 ends job 1; job 2 is still open, so no close-out.
    applyFinalState([{ sequence: 9, data: jobEnd(1, 2000) }], nbStore, CELL, false);
    expect(cell.numActiveJobs).toBe(1);
    expect(closeOut).not.toHaveBeenCalled();

    // Chunk 2 of 2 ends job 2: close-out runs exactly once and the line ends
    // at zero.
    applyFinalState([{ sequence: 10, data: jobEnd(2, 2100) }], nbStore, CELL, true);
    expect(cell.numActiveJobs).toBe(0);
    expect(closeOut).toHaveBeenCalledTimes(1);
    const y = cell.taskChartStore.taskDataY;
    expect(y[y.length - 1]).toBe(0);
  });

  it('ignores malformed entries and unknown cells', () => {
    expect(() =>
      applyFinalState([{ sequence: 1 }, null as any, { data: {} }], nbStore, CELL),
    ).not.toThrow();
    expect(() => applyFinalState([], nbStore, 'no-such-cell')).not.toThrow();
  });
});
