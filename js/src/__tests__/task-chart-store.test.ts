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

import { TaskChartStore } from '../store/task-chart-store';
import { NotebookStore } from '../store/notebook';

function isNonDecreasing(xs: number[]): boolean {
  return xs.every((x, i) => i === 0 || x >= xs[i - 1]);
}

describe('TaskChartStore timestamps', () => {
  let notebookStore: NotebookStore;
  let store: TaskChartStore;

  beforeEach(() => {
    notebookStore = new NotebookStore('test-nb');
    notebookStore.numTotalCores = 2;
    store = new TaskChartStore(notebookStore);
  });

  it('keeps in-order timestamps unchanged', () => {
    store.onSparkJobStart({ jobId: 1, submissionTime: 1000 });
    store.onSparkStageActive(1250, 1);
    store.onSparkStageActive(1500, 2);
    store.onSparkJobEnd({ jobId: 1, completionTime: 2000 });

    expect(store.taskDataX).toEqual([1000, 1250, 1500, 2000]);
    expect(store.taskDataY).toEqual([0, 1, 2, 0]);
    expect(store.jobDataX).toEqual([1000, 2000]);
  });

  it('clamps a late-arriving sample that is stamped before the previous point', () => {
    // Screenshot scenario: the job ran 13.53s -> 15.83s on the driver clock,
    // but the only active-task sample was stamped with the browser clock
    // (~16.10s) and then the driver-timed stage/job end arrived.
    store.onSparkJobStart({ jobId: 2, submissionTime: 13530 });
    store.onSparkStageActive(16100, 1); // browser Date.now() fallback
    store.onSparkStageActive(15830, 0); // stage completed (driver clock)
    store.onSparkJobEnd({ jobId: 2, completionTime: 15830 });

    expect(isNonDecreasing(store.taskDataX)).toBe(true);
    expect(store.taskDataX).toEqual([13530, 16100, 16100, 16100]);
    expect(store.taskDataY).toEqual([0, 1, 0, 0]);
    // The job-end marker is clamped too so it never sits left of the series.
    expect(store.jobDataX).toEqual([13530, 16100]);
  });

  it('keeps the task and executor series aligned after clamping', () => {
    store.onSparkJobStart({ jobId: 3, submissionTime: 5000 });
    store.onSparkStageActive(7000, 2);
    notebookStore.numTotalCores = 4;
    store.onSparkStageActive(6000, 3); // out of order

    expect(store.executorDataX).toEqual(store.taskDataX);
    expect(store.executorDataY).toEqual([2, 2, 4]);
    expect(store.taskDataX).toEqual([5000, 7000, 7000]);
  });

  it('does not let an unparsable timestamp poison the series', () => {
    store.onSparkJobStart({ jobId: 4, submissionTime: 1000 });
    store.onSparkStageActive(Number.NaN, 1);

    expect(store.taskDataX).toEqual([1000, 1000]);
    expect(store.taskDataY).toEqual([0, 1]);
  });

  it('does not plot a missing timestamp at the epoch', () => {
    // `new Date(null).getTime()` is 0, so null must be treated as missing,
    // not as a valid 1970 timestamp - including for the very first point.
    const before = Date.now();
    store.onSparkJobStart({ jobId: 6, submissionTime: null });
    expect(store.taskDataX[0]).toBeGreaterThanOrEqual(before);

    store.onSparkStageActive(before + 500, 2);
    store.onSparkStageActive(null as unknown as number, 1);
    store.onSparkStageActive(undefined as unknown as number, 0);
    expect(store.taskDataX.slice(1)).toEqual([before + 500, before + 500, before + 500]);
    expect(store.taskDataY).toEqual([0, 2, 1, 0]);
  });

  it('reset clears the clamp state', () => {
    store.onSparkJobStart({ jobId: 5, submissionTime: 9000 });
    store.reset();
    store.onSparkJobStart({ jobId: 6, submissionTime: 1000 });

    expect(store.taskDataX).toEqual([1000]);
    expect(store.jobDataX).toEqual([1000]);
  });
});
