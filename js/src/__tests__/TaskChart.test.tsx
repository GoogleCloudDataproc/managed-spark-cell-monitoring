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

import React from 'react';
import { render } from '@testing-library/react';
import TaskChart, {
  createChartData,
  hasExecutorData,
  TaskChartSeries,
} from '../components/task-chart';
import { NotebookStore } from '../store/notebook';
import { Cell } from '../store/cell';
import { NotebookStoreContext, CellStoreContext } from '../store';

jest.mock('react-plotly.js', () => ({
  __esModule: true,
  default: () => <div data-testid="plotly-mock" />
}));

const traceNames = (traces: Array<{ name?: string }>) =>
  traces.map((trace) => trace.name);

// Plotly.Data is a union; the traces built here are all scatter traces.
const yValues = (trace: unknown) => (trace as { y: number[] }).y;

const series = (executorDataY: number[]): TaskChartSeries => ({
  taskDataX: [1000, 2000, 3000],
  taskDataY: [0, 6, 2],
  executorDataX: [1000, 2000, 3000],
  executorDataY,
  jobDataX: [1000, 3000],
  jobDataY: [0, 0],
  jobDataText: ['Job 1 started', 'Job 1 ended'],
});

describe('TaskChart Component', () => {
  it('renders gracefully', () => {
    const notebookStore = new NotebookStore('test-nb');
    const cellStore = new Cell('test-cell', notebookStore);

    render(
      <NotebookStoreContext.Provider value={notebookStore}>
        <CellStoreContext.Provider value={cellStore}>
          <TaskChart />
        </CellStoreContext.Provider>
      </NotebookStoreContext.Provider>
    );
  });
});

describe('hasExecutorData', () => {
  it('is false when no executor cores were ever reported (Spark Connect)', () => {
    expect(hasExecutorData([])).toBe(false);
    expect(hasExecutorData([0, 0, 0])).toBe(false);
  });

  it('is true once any executor cores sample is positive (remote kernel)', () => {
    expect(hasExecutorData([0, 4, 4])).toBe(true);
  });
});

describe('createChartData', () => {
  it('draws only running tasks and job markers without executor data', () => {
    const traces = createChartData(series([0, 0, 0]));

    expect(traceNames(traces)).toEqual([
      'Running Tasks',
      undefined, // job markers
      'Running Tasks', // legend entry
    ]);
    expect(traceNames(traces)).not.toContain('Scheduled Tasks');
    expect(traceNames(traces)).not.toContain('Executor Cores');
    // Raw active task counts, not capped at (zero) cores.
    expect(yValues(traces[0])).toEqual([0, 6, 2]);
  });

  it('draws running, scheduled and executor series with executor data', () => {
    const traces = createChartData(series([4, 4, 4]));

    expect(traceNames(traces)).toEqual([
      'Running Tasks',
      'Scheduled Base',
      'Scheduled Tasks',
      'Executor Cores',
      undefined, // job markers
      'Running Tasks',
      'Scheduled Tasks',
      'Executor Cores',
    ]);
    // Running tasks are capped at the executor cores; the rest is "scheduled".
    expect(yValues(traces[0])).toEqual([0, 4, 2]);
    expect(yValues(traces[3])).toEqual([4, 4, 4]);
  });

  it('uses the Spark Connect variant for a cell with no telemetry yet', () => {
    const notebookStore = new NotebookStore('test-nb');
    const cellStore = new Cell('test-cell', notebookStore);

    const traces = createChartData(cellStore.taskChartStore);

    expect(traceNames(traces)).toEqual(['Running Tasks', undefined, 'Running Tasks']);
  });

  it('switches to the full chart when the notebook reports executor cores', () => {
    const notebookStore = new NotebookStore('test-nb');
    const cellStore = new Cell('test-cell', notebookStore);
    notebookStore.numTotalCores = 8;
    cellStore.taskChartStore.onSparkStageActive(1000, 3);

    const traces = createChartData(cellStore.taskChartStore);

    expect(traceNames(traces)).toContain('Executor Cores');
    expect(yValues(traces[0])).toEqual([3]);
  });
});
