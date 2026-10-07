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
import { act, render, screen } from '@testing-library/react';
import { runInAction } from 'mobx';
import {
  JobTable,
  formatStartTime,
  formatStartTimestamp,
} from '../components/job-table';
import { NotebookStore } from '../store/notebook';
import { Cell } from '../store/cell';
import { NotebookStoreContext, CellStoreContext } from '../store';
import { SparkJob } from '../store/spark-job';

jest.mock('pretty-ms', () => ({
  __esModule: true,
  default: (ms: number) => `mock-time:${ms}`,
}));

function renderTable(startTime = new Date(2025, 0, 1, 15, 36, 3)) {
  const notebookStore = new NotebookStore('test-nb');
  const cellStore = new Cell('test-cell', notebookStore);
  notebookStore.cells['test-cell'] = cellStore;

  const job = new SparkJob(notebookStore);
  job.uniqueId = 'test-nb-job-1';
  job.jobId = '1';
  job.name = 'Test Job 1';
  job.status = 'RUNNING';
  // Local-time constructor so the expected wall-clock text is TZ independent.
  job.startTime = startTime;
  job.numTasks = 10;
  job.numCompletedTasks = 5;
  job.uniqueStageIds = ['test-nb-stage-1'];
  notebookStore.jobs[job.uniqueId] = job;
  cellStore.uniqueJobIds.push(job.uniqueId);

  const utils = render(
    <NotebookStoreContext.Provider value={notebookStore}>
      <CellStoreContext.Provider value={cellStore}>
        <JobTable />
      </CellStoreContext.Provider>
    </NotebookStoreContext.Provider>
  );
  return { notebookStore, cellStore, job, ...utils };
}

describe('JobTable Component', () => {
  it('renders the columns in order: Job Name, Start Time, Status, Tasks, Duration', () => {
    const { container } = renderTable();
    const headers = Array.from(container.querySelectorAll('thead th')).map(
      (th) => th.textContent,
    );
    expect(headers).toEqual(['Job Name', 'Start Time', 'Status', 'Tasks', 'Duration']);
  });

  it('renders one flat row per job without Job ID, Stages or an expander', () => {
    const { container, job } = renderTable();
    expect(screen.getByText('Test Job 1')).toBeInTheDocument();
    expect(screen.getByText('Running')).toBeInTheDocument();
    const startCell = container.querySelector('.tdjobstarttime');
    expect(startCell).toHaveTextContent(/36.03/);
    expect(startCell).toHaveAttribute('title', formatStartTimestamp(job.startTime));

    const cells = container.querySelectorAll('tbody tr.jobrow td');
    expect(cells).toHaveLength(5);
    expect(container.querySelector('.tdstagebutton')).toBeNull();
    expect(container.querySelector('.tdjobid')).toBeNull();
    expect(container.querySelector('.tdjobstages')).toBeNull();
    expect(container.querySelector('.stagetable')).toBeNull();
  });

  it('shows the duration only once the job has ended', () => {
    const { container, job } = renderTable();
    expect(container.querySelector('.tdjobduration')).toHaveTextContent('-');

    act(() => {
      runInAction(() => {
        job.status = 'COMPLETED';
        job.endTime = new Date(2025, 0, 1, 15, 36, 8);
      });
    });
    expect(screen.getByText('Completed')).toBeInTheDocument();
    expect(container.querySelector('.tdjobduration')).toHaveTextContent('mock-time');
  });

  it('shows a placeholder instead of the epoch when the start time is unknown', () => {
    // new Date(0) is what a missing submissionTime decodes to.
    const { container, job } = renderTable(new Date(0));
    act(() => {
      runInAction(() => {
        job.endTime = new Date(2025, 0, 1, 15, 36, 8);
      });
    });
    expect(container.querySelector('.tdjobstarttime')).toHaveTextContent('\u2014');
    expect(container.querySelector('.tdjobstarttime')).toHaveAttribute('title', '');
    // No "56 years" duration measured from the epoch.
    expect(container.querySelector('.tdjobduration')).toHaveTextContent('-');
  });

  it('shows a placeholder when the end time is the epoch or invalid', () => {
    const { container, job } = renderTable();
    act(() => {
      runInAction(() => {
        job.status = 'COMPLETED';
        job.endTime = new Date(0);
      });
    });
    expect(container.querySelector('.tdjobduration')).toHaveTextContent('-');

    act(() => {
      runInAction(() => {
        job.endTime = new Date(NaN);
      });
    });
    expect(container.querySelector('.tdjobduration')).toHaveTextContent('-');
  });

  it('never shows a negative duration when the end time precedes the start', () => {
    const { container, job } = renderTable(new Date(2025, 0, 1, 15, 36, 8));
    act(() => {
      runInAction(() => {
        job.status = 'COMPLETED';
        job.endTime = new Date(2025, 0, 1, 15, 36, 3);
      });
    });
    expect(container.querySelector('.tdjobduration')).toHaveTextContent('mock-time:0');
  });

  it('applies the lowercase status class so the badge styles match', () => {
    const { container, job } = renderTable();
    const badge = () => container.querySelector('.tditemjobstatus') as HTMLElement;
    expect(badge()).toHaveClass('running');
    expect(badge()).not.toHaveClass('RUNNING');

    act(() => {
      runInAction(() => {
        job.status = 'FAILED';
        job.endTime = new Date(2025, 0, 1, 15, 36, 8);
      });
    });
    expect(badge()).toHaveClass('failed');
    expect(badge()).toHaveTextContent('Failed');
  });
});

describe('formatStartTime', () => {
  it('renders a local wall-clock time with seconds', () => {
    const date = new Date(2025, 0, 1, 15, 36, 3);
    expect(formatStartTime(date)).toBe(
      date.toLocaleTimeString(undefined, {
        hour: 'numeric',
        minute: '2-digit',
        second: '2-digit',
      }),
    );
    expect(formatStartTime(date)).toMatch(/36.03/);
  });

  it('returns a placeholder for missing, invalid or epoch dates', () => {
    expect(formatStartTime(undefined)).toBe('\u2014');
    expect(formatStartTime(new Date(NaN))).toBe('\u2014');
    expect(formatStartTime(new Date(0))).toBe('\u2014');
  });
});
