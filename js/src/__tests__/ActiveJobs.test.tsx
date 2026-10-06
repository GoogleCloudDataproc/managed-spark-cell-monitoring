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
import { runInAction } from 'mobx';
import { act, fireEvent, render, screen } from '@testing-library/react';
import { ACTIVE_JOB_REVEAL_DELAY_MS } from '../components/active-jobs';
import {
  computeRunningSummaryParts,
  FAILED_DETAIL_MIN_WIDTH_PX,
} from '../components/active-jobs-layout';
import { CellMonitorHeader } from '../components/header';
import { StackedProgressBar } from '../components/stacked-progress-bar';
import { CellStoreContext, NotebookStoreContext } from '../store';
import { Cell } from '../store/cell';
import { NotebookStore } from '../store/notebook';
import { SparkJob } from '../store/spark-job';

type ResizeCallback = () => void;

let observed: Array<{ element: Element; callback: ResizeCallback }> = [];
const originalResizeObserver = window.ResizeObserver;

class MockResizeObserver {
  constructor(private readonly callback: ResizeCallback) {}
  observe(element: Element) {
    observed.push({ element, callback: this.callback });
  }
  unobserve() {}
  disconnect() {}
}

/**
 * Simulates the browser laying out the active-job strip at a new width: the
 * hook re-measures the element's border box when ResizeObserver fires.
 */
const setStripWidth = (width: number) => {
  act(() => {
    observed.forEach(({ element, callback }) => {
      element.getBoundingClientRect = () => ({ width }) as DOMRect;
      callback();
    });
  });
};

const revealJobs = () => {
  act(() => {
    jest.advanceTimersByTime(ACTIVE_JOB_REVEAL_DELAY_MS);
  });
};

function addJob(
  notebook: NotebookStore,
  cell: Cell,
  options: {
    id: number;
    name: string;
    status?: 'RUNNING' | 'COMPLETED' | 'FAILED';
    startTime?: number;
    numTasks?: number;
    completed?: number;
    active?: number;
    failed?: number;
  },
) {
  // act() flushes the observer re-render triggered by the store mutation.
  act(() => {
    runInAction(() => {
      const job = new SparkJob(notebook);
      job.uniqueId = `nb-job-${options.id}`;
      job.jobId = String(options.id);
      job.name = options.name;
      job.status = options.status ?? 'RUNNING';
      job.startTime = new Date(options.startTime ?? 1000 + options.id);
      job.numTasks = options.numTasks ?? 10;
      job.numCompletedTasks = options.completed ?? 0;
      job.numActiveTasks = options.active ?? 0;
      job.numFailedTasks = options.failed ?? 0;
      notebook.jobs[job.uniqueId] = job;
      cell.uniqueJobIds.push(job.uniqueId);
    });
  });
}

function finishCell(cell: Cell) {
  act(() => {
    cell.setCellFinished(true);
  });
}

function setExecutors(notebook: NotebookStore, executors?: number, cores?: number) {
  act(() => {
    runInAction(() => {
      notebook.numExecutors = executors;
      notebook.numTotalCores = cores;
    });
  });
}

function renderHeader(props: { viewUrl?: string } = {}) {
  const notebook = new NotebookStore('nb');
  const cell = new Cell('cell', notebook);
  const utils = render(
    <NotebookStoreContext.Provider value={notebook}>
      <CellStoreContext.Provider value={cell}>
        <CellMonitorHeader {...props} />
      </CellStoreContext.Provider>
    </NotebookStoreContext.Provider>,
  );
  return { notebook, cell, ...utils };
}

describe('computeRunningSummaryParts', () => {
  it('drops the task count, then the bar, then the jobs counter as width shrinks', () => {
    expect(computeRunningSummaryParts(500)).toEqual({
      showJobsDone: true,
      showBar: true,
      showTaskCount: true,
    });
    expect(computeRunningSummaryParts(400)).toEqual({
      showJobsDone: true,
      showBar: true,
      showTaskCount: false,
    });
    expect(computeRunningSummaryParts(250)).toEqual({
      showJobsDone: true,
      showBar: false,
      showTaskCount: false,
    });
    expect(computeRunningSummaryParts(100)).toEqual({
      showJobsDone: false,
      showBar: false,
      showTaskCount: false,
    });
  });
});

describe('StackedProgressBar', () => {
  it('sizes segments relative to the total and exposes progress semantics', () => {
    render(<StackedProgressBar total={10} completed={5} running={2} failed={1} label="job" />);
    const bar = screen.getByRole('progressbar', { name: 'job' });
    expect(bar).toHaveAttribute('aria-valuenow', '5');
    expect(bar).toHaveAttribute('aria-valuemax', '10');
    const [done, running, failed] = Array.from(bar.children) as HTMLElement[];
    expect(done.style.width).toBe('50%');
    expect(running.style.width).toBe('20%');
    expect(failed.style.width).toBe('10%');
  });

  it('renders empty segments when the total is unknown', () => {
    render(<StackedProgressBar total={0} completed={3} label="job" />);
    const [done] = Array.from(screen.getByRole('progressbar').children) as HTMLElement[];
    expect(done.style.width).toBe('0%');
  });
});

describe('ActiveJobs header strip', () => {
  beforeEach(() => {
    jest.useFakeTimers();
    observed = [];
    window.ResizeObserver = MockResizeObserver as unknown as typeof ResizeObserver;
  });

  afterEach(() => {
    jest.useRealTimers();
    window.ResizeObserver = originalResizeObserver;
  });

  it('no longer renders the Jobs count badges', () => {
    const { notebook, cell } = renderHeader();
    addJob(notebook, cell, { id: 1, name: 'done', status: 'COMPLETED' });
    expect(screen.queryByText('Jobs:')).not.toBeInTheDocument();
    expect(screen.queryByText(/Completed/)).not.toBeInTheDocument();
  });

  it('waits for the reveal delay before showing a running job', () => {
    const { notebook, cell } = renderHeader();
    setStripWidth(1000);
    addJob(notebook, cell, { id: 1, name: 'count', numTasks: 8, completed: 3, active: 2 });

    act(() => {
      jest.advanceTimersByTime(ACTIVE_JOB_REVEAL_DELAY_MS - 1);
    });
    expect(screen.queryByText('count')).not.toBeInTheDocument();

    act(() => {
      jest.advanceTimersByTime(1);
    });
    expect(screen.getByText('count')).toBeInTheDocument();
    expect(screen.getByText('3/8 (2 running)')).toBeInTheDocument();
    expect(
      screen.getByRole('progressbar', { name: 'Tasks for count: 3/8 completed, 2 running' }),
    ).toBeInTheDocument();
    expect(screen.getByRole('img', { name: 'Jobs: 0/1 completed, 1 running' })).toHaveTextContent(
      '0/1',
    );
  });

  it('removes the summary once the job finishes', () => {
    const { notebook, cell } = renderHeader();
    setStripWidth(1000);
    addJob(notebook, cell, { id: 1, name: 'count' });
    revealJobs();
    expect(screen.getByText('count')).toBeInTheDocument();

    act(() => {
      runInAction(() => {
        notebook.jobs['nb-job-1'].status = 'COMPLETED';
      });
    });
    expect(screen.queryByText('count')).not.toBeInTheDocument();
    expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
  });

  it('combines several running jobs into one summary with summed tasks', () => {
    const { notebook, cell } = renderHeader();
    setStripWidth(1000);
    addJob(notebook, cell, { id: 9, name: 'finished', status: 'COMPLETED', startTime: 500 });
    addJob(notebook, cell, {
      id: 2,
      name: 'newest',
      startTime: 3000,
      numTasks: 10,
      completed: 4,
      active: 2,
    });
    addJob(notebook, cell, {
      id: 1,
      name: 'oldest',
      startTime: 1000,
      numTasks: 20,
      completed: 6,
      active: 3,
      failed: 1,
    });
    revealJobs();

    const name = screen.getByText('2 jobs');
    expect(name).toHaveAttribute('title', 'oldest\nnewest');
    expect(screen.queryByText('oldest')).not.toBeInTheDocument();
    expect(screen.getByText('10/30 (5 running)')).toBeInTheDocument();
    expect(
      screen.getByRole('progressbar', {
        name: 'Tasks across all running jobs: 10/30 completed, 5 running',
      }),
    ).toBeInTheDocument();
    expect(screen.getAllByRole('progressbar')).toHaveLength(1);
    expect(screen.getByRole('img', { name: 'Jobs: 1/3 completed, 2 running' })).toHaveTextContent(
      '1/3',
    );
    expect(screen.queryByText(/more/)).not.toBeInTheDocument();
  });

  it('hides summary parts as the available width shrinks', () => {
    const { notebook, cell } = renderHeader();
    setStripWidth(1000);
    addJob(notebook, cell, { id: 1, name: 'count', numTasks: 8, completed: 3, active: 2 });
    revealJobs();
    const jobsDone = { name: /^Jobs:/ };

    expect(screen.getByText('3/8 (2 running)')).toBeInTheDocument();

    setStripWidth(400);
    expect(screen.queryByText('3/8 (2 running)')).not.toBeInTheDocument();
    expect(screen.getByRole('progressbar')).toBeInTheDocument();

    setStripWidth(250);
    expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
    expect(screen.getByRole('img', jobsDone)).toBeInTheDocument();

    setStripWidth(100);
    expect(screen.queryByRole('img', jobsDone)).not.toBeInTheDocument();
    expect(screen.getByText('count')).toBeInTheDocument();
    expect(screen.getByRole('img', { name: 'Running' })).toBeInTheDocument();
  });

  it('stays empty between jobs while the cell is still executing', () => {
    const { notebook, cell } = renderHeader();
    setStripWidth(1000);
    addJob(notebook, cell, { id: 1, name: 'done', status: 'COMPLETED' });
    addJob(notebook, cell, { id: 2, name: 'broken', status: 'FAILED' });
    expect(screen.queryByText(/completed/)).not.toBeInTheDocument();
    expect(screen.queryByText(/Failed|failed/)).not.toBeInTheDocument();
  });

  it('shows the number of completed jobs once the cell finished', () => {
    const { notebook, cell } = renderHeader();
    setStripWidth(1000);
    addJob(notebook, cell, { id: 1, name: 'a', status: 'COMPLETED' });
    addJob(notebook, cell, { id: 2, name: 'b', status: 'COMPLETED' });
    addJob(notebook, cell, { id: 3, name: 'c', status: 'COMPLETED' });
    finishCell(cell);
    expect(screen.getByText('3 jobs completed')).toBeInTheDocument();
    expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
  });

  it('uses the singular for one completed job', () => {
    const { notebook, cell } = renderHeader();
    addJob(notebook, cell, { id: 1, name: 'only', status: 'COMPLETED' });
    finishCell(cell);
    expect(screen.getByText('1 job completed')).toBeInTheDocument();
  });

  it('shows nothing when the cell finished without any job', () => {
    const { cell, container } = renderHeader();
    finishCell(cell);
    expect(container.querySelector('.active-job')).toBeNull();
  });

  it('keeps showing the running summary after the cell finished until jobs end', () => {
    const { notebook, cell } = renderHeader();
    setStripWidth(1000);
    addJob(notebook, cell, { id: 1, name: 'async', numTasks: 4, completed: 1, active: 1 });
    revealJobs();
    finishCell(cell);
    expect(screen.getByText('async')).toBeInTheDocument();
    expect(screen.queryByText(/jobs? completed/)).not.toBeInTheDocument();

    act(() => {
      runInAction(() => {
        notebook.jobs['nb-job-1'].status = 'COMPLETED';
      });
    });
    expect(screen.queryByText('async')).not.toBeInTheDocument();
    expect(screen.getByText('1 job completed')).toBeInTheDocument();
  });

  it('shows a single failed job by name with the completed count once the cell finished', () => {
    const { notebook, cell } = renderHeader();
    setStripWidth(1000);
    addJob(notebook, cell, { id: 1, name: 'ok-1', status: 'COMPLETED' });
    addJob(notebook, cell, { id: 2, name: 'ok-2', status: 'COMPLETED' });
    addJob(notebook, cell, {
      id: 3,
      name: 'broken',
      status: 'FAILED',
      numTasks: 10,
      completed: 4,
    });
    finishCell(cell);
    expect(screen.getByText('broken')).toBeInTheDocument();
    expect(screen.getByText('Failed')).toBeInTheDocument();
    expect(screen.getByText(/· 2 jobs completed/)).toBeInTheDocument();
    expect(screen.queryByText(/tasks/)).not.toBeInTheDocument();
  });

  it('omits the completed count when no job completed', () => {
    const { notebook, cell } = renderHeader();
    setStripWidth(1000);
    addJob(notebook, cell, { id: 1, name: 'broken', status: 'FAILED' });
    finishCell(cell);
    expect(screen.getByText('Failed')).toBeInTheDocument();
    expect(screen.queryByText(/completed/)).not.toBeInTheDocument();
  });

  it('hides the completed count on narrow strips', () => {
    const { notebook, cell } = renderHeader();
    setStripWidth(FAILED_DETAIL_MIN_WIDTH_PX - 1);
    addJob(notebook, cell, { id: 1, name: 'ok', status: 'COMPLETED' });
    addJob(notebook, cell, { id: 2, name: 'broken', status: 'FAILED' });
    finishCell(cell);
    expect(screen.getByText('Failed')).toBeInTheDocument();
    expect(screen.queryByText(/completed/)).not.toBeInTheDocument();
  });

  it('shows "N failed · M completed" when more than one job failed', () => {
    const { notebook, cell } = renderHeader();
    setStripWidth(1000);
    addJob(notebook, cell, { id: 3, name: 'ok', status: 'COMPLETED', startTime: 3000 });
    addJob(notebook, cell, { id: 2, name: 'second', status: 'FAILED', startTime: 2000 });
    addJob(notebook, cell, { id: 1, name: 'first', status: 'FAILED', startTime: 1000 });
    finishCell(cell);

    const failed = screen.getByText('2 failed');
    expect(failed.closest('.active-job')).toHaveAttribute('title', 'first\nsecond');
    expect(screen.getByText(/· 1 completed/)).toBeInTheDocument();
    expect(screen.queryByText('first')).not.toBeInTheDocument();
  });

  it('replaces the final summary when a new job starts running', () => {
    const { notebook, cell } = renderHeader();
    setStripWidth(1000);
    addJob(notebook, cell, { id: 1, name: 'broken', status: 'FAILED' });
    finishCell(cell);
    expect(screen.getByText('Failed')).toBeInTheDocument();

    addJob(notebook, cell, { id: 2, name: 'retry' });
    expect(screen.queryByText('broken')).not.toBeInTheDocument();
    expect(screen.queryByText('Failed')).not.toBeInTheDocument();
  });

  it('toggles the header collapse when the strip area is clicked', () => {
    const { cell, container } = renderHeader();
    const before = cell.isHeaderCollapsed;
    fireEvent.click(container.querySelector('.active-jobs') as Element);
    expect(cell.isHeaderCollapsed).toBe(!before);
  });
});

describe('Header layout', () => {
  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('shows the "Job execution" label instead of the resource chips', () => {
    renderHeader();
    expect(screen.getByText('Job execution')).toBeInTheDocument();
    expect(screen.queryByText(/Apache Spark/)).not.toBeInTheDocument();
    expect(document.querySelector('.badgeexecutor')).not.toBeInTheDocument();
  });

  it('shows executors and cores on the right only while a job is running', () => {
    const { notebook, cell, container } = renderHeader();
    setExecutors(notebook, 2, 8);
    expect(screen.queryByText(/executors/)).not.toBeInTheDocument();

    addJob(notebook, cell, { id: 1, name: 'count' });
    revealJobs();
    const resources = screen.getByText('2 executors · 8 cores');
    expect(container.querySelector('.titleright')).toContainElement(resources);

    act(() => {
      runInAction(() => {
        notebook.jobs['nb-job-1'].status = 'COMPLETED';
      });
    });
    expect(screen.queryByText(/executors/)).not.toBeInTheDocument();
  });

  it('uses singular nouns for a count of one', () => {
    const { notebook, cell } = renderHeader();
    setExecutors(notebook, 1, 1);
    addJob(notebook, cell, { id: 1, name: 'count' });
    revealJobs();
    expect(screen.getByText('1 executor · 1 core')).toBeInTheDocument();
  });

  it('hides executors and cores when the session reports none (Spark Connect)', () => {
    const { notebook, cell } = renderHeader();
    setExecutors(notebook, undefined, undefined);
    addJob(notebook, cell, { id: 1, name: 'count' });
    revealJobs();
    expect(screen.getByText('count')).toBeInTheDocument();
    expect(screen.queryByText(/executor/)).not.toBeInTheDocument();
  });

  it('orders the right-side controls as executors, views, console link, close', () => {
    const { notebook, cell, container } = renderHeader({
      viewUrl: 'https://console.cloud.google.com/dataproc',
    });
    setExecutors(notebook, 2, 8);
    addJob(notebook, cell, { id: 1, name: 'count' });
    revealJobs();

    const right = container.querySelector('.titleright') as HTMLElement;
    const order = Array.from(right.querySelectorAll('.header-resources, .tabbutton')).map(
      (element) => element.getAttribute('title') ?? element.textContent,
    );
    expect(order).toEqual([
      '2 executors · 8 cores',
      'Jobs',
      'Tasks',
      'Event Timeline',
      'View in Google Cloud',
      'Close Display',
    ]);
  });
});

describe('ConsoleLink in header', () => {
  it('is hidden when no URL is provided', () => {
    renderHeader();
    expect(screen.queryByRole('link')).not.toBeInTheDocument();
  });

  it('renders an https URL as an icon-only new-tab link', () => {
    renderHeader({ viewUrl: 'https://console.cloud.google.com/dataproc/clusters' });
    const link = screen.getByRole('link', {
      name: 'View in Google Cloud (opens in a new tab)',
    });
    expect(link).toHaveAttribute('href', 'https://console.cloud.google.com/dataproc/clusters');
    expect(link).toHaveAttribute('target', '_blank');
    expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    expect(link).toHaveTextContent('');
  });

  it('rejects non-https URLs', () => {
    renderHeader({ viewUrl: 'javascript:alert(1)' });
    expect(screen.queryByRole('link')).not.toBeInTheDocument();
  });

  it('reads viewUrl reactively from NotebookStore when prop is not passed', () => {
    const { notebook } = renderHeader();
    expect(screen.queryByRole('link')).not.toBeInTheDocument();

    act(() => {
      notebook.setViewUrl(
        'https://console.cloud.google.com/dataproc/interactive/us-central1/sess-1/sparkApplications/applications/application_1_1?project=my-proj',
      );
    });

    const link = screen.getByRole('link', {
      name: 'View in Google Cloud (opens in a new tab)',
    });
    expect(link).toHaveAttribute(
      'href',
      'https://console.cloud.google.com/dataproc/interactive/us-central1/sess-1/sparkApplications/applications/application_1_1?project=my-proj',
    );
  });
});
