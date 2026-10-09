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

import fs from 'fs';
import path from 'path';
import React from 'react';
import { act, render, screen } from '@testing-library/react';
import { runInAction } from 'mobx';
import { JobTable, formatStartTime, formatStartTimestamp } from '../components/job-table';
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

  const job = new SparkJob();
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
    </NotebookStoreContext.Provider>,
  );
  return { notebookStore, cellStore, job, ...utils };
}

describe('JobTable Component', () => {
  it('renders the columns in order: Job Name, Start Time, Status, Tasks, Duration', () => {
    const { container } = renderTable();
    const headers = Array.from(container.querySelectorAll('thead th')).map((th) => th.textContent);
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

  it('renders the full job name in a clipped span with a tooltip', () => {
    const longName = 'collect at /home/jupyter/notebooks/wine_quality_analysis_pipeline.py:142';
    const { container, job } = renderTable();
    act(() => {
      runInAction(() => {
        job.name = longName;
      });
    });
    const nameCell = container.querySelector('.tdjobname') as HTMLElement;
    const clip = nameCell.querySelector('.jobname') as HTMLElement;
    // Truncation is purely CSS (max-width + ellipsis); the DOM keeps the full
    // text so copy/paste and the tooltip both show the whole call site.
    expect(clip).toHaveTextContent(longName);
    expect(clip).toHaveAttribute('title', longName);
    expect(nameCell.children).toHaveLength(1);
    // The wrapper is the container the cqw-based cap is measured against.
    expect(container.querySelector('.tabcontent')).toHaveClass('jobtable-content');
  });

  it('falls back to "Unnamed" when the job has no name', () => {
    const { container, job } = renderTable();
    act(() => {
      runInAction(() => {
        job.name = '';
      });
    });
    const clip = container.querySelector('.tdjobname .jobname') as HTMLElement;
    expect(clip).toHaveTextContent('Unnamed');
    expect(clip).toHaveAttribute('title', 'Unnamed');
  });

  it('applies a stable lowercase status class', () => {
    const { container, job } = renderTable();
    const status = () => container.querySelector('.tditemjobstatus') as HTMLElement;
    expect(status()).toHaveClass('running');
    expect(status()).not.toHaveClass('RUNNING');

    act(() => {
      runInAction(() => {
        job.status = 'FAILED';
        job.endTime = new Date(2025, 0, 1, 15, 36, 8);
      });
    });
    expect(status()).toHaveClass('failed');
    expect(status()).toHaveTextContent('Failed');
  });

  it.each([
    ['RUNNING', 'Running'],
    ['COMPLETED', 'Completed'],
    ['FAILED', 'Failed'],
    ['PENDING', 'Pending'],
    ['SKIPPED', 'Skipped'],
    ['UNKNOWN', 'Unknown'],
  ])('renders status %s as plain capitalised text %s with no inline styling', (raw, text) => {
    const { container, job } = renderTable();
    act(() => {
      runInAction(() => {
        // Cast: the table must cope with any status string the listener may send.
        job.status = raw as SparkJob['status'];
      });
    });
    const status = container.querySelector('.tditemjobstatus') as HTMLElement;
    expect(status).toHaveTextContent(text);
    expect(status).toHaveClass(raw.toLowerCase());
    // No inline styling: appearance must come only from the table text.
    expect(status.getAttribute('style')).toBeNull();
  });

  it('falls back to "Unknown" when the status is missing', () => {
    const { container, job } = renderTable();
    act(() => {
      runInAction(() => {
        job.status = undefined as unknown as SparkJob['status'];
      });
    });
    expect(container.querySelector('.tditemjobstatus')).toHaveTextContent('Unknown');
  });
});

describe('job status stylesheet', () => {
  // Jest maps CSS imports to identity-obj-proxy, so jsdom never sees the real
  // rules. Read the stylesheet directly to guard against a badge being
  // reintroduced for any status value. Done in beforeAll so the I/O happens
  // only when this suite runs, not during test collection.
  let css: string;
  beforeAll(() => {
    css = fs.readFileSync(path.join(__dirname, '../../style/jobtable.css'), 'utf8');
  });
  const statusClasses = ['running', 'completed', 'failed', 'pending', 'skipped', 'unknown'];

  it('does not style any status value as a badge', () => {
    for (const cls of statusClasses) {
      // Matches selectors such as ".pm .completed {" or ".completed," anywhere.
      expect(css).not.toMatch(new RegExp(`\\.${cls}\\b`));
    }
  });

  it('does not give the status span a background, colour, radius or padding', () => {
    expect(css).not.toMatch(/\.tditemjobstatus\b/);
    expect(css).not.toMatch(/\.tdjobstatus\s*>\s*span/);
  });
});

describe('job table column sizing stylesheet', () => {
  const css = fs.readFileSync(path.join(__dirname, '../../style/jobtable.css'), 'utf8');
  // Returns the declarations of every rule whose selector list mentions `cls`.
  const rulesFor = (cls: string) =>
    Array.from(css.matchAll(/([^{}]+)\{([^}]*)\}/g))
      .filter((m) => new RegExp(`\\.${cls}\\b`).test(m[1]))
      .map((m) => m[2]);

  it('gives the Job Name column a floor so short names stay visibly centred', () => {
    expect(rulesFor('thjobname').join(' ')).toMatch(/width\s*:\s*18%/);
  });

  it('keeps the job name on one line, capped relative to the widget width', () => {
    const decl = rulesFor('jobname').join(' ');
    expect(decl).toMatch(/min-width\s*:/);
    // The cap must track the wrapper (container-query units), not a fixed ch/px.
    expect(decl).toMatch(/max-width\s*:[^;]*cqw/);
    expect(decl).toMatch(/white-space\s*:\s*nowrap/);
    expect(decl).toMatch(/text-overflow\s*:\s*ellipsis/);
  });

  it('makes only the job table wrapper a size container', () => {
    expect(rulesFor('jobtable-content').join(' ')).toMatch(/container-type\s*:\s*inline-size/);
    expect(rulesFor('tabcontent').join(' ')).not.toMatch(/container-type/);
  });

  it('gives Start Time, Status and Duration fixed percentage shares', () => {
    for (const cls of ['thjobstart', 'thjobstatus', 'thjobtime']) {
      expect(rulesFor(cls).join(' ')).toMatch(/(^|[^-])width\s*:\s*\d+%/);
    }
  });

  it('lets Tasks absorb the slack while keeping room for the progress bar', () => {
    const decl = rulesFor('thjobtasks').join(' ');
    expect(decl).not.toMatch(/(^|[^-])width\s*:/);
    expect(decl).toMatch(/min-width\s*:/);
  });

  it('keeps every column centred', () => {
    for (const cls of ['thjobname', 'tdjobname', 'jobname']) {
      expect(rulesFor(cls).join(' ')).not.toMatch(/text-align\s*:\s*left/);
    }
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
