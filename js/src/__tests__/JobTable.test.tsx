/*
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
import { render, screen, fireEvent } from '@testing-library/react';
import { JobTable } from '../components/job-table';
import { NotebookStore } from '../store/notebook';
import { Cell } from '../store/cell';
import { NotebookStoreContext, CellStoreContext } from '../store';
import { SparkJob } from '../store/spark-job';
import { SparkStage } from '../store/spark-stage';

jest.mock('react-timeago', () => ({
  __esModule: true,
  default: () => <span data-testid="timeago-mock">TimeAgo Mock</span>
}));

jest.mock('pretty-ms', () => ({
  __esModule: true,
  default: () => "mock-time"
}));

describe('JobTable Component', () => {
  it('renders standard table headers and a job row', () => {
    const notebookStore = new NotebookStore('test-nb');
    const cellStore = new Cell('test-cell', notebookStore);
    
    // Setup Mock Jobs and Stages
    const job = new SparkJob(notebookStore);
    job.uniqueId = 'test-nb-job-1';
    job.jobId = '1';
    job.name = 'Test Job 1';
    job.status = 'RUNNING';
    job.startTime = new Date('2025-01-01');
    job.numTasks = 10;
    job.numCompletedTasks = 5;
    job.uniqueStageIds = ['test-nb-stage-1'];

    const stage = new SparkStage();
    stage.uniqueId = 'test-nb-stage-1';
    stage.stageId = '1';
    stage.name = 'Test Stage 1';
    stage.status = 'RUNNING';
    stage.numTasks = 10;
    stage.numCompletedTasks = 5;
    stage.uniqueJobId = 'test-nb-job-1';

    notebookStore.jobs[job.uniqueId] = job;
    notebookStore.stages[stage.uniqueId] = stage;
    cellStore.uniqueJobIds.push(job.uniqueId);

    notebookStore.cells['test-cell'] = cellStore;

    const { container } = render(
      <NotebookStoreContext.Provider value={notebookStore}>
        <CellStoreContext.Provider value={cellStore}>
          <JobTable />
        </CellStoreContext.Provider>
      </NotebookStoreContext.Provider>
    );

    expect(screen.getByText(/Job Name/i)).toBeInTheDocument();
    expect(screen.getByText(/Test Job 1/i)).toBeInTheDocument();
    
    // Click the expansion icon to render the test stage
    const toggleNodes = container.querySelectorAll('.tdstageicon');
    if (toggleNodes.length > 0) {
      fireEvent.click(toggleNodes[0]);
      expect(screen.getByText(/Test Stage 1/i)).toBeInTheDocument();
    }
  });
});
