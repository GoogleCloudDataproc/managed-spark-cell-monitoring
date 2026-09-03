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
import { CellMonitorHeader } from '../components/header';
import { NotebookStore } from '../store/notebook';
import { Cell } from '../store/cell';
import { NotebookStoreContext, CellStoreContext } from '../store';
import { SparkJob } from '../store/spark-job';

describe('Header Component', () => {
  it('renders and allows tab switching', () => {
    const notebookStore = new NotebookStore('test-nb');
    notebookStore.applicationName = 'Test App';
    const cellStore = new Cell('test-cell', notebookStore);
    cellStore.isHeaderCollapsed = false;
    cellStore.isRemoved = false;

    const job = new SparkJob(notebookStore);
    job.uniqueId = 'test-nb-job-1';
    job.status = 'RUNNING';
    job.numTasks = 10;
    job.numCompletedTasks = 5;
    notebookStore.jobs[job.uniqueId] = job;
    cellStore.uniqueJobIds.push(job.uniqueId);

    const { container } = render(
      <NotebookStoreContext.Provider value={notebookStore}>
        <CellStoreContext.Provider value={cellStore}>
          <CellMonitorHeader />
        </CellStoreContext.Provider>
      </NotebookStoreContext.Provider>
    );

    // Verify a rendering item
    expect(screen.getByText(/Apache Spark:/i)).toBeInTheDocument();
    
    // Switch tabs
    const taskChartTab = screen.getByTitle(/Tasks/i);
    fireEvent.click(taskChartTab);
    expect(cellStore.view).toBe('taskchart');

    const timelineTab = screen.getByTitle(/Event Timeline/i);
    fireEvent.click(timelineTab);
    expect(cellStore.view).toBe('timeline');
    
    const jobsTab = screen.getByTitle(/Jobs/i);
    fireEvent.click(jobsTab);
    expect(cellStore.view).toBe('jobs');

    const closeButton = screen.getByTitle(/Close Display/i);
    fireEvent.click(closeButton);
    expect(cellStore.isRemoved).toBe(true);

    const titleLeft = container.querySelector('.titleleft');
    if (titleLeft) {
      fireEvent.click(titleLeft);
      expect(cellStore.isHeaderCollapsed).toBe(true);
    }
  });
});
