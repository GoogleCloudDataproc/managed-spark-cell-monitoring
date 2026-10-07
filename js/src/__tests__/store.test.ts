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

import { Cell } from '../store/cell';
import { NotebookStore } from '../store/notebook';
import { SparkJob } from '../store/spark-job';

describe('MobX Store Tests', () => {
  let nbStore: NotebookStore;
  
  beforeEach(() => {
    nbStore = new NotebookStore('test-nb');
  });

  it('initializes a Cell store correctly and tests view changes', () => {
    const cell = new Cell('cell-1', nbStore);
    expect(cell.cellId).toBe('cell-1');
    expect(cell.uniqueJobIds).toEqual([]);
    expect(cell.view).toBe('jobs');
    
    cell.reset();
    expect(cell.isRemoved).toBe(false);
    cell.isHeaderCollapsed = false;
  });

  it('correctly maps jobs and stages in NotebookStore', () => {
    const cell = new Cell('cell-1', nbStore);
    nbStore.cells['cell-1'] = cell;

    nbStore.onSparkStageSubmitted('cell-1', {
      msgtype: 'sparkStageSubmitted',
      stageId: 1,
      name: 'Test Stage',
      numTasks: 10
    });
    
    nbStore.onSparkJobStart('cell-1', {
      msgtype: 'sparkJobStart',
      jobId: 1,
      name: 'Test Job',
      stageIds: [1],
      status: 'RUNNING'
    });
    
    expect(Object.keys(nbStore.jobs).length).toBe(1);
    expect(nbStore.jobs['test-nb-job-1'].status).toBe('RUNNING');
    expect(nbStore.jobs['test-nb-job-1'].name).toBe('Test Job');
    
    nbStore.onSparkStageActive({
      msgtype: 'sparkStageActive',
      stageId: 1,
      numActiveTasks: 2,
      numCompletedTasks: 3,
      numFailedTasks: 0
    });
    
    nbStore.onSparkTaskStart({
      msgtype: 'sparkTaskStart',
      stageId: 1,
      taskId: 1,
      launchTime: 50
    });

    nbStore.onSparkTaskEnd({
      msgtype: 'sparkTaskEnd',
      stageId: 1,
      taskId: 1,
      finishTime: 100
    });
    
    nbStore.onSparkStageCompleted({
      msgtype: 'sparkStageCompleted',
      stageId: 1,
      status: 'COMPLETED',
      submissionTime: '1000',
      completionTime: '2000'
    });
    
    expect(nbStore.stages['test-nb-stage-1'].status).toBe('COMPLETED');
    
    nbStore.onSparkJobEnd({
      jobId: 1,
      status: 'SUCCEEDED'
    });
    expect(nbStore.jobs['test-nb-job-1'].status).toBe('SUCCEEDED');
    
    nbStore.onSparkApplicationStart({
      appName: 'App1',
      appId: 'AppId1'
    });
    expect(nbStore.applicationName).toBe('App1');
  });

  describe('executor and core counts', () => {
    it('uses the counts carried by the listener payload when present', () => {
      nbStore.onSparkExecutorAdded({ executorId: 'exec1', totalCores: 8, numExecutors: 2 });
      expect(nbStore.numExecutors).toBe(2);
      expect(nbStore.numTotalCores).toBe(8);

      nbStore.onSparkExecutorRemoved({ executorId: 'exec1', totalCores: 4, numExecutors: 1 });
      expect(nbStore.numExecutors).toBe(1);
      expect(nbStore.numTotalCores).toBe(4);
    });

    it('falls back to counting locally when the payload has no numExecutors', () => {
      nbStore.onSparkExecutorAdded({ executorId: 'exec1', totalCores: 4 });
      nbStore.onSparkExecutorAdded({ executorId: 'exec2', totalCores: 8 });
      expect(nbStore.numExecutors).toBe(2);
      expect(nbStore.numTotalCores).toBe(8);

      nbStore.onSparkExecutorRemoved({ executorId: 'exec1', totalCores: 4 });
      expect(nbStore.numExecutors).toBe(1);

      nbStore.onSparkExecutorRemoved({ executorId: 'exec2', totalCores: 0 });
      nbStore.onSparkExecutorRemoved({ executorId: 'exec2', totalCores: 0 });
      expect(nbStore.numExecutors).toBe(0);
      expect(nbStore.numTotalCores).toBe(0);
    });

    it('clamps negative counts from the payload to zero', () => {
      nbStore.onSparkExecutorRemoved({ executorId: 'exec1', totalCores: -4, numExecutors: -1 });
      expect(nbStore.numExecutors).toBe(0);
      expect(nbStore.numTotalCores).toBe(0);

      nbStore.onSparkExecutorAdded({ executorId: 'exec1', totalCores: -1, numExecutors: -2 });
      expect(nbStore.numExecutors).toBe(0);
      expect(nbStore.numTotalCores).toBe(0);
    });

    it('clamps the counts carried by sparkJobStart', () => {
      nbStore.onSparkJobStart('cell-1', {
        jobId: 7,
        name: 'Job',
        stageIds: [],
        status: 'RUNNING',
        totalCores: -3,
        numExecutors: -1
      });
      expect(nbStore.numExecutors).toBe(0);
      expect(nbStore.numTotalCores).toBe(0);

      nbStore.onSparkJobStart('cell-1', {
        jobId: 8,
        name: 'Job',
        stageIds: [],
        status: 'RUNNING',
        totalCores: 16,
        numExecutors: 4
      });
      expect(nbStore.numExecutors).toBe(4);
      expect(nbStore.numTotalCores).toBe(16);
    });
  });

  it('correctly updates TaskChartStore through Cell', () => {
    const cell = new Cell('cell-1', nbStore);
    nbStore.cells['cell-1'] = cell;

    cell.taskChartStore.onSparkTaskStart({
      stageId: 2,
      taskId: 2,
      launchTime: 50,
      executorId: 'exec1',
      host: 'localhost'
    });

    cell.taskChartStore.onSparkTaskEnd({
      stageId: 2,
      taskId: 2,
      finishTime: 100,
      taskMetrics: {
        executorRunTime: 40
      },
      taskType: 'ResultTask'
    });
    
    expect(cell.taskChartStore.taskDataX.length).toBeGreaterThanOrEqual(1);
    expect(cell.taskChartStore.taskDataY.length).toBeGreaterThanOrEqual(1);
  });

  it('computes logic for SparkJob correctly', () => {
    const job = new SparkJob(nbStore);
    job.uniqueId = 'test-job-uniq';
    job.status = 'COMPLETED';
    nbStore.stages['test-stage-1'] = { status: 'PENDING' } as any;
    job.uniqueStageIds = ['test-stage-1'];

    // without stage initialized, numActiveStages shouldn't crash
    expect(job.numActiveStages).toBe(1);
    expect(job.numFailedStages).toBe(0);
    expect(job.numCompletedStages).toBe(0);
  });
});
