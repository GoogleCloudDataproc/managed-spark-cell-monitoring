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

import { autorun, isObservableProp } from 'mobx';
import { Cell } from '../store/cell';
import { NotebookStore } from '../store/notebook';
import { SparkJob } from '../store/spark-job';

describe('MobX Store Tests', () => {
  let nbStore: NotebookStore;

  beforeEach(() => {
    nbStore = new NotebookStore('test-nb');
    nbStore.cells['cell-1'] = new Cell('cell-1', nbStore);
  });

  it('initializes a Cell store correctly and tests view changes', () => {
    const cell = new Cell('cell-1', nbStore);
    expect(cell.cellId).toBe('cell-1');
    expect(cell.uniqueJobIds).toEqual([]);
    expect(cell.view).toBe('jobs');
    expect(cell.cellFinished).toBe(false);

    cell.setCellFinished(true);
    cell.reset();
    expect(cell.isRemoved).toBe(false);
    expect(cell.cellFinished).toBe(false);
  });

  it('tracks a job through its stages and aggregates task counts', () => {
    nbStore.onSparkStageSubmitted({
      msgtype: 'sparkStageSubmitted',
      stageId: 1,
      numTasks: 10,
    });
    nbStore.onSparkJobStart('cell-1', {
      msgtype: 'sparkJobStart',
      jobId: 1,
      name: 'Test Job at <console>:1',
      stageIds: [1, 2],
      stageInfos: { 1: { numTasks: 10, name: 'stage one' }, 2: { numTasks: 4, name: 'stage two' } },
      numTasks: 14,
      status: 'RUNNING',
      submissionTime: 1000,
    });

    const job = nbStore.jobs['test-nb-job-1'];
    expect(job.status).toBe('RUNNING');
    expect(job.name).toBe('Test Job');
    expect(job.uniqueStageIds).toEqual(['test-nb-stage-1', 'test-nb-stage-2']);
    expect(job.numTasks).toBe(14);
    expect(nbStore.cells['cell-1'].uniqueJobIds).toEqual(['test-nb-job-1']);

    nbStore.onSparkStageActive({
      msgtype: 'sparkStageActive',
      stageId: 1,
      numActiveTasks: 2,
      numCompletedTasks: 3,
      numFailedTasks: 1,
    });
    expect(job.numActiveTasks).toBe(2);
    expect(job.numCompletedTasks).toBe(3);
    expect(job.numFailedTasks).toBe(1);

    nbStore.onSparkStageCompleted({
      msgtype: 'sparkStageCompleted',
      stageId: 1,
      status: 'COMPLETED',
      numTasks: 10,
      numCompletedTasks: 9,
      numFailedTasks: 1,
      completionTime: 2000,
    });
    expect(nbStore.stages['test-nb-stage-1'].status).toBe('COMPLETED');
    expect(job.numActiveTasks).toBe(0);
    expect(job.numCompletedTasks).toBe(9);

    // Stage 2 was never submitted; at job end it is skipped and counts as done.
    nbStore.onSparkJobEnd({ jobId: 1, status: 'COMPLETED', completionTime: 3000 });
    expect(job.status).toBe('COMPLETED');
    expect(job.endTime?.getTime()).toBe(3000);
    expect(nbStore.stages['test-nb-stage-2'].status).toBe('SKIPPED');
    expect(job.numTasks).toBe(14);
    expect(job.numCompletedTasks).toBe(13);
  });

  it('ignores a replayed stage completion and a replayed job end', () => {
    nbStore.onSparkJobStart('cell-1', {
      jobId: 1,
      name: 'job',
      stageIds: [1],
      stageInfos: { 1: { numTasks: 5, name: 's' } },
      numTasks: 5,
      status: 'RUNNING',
      submissionTime: 1000,
    });
    const completed = {
      stageId: 1,
      status: 'COMPLETED',
      numTasks: 5,
      numCompletedTasks: 5,
      numFailedTasks: 0,
      completionTime: 2000,
    };
    nbStore.onSparkStageCompleted(completed);
    nbStore.onSparkStageActive({ stageId: 1, numActiveTasks: 3, numCompletedTasks: 1, numFailedTasks: 0 });
    nbStore.onSparkStageCompleted({ ...completed, numCompletedTasks: 1 });
    expect(nbStore.jobs['test-nb-job-1'].numCompletedTasks).toBe(5);

    nbStore.onSparkJobEnd({ jobId: 1, status: 'COMPLETED', completionTime: 3000 });
    nbStore.onSparkJobEnd({ jobId: 1, status: 'FAILED', completionTime: 4000 });
    expect(nbStore.jobs['test-nb-job-1'].status).toBe('COMPLETED');
    expect(nbStore.jobs['test-nb-job-1'].endTime?.getTime()).toBe(3000);
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

    it('keeps known counts when a payload carries none', () => {
      nbStore.onSparkExecutorAdded({ executorId: 'exec1', totalCores: 8, numExecutors: 2 });
      nbStore.onSparkJobStart('cell-1', { jobId: 9, name: 'Job', stageIds: [], status: 'RUNNING' });
      expect(nbStore.numExecutors).toBe(2);
      expect(nbStore.numTotalCores).toBe(8);

      nbStore.onSparkExecutorRemoved({ executorId: 'exec1' });
      expect(nbStore.numExecutors).toBe(1);
      expect(nbStore.numTotalCores).toBe(8);
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

  it('falls back to the last stage name when the job has no call site', () => {
    nbStore.onSparkJobStart('cell-1', {
      jobId: 7,
      name: 'null',
      stageIds: [3, 4],
      stageInfos: { 3: { numTasks: 1, name: 'first' }, 4: { numTasks: 1, name: 'collect at x' } },
      numTasks: 2,
      status: 'RUNNING',
      submissionTime: 1,
    });
    expect(nbStore.jobs['test-nb-job-7'].name).toBe('collect at x');
  });

  it('records application and executor information', () => {
    nbStore.onSparkApplicationStart({ appId: 'app-1', appAttemptId: '1' });
    expect(nbStore.applicationId).toBe('app-1');
    expect(nbStore.uniqueId).toBe('appapp-1-attempt1');

    nbStore.onSparkExecutorAdded({ executorId: 'exec1', totalCores: 4 });
    nbStore.onSparkExecutorAdded({ executorId: 'exec2', totalCores: 8 });
    expect(nbStore.numExecutors).toBe(2);
    expect(nbStore.numTotalCores).toBe(8);

    nbStore.onSparkExecutorRemoved({ executorId: 'exec1', totalCores: 4 });
    expect(nbStore.numExecutors).toBe(1);
    expect(nbStore.numTotalCores).toBe(4);
  });

  it('feeds the task chart from stage updates', () => {
    const cell = nbStore.cells['cell-1'];
    nbStore.onSparkJobStart('cell-1', {
      jobId: 1,
      name: 'job',
      stageIds: [1],
      stageInfos: { 1: { numTasks: 5, name: 's' } },
      numTasks: 5,
      status: 'RUNNING',
      submissionTime: 1000,
    });
    nbStore.onSparkStageSubmitted({ stageId: 1, numTasks: 5 });
    nbStore.onSparkStageActive({ stageId: 1, numActiveTasks: 3, numCompletedTasks: 0, numFailedTasks: 0, time: 1500 });

    expect(cell.taskChartStore.taskDataX).toContain(1500);
    expect(cell.taskChartStore.taskDataY[cell.taskChartStore.taskDataY.length - 1]).toBe(3);
  });

  it('makes optional job and notebook fields observable', () => {
    const job = new SparkJob();
    expect(isObservableProp(job, 'endTime')).toBe(true);
    expect(isObservableProp(nbStore, 'numExecutors')).toBe(true);

    const seen: Array<Date | undefined> = [];
    const dispose = autorun(() => seen.push(job.endTime));
    job.endTime = new Date(5);
    dispose();
    expect(seen).toHaveLength(2);
  });
});
