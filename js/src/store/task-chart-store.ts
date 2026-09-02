/**
 * @license
 * Copyright 2025 Google LLC
 */

import { NotebookStore } from './notebook';

export class TaskChartStore {
  jobDataX: Array<number> = [];
  jobDataY: Array<number> = [];
  jobDataText: Array<string> = [];
  executorDataX: Array<number> = [];
  executorDataY: Array<number> = [];
  taskDataX: Array<number> = [];
  taskDataY: Array<number> = [];
  numActiveTasks = 0;

  constructor(private notebookStore: NotebookStore) {}
  reset() {
    this.jobDataX = [];
    this.jobDataY = [];
    this.jobDataText = [];
    this.executorDataX = [];
    this.executorDataY = [];
    this.taskDataX = [];
    this.taskDataY = [];
    this.numActiveTasks = 0;
  }

  addExecutorData(time: number, numCores: number) {
    this.executorDataX.push(time);
    this.executorDataY.push(numCores);
  }

  addTaskData(time: number, numTasks: number) {
    this.taskDataX.push(new Date(time).getTime());
    this.taskDataY.push(numTasks);
    this.addExecutorData(
      new Date(time).getTime(),
      this.notebookStore.numTotalCores || 0
    );
  }

  onSparkJobStart(data: any) {
    const submissionTimestamp = new Date(data.submissionTime).getTime();
    this.jobDataX.push(submissionTimestamp);
    this.jobDataY.push(0);
    this.jobDataText.push(`Job ${data.jobId} started`);

    this.addTaskData(submissionTimestamp, 0);
  }

  onSparkJobEnd(data: any) {
    const completionTime = new Date(
      data.completionTime || Date.now()
    ).getTime();
    this.jobDataX.push(completionTime);
    this.jobDataY.push(0);
    this.jobDataText.push(`Job ${data.jobId} ended`);

    this.addTaskData(completionTime, 0);
  }

  onSparkTaskStart(data: any) {
    this.addTaskData(data.launchTime, this.numActiveTasks);
    this.numActiveTasks += 1;
    this.addTaskData(data.launchTime, this.numActiveTasks);
  }

  onSparkTaskEnd(data: any) {
    this.addTaskData(data.finishTime, this.numActiveTasks);
    this.numActiveTasks = Math.max(0, this.numActiveTasks - 1);
    this.addTaskData(data.finishTime, this.numActiveTasks);
  }

  onSparkStageActive(time: number, numActiveTasks: number) {
    this.numActiveTasks = numActiveTasks;
    this.addTaskData(time, numActiveTasks);
  }

  loadGraphSnapshot(graph: any) {
    if (!graph) return;
    const newX: number[] = [];
    const newY: number[] = [];
    Object.values(graph).forEach((points: any) => {
      if (Array.isArray(points)) {
        points.forEach((pt: any) => {
          const t = new Date(pt.timestamp || pt.time || Date.now()).getTime();
          const activeTasks = pt.activeTasks ?? pt.numActiveTasks ?? 0;
          newX.push(t);
          newY.push(activeTasks);
        });
      }
    });
    if (newX.length > 0) {
      this.taskDataX = newX;
      this.taskDataY = newY;
      const numCores = this.notebookStore.numTotalCores || 0;
      this.executorDataX = newX.slice();
      this.executorDataY = newX.map(() => numCores);
    }
  }
}

