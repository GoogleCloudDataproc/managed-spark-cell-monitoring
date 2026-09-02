/**
 * @license
 * Copyright 2025 Google LLC
 */

import { makeAutoObservable } from 'mobx';

export class SparkStage {
  uniqueId!: string;
  uniqueJobId!: string;
  cellId!: string;
  stageId!: string;
  status!:
    | 'Unknown'
    | 'COMPLETED'
    | 'FAILED'
    | 'RUNNING'
    | 'PENDING'
    | 'SKIPPED';
  name!: string;

  numTasks!: number;
  numActiveTasks = 0;
  numCompletedTasks = 0;
  numFailedTasks = 0;
  submissionTime!: Date;
  completionTime?: Date;

  constructor() {
    makeAutoObservable(this);
  }
}
