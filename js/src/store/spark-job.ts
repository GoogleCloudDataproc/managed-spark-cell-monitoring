/*
 * Copyright 2026 Google LLC
 * Copyright 2017 CERN
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

import { makeAutoObservable } from 'mobx';

import type { Cell } from './cell';
import type { NotebookStore } from './notebook';

export class SparkJob {
  uniqueId!: string;
  cellId!: string;
  jobId!: string;
  status: 'RUNNING' | 'COMPLETED' | 'FAILED' = 'RUNNING';
  name = 'Unnamed';
  startTime!: Date;
  endTime?: Date;
  stageIds: string[] = [];
  uniqueStageIds: string[] = [];

  numStages = 0;

  numTasks = 0;
  numActiveTasks = 0;
  numCompletedTasks = 0;
  numFailedTasks = 0;

  cell?: Cell;

  get numActiveStages() {
    return this.uniqueStageIds.filter((stageId) => {
      return this.notebookStore.stages[stageId]?.status === 'PENDING';
    }).length;
  }

  get numCompletedStages() {
    return this.uniqueStageIds.filter((stageId) => {
      return this.notebookStore.stages[stageId]?.status === 'COMPLETED';
    }).length;
  }

  get numFailedStages() {
    return this.uniqueStageIds.filter((stageId) => {
      return this.notebookStore.stages[stageId]?.status === 'FAILED';
    }).length;
  }

  get numSkippedStages() {
    return this.uniqueStageIds.filter((stageId) => {
      return this.notebookStore.stages[stageId]?.status === 'SKIPPED';
    }).length;
  }

  constructor(private notebookStore: NotebookStore) {
    makeAutoObservable(this);
  }
}
