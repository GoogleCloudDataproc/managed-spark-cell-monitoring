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
