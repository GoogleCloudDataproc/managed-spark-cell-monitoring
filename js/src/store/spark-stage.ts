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

/**
 * Per-stage state. Today stages only feed the task totals of the job that
 * owns them (see NotebookStore); no view renders them since the Event
 * Timeline tab was removed. Their timing metadata is still recorded and
 * observable so a future timeline view only needs UI work.
 */
export class SparkStage {
  uniqueId!: string;
  uniqueJobId!: string;
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
  // Initialised explicitly so MobX observes them (see NotebookStore).
  submissionTime: Date | undefined = undefined;
  completionTime: Date | undefined = undefined;

  constructor() {
    makeAutoObservable(this);
  }
}
