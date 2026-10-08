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

import { TaskChartStore } from './task-chart-store';
import type { NotebookStore } from './notebook';

export class Cell {
  view: 'jobs' | 'taskchart' | 'timeline' = 'jobs';
  isCollapsed = false;
  isHeaderCollapsed = true;
  isRemoved = false;
  /** True once the kernel reports that the IPython cell finished executing. */
  cellFinished = false;
  uniqueJobIds: Array<string> = [];
  taskChartStore: TaskChartStore;
  constructor(
    public cellId: string,
    private notebookStore: NotebookStore
  ) {
    makeAutoObservable(this);
    this.taskChartStore = new TaskChartStore(this.notebookStore);
  }

  reset() {
    this.view = 'jobs';
    this.isCollapsed = false;
    this.isHeaderCollapsed = true;
    this.isRemoved = false;
    this.cellFinished = false;
    this.uniqueJobIds = [];
    this.taskChartStore.reset();
  }

  setCellFinished(finished: boolean) {
    this.cellFinished = finished;
  }

  toggleCollapseCellDisplay() {
    this.isCollapsed = !this.isCollapsed;
  }

  toggleHeaderCollapse() {
    this.isHeaderCollapsed = !this.isHeaderCollapsed;
  }

  toggleHideCellDisplay() {
    this.isRemoved = !this.isRemoved;
  }

  setView(view: 'jobs' | 'taskchart' | 'timeline') {
    this.view = view;
    this.isCollapsed = false;
    this.isRemoved = false;
    this.isHeaderCollapsed = false;
  }

  get jobs() {
    return this.uniqueJobIds
      .map((id) => this.notebookStore.jobs[id])
      .filter((job): job is NonNullable<typeof job> => !!job);
  }

  get numActiveJobs() {
    return this.jobs.filter(job => job.status === 'RUNNING').length;
  }
  get numFailedJobs() {
    return this.jobs.filter(job => job.status === 'FAILED').length;
  }

  get numCompletedJobs() {
    return this.jobs.filter(job => job.status === 'COMPLETED').length;
  }

  get numTotalJobs() {
    return this.uniqueJobIds.length;
  }
}
