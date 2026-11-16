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

import { makeAutoObservable, observable } from 'mobx';
import type { NotebookStore } from './notebook';

export interface RunningTasksSnapshot {
  slot_width_ms: number;
  start_time_ms: number;
  values: number[];
}

export interface OperationJobMarker {
  time: number;
  text: string;
}

type StepPoint = {
  time: number;
  value: number;
  syntheticEnd?: boolean;
};

export class TaskChartStore {
  jobDataX: Array<number> = [];
  jobDataY: Array<number> = [];
  jobDataText: Array<string> = [];
  executorDataX: Array<number> = [];
  executorDataY: Array<number> = [];
  taskDataX: Array<number> = [];
  taskDataY: Array<number> = [];
  numActiveTasks = 0;

  private operationSeries = new Map<string, StepPoint[]>();
  private operationJobMarkers = new Map<string, OperationJobMarker[]>();

  constructor(private notebookStore: NotebookStore) {
    makeAutoObservable<
      TaskChartStore,
      'notebookStore' | 'operationSeries' | 'operationJobMarkers'
    >(this, {
      notebookStore: false,
      operationSeries: false,
      operationJobMarkers: false,
      jobDataX: observable.shallow,
      jobDataY: observable.shallow,
      jobDataText: observable.shallow,
      executorDataX: observable.shallow,
      executorDataY: observable.shallow,
      taskDataX: observable.shallow,
      taskDataY: observable.shallow,
    });
  }

  reset() {
    this.jobDataX = [];
    this.jobDataY = [];
    this.jobDataText = [];
    this.executorDataX = [];
    this.executorDataY = [];
    this.taskDataX = [];
    this.taskDataY = [];
    this.numActiveTasks = 0;
    this.operationSeries.clear();
    this.operationJobMarkers.clear();
  }

  removeOperation(operationId: string) {
    const hadSeries = this.operationSeries.delete(operationId);
    const hadMarkers = this.operationJobMarkers.delete(operationId);
    if (hadSeries || hadMarkers) {
      this.rebuildFromOperations();
    }
  }

  getOperationMaxTime(operationId: string): number {
    const pts = this.operationSeries.get(operationId);
    if (!pts || pts.length === 0) {
      return Number.NaN;
    }
    return pts[pts.length - 1].time;
  }

  /**
   * Replaces `operationId`'s RunningTasks step series and job boundary markers,
   * then recomputes the merged cell-level chart series across all operations in
   * the cell. When `closeOutAt` is provided (terminal operation), ensures the
   * operation's running-task contribution drops to 0.
   */
  updateOperationSeries(
    operationId: string,
    runningTasks: RunningTasksSnapshot | null | undefined,
    jobMarkers: OperationJobMarker[],
    options: {
      replaceSeries: boolean;
      closeOutAt?: number;
    },
  ) {
    this.operationJobMarkers.set(operationId, jobMarkers);

    const isRunning = options.closeOutAt === undefined;
    let points: StepPoint[] | undefined;
    if (options.replaceSeries) {
      points = runningTasks
        ? this.decodeRunningTasks(runningTasks, isRunning)
        : [];
    } else {
      const existing = this.operationSeries.get(operationId);
      points = existing
        ? existing
            .filter((p) => isRunning || !p.syntheticEnd)
            .map((p) => ({ ...p }))
        : [];
    }

    if (options.closeOutAt !== undefined && points.length > 0) {
      const last = points[points.length - 1];
      if (last.value > 0) {
        const end = Number.isFinite(options.closeOutAt)
          ? Math.max(options.closeOutAt, last.time)
          : last.time;
        points.push({ time: end, value: 0 });
      }
    }

    this.operationSeries.set(operationId, points);
    this.rebuildFromOperations();
  }

  private decodeRunningTasks(
    rt: RunningTasksSnapshot,
    extendLatestSlot: boolean,
  ): StepPoint[] {
    const slotWidth = Number(rt.slot_width_ms);
    const rawStart =
      rt.start_time_ms ?? (rt as unknown as { start_time?: number }).start_time;
    const startTime = Number(rawStart);
    const values = Array.isArray(rt.values) ? rt.values : [];

    if (
      !Number.isFinite(slotWidth) ||
      slotWidth <= 0 ||
      !Number.isFinite(startTime) ||
      values.length === 0
    ) {
      return [];
    }

    const points: StepPoint[] = [];
    for (let i = 0; i < values.length; i++) {
      const val = Math.max(0, Number(values[i]) || 0);
      if (points.length === 0 || points[points.length - 1].value !== val) {
        points.push({ time: startTime + i * slotWidth, value: val });
      }
    }

    if (extendLatestSlot && points.length > 0) {
      const last = points[points.length - 1];
      const slotEnd = startTime + values.length * slotWidth;
      if (last.value > 0 && last.time < slotEnd) {
        points.push({ time: slotEnd, value: last.value, syntheticEnd: true });
      }
    }

    return points;
  }

  private rebuildFromOperations() {
    type TaggedEvent = {
      time: number;
      opOrder: number;
      seq: number;
      opId: string;
      value: number;
    };

    const events: TaggedEvent[] = [];
    let opOrder = 0;
    this.operationSeries.forEach((points, opId) => {
      for (let i = 0; i < points.length; i++) {
        events.push({
          time: points[i].time,
          opOrder,
          seq: i,
          opId,
          value: points[i].value,
        });
      }
      opOrder++;
    });

    events.sort(
      (a, b) => a.time - b.time || a.opOrder - b.opOrder || a.seq - b.seq,
    );

    const currentByOp = new Map<string, number>();
    let runningTotal = 0;
    const newTaskX: number[] = [];
    const newTaskY: number[] = [];

    let idx = 0;
    while (idx < events.length) {
      const t = events[idx].time;
      const seenOpsAtTime = new Set<string>();
      while (idx < events.length && events[idx].time === t) {
        const ev = events[idx];
        if (seenOpsAtTime.has(ev.opId)) {
          // Same operation changed twice at identical timestamp (e.g. clamped
          // spike -> closeOut): flush intermediate point before overwriting.
          newTaskX.push(t);
          newTaskY.push(runningTotal);
          seenOpsAtTime.clear();
        }
        seenOpsAtTime.add(ev.opId);
        const prev = currentByOp.get(ev.opId) ?? 0;
        currentByOp.set(ev.opId, ev.value);
        runningTotal += ev.value - prev;
        idx++;
      }
      newTaskX.push(t);
      newTaskY.push(Math.max(0, runningTotal));
    }

    const cores = this.notebookStore.numTotalCores || 0;
    const newExecX = newTaskX.slice();
    const newExecY = new Array<number>(newTaskX.length).fill(cores);

    const allMarkers: OperationJobMarker[] = [];
    this.operationJobMarkers.forEach((markers) => {
      for (const m of markers) {
        if (Number.isFinite(m.time) && m.time > 0) {
          allMarkers.push(m);
        }
      }
    });
    allMarkers.sort((a, b) => a.time - b.time);

    this.taskDataX = newTaskX;
    this.taskDataY = newTaskY;
    this.executorDataX = newExecX;
    this.executorDataY = newExecY;
    this.jobDataX = allMarkers.map((m) => m.time);
    this.jobDataY = new Array<number>(allMarkers.length).fill(0);
    this.jobDataText = allMarkers.map((m) => m.text);
    this.numActiveTasks =
      newTaskY.length > 0 ? newTaskY[newTaskY.length - 1] : 0;
  }

  /**
   * Ensures the active-task line ends at zero once nothing is running any
   * more. A lost stage-completion event can leave the last plotted point above
   * zero; this draws the missing drop at `time` (never earlier than the last
   * plotted point, so the series stays monotonic).
   */
  closeOut(time: number) {
    const last = this.taskDataY.length - 1;
    if (last < 0 || this.taskDataY[last] === 0) {
      return;
    }
    const lastTime = this.taskDataX[last];
    const t = new Date(time).getTime();
    this.addTaskData(Number.isFinite(t) ? Math.max(t, lastTime) : lastTime, 0);
  }

  addExecutorData(time: number, numCores: number) {
    this.executorDataX.push(time);
    this.executorDataY.push(numCores);
  }

  /**
   * Returns `time` clamped so the series never goes backwards.
   *
   * The chart is drawn as a step line in insertion order. Timestamps come from
   * the Spark driver (job/stage events) and, with an older listener JAR, from
   * the browser clock (periodic task samples). Clock skew, delivery latency
   * and the 250ms sampling timer can therefore deliver a point whose time is
   * earlier than the previous one; plotting it as-is folds the line back on
   * itself. Clamping to the last plotted time keeps the step semantics ("state
   * changed no earlier than the previous point") and keeps the task and
   * executor series aligned index-for-index.
   */
  private monotonic(time: number | string | null | undefined): number {
    const last = this.taskDataX[this.taskDataX.length - 1];
    // `new Date(null)` is the epoch, not an invalid date, so a missing value
    // has to be rejected before parsing or it would plot at 1970.
    const t = time == null ? Number.NaN : new Date(time).getTime();
    if (!Number.isFinite(t)) {
      // A missing or unparsable timestamp must not poison the series; reuse
      // the last plotted time (or "now" for the very first point).
      return last ?? Date.now();
    }
    return last === undefined ? t : Math.max(t, last);
  }

  addTaskData(time: number, numTasks: number) {
    const t = this.monotonic(time);
    this.taskDataX.push(t);
    this.taskDataY.push(numTasks);
    this.addExecutorData(t, this.notebookStore.numTotalCores || 0);
  }

  onSparkJobStart(data: any) {
    const submissionTimestamp = this.monotonic(data.submissionTime);
    this.jobDataX.push(submissionTimestamp);
    this.jobDataY.push(0);
    this.jobDataText.push(`Job ${data.jobId} started`);

    this.addTaskData(submissionTimestamp, 0);
  }

  onSparkJobEnd(data: any) {
    const completionTime = this.monotonic(data.completionTime || Date.now());
    this.jobDataX.push(completionTime);
    this.jobDataY.push(0);
    this.jobDataText.push(`Job ${data.jobId} ended`);

    this.addTaskData(completionTime, 0);
  }

  onSparkStageActive(time: number, numActiveTasks: number) {
    this.numActiveTasks = numActiveTasks;
    this.addTaskData(time, numActiveTasks);
  }
}
