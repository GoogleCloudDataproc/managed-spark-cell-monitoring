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

import type { OperationJobMarker, RunningTasksSnapshot } from './task-chart-store';

export type StepPoint = {
  time: number;
  value: number;
  syntheticEnd?: boolean;
};

export interface MergedOperationSeries {
  taskDataX: number[];
  taskDataY: number[];
  executorDataX: number[];
  executorDataY: number[];
  jobDataX: number[];
  jobDataY: number[];
  jobDataText: string[];
  numActiveTasks: number;
}

/**
 * Decodes a compact `RunningTasks` slot array into sparse step-change points.
 */
export function decodeRunningTasksSeries(
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

/**
 * Builds the updated step series for a single operation, either decoding a new
 * `RunningTasks` payload or closing out an existing series when terminal.
 */
export function prepareOperationSeriesPoints(
  existing: StepPoint[] | undefined,
  runningTasks: RunningTasksSnapshot | null | undefined,
  options: {
    replaceSeries: boolean;
    closeOutAt?: number;
  },
): StepPoint[] {
  const isRunning = options.closeOutAt === undefined;
  let points: StepPoint[];
  if (options.replaceSeries) {
    points = runningTasks
      ? decodeRunningTasksSeries(runningTasks, isRunning)
      : [];
  } else {
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

  return points;
}

/**
 * Sums step-function series and sorts job markers across all operations in a cell.
 */
export function mergeOperationSeries(
  operationSeries: ReadonlyMap<string, StepPoint[]>,
  operationJobMarkers: ReadonlyMap<string, OperationJobMarker[]>,
  numTotalCores: number,
): MergedOperationSeries {
  type TaggedEvent = {
    time: number;
    opOrder: number;
    seq: number;
    opId: string;
    value: number;
  };

  const events: TaggedEvent[] = [];
  let opOrder = 0;
  operationSeries.forEach((points, opId) => {
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
  const taskDataX: number[] = [];
  const taskDataY: number[] = [];

  let idx = 0;
  while (idx < events.length) {
    const t = events[idx].time;
    const seenOpsAtTime = new Set<string>();
    while (idx < events.length && events[idx].time === t) {
      const ev = events[idx];
      if (seenOpsAtTime.has(ev.opId)) {
        // Same operation changed twice at identical timestamp (e.g. clamped
        // spike -> closeOut): flush intermediate point before overwriting.
        taskDataX.push(t);
        taskDataY.push(runningTotal);
        seenOpsAtTime.clear();
      }
      seenOpsAtTime.add(ev.opId);
      const prev = currentByOp.get(ev.opId) ?? 0;
      currentByOp.set(ev.opId, ev.value);
      runningTotal += ev.value - prev;
      idx++;
    }
    taskDataX.push(t);
    taskDataY.push(Math.max(0, runningTotal));
  }

  const executorDataX = taskDataX.slice();
  const executorDataY = new Array<number>(taskDataX.length).fill(numTotalCores);

  const allMarkers: OperationJobMarker[] = [];
  operationJobMarkers.forEach((markers) => {
    for (const m of markers) {
      if (Number.isFinite(m.time) && m.time > 0) {
        allMarkers.push(m);
      }
    }
  });
  allMarkers.sort((a, b) => a.time - b.time);

  return {
    taskDataX,
    taskDataY,
    executorDataX,
    executorDataY,
    jobDataX: allMarkers.map((m) => m.time),
    jobDataY: new Array<number>(allMarkers.length).fill(0),
    jobDataText: allMarkers.map((m) => m.text),
    numActiveTasks: taskDataY.length > 0 ? taskDataY[taskDataY.length - 1] : 0,
  };
}
