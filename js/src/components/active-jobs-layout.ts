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

/**
 * Width budgeting for the header's active-job strip.
 *
 * The strip only gets the space left over between the left title group and
 * the right-hand buttons and always stays on one line. When space runs out,
 * parts of the running-jobs summary are dropped in this order: task count,
 * task progress bar, then the completed-jobs counter. The spinner and name
 * always remain (the name truncates with an ellipsis).
 *
 * Widths below are minimums derived from the CSS in header.css.
 */

/**
 * Spinner (16) + name (min 80) + jobs-done counter (~60) + divider (1) +
 * bar (140) + "1234/5678 (123 running)" count (~150) + 5 gaps (10 each).
 */
export const FULL_SUMMARY_MIN_WIDTH_PX = 500;

/** Same as full, without the task count text. */
export const NO_TASK_COUNT_MIN_WIDTH_PX = 340;

/** Spinner + name + jobs-done counter only. */
export const JOBS_DONE_MIN_WIDTH_PX = 190;

/** Minimum width to show " · x/y tasks" on a single failed job. */
export const FAILED_DETAIL_MIN_WIDTH_PX = 200;

export type RunningSummaryParts = {
  showJobsDone: boolean;
  showBar: boolean;
  showTaskCount: boolean;
};

/** Picks which parts of the running-jobs summary fit in `width` px. */
export function computeRunningSummaryParts(width: number): RunningSummaryParts {
  return {
    showJobsDone: width >= JOBS_DONE_MIN_WIDTH_PX,
    showBar: width >= NO_TASK_COUNT_MIN_WIDTH_PX,
    showTaskCount: width >= FULL_SUMMARY_MIN_WIDTH_PX,
  };
}
