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

import { observer } from 'mobx-react-lite';
import React from 'react';
import { useCellStore } from '../store';
import type { Cell } from '../store/cell';
import type { SparkJob } from '../store/spark-job';
import { computeRunningSummaryParts, FAILED_DETAIL_MIN_WIDTH_PX } from './active-jobs-layout';
import { StackedProgressBar } from './stacked-progress-bar';
import { useElementWidth } from './use-element-width';

/**
 * A running job must have been observed for this long before it is shown in
 * the header. Prevents bursts of sub-second jobs from flickering in and out.
 */
export const ACTIVE_JOB_REVEAL_DELAY_MS = 300;

const CHECK_CIRCLE_ICON_PATH =
  'M16.59 7.58 10 14.17l-3.59-3.58L5 12l5 5 8-8zM12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm0 18c-4.42 0-8-3.58-8-8s3.58-8 8-8 8 3.58 8 8-3.58 8-8 8z';
const ERROR_ICON_PATH =
  'M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-2h2v2zm0-4h-2V7h2v6z';

const startTimeMs = (job: SparkJob) => {
  const time = job.startTime?.getTime();
  return time !== undefined && Number.isFinite(time) ? time : Number.POSITIVE_INFINITY;
};

/** Oldest submission first; jobs without a start time go last, by job id. */
const compareOldestFirst = (a: SparkJob, b: SparkJob) => {
  const byStartTime = startTimeMs(a) - startTimeMs(b);
  if (byStartTime !== 0 && !Number.isNaN(byStartTime)) {
    return byStartTime;
  }
  return (Number(a.jobId) || 0) - (Number(b.jobId) || 0);
};

const jobNames = (jobs: SparkJob[]) => jobs.map((job) => job.name).join('\n');

/**
 * Returns the subset of `runningJobs` that has been running (as observed by
 * this component) for at least `delayMs`.
 */
function useRevealedJobs(runningJobs: SparkJob[], delayMs: number): SparkJob[] {
  const firstSeenMs = React.useRef(new Map<string, number>());
  const [, forceRender] = React.useReducer((count: number) => count + 1, 0);

  // Runs after every render: records newly seen jobs, forgets finished ones,
  // and schedules a re-render for the next job that becomes due.
  React.useEffect(() => {
    const now = Date.now();
    const seen = firstSeenMs.current;
    const runningIds = new Set(runningJobs.map((job) => job.uniqueId));
    Array.from(seen.keys()).forEach((id) => {
      if (!runningIds.has(id)) {
        seen.delete(id);
      }
    });

    let nextRevealInMs = Number.POSITIVE_INFINITY;
    runningIds.forEach((id) => {
      if (!seen.has(id)) {
        seen.set(id, now);
      }
      const remainingMs = (seen.get(id) as number) + delayMs - now;
      if (remainingMs > 0) {
        nextRevealInMs = Math.min(nextRevealInMs, remainingMs);
      }
    });

    if (nextRevealInMs === Number.POSITIVE_INFINITY) {
      return;
    }
    const timer = setTimeout(forceRender, nextRevealInMs);
    return () => clearTimeout(timer);
  });

  const now = Date.now();
  return runningJobs.filter((job) => {
    const firstSeen = firstSeenMs.current.get(job.uniqueId);
    return firstSeen !== undefined && now - firstSeen >= delayMs;
  });
}

/**
 * Running jobs of `cell` that have passed the reveal delay, oldest first.
 * Called once by the header so that the strip and the executors/cores text
 * agree on whether anything is running.
 */
export function useVisibleRunningJobs(cell: Cell): SparkJob[] {
  const runningJobs = cell.jobs.filter((job) => job.status === 'RUNNING');
  return useRevealedJobs(runningJobs, ACTIVE_JOB_REVEAL_DELAY_MS).sort(compareOldestFirst);
}

const Icon = (props: { path: string; size: number; className: string }) => (
  <svg
    className={props.className}
    width={props.size}
    height={props.size}
    viewBox="0 0 24 24"
    fill="currentColor"
    aria-hidden="true"
  >
    <path d={props.path} />
  </svg>
);

const Spinner = () => <span className="active-job-spinner" role="img" aria-label="Running" />;

/**
 * One summary item for all running jobs: spinner, name (the job's name, or
 * "N jobs"), completed-jobs counter, and one progress bar plus task count
 * summed across the running jobs.
 */
const RunningJobsSummary = observer(
  (props: { running: SparkJob[]; allJobs: SparkJob[]; width: number }) => {
    const { running, allJobs } = props;
    const parts = computeRunningSummaryParts(props.width);
    const sum = (pick: (job: SparkJob) => number) =>
      running.reduce((acc, job) => acc + (pick(job) || 0), 0);
    const total = sum((job) => job.numTasks);
    const completed = sum((job) => job.numCompletedTasks);
    const active = sum((job) => job.numActiveTasks);
    const failed = sum((job) => job.numFailedTasks);
    const completedJobs = allJobs.filter((job) => job.status === 'COMPLETED').length;

    const name = running.length === 1 ? running[0].name : `${running.length} jobs`;
    const jobsSummary = `Jobs: ${completedJobs}/${allJobs.length} completed, ${running.length} running`;
    const tasksSummary =
      (running.length === 1 ? `Tasks for ${running[0].name}` : 'Tasks across all running jobs') +
      `: ${completed}/${total} completed, ${active} running`;

    return (
      <span className="active-job">
        <Spinner />
        <span className="active-job-name" title={jobNames(running)}>
          {name}
        </span>
        {parts.showJobsDone && (
          <span className="active-jobs-done" role="img" aria-label={jobsSummary}>
            <Icon className="active-jobs-done-icon" path={CHECK_CIRCLE_ICON_PATH} size={16} />
            {completedJobs}/{allJobs.length}
          </span>
        )}
        {parts.showJobsDone && parts.showBar && (
          <span className="active-job-divider" aria-hidden="true" />
        )}
        {parts.showBar && (
          <StackedProgressBar
            className="active-job-progress"
            total={total}
            completed={completed}
            running={active}
            failed={failed}
            label={tasksSummary}
          />
        )}
        {parts.showTaskCount && (
          <span className="active-job-count">
            {completed}/{total} ({active} running)
          </span>
        )}
      </span>
    );
  },
);

/**
 * Shown when nothing is running: the failed job's name and task progress if
 * exactly one job failed, otherwise "N failed".
 */
const FailedJobsSummary = observer((props: { failed: SparkJob[]; width: number }) => {
  const { failed } = props;
  if (failed.length === 1) {
    const job = failed[0];
    return (
      <span className="active-job active-job--failed">
        <Icon className="active-job-failed-icon" path={ERROR_ICON_PATH} size={18} />
        <span className="active-job-name" title={job.name}>
          {job.name}
        </span>
        <span className="active-job-count">
          <span className="active-job-failed-text">Failed</span>
          {props.width >= FAILED_DETAIL_MIN_WIDTH_PX &&
            ` · ${job.numCompletedTasks || 0}/${job.numTasks || 0} tasks`}
        </span>
      </span>
    );
  }
  return (
    <span className="active-job active-job--failed" title={jobNames(failed)}>
      <Icon className="active-job-failed-icon" path={ERROR_ICON_PATH} size={18} />
      <span className="active-job-failed-text">{failed.length} failed</span>
    </span>
  );
});

/**
 * Live job strip shown in the header between the title and the view buttons.
 * While jobs run it shows one summary item for all of them; when nothing is
 * running it shows the cell's failed jobs, if any.
 *
 * Clicking anywhere in the strip toggles the header collapse, preserving
 * the behavior of the full-width left header area it replaces.
 */
export const ActiveJobs = observer((props: { runningJobs: SparkJob[] }) => {
  const cell = useCellStore();
  const [stripRef, stripWidth] = useElementWidth<HTMLDivElement>();
  const { runningJobs } = props;

  let content: React.ReactNode = null;
  if (runningJobs.length > 0) {
    content = <RunningJobsSummary running={runningJobs} allJobs={cell.jobs} width={stripWidth} />;
  } else if (cell.numActiveJobs === 0) {
    const failedJobs = cell.jobs.filter((job) => job.status === 'FAILED').sort(compareOldestFirst);
    if (failedJobs.length > 0) {
      content = <FailedJobsSummary failed={failedJobs} width={stripWidth} />;
    }
  }

  return (
    <div className="active-jobs" ref={stripRef} onClick={() => cell.toggleHeaderCollapse()}>
      {content}
    </div>
  );
});
