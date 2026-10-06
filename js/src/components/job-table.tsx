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

import prettyMilliseconds from 'pretty-ms';
import { useCellStore, useNotebookStore } from '../store';
import { ErrorBoundary } from './error-boundary';
import { ProgressBar } from './progress-bar';

const EMPTY_VALUE = '\u2014'; // em dash

/**
 * True when the date is a real timestamp. Rejects undefined, Invalid Date and
 * the Unix epoch, which is what an absent `submissionTime` decodes to.
 */
export function isValidStartTime(date: Date | undefined): date is Date {
  return !!date && date.getTime() > 0;
}

/**
 * Formats a job start time as a local wall-clock time, e.g. "3:36:03 PM"
 * (or "15:36:03" in 24-hour locales). The timestamp arrives from the driver
 * as epoch milliseconds, so Intl renders it in the browser's time zone.
 */
export function formatStartTime(date: Date | undefined): string {
  if (!isValidStartTime(date)) {
    return EMPTY_VALUE;
  }
  return date.toLocaleTimeString(undefined, {
    hour: 'numeric',
    minute: '2-digit',
    second: '2-digit',
  });
}

/** Full date and time for the Start Time tooltip, e.g. "Oct 6, 2026, 3:36:03 PM". */
export function formatStartTimestamp(date: Date | undefined): string {
  if (!isValidStartTime(date)) {
    return '';
  }
  return date.toLocaleString(undefined, {
    dateStyle: 'medium',
    timeStyle: 'medium',
  });
}

const JobItem = observer((props: { jobId: string }) => {
  const notebook = useNotebookStore();
  const job = notebook?.jobs[props.jobId];
  if (!job) {
    return null;
  }
  const durationMs =
    job.endTime && isValidStartTime(job.startTime)
      ? job.endTime.getTime() - job.startTime.getTime()
      : NaN;
  return (
    <tr className="jobrow">
      <td className="tdjobname">{job.name ? job.name : 'Unnamed'}</td>
      <td className="tdjobstarttime" title={formatStartTimestamp(job.startTime)}>
        {formatStartTime(job.startTime)}
      </td>
      <td className="tdjobstatus">
        <span className={'tditemjobstatus ' + job.status}>
          {job.status
            ? String(job.status).charAt(0).toUpperCase() +
              String(job.status).slice(1).toLowerCase()
            : 'Unknown'}
        </span>
      </td>
      <td className="tdtasks">
        <ProgressBar
          total={job.numTasks}
          running={job.numActiveTasks}
          completed={job.numCompletedTasks}
        />
      </td>
      <td className="tdjobduration">
        {Number.isNaN(durationMs) ? '-' : prettyMilliseconds(durationMs)}
      </td>
    </tr>
  );
});

export const JobTable = observer(() => {
  const cell = useCellStore();

  return (
    <ErrorBoundary>
      <div className="tabcontent">
        <table className="jobtable">
          <thead>
            <tr>
              <th className="thjobname">Job Name</th>
              <th className="thjobstart">Start Time</th>
              <th className="thjobstatus">Status</th>
              <th className="thjobtasks">Tasks</th>
              <th className="thjobtime">Duration</th>
            </tr>
          </thead>
          <tbody className="jobtablebody">
            {cell.uniqueJobIds.map((jobId) => (
              <JobItem jobId={jobId} key={jobId} />
            ))}
          </tbody>
        </table>
      </div>
    </ErrorBoundary>
  );
});
