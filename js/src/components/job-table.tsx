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
import TimeAgo from 'react-timeago';

import prettyMilliseconds from 'pretty-ms';
import { useCellStore, useNotebookStore } from '../store';
import { ErrorBoundary } from './error-boundary';
import { ProgressBar } from './progress-bar';

const StageItem = observer((props: { stageId: string }) => {
  const notebook = useNotebookStore();
  const stage = notebook.stages[props.stageId];
  if (!stage) {
    return null;
  }
  return (
    <tr className="stagerow">
      <td className="tdstageid">{stage.stageId}</td>
      <td className="tdstagename">
        {stage.name
          ? String(stage.name).charAt(0).toUpperCase() + String(stage.name).slice(1).toLowerCase()
          : 'Unnamed'}
      </td>
      <td className="tdstagestatus">
        <span className={stage.status}>
          {stage.status
            ? String(stage.status).charAt(0).toUpperCase() +
              String(stage.status).slice(1).toLowerCase()
            : 'Unknown'}
        </span>
      </td>
      <td className="tdtasks">
        <ProgressBar
          total={stage.numTasks}
          running={stage.numActiveTasks}
          completed={stage.numCompletedTasks}
        />
      </td>
      <td className="tdstagestarttime">
        <TimeAgo date={stage.submissionTime} minPeriod={10} />
      </td>
      <td className="tdstageduration">
        {stage.completionTime &&
        !isNaN(stage.completionTime.getTime() - stage.submissionTime.getTime())
          ? prettyMilliseconds(stage.completionTime.getTime() - stage.submissionTime.getTime())
          : '-'}
      </td>
    </tr>
  );
});

const StageTable = observer((props: { jobId: string }) => {
  const notebook = useNotebookStore();
  const stageIds = notebook.jobs[props.jobId].uniqueStageIds;
  const rows = stageIds.map((stageId) => {
    return <StageItem stageId={stageId} key={stageId} />;
  });
  return (
    <table className="stagetable">
      <thead>
        <tr>
          <th className="thstageid">Stage ID</th>
          <th className="thstagename">Stage Name</th>
          <th className="thstagestatus">Status</th>
          <th className="thstagetasks">Tasks</th>
          <th className="thstagestart">Submission Time</th>
          <th className="thstageduration">Duration</th>
        </tr>
      </thead>
      <tbody>{rows}</tbody>
    </table>
  );
});

const JobItem = observer((props: { jobId: string }) => {
  const notebook = useNotebookStore();
  const job = notebook?.jobs[props.jobId];
  const [stagesCollapsed, setStageTableCollapsed] = React.useState(true);
  const onClickCollapseStageTable = () => {
    setStageTableCollapsed((value) => !value);
  };
  if (!job) {
    return null;
  }
  return (
    <>
      <tr className="jobrow">
        <td className="tdstagebutton" onClick={onClickCollapseStageTable}>
          <span
            className={stagesCollapsed ? 'tdstageicon' : 'tdstageicon tdstageiconcollapsed'}
          ></span>
        </td>
        <td className="tdjobid">{job.jobId}</td>
        <td className="tdjobname">{job.name ? job.name : 'Unnamed'}</td>
        <td className="tdjobstatus">
          <span className={'tditemjobstatus ' + job.status}>
            {job.status
              ? String(job.status).charAt(0).toUpperCase() +
                String(job.status).slice(1).toLowerCase()
              : 'Unknown'}
          </span>
        </td>
        <td className="tdjobstages">
          {job.numCompletedStages}/{job.numStages}
        </td>
        <td className="tdtasks">
          <ProgressBar
            total={job.numTasks}
            running={job.numActiveTasks}
            completed={job.numCompletedTasks}
          />
        </td>
        <td className="tdjobstarttime">
          <TimeAgo date={job.startTime} minPeriod={10} />
        </td>
        <td className="tdjobduration">
          {job.endTime && !isNaN(job.endTime.getTime() - job.startTime.getTime())
            ? prettyMilliseconds(job.endTime.getTime() - job.startTime.getTime())
            : '-'}
        </td>
      </tr>
      {!stagesCollapsed && (
        <tr className="jobstagedatarow">
          <td colSpan={8} className="stagedata">
            <StageTable jobId={props.jobId} />
          </td>
        </tr>
      )}
    </>
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
              <th className="thbutton"></th>
              <th className="thjobid">Job ID</th>
              <th className="thjobname">Job Name</th>
              <th className="thjobstatus">Status</th>
              <th className="thjobstages">Stages</th>
              <th className="thjobtasks">Tasks</th>
              <th className="thjobstart">Submission Time</th>
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
