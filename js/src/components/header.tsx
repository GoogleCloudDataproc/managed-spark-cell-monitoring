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

import {observer} from 'mobx-react-lite';
import React from 'react';
import {useCellStore, useNotebookStore} from '../store';

export const CellMonitorHeader = observer(() => {
  const notebook = useNotebookStore();
  const cell = useCellStore();

  const isButtonActive = (view: string) =>
    !cell.isCollapsed && cell.view === view ? 'tabbuttonactive' : '';
  const jobButtonClassNames =
    'jobtabletabbuttonicon tabbutton ' + isButtonActive('jobs');
  const tasksButtonClassNames =
    'taskviewtabbuttonicon tabbutton ' + isButtonActive('taskchart');
  const timelineButtonClassNames =
    'timelinetabbuttonicon tabbutton ' + isButtonActive('timeline');

  return (
    <div className="title">
      <div className="titleleft" onClick={() => cell.toggleHeaderCollapse()}>
        <span
          className={
            cell.isHeaderCollapsed
              ? 'tdstageicon'
              : 'tdstageicon tdstageiconcollapsed'
          }></span>
        <span className="tbitem badgecontainer">
          Apache Spark:
          <span className="badgesspan">
            <span className="badgeexecutor">
              <span className="badgeexecutorcount">
                {notebook.numExecutors}
              </span>{' '}
              Executors
            </span>
            <span className="badgeexecutorcores">
              <span className="badgeexecutorcorescount">
                {notebook.numTotalCores}
              </span>{' '}
              Cores
            </span>
          </span>
          <span className="jobstag">Jobs:</span>
          <span className="badges">
            {cell.numActiveJobs ? (
              <span className="badgerunning">
                <span className="badgerunningcount">{cell.numActiveJobs}</span>{' '}
                Running
              </span>
            ) : (
              ''
            )}
            {cell.numCompletedJobs ? (
              <span className="badgecompleted">
                <span className="badgecompletedcount">
                  {cell.numCompletedJobs}
                </span>{' '}
                Completed
              </span>
            ) : (
              ''
            )}
            {cell.numFailedJobs ? (
              <span className="badgefailed">
                <span className="badgefailedcount">{cell.numFailedJobs}</span>{' '}
                Failed
              </span>
            ) : (
              ''
            )}
          </span>
        </span>
      </div>
      <div className="titleright">
        <div className="tabbuttons">
          <span
            className={jobButtonClassNames}
            title="Jobs"
            onClick={() => {
              if (cell.view === 'jobs' && !cell.isCollapsed) {
                cell.toggleCollapseCellDisplay();
              } else {
                cell.setView('jobs');
              }
            }}
          />
          <span
            className={tasksButtonClassNames}
            title="Tasks"
            onClick={() => {
              if (cell.view === 'taskchart' && !cell.isCollapsed) {
                cell.toggleCollapseCellDisplay();
              } else {
                cell.setView('taskchart');
              }
            }}
          />
          <span
            className={timelineButtonClassNames}
            title="Event Timeline"
            onClick={() => {
              if (cell.view === 'timeline' && !cell.isCollapsed) {
                cell.toggleCollapseCellDisplay();
              } else {
                cell.setView('timeline');
              }
            }}
          />
          <span
            className="closebuttonicon tabbutton"
            title="Close Display"
            onClick={() => {
              cell.toggleHideCellDisplay();
            }}
          />
        </div>
      </div>
    </div>
  );
});
