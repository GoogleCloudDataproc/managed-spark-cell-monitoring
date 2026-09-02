/**
 * @license
 * Copyright 2025 Google LLC
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
