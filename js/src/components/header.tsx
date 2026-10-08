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

import {observer} from 'mobx-react-lite';
import {useCellStore, useNotebookStore} from '../store';
import {ActiveJobs, useVisibleRunningJobs} from './active-jobs';
import {ConsoleLink} from './console-link';

type CellMonitorHeaderProps = {
  /**
   * Overrides the "View in Google Cloud" link URL. Defaults to the
   * notebook's Spark UI URL; the link is hidden when neither is set.
   */
  viewUrl?: string;
  viewLabel?: string;
};

const pluralize = (count: number, noun: string) =>
  `${count} ${count === 1 ? noun : `${noun}s`}`;

export const CellMonitorHeader = observer((props: CellMonitorHeaderProps) => {
  const notebook = useNotebookStore();
  const cell = useCellStore();
  const runningJobs = useVisibleRunningJobs(cell);

  // Executor counts only arrive from the Spark listener; Spark Connect
  // sessions never report them, so a missing count means "don't show".
  const numExecutors = notebook.numExecutors;
  const showResources =
    typeof numExecutors === 'number' &&
    Number.isFinite(numExecutors) &&
    runningJobs.length > 0;

  const isButtonActive = (view: string) =>
    !cell.isCollapsed && cell.view === view ? 'tabbuttonactive' : '';
  const jobButtonClassNames =
    'jobtabletabbuttonicon tabbutton ' + isButtonActive('jobs');
  const tasksButtonClassNames =
    'taskviewtabbuttonicon tabbutton ' + isButtonActive('taskchart');

  return (
    <div className="title">
      <div className="titleleft" onClick={() => cell.toggleHeaderCollapse()}>
        <span
          className={
            cell.isHeaderCollapsed
              ? 'tdstageicon'
              : 'tdstageicon tdstageiconcollapsed'
          }></span>
        <span className="tbitem badgecontainer">Job execution</span>
      </div>
      <ActiveJobs runningJobs={runningJobs} />
      <div className="titleright">
        {showResources && (
          <span className="header-resources">
            {pluralize(numExecutors, 'executor')} ·{' '}
            {pluralize(notebook.numTotalCores || 0, 'core')}
          </span>
        )}
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
          <ConsoleLink
            url={props.viewUrl ?? notebook.viewUrl}
            label={props.viewLabel}
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
