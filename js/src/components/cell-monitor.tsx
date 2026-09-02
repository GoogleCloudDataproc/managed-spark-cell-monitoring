/**
 * @license
 * Copyright 2025 Google LLC
 */

import {observer} from 'mobx-react-lite';
import React from 'react';

import {useCellStore, useNotebookStore} from '../store';
import {CellMonitorHeader} from './header';
import {JobTable} from './job-table';
import {LazyTaskChart} from './lazy-task-chart';
import {LazyTimeline} from './lazy-timeline';

export const CellMonitor = observer(() => {
  const notebook = useNotebookStore();
  const cell = useCellStore();

  // If the cell has no spark job
  if (
    !cell ||
    cell?.uniqueJobIds?.length <= 0 ||
    cell.isRemoved ||
    notebook?.hideAllDisplays
  ) {
    return <div className="managedSparkCellMonitoringCellRoot" />;
  }

  let tabContent = <></>;
  if (cell?.view === 'jobs') {
    tabContent = <JobTable />;
  } else if (cell?.view === 'taskchart') {
    tabContent = <LazyTaskChart />;
  } else if (cell?.view === 'timeline') {
    tabContent = <LazyTimeline />;
  }

  return (
    <div className="managedSparkCellMonitoringCellRoot CellMonitor pm">
      <CellMonitorHeader />
      {!cell.isHeaderCollapsed && !cell.isCollapsed && (
        <div className="content">{tabContent}</div>
      )}
    </div>
  );
});
