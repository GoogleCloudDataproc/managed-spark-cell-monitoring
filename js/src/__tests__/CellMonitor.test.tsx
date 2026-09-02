import React from 'react';
import { render, screen } from '@testing-library/react';
import { CellMonitor } from '../components/cell-monitor';
import { NotebookStore } from '../store/notebook';
import { Cell } from '../store/cell';
import { NotebookStoreContext, CellStoreContext } from '../store';
import { store } from '../store/index';

jest.mock('react-timeago', () => ({
  __esModule: true,
  default: () => <span data-testid="timeago-mock">TimeAgo Mock</span>
}));

jest.mock('pretty-ms', () => ({
  __esModule: true,
  default: () => "mock-time"
}));

describe('CellMonitor Component', () => {
  it('renders the header and defaults to the jobs table', () => {
    const notebookStore = new NotebookStore('test-nb');
    
    // Fill in dummy job so cell.jobs getter doesn't return undefined items
    notebookStore.jobs['test-nb-job-1'] = {
      status: 'RUNNING',
      name: 'Test Job',
      stageIds: [1, 2]
    } as any;

    const cellStore = new Cell('test-cell', notebookStore);
    cellStore.uniqueJobIds.push('test-nb-job-1');
    
    store.notebooks['test-nb'] = notebookStore;
    notebookStore.cells['test-cell'] = cellStore;

    render(
      <NotebookStoreContext.Provider value={notebookStore}>
        <CellStoreContext.Provider value={cellStore}>
          <CellMonitor />
        </CellStoreContext.Provider>
      </NotebookStoreContext.Provider>
    );
    
    expect(screen.getByText(/Jobs/i)).toBeInTheDocument();
  });
});
