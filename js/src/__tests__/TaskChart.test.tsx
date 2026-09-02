import React from 'react';
import { render } from '@testing-library/react';
import TaskChart from '../components/task-chart';
import { NotebookStore } from '../store/notebook';
import { Cell } from '../store/cell';
import { NotebookStoreContext, CellStoreContext } from '../store';

jest.mock('react-plotly.js', () => ({
  __esModule: true,
  default: () => <div data-testid="plotly-mock" />
}));

describe('TaskChart Component', () => {
  it('renders gracefully', () => {
    const notebookStore = new NotebookStore('test-nb');
    const cellStore = new Cell('test-cell', notebookStore);

    render(
      <NotebookStoreContext.Provider value={notebookStore}>
        <CellStoreContext.Provider value={cellStore}>
          <TaskChart />
        </CellStoreContext.Provider>
      </NotebookStoreContext.Provider>
    );
  });
});
