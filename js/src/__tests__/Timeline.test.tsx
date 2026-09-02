import React from 'react';
import { render } from '@testing-library/react';
import Timeline from '../components/timeline';
import { NotebookStore } from '../store/notebook';
import { Cell } from '../store/cell';
import { NotebookStoreContext, CellStoreContext } from '../store';

jest.mock('vis-timeline/standalone/umd/vis-timeline-graph2d.min.js', () => ({
  Timeline: jest.fn().mockImplementation(() => ({
    destroy: jest.fn(),
    setItems: jest.fn(),
    setGroups: jest.fn(),
    setOptions: jest.fn(),
    on: jest.fn()
  })),
  DataSet: jest.fn().mockImplementation(() => ({
    add: jest.fn(),
    update: jest.fn(),
    clear: jest.fn(),
    get: jest.fn().mockReturnValue([])
  }))
}));

describe('Timeline Component', () => {
  it('renders successfully without crashing', () => {
    const notebookStore = new NotebookStore('test-nb');
    const cellStore = new Cell('test-cell', notebookStore);

    const { container } = render(
      <NotebookStoreContext.Provider value={notebookStore}>
        <CellStoreContext.Provider value={cellStore}>
          <Timeline />
        </CellStoreContext.Provider>
      </NotebookStoreContext.Provider>
    );
    expect(container.firstChild).toBeInTheDocument();
  });
});
