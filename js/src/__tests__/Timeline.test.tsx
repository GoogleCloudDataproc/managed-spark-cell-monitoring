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
