/*
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
