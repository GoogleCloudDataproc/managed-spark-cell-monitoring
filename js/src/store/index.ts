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
import { makeAutoObservable } from 'mobx';
import { NotebookStore } from './notebook';
import type { Cell } from './cell';

class ManagedSparkCellMonitoringStore {
  notebooks: { [notebookId: string]: NotebookStore } = {};
  constructor() {
    makeAutoObservable(this);
  }
}

export const store = new ManagedSparkCellMonitoringStore();

const StoreContext = React.createContext(store);
// Use a non-null assertion here so that we can avoid an unnecessary check for null down the component hierarchy.
export const NotebookStoreContext = React.createContext<NotebookStore>(
  undefined!
);
export const CellStoreContext = React.createContext<Cell>(undefined!);

export const useStore = () => {
  return React.useContext(StoreContext);
};

export const useNotebookStore = () => {
  return React.useContext(NotebookStoreContext);
};

export const useCellStore = () => {
  return React.useContext(CellStoreContext);
};

export { CellMessageSequencer } from './sequencer';
export type { SequencedEvent, SequencerOptions } from './sequencer';
