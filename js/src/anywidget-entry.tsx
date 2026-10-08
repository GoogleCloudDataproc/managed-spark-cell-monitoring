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

import {runInAction} from 'mobx';
import {createRoot} from 'react-dom/client';
import '../style/jobtable.css';
import '../style/styles.css';
import '../style/header.css';
import '../style/task-chart.css';
import {CellWidget} from './components';
import {store, CellMessageSequencer} from './store';
import {Cell} from './store/cell';
import {NotebookStore} from './store/notebook';

function routeSparkMessageToStore(
  msg: any,
  notebookStore: NotebookStore,
  cellId: string,
) {
  if (!msg || !msg.msgtype) return;
  try {
    switch (msg.msgtype) {
      case 'sparkJobStart':
        notebookStore.onSparkJobStart(cellId, msg);
        break;
      case 'sparkJobEnd':
        notebookStore.onSparkJobEnd(msg);
        break;
      case 'sparkStageSubmitted':
        notebookStore.onSparkStageSubmitted(cellId, msg);
        break;
      case 'sparkStageCompleted':
        notebookStore.onSparkStageCompleted(msg);
        break;
      case 'sparkStageActive':
        notebookStore.onSparkStageActive(msg);
        break;
      case 'sparkApplicationStart':
        notebookStore.onSparkApplicationStart(msg);
        break;
      case 'sparkExecutorAdded':
        notebookStore.onSparkExecutorAdded(msg);
        break;
      case 'sparkExecutorRemoved':
        notebookStore.onSparkExecutorRemoved(msg);
        break;
      default:
        break;
    }
  } catch (err) {
    console.error(`[Managed Spark Cell Monitor ESM] Error routing event:`, err);
  }
}

export default {
  initialize({model}: {model: any}) {
    const runId = model.get('run_id');
    const sessionId = model.get('session_id');

    const cellId = runId;
    const notebookId = sessionId; // Map unique session UUID to prevent tab collisions

    // Ensure Global NotebookStore and Cell exist in MobX
    if (!store.notebooks[notebookId]) {
      store.notebooks[notebookId] = new NotebookStore(notebookId);
    }
    const notebookStore = store.notebooks[notebookId];

    const initialSparkUiUrl = model.get('spark_ui_url') as string | undefined;
    if (initialSparkUiUrl) {
      runInAction(() => {
        notebookStore.setViewUrl(initialSparkUiUrl);
      });
    }

    if (!notebookStore.cells[cellId]) {
      notebookStore.cells[cellId] = new Cell(cellId, notebookStore);
    }
    const cell = notebookStore.cells[cellId];

    const syncCellFinished = () => {
      runInAction(() => {
        cell.setCellFinished(Boolean(model.get('cell_finished')));
      });
    };
    syncCellFinished();

    const sequencer = new CellMessageSequencer({
      onProcessEvent: (eventData: any) => {
        routeSparkMessageToStore(eventData, notebookStore, cellId);
      },
      onRequestReplay: (fromSequence: number, toSequence?: number) => {
        model.send({
          type: 'request_history',
          from_sequence: fromSequence,
          to_sequence: toSequence,
        });
      },
    });

    const handleCustomMessage = (msg: any) => {
      if (!msg) return;

      if (msg.type === 'spark_event') {
        runInAction(() => {
          sequencer.handleLiveEvent(msg);
        });
      } else if (msg.type === 'replay_events' && Array.isArray(msg.events)) {
        runInAction(() => {
          sequencer.handleReplayEvents(msg.events);
        });
      }
    };

    const handleSparkUiUrlChange = () => {
      const updatedUrl = model.get('spark_ui_url') as string | undefined;
      runInAction(() => {
        notebookStore.setViewUrl(updatedUrl);
      });
    };

    model.on('msg:custom', handleCustomMessage);
    model.on('change:spark_ui_url', handleSparkUiUrlChange);
    model.on('change:cell_finished', syncCellFinished);

    return () => {
      sequencer.reset();
      model.send({ type: 'widget_unmount' });
      model.off('msg:custom', handleCustomMessage);
      model.off('change:spark_ui_url', handleSparkUiUrlChange);
      model.off('change:cell_finished', syncCellFinished);
    };
  },

  render({model, el}: {model: any; el: HTMLElement}) {
    const runId = model.get('run_id');
    const sessionId = model.get('session_id');

    const cellId = runId;
    const notebookId = sessionId;

    if (!store.notebooks[notebookId]) {
      store.notebooks[notebookId] = new NotebookStore(notebookId);
    }
    const notebookStore = store.notebooks[notebookId];

    if (!notebookStore.cells[cellId]) {
      notebookStore.cells[cellId] = new Cell(cellId, notebookStore);
    }

    // Tag root element so CSS in styles.css can scope transparent background-color
    // on VS Code's .cell-output-ipywidget-background wrapper in dark mode.
    // See: https://github.com/microsoft/vscode-jupyter/issues/9403
    el.classList.add('managed-spark-cell-widget');

    // Mount isolated React App inside standard DOM container passed by widget manager
    const container = document.createElement('div');
    el.appendChild(container);
    const root = createRoot(container);

    root.render(<CellWidget notebookId={notebookId} cellId={cellId} />);

    return () => {
      root.unmount();
    };
  },
};
