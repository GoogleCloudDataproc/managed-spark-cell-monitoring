/**
 * @license
 * Copyright 2025 Google LLC
 */

import * as React from 'react';
import {createRoot} from 'react-dom/client';
import '../style/jobtable.css';
import '../style/styles.css';
import '../style/timeline.css';
import '../style/task-chart.css';
import {CellWidget} from './components';
import {store} from './store';
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

    if (!notebookStore.cells[cellId]) {
      notebookStore.cells[cellId] = new Cell(cellId, notebookStore);
    }

    const handleCustomMessage = (msg: any) => {
      if (!msg) return;

      if (msg.type === 'spark_event' && msg.data) {
        routeSparkMessageToStore(msg.data, notebookStore, cellId);
      }
    };

    model.on('msg:custom', handleCustomMessage);

    return () => {
      model.send({ type: 'widget_unmount' });
      model.off('msg:custom', handleCustomMessage);
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
