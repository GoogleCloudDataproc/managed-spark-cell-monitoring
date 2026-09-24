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

import { CellMessageSequencer } from '../store/sequencer';

describe('CellMessageSequencer', () => {
  let processedEvents: any[];
  let replayRequests: Array<{ from: number; to?: number }>;
  let sequencer: CellMessageSequencer;

  beforeEach(() => {
    processedEvents = [];
    replayRequests = [];
    sequencer = new CellMessageSequencer({
      onProcessEvent: (data) => processedEvents.push(data),
      onRequestReplay: (from, to) => replayRequests.push({ from, to }),
      replayTimeoutMs: 1000,
    });
  });

  afterEach(() => {
    sequencer.reset();
  });

  it('processes live events in order', () => {
    sequencer.handleLiveEvent({ sequence: 1, data: { msgtype: 'sparkJobStart', id: 1 } });
    sequencer.handleLiveEvent({ sequence: 2, data: { msgtype: 'sparkJobEnd', id: 1 } });
    sequencer.handleLiveEvent({ sequence: 3, data: { msgtype: 'sparkJobStart', id: 2 } });

    expect(sequencer.getLastProcessedSequence()).toBe(3);
    expect(processedEvents).toHaveLength(3);
    expect(processedEvents[0]).toEqual({ msgtype: 'sparkJobStart', id: 1 });
    expect(processedEvents[1]).toEqual({ msgtype: 'sparkJobEnd', id: 1 });
    expect(processedEvents[2]).toEqual({ msgtype: 'sparkJobStart', id: 2 });
    expect(replayRequests).toHaveLength(0);
    expect(sequencer.getPendingQueueSize()).toBe(0);
  });

  it('ignores duplicate or older sequence numbers', () => {
    sequencer.handleLiveEvent({ sequence: 1, data: { val: 'first' } });
    sequencer.handleLiveEvent({ sequence: 2, data: { val: 'second' } });
    // Duplicate
    sequencer.handleLiveEvent({ sequence: 2, data: { val: 'second-dup' } });
    sequencer.handleLiveEvent({ sequence: 1, data: { val: 'first-dup' } });

    expect(sequencer.getLastProcessedSequence()).toBe(2);
    expect(processedEvents).toHaveLength(2);
    expect(replayRequests).toHaveLength(0);
  });

  it('detects sequence gaps, buffers future messages, and requests replay', () => {
    sequencer.handleLiveEvent({ sequence: 1, data: { val: 1 } });
    sequencer.handleLiveEvent({ sequence: 2, data: { val: 2 } });

    // Messages 3 and 4 are dropped, message 5 arrives
    sequencer.handleLiveEvent({ sequence: 5, data: { val: 5 } });

    expect(sequencer.getLastProcessedSequence()).toBe(2);
    expect(processedEvents).toHaveLength(2);
    expect(sequencer.getPendingQueueSize()).toBe(1);
    expect(replayRequests).toEqual([{ from: 3, to: 4 }]);
    expect(sequencer.getIsRequestingReplay()).toBe(true);

    // Message 6 arrives while waiting for replay
    sequencer.handleLiveEvent({ sequence: 6, data: { val: 6 } });
    expect(sequencer.getPendingQueueSize()).toBe(2);
    // Should debounce: no duplicate request sent
    expect(replayRequests).toHaveLength(1);
  });

  it('drains pending queue contiguously after replay batch arrives', () => {
    sequencer.handleLiveEvent({ sequence: 1, data: { val: 1 } });
    sequencer.handleLiveEvent({ sequence: 4, data: { val: 4 } });
    sequencer.handleLiveEvent({ sequence: 5, data: { val: 5 } });

    expect(processedEvents).toHaveLength(1);
    expect(sequencer.getPendingQueueSize()).toBe(2);

    // Replay arrives with missing sequence 2 and 3 (out of order in batch)
    sequencer.handleReplayEvents([
      { sequence: 3, data: { val: 3 } },
      { sequence: 2, data: { val: 2 } },
    ]);

    expect(sequencer.getLastProcessedSequence()).toBe(5);
    expect(processedEvents.map((e) => e.val)).toEqual([1, 2, 3, 4, 5]);
    expect(sequencer.getPendingQueueSize()).toBe(0);
    expect(sequencer.getIsRequestingReplay()).toBe(false);
  });

  it('handles partial replay and re-requests remaining gap', () => {
    sequencer.handleLiveEvent({ sequence: 1, data: { val: 1 } });
    sequencer.handleLiveEvent({ sequence: 5, data: { val: 5 } });

    expect(replayRequests).toEqual([{ from: 2, to: 4 }]);

    // Server only replays sequence 2 (3 and 4 still missing)
    sequencer.handleReplayEvents([{ sequence: 2, data: { val: 2 } }]);

    expect(sequencer.getLastProcessedSequence()).toBe(2);
    expect(processedEvents.map((e) => e.val)).toEqual([1, 2]);
    expect(sequencer.getPendingQueueSize()).toBe(1); // sequence 5 is still pending

    // Should request next gap: from 3 to 4
    expect(replayRequests).toEqual([
      { from: 2, to: 4 },
      { from: 3, to: 4 },
    ]);
  });

  it('resets all state correctly', () => {
    sequencer.handleLiveEvent({ sequence: 1, data: { val: 1 } });
    sequencer.handleLiveEvent({ sequence: 4, data: { val: 4 } });

    expect(sequencer.getLastProcessedSequence()).toBe(1);
    expect(sequencer.getPendingQueueSize()).toBe(1);

    sequencer.reset();

    expect(sequencer.getLastProcessedSequence()).toBe(0);
    expect(sequencer.getPendingQueueSize()).toBe(0);
    expect(sequencer.getIsRequestingReplay()).toBe(false);

    // After reset, sequence 1 is accepted as new start
    sequencer.handleLiveEvent({ sequence: 1, data: { val: 'new-run' } });
    expect(sequencer.getLastProcessedSequence()).toBe(1);
    expect(processedEvents).toContainEqual({ val: 'new-run' });
  });

  it('passes non-sequenced events directly through', () => {
    sequencer.handleLiveEvent({ sequence: undefined as any, data: { msgtype: 'custom' } });
    expect(processedEvents).toEqual([{ msgtype: 'custom' }]);
    expect(sequencer.getLastProcessedSequence()).toBe(0);
  });

  it('flushes pending queue on timeout without spamming requests and processes late replay once', () => {
    jest.useFakeTimers();
    try {
      sequencer.handleLiveEvent({ sequence: 1, data: { val: 1 } });
      sequencer.handleLiveEvent({ sequence: 2, data: { val: 2 } });

      // Sequences 3 and 4 dropped; 5 and 6 arrive while kernel shell is busy
      sequencer.handleLiveEvent({ sequence: 5, data: { val: 5 } });
      sequencer.handleLiveEvent({ sequence: 6, data: { val: 6 } });

      expect(replayRequests).toEqual([{ from: 3, to: 4 }]);
      expect(processedEvents.map((e) => e.val)).toEqual([1, 2]);

      // Advance past grace timeout (1000ms): pendingQueue flushes so live UI never freezes
      jest.advanceTimersByTime(1000);

      expect(sequencer.getPendingQueueSize()).toBe(0);
      expect(sequencer.getLastProcessedSequence()).toBe(6);
      expect(processedEvents.map((e) => e.val)).toEqual([1, 2, 5, 6]);
      // Must NOT spam another request_history
      expect(replayRequests).toHaveLength(1);
      expect(sequencer.getMissingSequencesCount()).toBe(2);

      // Subsequent live event 7 flows directly without blocking
      sequencer.handleLiveEvent({ sequence: 7, data: { val: 7 } });
      expect(processedEvents.map((e) => e.val)).toEqual([1, 2, 5, 6, 7]);

      // Kernel finishes cell and delivers queued replay for 3 and 4
      sequencer.handleReplayEvents([
        { sequence: 3, data: { val: 3 } },
        { sequence: 4, data: { val: 4 } },
      ]);
      expect(processedEvents.map((e) => e.val)).toEqual([1, 2, 5, 6, 7, 3, 4]);
      expect(sequencer.getMissingSequencesCount()).toBe(0);

      // Duplicate replay is ignored (ACKed via missingSequences deletion)
      sequencer.handleReplayEvents([
        { sequence: 3, data: { val: '3-dup' } },
        { sequence: 4, data: { val: '4-dup' } },
      ]);
      expect(processedEvents.map((e) => e.val)).toEqual([1, 2, 5, 6, 7, 3, 4]);
    } finally {
      jest.useRealTimers();
    }
  });
});
