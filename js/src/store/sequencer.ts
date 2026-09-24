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

export interface SequencedEvent {
  sequence: number;
  data: any;
  [key: string]: any;
}

export interface SequencerOptions {
  onProcessEvent: (data: any) => void;
  onRequestReplay: (fromSequence: number, toSequence?: number) => void;
  replayTimeoutMs?: number;
}

/**
 * Ensures Spark telemetry messages for a cell are processed in strict sequential order.
 * If message packets are dropped due to WebSocket stutters or latency, it queues future
 * messages and initiates an automatic replay request to the Python kernel.
 */
export class CellMessageSequencer {
  private lastProcessedSequence: number = 0;
  private pendingQueue: Map<number, SequencedEvent> = new Map();
  private missingSequences: Set<number> = new Set();
  private requestedSequences: Set<number> = new Set();
  private isRequestingReplay: boolean = false;
  private replayTimer: ReturnType<typeof setTimeout> | null = null;
  private readonly onProcessEvent: (data: any) => void;
  private readonly onRequestReplay: (fromSequence: number, toSequence?: number) => void;
  private readonly replayTimeoutMs: number;

  constructor(options: SequencerOptions) {
    this.onProcessEvent = options.onProcessEvent;
    this.onRequestReplay = options.onRequestReplay;
    this.replayTimeoutMs = options.replayTimeoutMs ?? 1000;
  }

  public getLastProcessedSequence(): number {
    return this.lastProcessedSequence;
  }

  public getPendingQueueSize(): number {
    return this.pendingQueue.size;
  }

  public getIsRequestingReplay(): boolean {
    return this.isRequestingReplay;
  }

  public getMissingSequencesCount(): number {
    return this.missingSequences.size;
  }

  /**
   * Ingests an incoming live event from Jupyter Comm.
   */
  public handleLiveEvent(event: SequencedEvent): void {
    if (typeof event.sequence !== 'number') {
      // Non-sequenced event: process directly
      if (event.data) {
        this.onProcessEvent(event.data);
      }
      return;
    }

    const seq = event.sequence;

    // Case 1: In order (and no pending items ahead of it waiting for an active grace window)
    if (seq === this.lastProcessedSequence + 1 && !this.isRequestingReplay) {
      this.processSingleEvent(event);
      this.drainContiguousQueue();
      return;
    }

    // Case 2: Older or duplicate sequence number
    if (seq <= this.lastProcessedSequence) {
      // If this sequence was previously skipped as a missing gap, process it once and ACK
      if (this.missingSequences.has(seq)) {
        this.missingSequences.delete(seq);
        this.requestedSequences.delete(seq);
        if (event.data) {
          this.onProcessEvent(event.data);
        }
      }
      return;
    }

    // Case 3: Gap detected (seq > lastProcessedSequence + 1)
    for (let s = this.lastProcessedSequence + 1; s < seq; s++) {
      if (!this.pendingQueue.has(s)) {
        this.missingSequences.add(s);
      }
    }
    this.pendingQueue.set(seq, event);
    this.triggerReplayRequest(this.lastProcessedSequence + 1, seq - 1);
  }

  /**
   * Ingests a replayed batch of historical events sent from the Python kernel.
   */
  public handleReplayEvents(events: SequencedEvent[]): void {
    this.clearReplayTimer();
    this.isRequestingReplay = false;

    // Sort ascending by sequence number
    const sorted = [...events].sort((a, b) => a.sequence - b.sequence);

    for (const event of sorted) {
      const seq = event.sequence;
      if (seq === this.lastProcessedSequence + 1) {
        this.processSingleEvent(event);
        this.drainContiguousQueue();
      } else if (this.missingSequences.has(seq)) {
        // Late replay for a previously skipped gap (or out-of-order replay batch item)
        if (seq < this.lastProcessedSequence) {
          this.missingSequences.delete(seq);
          this.requestedSequences.delete(seq);
          if (event.data) {
            this.onProcessEvent(event.data);
          }
        } else {
          this.pendingQueue.set(seq, event);
        }
      } else if (seq > this.lastProcessedSequence + 1) {
        this.pendingQueue.set(seq, event);
      }
    }

    this.drainContiguousQueue();

    // If there is still a gap remaining before the earliest pending item, request it
    if (this.pendingQueue.size > 0) {
      const keys = Array.from(this.pendingQueue.keys()).sort((a, b) => a - b);
      const earliest = keys[0];
      if (earliest > this.lastProcessedSequence + 1) {
        if (events.length > 0) {
          for (let s = this.lastProcessedSequence + 1; s < earliest; s++) {
            this.requestedSequences.delete(s);
          }
        }
        this.triggerReplayRequest(this.lastProcessedSequence + 1, earliest - 1);
      }
    }
  }

  /**
   * Resets the sequencer state (e.g. upon cell unmount or re-execution).
   */
  public reset(): void {
    this.clearReplayTimer();
    this.lastProcessedSequence = 0;
    this.pendingQueue.clear();
    this.missingSequences.clear();
    this.requestedSequences.clear();
    this.isRequestingReplay = false;
  }

  private processSingleEvent(event: SequencedEvent): void {
    this.missingSequences.delete(event.sequence);
    this.requestedSequences.delete(event.sequence);
    if (event.sequence > this.lastProcessedSequence) {
      this.lastProcessedSequence = event.sequence;
    }
    if (event.data) {
      this.onProcessEvent(event.data);
    }
  }

  private drainContiguousQueue(): void {
    while (this.pendingQueue.has(this.lastProcessedSequence + 1)) {
      const nextSeq = this.lastProcessedSequence + 1;
      const nextEvent = this.pendingQueue.get(nextSeq)!;
      this.pendingQueue.delete(nextSeq);
      this.processSingleEvent(nextEvent);
    }
  }

  /**
   * If the kernel's shell channel is busy executing a long-running cell and cannot
   * reply within replayTimeoutMs, flush the pending queue so live jobs/stages continue
   * updating in real time without head-of-line blocking. Missing sequences remain in
   * missingSequences and will be applied once when the queued replay_events arrives.
   */
  private flushPendingQueueOnTimeout(): void {
    if (this.pendingQueue.size === 0) return;
    const sortedKeys = Array.from(this.pendingQueue.keys()).sort((a, b) => a - b);
    for (const seq of sortedKeys) {
      for (let s = this.lastProcessedSequence + 1; s < seq; s++) {
        if (!this.pendingQueue.has(s)) {
          this.missingSequences.add(s);
        }
      }
      const ev = this.pendingQueue.get(seq)!;
      this.pendingQueue.delete(seq);
      this.processSingleEvent(ev);
    }
  }

  private triggerReplayRequest(fromSeq: number, toSeq?: number): void {
    if (this.isRequestingReplay) return;

    const endSeq = toSeq ?? fromSeq;
    let hasUnrequested = false;
    for (let s = fromSeq; s <= endSeq; s++) {
      if (!this.requestedSequences.has(s)) {
        hasUnrequested = true;
        this.requestedSequences.add(s);
      }
    }

    if (!hasUnrequested) {
      // Already requested this range once; flush pending items so UI doesn't block
      this.flushPendingQueueOnTimeout();
      return;
    }

    this.isRequestingReplay = true;
    this.onRequestReplay(fromSeq, toSeq);

    // Grace window: if kernel replies immediately, handleReplayEvents processes in order.
    // If kernel shell is busy running the cell, flush pendingQueue so live UI doesn't freeze,
    // without spamming duplicate request_history messages.
    this.clearReplayTimer();
    this.replayTimer = setTimeout(() => {
      this.isRequestingReplay = false;
      this.flushPendingQueueOnTimeout();
    }, this.replayTimeoutMs);
  }

  private clearReplayTimer(): void {
    if (this.replayTimer) {
      clearTimeout(this.replayTimer);
      this.replayTimer = null;
    }
  }
}
