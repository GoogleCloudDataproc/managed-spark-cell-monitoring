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
  private isRequestingReplay: boolean = false;
  private replayTimer: ReturnType<typeof setTimeout> | null = null;
  private readonly onProcessEvent: (data: any) => void;
  private readonly onRequestReplay: (fromSequence: number, toSequence?: number) => void;
  private readonly replayTimeoutMs: number;

  constructor(options: SequencerOptions) {
    this.onProcessEvent = options.onProcessEvent;
    this.onRequestReplay = options.onRequestReplay;
    this.replayTimeoutMs = options.replayTimeoutMs ?? 3000;
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

    // Case 1: In order
    if (seq === this.lastProcessedSequence + 1) {
      this.processSingleEvent(event);
      this.drainContiguousQueue();
      return;
    }

    // Case 2: Duplicate / Already processed
    if (seq <= this.lastProcessedSequence) {
      return;
    }

    // Case 3: Gap detected (seq > lastProcessedSequence + 1)
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
      if (event.sequence === this.lastProcessedSequence + 1) {
        this.processSingleEvent(event);
      } else if (event.sequence > this.lastProcessedSequence + 1) {
        this.pendingQueue.set(event.sequence, event);
      }
    }

    this.drainContiguousQueue();

    // If there is still a gap remaining before the earliest pending item, request it
    if (this.pendingQueue.size > 0) {
      const keys = Array.from(this.pendingQueue.keys()).sort((a, b) => a - b);
      const earliest = keys[0];
      if (earliest > this.lastProcessedSequence + 1) {
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
    this.isRequestingReplay = false;
  }

  private processSingleEvent(event: SequencedEvent): void {
    this.lastProcessedSequence = event.sequence;
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

  private triggerReplayRequest(fromSeq: number, toSeq?: number): void {
    if (this.isRequestingReplay) return;
    this.isRequestingReplay = true;

    this.onRequestReplay(fromSeq, toSeq);

    // Set up retry timer in case replay response packet is lost in transit
    this.clearReplayTimer();
    this.replayTimer = setTimeout(() => {
      this.isRequestingReplay = false;
      if (this.pendingQueue.size > 0) {
        const keys = Array.from(this.pendingQueue.keys()).sort((a, b) => a - b);
        const earliest = keys[0];
        if (earliest > this.lastProcessedSequence + 1) {
          this.triggerReplayRequest(this.lastProcessedSequence + 1, earliest - 1);
        }
      }
    }, this.replayTimeoutMs);
  }

  private clearReplayTimer(): void {
    if (this.replayTimer) {
      clearTimeout(this.replayTimer);
      this.replayTimer = null;
    }
  }
}
