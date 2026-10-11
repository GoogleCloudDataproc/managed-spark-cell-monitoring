# -*- coding: utf-8 -*-
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Managed Spark Cell Monitoring AnyWidget Backend."""

import collections
import json
import pathlib
import anywidget
import traitlets

# Maintained for Spark Connect which captures globals.
# Classic PySpark routes via jobGroup.
ACTIVE_WIDGET = None


def _serialized_size(event):
  """Returns the JSON size in bytes of one event entry as it will be sent."""
  return len(
      json.dumps(event, separators=(",", ":"), default=str).encode("utf-8")
  )


def _chunk_by_size(events, max_bytes, max_events):
  """Splits events into consecutive chunks bounded by serialized size.

  Each chunk's serialized JSON stays under ``max_bytes`` (counting the
  per-element separators) and holds at most ``max_events`` entries. An entry
  that is larger than ``max_bytes`` on its own is placed in a chunk by itself:
  it cannot be split, and it already crossed the wire once as a live event.
  Order is preserved.
  """
  chunks = []
  current = []
  current_bytes = 0
  for event in events:
    size = _serialized_size(event) + 1  # +1 for the list separator
    if current and (
        current_bytes + size > max_bytes or len(current) >= max_events
    ):
      chunks.append(current)
      current = []
      current_bytes = 0
    current.append(event)
    current_bytes += size
  if current:
    chunks.append(current)
  return chunks


class ManagedSparkCellWidget(anywidget.AnyWidget):
  """AnyWidget backend defining telemetry fields and syncing to standard Jupyter Comms."""

  _esm = pathlib.Path(__file__).parent / "static" / "widget.js"
  _css = pathlib.Path(__file__).parent / "static" / "widget.css"

  # State synchronization fields
  run_id = traitlets.Unicode("").tag(sync=True)
  session_id = traitlets.Unicode("").tag(sync=True)
  spark_ui_url = traitlets.Unicode("").tag(sync=True)
  # Set by the kernel extension when the IPython cell finishes executing.
  # The frontend uses it to decide when to show the final job summary.
  cell_finished = traitlets.Bool(False).tag(sync=True)

  # Byte budget for the serialized events of one final_state message, so a
  # large cell is split across several messages instead of producing one
  # oversized websocket frame.
  FINAL_STATE_CHUNK_BYTES = 2 * 1024 * 1024
  # Secondary guard on the number of events per message.
  FINAL_STATE_CHUNK_EVENTS = 5000

  def __init__(
      self,
      run_id,
      session_id,
      spark_ui_url="",
      max_history_events=50000,
      **kwargs,
  ):
    super(ManagedSparkCellWidget, self).__init__(
        run_id=run_id,
        session_id=session_id,
        spark_ui_url=spark_ui_url,
        **kwargs,
    )
    global ACTIVE_WIDGET
    ACTIVE_WIDGET = self
    self.add_class("managed-spark-cell-widget")
    self.active_jobs_count = 0
    self.max_history_events = max_history_events
    self.event_history = collections.deque(maxlen=max_history_events)
    # Final job/stage events for this cell, keyed by job or stage id. Unlike
    # event_history this is bounded by the number of jobs and stages, not by
    # the event rate, so it survives long cells and is small enough to push in
    # one go when the cell completes (see send_final_state).
    self.final_job_starts = {}
    self.final_job_ends = {}
    self.final_stage_completions = {}
    self.final_state_sent = False
    self.on_msg(self._handle_frontend_message)

  def cleanup(self, clear_history=False):
    global ACTIVE_WIDGET
    if ACTIVE_WIDGET is self:
      ACTIVE_WIDGET = None
    if clear_history:
      self.event_history.clear()
      self.final_job_starts.clear()
      self.final_job_ends.clear()
      self.final_stage_completions.clear()

  def append_event(self, event, sequence):
    """Sync a new telemetry event packet to the frontend Backbone model."""
    self.event_history.append({"sequence": sequence, "data": event})
    self._record_final_event(event, sequence)
    # Send raw event instantly over high-speed custom messaging Comm channel
    self.send({"type": "spark_event", "data": event, "sequence": sequence})

  def _record_final_event(self, event, sequence):
    """Remembers the events that define a job's or stage's final state."""
    if not isinstance(event, dict):
      return
    msgtype = event.get("msgtype")
    entry = {"sequence": sequence, "data": event}
    if msgtype == "sparkJobStart" and "jobId" in event:
      self.final_job_starts[event["jobId"]] = entry
    elif msgtype == "sparkJobEnd" and "jobId" in event:
      self.final_job_ends[event["jobId"]] = entry
    elif msgtype == "sparkStageCompleted" and "stageId" in event:
      self.final_stage_completions[event["stageId"]] = entry

  def send_final_state(self):
    """Pushes the final state of every job and stage of this cell.

    Live events can be lost in transit (e.g. a websocket reconnect while a
    long cell runs) and the replay mechanism cannot repair them until the
    cell ends, by which time the history may have been evicted. This sends,
    once, the retained job-start/job-end and stage-completed events so the
    frontend can close out anything it still shows as running. The frontend
    applies them idempotently.

    Must only be called once the kernel knows no job of this cell is still
    running; the caller owns that decision.
    """
    if self.final_state_sent:
      return
    self.final_state_sent = True
    events = (
        list(self.final_job_starts.values())
        + list(self.final_job_ends.values())
        + list(self.final_stage_completions.values())
    )
    if not events:
      # Nothing ran in this cell: there is nothing to reconcile.
      return
    events.sort(key=lambda item: item["sequence"])
    chunks = _chunk_by_size(
        events, self.FINAL_STATE_CHUNK_BYTES, self.FINAL_STATE_CHUNK_EVENTS
    )
    for index, chunk in enumerate(chunks):
      self.send({
          "type": "final_state",
          "events": chunk,
          "chunk": index,
          "total_chunks": len(chunks),
      })

  def _handle_frontend_message(self, widget, content, buffers=None):
    """Handle custom comm messages sent from the frontend widget."""
    if not isinstance(content, dict):
      return
    msg_type = content.get("type")
    if msg_type == "request_history":
      from_seq = content.get("from_sequence", 1)
      to_seq = content.get("to_sequence")
      matching = [
          item for item in self.event_history
          if item["sequence"] >= from_seq
          and (to_seq is None or item["sequence"] <= to_seq)
      ]
      self.send({
          "type": "replay_events",
          "from_sequence": from_seq,
          "events": matching,
      })
    elif msg_type == "widget_unmount":
      self.cleanup(clear_history=True)
