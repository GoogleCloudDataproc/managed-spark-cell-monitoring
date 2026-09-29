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
import pathlib
import anywidget
import traitlets

# Maintained for Spark Connect which captures globals.
# Classic PySpark routes via jobGroup.
ACTIVE_WIDGET = None


class ManagedSparkCellWidget(anywidget.AnyWidget):
  """AnyWidget backend defining telemetry fields and syncing to standard Jupyter Comms."""

  _esm = pathlib.Path(__file__).parent / "static" / "widget.js"
  _css = pathlib.Path(__file__).parent / "static" / "widget.css"

  # State synchronization fields
  run_id = traitlets.Unicode("").tag(sync=True)
  session_id = traitlets.Unicode("").tag(sync=True)

  def __init__(self, run_id, session_id, max_history_events=50000, **kwargs):
    super(ManagedSparkCellWidget, self).__init__(
        run_id=run_id, session_id=session_id, **kwargs
    )
    global ACTIVE_WIDGET
    ACTIVE_WIDGET = self
    self.add_class("managed-spark-cell-widget")
    self.active_jobs_count = 0
    self.max_history_events = max_history_events
    self.event_history = collections.deque(maxlen=max_history_events)
    self.on_msg(self._handle_frontend_message)

  def cleanup(self, clear_history=False):
    global ACTIVE_WIDGET
    if ACTIVE_WIDGET is self:
      ACTIVE_WIDGET = None
    if clear_history:
      self.event_history.clear()

  def append_event(self, event, sequence):
    """Sync a new telemetry event packet to the frontend Backbone model."""
    self.event_history.append({"sequence": sequence, "data": event})
    # Send raw event instantly over high-speed custom messaging Comm channel
    self.send({"type": "spark_event", "data": event, "sequence": sequence})

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
