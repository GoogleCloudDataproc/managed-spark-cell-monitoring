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

  def __init__(self, run_id, session_id, **kwargs):
    super(ManagedSparkCellWidget, self).__init__(
        run_id=run_id, session_id=session_id, **kwargs
    )
    global ACTIVE_WIDGET
    ACTIVE_WIDGET = self
    self.add_class("managed-spark-cell-widget")
    self.active_jobs_count = 0

  def cleanup(self):
    global ACTIVE_WIDGET
    if ACTIVE_WIDGET is self:
      ACTIVE_WIDGET = None

  def append_event(self, event, sequence):
    """Sync a new telemetry event packet to the frontend Backbone model."""
    # Send raw event instantly over high-speed custom messaging Comm channel
    self.send({"type": "spark_event", "data": event, "sequence": sequence})
