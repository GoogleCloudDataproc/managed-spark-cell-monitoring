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
"""Tests for Managed Spark Cell Monitoring widget backend."""

from unittest import mock

import managed_spark_cell_monitoring.widget as widget_module
from managed_spark_cell_monitoring.widget import ManagedSparkCellWidget


def test_widget_initialization():
  """Test that widget initializes with proper traits and global state."""
  widget = ManagedSparkCellWidget(run_id="run-123", session_id="sess-456")

  assert widget.run_id == "run-123"
  assert widget.session_id == "sess-456"
  assert widget.active_jobs_count == 0
  assert widget_module.ACTIVE_WIDGET is widget
  assert "managed-spark-cell-widget" in widget._dom_classes


def test_append_event_sends_sequence():
  """Test that append_event calls send with the sequence number payload."""
  widget = ManagedSparkCellWidget(run_id="run-1", session_id="sess-1")
  widget.send = mock.MagicMock()

  event_payload = {"some": "data"}
  widget.append_event(event_payload, sequence=5)

  widget.send.assert_called_once_with(
      {"type": "spark_event", "data": {"some": "data"}, "sequence": 5}
  )
  assert len(widget.event_history) == 1
  assert widget.event_history[0] == {"sequence": 5, "data": {"some": "data"}}


def test_append_event_respects_max_history():
  """Test that event_history is bounded by max_history_events."""
  widget = ManagedSparkCellWidget(
      run_id="run-1", session_id="sess-1", max_history_events=3
  )
  for i in range(1, 6):
    widget.append_event({"event_id": i}, sequence=i)

  assert len(widget.event_history) == 3
  assert [item["sequence"] for item in widget.event_history] == [3, 4, 5]


def test_handle_request_history():
  """Test that request_history comm message returns matching replay_events."""
  widget = ManagedSparkCellWidget(run_id="run-1", session_id="sess-1")
  widget.send = mock.MagicMock()

  for i in range(1, 6):
    widget.append_event({"data": f"msg-{i}"}, sequence=i)

  widget.send.reset_mock()

  # Request range from 2 to 4
  widget._handle_frontend_message(
      widget, {"type": "request_history", "from_sequence": 2, "to_sequence": 4}
  )

  widget.send.assert_called_once_with({
      "type": "replay_events",
      "from_sequence": 2,
      "events": [
          {"sequence": 2, "data": {"data": "msg-2"}},
          {"sequence": 3, "data": {"data": "msg-3"}},
          {"sequence": 4, "data": {"data": "msg-4"}},
      ],
  })


def test_cleanup_preserves_history_until_unmount():
  """Test that post-cell cleanup resets ACTIVE_WIDGET while preserving history until unmount."""
  widget = ManagedSparkCellWidget(run_id="run-1", session_id="sess-1")
  widget.append_event({"test": 1}, sequence=1)
  assert len(widget.event_history) == 1

  # Post-cell cleanup keeps history so queued request_history comm messages can be served
  widget.cleanup()
  assert len(widget.event_history) == 1
  assert widget_module.ACTIVE_WIDGET is None

  # Unmount clears history
  widget._handle_frontend_message(widget, {"type": "widget_unmount"})
  assert len(widget.event_history) == 0
