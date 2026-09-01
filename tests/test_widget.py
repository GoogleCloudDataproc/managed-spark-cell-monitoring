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


def test_append_event_sends_sequence():
  """Test that append_event calls send with the sequence number payload."""
  widget = ManagedSparkCellWidget(run_id="run-1", session_id="sess-1")
  widget.send = mock.MagicMock()

  event_payload = {"some": "data"}
  widget.append_event(event_payload, sequence=5)

  widget.send.assert_called_once_with({"type": "spark_event", "data": {"some": "data"}, "sequence": 5})
