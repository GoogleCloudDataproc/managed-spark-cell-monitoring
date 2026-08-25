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
"""Tests for Managed Spark Cell Monitoring kernel extension."""

import os
import socket
from unittest import mock

from managed_spark_cell_monitoring import kernelextension


def test_start_server():
  """Test TCP server binds and starts socket thread."""
  mock_ipython = mock.MagicMock()
  extension = kernelextension.CellMonitorExtension(mock_ipython)

  with mock.patch.object(socket, "socket") as mock_socket, mock.patch.object(
      kernelextension, "SocketThread"
  ) as mock_thread, mock.patch.dict("os.environ", {}):

    # Setup mock socket
    mock_sock_inst = mock.MagicMock()
    mock_sock_inst.getsockname.return_value = ("127.0.0.1", 12345)
    mock_socket.return_value = mock_sock_inst

    extension.start_server()

    mock_socket.assert_called_once()
    mock_sock_inst.bind.assert_called_once_with(("localhost", 0))
    mock_sock_inst.listen.assert_called_once_with(5)
    assert os.environ["SPARK_CELL_MONITOR_KERNEL_PORT"] == "12345"
    mock_thread.assert_called_once()
    mock_thread.return_value.start.assert_called_once()


def test_pre_run_cell_hook():
  """Test widget creation and spark context patching on pre-run."""
  mock_ipython = mock.MagicMock()
  extension = kernelextension.CellMonitorExtension(mock_ipython)

  with mock.patch("IPython.display.display") as mock_display, mock.patch.dict(
      "sys.modules", {"pyspark": mock.MagicMock()}
  ):

    # Mock active SparkContext
    mock_sc = mock.MagicMock()
    import pyspark

    pyspark.SparkContext._active_spark_context = mock_sc

    extension.pre_run_cell_hook()

    assert extension.run_id is not None
    assert extension.run_id in extension.active_widgets
    mock_display.assert_called_once()
    mock_sc.setJobGroup.assert_called_once_with(
        extension.run_id, "ManagedSparkCellMonitoring cell tracking"
    )


def test_spark_job_start_routing():
  """Test that a sparkJobStart event maps jobId to run_id."""
  mock_ipython = mock.MagicMock()
  extension = kernelextension.CellMonitorExtension(mock_ipython)
  extension.run_id = "test-run-id"

  # Mock a widget in active_widgets
  mock_widget = mock.MagicMock()
  mock_widget.run_id = "test-run-id"
  mock_widget.active_jobs_count = 0
  extension.active_widgets["test-run-id"] = mock_widget

  # Send JobStart event with jobGroup
  msg = {"msgtype": "sparkJobStart", "jobId": 100, "jobGroup": "test-run-id"}
  extension.send_to_frontend(msg)

  assert extension.job_to_run_id[100] == "test-run-id"
  assert mock_widget.active_jobs_count == 1
  mock_widget.append_event.assert_called_once_with(msg, 1)


def test_spark_job_end_eviction():
  """Test memory leak fix: job_to_run_id is evicted on sparkJobEnd."""
  mock_ipython = mock.MagicMock()
  extension = kernelextension.CellMonitorExtension(mock_ipython)
  extension.run_id = "test-run-id"

  mock_widget = mock.MagicMock()
  mock_widget.run_id = "test-run-id"
  mock_widget.active_jobs_count = 1
  mock_widget.cell_finished = True
  extension.active_widgets["test-run-id"] = mock_widget

  # Setup job routing map
  extension.job_to_run_id[100] = "test-run-id"

  msg = {"msgtype": "sparkJobEnd", "jobId": 100}
  extension.send_to_frontend(msg)

  assert mock_widget.active_jobs_count == 0
  # Assert eviction (Memory leak fix)
  assert 100 not in extension.job_to_run_id
  # Assert widget cleanup because cell_finished is True and jobs = 0
  assert "test-run-id" not in extension.active_widgets


def test_post_run_cell_hook():
  """Test widget cleanup logic after a cell finishes."""
  mock_ipython = mock.MagicMock()
  extension = kernelextension.CellMonitorExtension(mock_ipython)
  extension.run_id = "test-run"

  mock_widget = mock.MagicMock()
  mock_widget.active_jobs_count = 0
  extension.active_widgets["test-run"] = mock_widget

  extension.post_run_cell_hook(None)

  assert mock_widget.cell_finished is True
  assert "test-run" not in extension.active_widgets


def test_patch_spark_context():
  """Test monkey-patching of SparkContext."""
  mock_extension = mock.MagicMock()
  mock_extension.run_id = "test-run"

  class DummySparkContext:

    def __init__(self):
      pass

    def setJobGroup(self, group_id, description):
      pass

  mock_pyspark = mock.MagicMock()
  mock_pyspark.SparkContext = DummySparkContext

  with mock.patch.dict("sys.modules", {"pyspark": mock_pyspark}):
    kernelextension._patch_spark_context(mock_extension)

    with mock.patch.object(DummySparkContext, "setJobGroup") as mock_set:
      _ = DummySparkContext()
      mock_set.assert_called_once_with(
          "test-run", "ManagedSparkCellMonitoring cell tracking"
      )


def test_load_ipython_extension():
  """Test entrypoint."""
  mock_ipython = mock.MagicMock()
  from ipykernel import zmqshell

  mock_ipython.__class__ = zmqshell.ZMQInteractiveShell

  with mock.patch.object(kernelextension, "CellMonitorExtension") as mock_ext:
    mock_instance = mock_ext.return_value
    kernelextension.load_ipython_extension(mock_ipython)

    mock_instance.start_server.assert_called_once()
    mock_ipython.events.register.assert_any_call(
        "pre_run_cell", mock_instance.pre_run_cell_hook
    )
    mock_ipython.events.register.assert_any_call(
        "post_run_cell", mock_instance.post_run_cell_hook
    )


def test_socket_reader():
  """Test custom socket reader parsing."""
  mock_client = mock.MagicMock()
  # Simulate receiving two messages split by ;EOD: and then closing
  mock_client.recv.side_effect = [
      b'{"msgtype": "test"};EOD:{"msgtype": "test2"};E',
      b"OD:",
      b"",
  ]
  mock_extension = mock.MagicMock()

  reader = kernelextension.SocketReader(mock_client, mock_extension)
  reader.run()

  assert mock_extension.send_to_frontend.call_count == 2
