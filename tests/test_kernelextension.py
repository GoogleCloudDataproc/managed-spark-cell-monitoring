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
import pytest

from managed_spark_cell_monitoring import kernelextension


@pytest.fixture
def monitor_extension():
  mock_ipython = mock.MagicMock()
  ext = kernelextension.CellMonitorExtension(mock_ipython)
  ext.run_id = "test-run-id"
  return ext

@pytest.fixture
def active_widget():
  mock_widget = mock.MagicMock()
  mock_widget.run_id = "test-run-id"
  mock_widget.active_jobs_count = 0
  return mock_widget


def test_start_server(monitor_extension):
  """Test TCP server binds and starts socket thread."""
  with mock.patch.object(socket, "socket") as mock_socket, mock.patch.object(
      kernelextension, "SocketThread"
  ) as mock_thread, mock.patch.object(
      monitor_extension, "_start_env_thread"
  ) as mock_start_env, mock.patch.dict("os.environ", {}):

    # Setup mock socket
    mock_sock_inst = mock.MagicMock()
    mock_sock_inst.getsockname.return_value = ("127.0.0.1", 12345)
    mock_socket.return_value = mock_sock_inst

    monitor_extension.start_server()

    mock_socket.assert_called_once()
    mock_sock_inst.bind.assert_called_once_with(("localhost", 0))
    mock_sock_inst.listen.assert_called_once_with(5)
    assert os.environ["SPARK_CELL_MONITOR_KERNEL_PORT"] == "12345"
    mock_thread.assert_called_once()
    mock_thread.return_value.start.assert_called_once()
    mock_start_env.assert_called_once()


def test_pre_run_cell_hook(monitor_extension):
  """Test widget creation and spark context patching on pre-run."""
  mock_session = mock.MagicMock()
  
  mock_pyspark_sql = mock.MagicMock()
  mock_pyspark_sql.SparkSession.getActiveSession.return_value = mock_session
  mock_pyspark_sql.SparkSession.getDefaultSession.return_value = None

  with mock.patch("IPython.display.display") as mock_display, mock.patch.dict(
      "sys.modules", {"pyspark": mock.MagicMock(), "pyspark.sql": mock_pyspark_sql}
  ):
    monitor_extension.pre_run_cell_hook()

    assert monitor_extension.run_id is not None
    assert monitor_extension.run_id in monitor_extension.active_widgets
    mock_display.assert_called_once()
    mock_session.sparkContext.setJobGroup.assert_called_once()


@pytest.mark.parametrize("msgtype, job_count_delta, deletes_job_id", [
    # (Message Type, Expected Change in Active Jobs, Does it evict the jobId?)
    ("sparkJobStart", 1, False),
    ("sparkJobEnd", -1, True),
    ("sparkStageSubmitted", 0, False),  # Generic event example
])
def test_send_to_frontend_routing(
    monitor_extension, active_widget, msgtype, job_count_delta, deletes_job_id
):
  """Parametrized test for message routing logic via msgtype."""
  # 1. Setup the active widget
  monitor_extension.active_widgets["test-run-id"] = active_widget
  active_widget.active_jobs_count = 1  # Base value to measure delta

  # 2. Pre-fill job_to_run_id mapping unless this is a JobStart initializing it
  if msgtype != "sparkJobStart":
    monitor_extension.job_to_run_id[100] = "test-run-id"

  # 3. Build varying payloads based on msgtype
  payload = {"msgtype": msgtype, "jobId": 100}
  if msgtype == "sparkJobStart":
    payload["jobGroup"] = "test-run-id"

  # 4. Route the message
  monitor_extension.send_to_frontend(payload)

  # 5. Assertions
  active_widget.append_event.assert_called_once_with(payload, 1)
  assert active_widget.active_jobs_count == 1 + job_count_delta
  
  if deletes_job_id:  # specific to sparkJobEnd
    assert 100 not in monitor_extension.job_to_run_id
  else:
    assert monitor_extension.job_to_run_id[100] == "test-run-id"


def test_spark_job_end_eviction(monitor_extension, active_widget):
  """Test memory leak fix: job_to_run_id is evicted on sparkJobEnd."""
  active_widget.active_jobs_count = 1
  active_widget.cell_finished = True
  monitor_extension.active_widgets["test-run-id"] = active_widget

  # Setup job routing map
  monitor_extension.job_to_run_id[100] = "test-run-id"

  msg = {"msgtype": "sparkJobEnd", "jobId": 100}
  monitor_extension.send_to_frontend(msg)

  assert active_widget.active_jobs_count == 0
  # Assert eviction (Memory leak fix)
  assert 100 not in monitor_extension.job_to_run_id
  # Assert widget cleanup because cell_finished is True and jobs = 0
  assert "test-run-id" not in monitor_extension.active_widgets


def test_post_run_cell_hook(monitor_extension, active_widget):
  """Test widget cleanup logic after a cell finishes."""
  monitor_extension.active_widgets["test-run-id"] = active_widget

  monitor_extension.post_run_cell_hook(None)

  assert active_widget.cell_finished is True
  assert "test-run-id" not in monitor_extension.active_widgets

def test_patch_spark_context(monitor_extension):
  """Test monkey-patching of SparkContext."""
  class DummySparkContext:

    def __init__(self):
      pass

    def setJobGroup(self, group_id, description):
      pass

  mock_pyspark = mock.MagicMock()
  mock_pyspark.SparkContext = DummySparkContext

  with mock.patch.dict("sys.modules", {"pyspark": mock_pyspark}):
    kernelextension._patch_spark_context(monitor_extension)

    with mock.patch.object(DummySparkContext, "setJobGroup") as mock_set:
      _ = DummySparkContext()
      mock_set.assert_called_once_with(
          "test-run-id", "ManagedSparkCellMonitoring cell tracking"
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


def test_serverless_spark_ui_url_resolution(monitor_extension, active_widget):
  """Test Serverless metadata resolution and appId deep-link upgrade."""
  monitor_extension.active_widgets["test-run-id"] = active_widget
  metadata_map = {
      "instance/attributes/dataproc-session-id": "sess-abc-123",
      "project/project-id": "my-gcp-project",
      "instance/attributes/dataproc-region": "us-central1",
  }
  with mock.patch.object(
      monitor_extension,
      "_fetch_metadata",
      side_effect=lambda path, timeout=0.5: metadata_map.get(path, ""),
  ), mock.patch.object(
      monitor_extension, "_read_spark_defaults_app_context", return_value=True
  ):
    monitor_extension._resolve_environment_bg()

  assert monitor_extension.env_type == "s8s"
  assert monitor_extension.spark_ui_url == (
      "https://console.cloud.google.com/dataproc/interactive/"
      "us-central1/sess-abc-123/sparkApplications/applications"
      "?project=my-gcp-project"
  )
  assert active_widget.spark_ui_url == monitor_extension.spark_ui_url

  # Upgrade to application deep-link on sparkJobStart
  monitor_extension.send_to_frontend({
      "msgtype": "sparkJobStart",
      "jobGroup": "test-run-id",
      "jobId": 1,
      "appId": "app-20261005200000-0001",
  })
  assert monitor_extension.spark_ui_url == (
      "https://console.cloud.google.com/dataproc/interactive/"
      "us-central1/sess-abc-123/sparkApplications/applications/"
      "app-20261005200000-0001?project=my-gcp-project"
  )
  assert active_widget.spark_ui_url == monitor_extension.spark_ui_url


def test_serverless_disabled_when_app_context_false(monitor_extension):
  """Test Serverless URL is suppressed when spark.dataproc.appContext.enabled=false."""
  metadata_map = {
      "instance/attributes/dataproc-session-id": "sess-abc-123",
      "project/project-id": "my-gcp-project",
      "instance/attributes/dataproc-region": "us-central1",
  }
  with mock.patch.object(
      monitor_extension,
      "_fetch_metadata",
      side_effect=lambda path, timeout=0.5: metadata_map.get(path, ""),
  ), mock.patch.object(
      monitor_extension, "_read_spark_defaults_app_context", return_value=False
  ):
    monitor_extension._resolve_environment_bg()

  assert monitor_extension.env_type == "s8s"
  assert monitor_extension.spark_ui_url == ""


def test_dpgce_spark_ui_url_and_invalid_appid_filtering(
    monitor_extension, active_widget, tmp_path
):
  """Test DPGCE Component Gateway URL generation and local-*/null rejection."""
  monitor_extension.active_widgets["test-run-id"] = active_widget
  props_file = tmp_path / "dataproc.properties"
  props_file.write_text(
      "# Dataproc properties\n"
      "dataproc.proxy.public.hostname.other=https\\://ignored.example.com/\n"
      "dataproc.proxy.public.hostname=https\\://abc123-dot-us-central1.dataproc.googleusercontent.com/\n",
      encoding="utf-8",
  )

  with mock.patch.object(
      monitor_extension, "_fetch_metadata", return_value=""
  ), mock.patch.object(
      kernelextension, "_DATAPROC_PROPERTIES_PATH", str(props_file)
  ):
    monitor_extension._resolve_environment_bg()

  assert monitor_extension.env_type == "dpgce"
  assert (
      monitor_extension.proxy_hostname
      == "https://abc123-dot-us-central1.dataproc.googleusercontent.com"
  )
  assert monitor_extension.spark_ui_url == ""

  # local-* and "null" appIds must be ignored
  monitor_extension._update_app_id("null")
  monitor_extension._update_app_id("local-1700000000000")
  assert monitor_extension.spark_ui_url == ""

  # Valid YARN application_* ID populates live YARN proxy link
  monitor_extension.send_to_frontend({
      "msgtype": "sparkJobStart",
      "jobGroup": "test-run-id",
      "jobId": 1,
      "appId": "application_1700000000000_0002",
  })
  assert monitor_extension.spark_ui_url == (
      "https://abc123-dot-us-central1.dataproc.googleusercontent.com"
      "/gateway/default/yarn/proxy/application_1700000000000_0002/"
  )
  assert active_widget.spark_ui_url == monitor_extension.spark_ui_url


def test_off_gce_fallback_no_url(monitor_extension, tmp_path):
  """Test off-GCE environment resolves to unknown with empty spark_ui_url."""
  missing_props = tmp_path / "nonexistent.properties"
  with mock.patch.object(
      monitor_extension, "_fetch_metadata", return_value=""
  ), mock.patch.object(
      kernelextension, "_DATAPROC_PROPERTIES_PATH", str(missing_props)
  ):
    monitor_extension._resolve_environment_bg()

  assert monitor_extension.env_type == "unknown"
  assert monitor_extension.spark_ui_url == ""


@pytest.mark.parametrize(
    "conf_line, expected",
    [
        ("spark.dataproc.appContext.enabled true", True),
        ("spark.dataproc.appContext.enabled=false", False),
        ("spark.dataproc.appContext.enabled = false", False),
        ("spark.dataproc.appContext.enabled\tTRUE", True),
    ],
)
def test_read_spark_defaults_app_context_formats(tmp_path, conf_line, expected):
  """Test spark-defaults.conf parsing for all key/value separators Spark accepts."""
  conf = tmp_path / "spark-defaults.conf"
  conf.write_text(
      "# comment\n"
      "\n"
      "spark.other.setting = 1\n"
      f"{conf_line}\n"
  )
  assert (
      kernelextension.CellMonitorExtension._read_spark_defaults_app_context(
          str(conf)
      )
      is expected
  )


def test_read_spark_defaults_app_context_defaults_to_true(tmp_path):
  """Test missing file or missing key leaves app context enabled."""
  read = kernelextension.CellMonitorExtension._read_spark_defaults_app_context
  assert read(str(tmp_path / "missing.conf")) is True
  conf = tmp_path / "spark-defaults.conf"
  conf.write_text("spark.other.setting=true\n")
  assert read(str(conf)) is True


def test_pre_run_cell_hook_seeds_widget_with_current_spark_ui_url(
    monitor_extension,
):
  """Test a widget created after the URL resolved starts with that URL."""
  monitor_extension.env_type = "s8s"
  monitor_extension.project_id = "my-gcp-project"
  monitor_extension.region = "us-central1"
  monitor_extension.dataproc_session_id = "sess-abc-123"
  monitor_extension._refresh_spark_ui_url()
  expected_url = monitor_extension.spark_ui_url
  assert expected_url.startswith("https://console.cloud.google.com/")

  mock_pyspark_sql = mock.MagicMock()
  mock_pyspark_sql.SparkSession.getActiveSession.return_value = None
  mock_pyspark_sql.SparkSession.getDefaultSession.return_value = None
  with mock.patch("IPython.display.display"), mock.patch.dict(
      "sys.modules", {"pyspark": mock.MagicMock(), "pyspark.sql": mock_pyspark_sql}
  ):
    monitor_extension.pre_run_cell_hook()
  widget = monitor_extension.active_widgets[monitor_extension.run_id]
  assert widget.spark_ui_url == expected_url
