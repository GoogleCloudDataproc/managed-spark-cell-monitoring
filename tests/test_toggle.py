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
"""Tests for the runtime on/off switch (`set_enabled` / `%cellmonitor`)."""

from unittest import mock

import pytest

import managed_spark_cell_monitoring as cell_monitoring
from managed_spark_cell_monitoring import kernelextension


@pytest.fixture(autouse=True)
def isolated_toggle_state():
  """Each test starts enabled with no listeners and leaves it that way."""
  saved = list(cell_monitoring._disable_listeners)
  cell_monitoring._disable_listeners.clear()
  cell_monitoring.set_enabled(True)
  yield
  cell_monitoring._disable_listeners.clear()
  cell_monitoring._disable_listeners.extend(saved)
  cell_monitoring.set_enabled(True)


@pytest.fixture
def extension():
  ext = kernelextension.CellMonitorExtension(mock.MagicMock())
  ext.run_id = "run-1"
  return ext


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, True),
        ("", True),
        ("1", True),
        ("true", True),
        ("anything-else", True),
        ("0", False),
        ("false", False),
        ("FALSE", False),
        (" off ", False),
        ("no", False),
    ],
)
def test_enabled_from_env_only_explicit_opt_out_disables(value, expected):
  environ = {} if value is None else {cell_monitoring.ENABLED_ENV_VAR: value}
  assert cell_monitoring._enabled_from_env(environ) is expected


def test_set_enabled_round_trip_fires_listener_only_when_turning_off():
  listener = mock.MagicMock()
  cell_monitoring.add_disable_listener(listener)
  cell_monitoring.add_disable_listener(listener)  # idempotent

  assert cell_monitoring.set_enabled(False) is False
  assert cell_monitoring.is_enabled() is False
  listener.assert_called_once()

  # Already off: no second notification.
  cell_monitoring.set_enabled(False)
  listener.assert_called_once()

  # Turning on never notifies disable listeners.
  assert cell_monitoring.set_enabled(True) is True
  assert cell_monitoring.is_enabled() is True
  listener.assert_called_once()

  cell_monitoring.remove_disable_listener(listener)
  cell_monitoring.set_enabled(False)
  listener.assert_called_once()


def test_disable_listener_errors_do_not_break_the_toggle():
  cell_monitoring.add_disable_listener(mock.MagicMock(side_effect=RuntimeError("boom")))
  assert cell_monitoring.set_enabled(False) is False
  assert cell_monitoring.is_enabled() is False


def test_pre_run_cell_hook_skips_widget_and_job_group_when_disabled(extension):
  cell_monitoring.set_enabled(False)
  mock_session = mock.MagicMock()
  mock_pyspark_sql = mock.MagicMock()
  mock_pyspark_sql.SparkSession.getActiveSession.return_value = mock_session

  with mock.patch("IPython.display.display") as mock_display, mock.patch.dict(
      "sys.modules", {"pyspark": mock.MagicMock(), "pyspark.sql": mock_pyspark_sql}
  ):
    extension.pre_run_cell_hook()

  assert extension.run_id is None
  assert not extension.active_widgets
  mock_display.assert_not_called()
  mock_session.sparkContext.setJobGroup.assert_not_called()

  # post_run is a no-op for an unmonitored cell.
  extension.post_run_cell_hook(None)
  assert not extension.active_widgets


def test_pre_run_cell_hook_resumes_after_re_enable(extension):
  cell_monitoring.set_enabled(False)
  with mock.patch("IPython.display.display") as mock_display, mock.patch.dict(
      "sys.modules", {"pyspark": mock.MagicMock(), "pyspark.sql": mock.MagicMock()}
  ):
    extension.pre_run_cell_hook()
    mock_display.assert_not_called()

    cell_monitoring.set_enabled(True)
    extension.pre_run_cell_hook()
    mock_display.assert_called_once()
    assert extension.run_id in extension.active_widgets


def test_send_to_frontend_drops_events_when_disabled(extension):
  widget = mock.MagicMock(active_jobs_count=0)
  extension.active_widgets["run-1"] = widget
  cell_monitoring.set_enabled(False)

  extension.send_to_frontend({"msgtype": "sparkJobStart", "jobId": 7, "jobGroup": "run-1"})

  widget.append_event.assert_not_called()
  assert 7 not in extension.job_to_run_id


def test_close_all_widgets_closes_and_forgets_everything(extension):
  w1, w2 = mock.MagicMock(), mock.MagicMock()
  w2.close.side_effect = RuntimeError("already closed")
  extension.active_widgets.update({"run-1": w1, "run-2": w2})
  extension.job_to_run_id[1] = "run-1"

  extension.close_all_widgets()

  w1.cleanup.assert_called_once_with(clear_history=True)
  w1.close.assert_called_once()
  w2.close.assert_called_once()  # error swallowed
  assert not extension.active_widgets
  assert not extension.job_to_run_id
  assert extension.run_id is None


def test_set_enabled_false_tears_down_live_widgets_via_extension(extension):
  widget = mock.MagicMock()
  extension.active_widgets["run-1"] = widget
  cell_monitoring.add_disable_listener(extension.close_all_widgets)

  cell_monitoring.set_enabled(False)

  widget.close.assert_called_once()
  assert not extension.active_widgets


def test_load_ipython_extension_registers_listener_and_magic():
  from ipykernel import zmqshell  # pylint: disable=g-import-not-at-top

  mock_ipython = mock.MagicMock()
  mock_ipython.__class__ = zmqshell.ZMQInteractiveShell

  with mock.patch.object(kernelextension, "CellMonitorExtension") as mock_ext:
    instance = mock_ext.return_value
    kernelextension.load_ipython_extension(mock_ipython)

  # The socket server starts regardless of the current state so the driver
  # can always find the port, and the extension is torn down on disable.
  instance.start_server.assert_called_once()
  assert instance.close_all_widgets in cell_monitoring._disable_listeners
  mock_ipython.register_magic_function.assert_called_once()
  _, kwargs = mock_ipython.register_magic_function.call_args
  assert kwargs == {"magic_kind": "line", "magic_name": "cellmonitor"}


def test_cellmonitor_magic_toggles_and_reports(capsys):
  mock_ipython = mock.MagicMock()
  kernelextension._register_cellmonitor_magic(mock_ipython)
  (magic,), _ = mock_ipython.register_magic_function.call_args

  magic("off")
  assert cell_monitoring.is_enabled() is False
  magic("status")
  magic("on")
  assert cell_monitoring.is_enabled() is True
  magic("")
  magic("bogus")
  assert cell_monitoring.is_enabled() is True

  out = capsys.readouterr().out.splitlines()
  assert out == [
      "Managed Spark cell monitoring is off",
      "Managed Spark cell monitoring is off",
      "Managed Spark cell monitoring is on",
      "Managed Spark cell monitoring is on",
      "Usage: %cellmonitor [on|off|status]",
  ]


def test_magic_registration_failure_is_non_fatal():
  mock_ipython = mock.MagicMock()
  mock_ipython.register_magic_function.side_effect = RuntimeError("no magics here")
  kernelextension._register_cellmonitor_magic(mock_ipython)  # must not raise
