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

import json
from unittest import mock

import pytest

import managed_spark_cell_monitoring as cell_monitoring
from managed_spark_cell_monitoring import kernelextension


@pytest.fixture(autouse=True)
def isolated_toggle_state():
  """Each test starts enabled with no listeners and leaves it that way."""
  saved = list(cell_monitoring._disable_listeners)
  saved_state = list(cell_monitoring._state_listeners)
  cell_monitoring._disable_listeners.clear()
  cell_monitoring._state_listeners.clear()
  cell_monitoring.set_enabled(True)
  yield
  cell_monitoring._disable_listeners.clear()
  cell_monitoring._disable_listeners.extend(saved)
  cell_monitoring._state_listeners.clear()
  cell_monitoring._state_listeners.extend(saved_state)
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


def test_freeze_all_widgets_keeps_panels_but_forgets_everything(extension):
  w1, w2 = mock.MagicMock(), mock.MagicMock()
  w2.cleanup.side_effect = RuntimeError("boom")
  extension.active_widgets.update({"run-1": w1, "run-2": w2})
  extension.job_to_run_id[1] = "run-1"

  extension.freeze_all_widgets()

  # Rendered panels stay on screen: history is released, nothing is closed.
  w1.cleanup.assert_called_once_with(clear_history=True)
  assert w1.cell_finished is True
  w1.close.assert_not_called()
  w2.close.assert_not_called()  # error swallowed
  assert not extension.active_widgets
  assert not extension.job_to_run_id
  assert extension.run_id is None


def test_set_enabled_false_freezes_live_widgets_via_extension(extension):
  widget = mock.MagicMock()
  extension.active_widgets["run-1"] = widget
  cell_monitoring.add_state_listener(extension.on_monitoring_state_changed)

  cell_monitoring.set_enabled(False)

  widget.cleanup.assert_called_once_with(clear_history=True)
  widget.close.assert_not_called()
  assert not extension.active_widgets


def test_state_listener_called_on_both_transitions():
  seen = []
  cell_monitoring.add_state_listener(seen.append)

  cell_monitoring.set_enabled(False)
  cell_monitoring.set_enabled(False)  # no change, no call
  cell_monitoring.set_enabled(True)

  assert seen == [False, True]
  cell_monitoring.remove_state_listener(seen.append)
  cell_monitoring.set_enabled(False)
  assert seen == [False, True]


# --- Driver control channel ------------------------------------------------


def _socketpair():
  import socket  # pylint: disable=g-import-not-at-top

  a, b = socket.socketpair()
  a.settimeout(2)
  b.settimeout(2)
  return a, b


def test_register_client_sends_current_state_on_connect(extension):
  kernel_side, driver_side = _socketpair()
  try:
    cell_monitoring.set_enabled(False)
    extension.register_client(kernel_side)
    assert extension.connected_driver_count() == 1
    frame = driver_side.recv(1024)
    assert frame.endswith(b";EOD:")
    assert json.loads(frame[: -len(b";EOD:")]) == {
        "msgtype": "control",
        "enabled": False,
    }
  finally:
    kernel_side.close()
    driver_side.close()


def test_toggle_broadcasts_control_frames_to_every_driver(extension):
  k1, d1 = _socketpair()
  k2, d2 = _socketpair()
  try:
    extension.register_client(k1)
    extension.register_client(k2)
    d1.recv(1024)  # initial state frames
    d2.recv(1024)
    cell_monitoring.add_state_listener(extension.on_monitoring_state_changed)

    cell_monitoring.set_enabled(False)
    cell_monitoring.set_enabled(True)

    for d in (d1, d2):
      data = b""
      while data.count(b";EOD:") < 2:
        data += d.recv(1024)
      frames = [json.loads(f) for f in data.split(b";EOD:") if f]
      assert [f["enabled"] for f in frames] == [False, True]
  finally:
    for sock in (k1, d1, k2, d2):
      sock.close()


def test_broadcast_tolerates_dead_driver_sockets(extension):
  kernel_side, driver_side = _socketpair()
  extension.register_client(kernel_side)
  driver_side.recv(1024)
  driver_side.close()
  kernel_side.close()

  extension.broadcast_control(False)  # must not raise
  extension.unregister_client(kernel_side)
  assert extension.connected_driver_count() == 0


def _run_reader_with(extension, chunks):
  """Feeds `chunks` through a SocketReader and returns the mock client."""
  client = mock.MagicMock()
  client.recv.side_effect = list(chunks) + [b""]
  reader = kernelextension.SocketReader(client, extension)
  reader.run()
  return client


def test_reader_drops_frames_without_parsing_while_disabled(extension):
  cell_monitoring.set_enabled(False)
  with mock.patch.object(extension, "send_to_frontend") as route, mock.patch(
      "managed_spark_cell_monitoring.kernelextension.json.loads"
  ) as loads:
    client = _run_reader_with(
        extension, [b'{"msgtype":"a"};EOD:{"msgtype":"b"};EOD:', b'{"msgtype":"c"};EOD:']
    )

  route.assert_not_called()
  loads.assert_not_called()
  client.close.assert_called_once()


def test_reader_keeps_framing_across_re_enable(extension):
  """A frame split across the flip must still be parsed once enabled."""
  routed = []

  original_recv = []

  def recv(_size):
    if not original_recv:
      original_recv.append(1)
      # First chunk arrives while disabled: a complete frame to drop plus
      # the first half of the next one.
      return b'{"msgtype":"dropped"};EOD:{"msgtype":"ke'
    if len(original_recv) == 1:
      original_recv.append(2)
      cell_monitoring.set_enabled(True)
      return b'pt"};EOD:'
    return b""

  cell_monitoring.set_enabled(False)
  client = mock.MagicMock()
  client.recv.side_effect = recv
  with mock.patch.object(extension, "send_to_frontend", side_effect=routed.append):
    kernelextension.SocketReader(client, extension).run()

  assert routed == [{"msgtype": "kept"}]


def test_reader_caps_idle_buffer_while_disabled(extension):
  cell_monitoring.set_enabled(False)
  huge = b"x" * (kernelextension._MAX_IDLE_BUFFER_BYTES + 10)
  with mock.patch.object(extension, "send_to_frontend") as route:
    _run_reader_with(extension, [huge, b'{"msgtype":"late"};EOD:'])
  route.assert_not_called()


def test_load_ipython_extension_registers_listener_and_magic():
  from ipykernel import zmqshell  # pylint: disable=g-import-not-at-top

  mock_ipython = mock.MagicMock()
  mock_ipython.__class__ = zmqshell.ZMQInteractiveShell

  with mock.patch.object(kernelextension, "CellMonitorExtension") as mock_ext:
    instance = mock_ext.return_value
    kernelextension.load_ipython_extension(mock_ipython)

  # The socket server starts regardless of the current state so the driver
  # can always find the port; state changes are relayed to the drivers and
  # freeze the widgets through the state listener.
  instance.start_server.assert_called_once()
  assert instance.on_monitoring_state_changed in cell_monitoring._state_listeners
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


# --- Concurrency: the toggle vs. the socket thread ------------------------


def test_disable_listener_may_call_back_into_the_module():
  """Listeners run outside _enabled_lock, so re-entrancy cannot deadlock."""
  seen = []

  def listener():
    seen.append(cell_monitoring.is_enabled())
    cell_monitoring.remove_disable_listener(listener)

  cell_monitoring.add_disable_listener(listener)
  cell_monitoring.set_enabled(False)
  assert seen == [False]
  assert listener not in cell_monitoring._disable_listeners


def test_listener_added_during_disable_is_not_called_for_that_transition():
  late = mock.MagicMock()

  def listener():
    cell_monitoring.add_disable_listener(late)

  cell_monitoring.add_disable_listener(listener)
  cell_monitoring.set_enabled(False)
  late.assert_not_called()  # snapshot was taken under the lock
  cell_monitoring.set_enabled(True)
  cell_monitoring.set_enabled(False)
  late.assert_called_once()


def test_send_to_frontend_survives_widgets_vanishing_mid_route(extension):
  """freeze_all_widgets() racing an incoming event must not raise or log."""
  widget = mock.MagicMock(active_jobs_count=1, run_id="run-1")
  extension.active_widgets["run-1"] = widget
  extension.job_to_run_id[7] = "run-1"

  # Emulate the kernel thread clearing everything right after routing picked
  # the widget: the job-end path then sees empty dicts.
  original = extension._handle_job_end

  def racing_job_end(w, msg):
    extension.freeze_all_widgets()
    original(w, msg)

  with mock.patch.object(extension, "_handle_job_end", racing_job_end), mock.patch.object(
      kernelextension.logger, "warning"
  ) as warn:
    extension.send_to_frontend({"msgtype": "sparkJobEnd", "jobId": 7})

  warn.assert_not_called()
  assert not extension.active_widgets
  assert not extension.job_to_run_id


def test_resolve_widget_and_fallback_tolerate_empty_tracking(extension):
  extension.run_id = "run-1"
  assert extension._resolve_widget("missing") is None
  assert extension._any_active_widget() is None
  with mock.patch.object(kernelextension.logger, "warning") as warn:
    extension.send_to_frontend({"msgtype": "sparkStageSubmitted", "jobIds": [1]})
  warn.assert_not_called()


def test_post_run_cell_hook_tolerates_widget_removed_by_socket_thread(extension):
  """If _handle_job_end wins the race, post_run must not clean up twice."""

  class VanishingDict(dict):
    """pop() finds the entry already gone, as if another thread removed it."""

    def pop(self, key, default=None):
      self.clear()
      return default

  widget = mock.MagicMock(active_jobs_count=0)
  extension.active_widgets = VanishingDict({"run-1": widget})

  extension.post_run_cell_hook(None)

  assert widget.cell_finished is True
  widget.cleanup.assert_not_called()  # the other thread owns the cleanup
  assert not extension.active_widgets

  # Nothing tracked at all: a plain no-op.
  extension.post_run_cell_hook(None)
  widget.cleanup.assert_not_called()
