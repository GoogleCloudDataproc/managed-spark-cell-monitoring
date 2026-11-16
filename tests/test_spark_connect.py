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
"""Tests for Spark Connect controller and widget snapshot mode."""

import contextvars
import socket
import urllib.request
from concurrent import futures
from unittest import mock

import pytest

import managed_spark_cell_monitoring as cell_monitoring
from managed_spark_cell_monitoring import spark_connect
from managed_spark_cell_monitoring.widget import ManagedSparkCellWidget


@pytest.fixture(autouse=True)
def _restore_monitoring_state():
    cell_monitoring.set_enabled(True)
    yield
    cell_monitoring.set_enabled(True)


def _make_mock_ipython():
    ip = mock.MagicMock()
    registered = {}

    def _register(event_name, hook):
        registered.setdefault(event_name, []).append(hook)

    def _unregister(event_name, hook):
        registered.get(event_name, []).remove(hook)

    ip.events.register.side_effect = _register
    ip.events.unregister.side_effect = _unregister
    return ip, registered


def test_load_extension_starts_no_tcp_or_metadata_probes():
    """Loading spark_connect registers %cellmonitor without TCP or metadata IO."""
    ip, registered = _make_mock_ipython()
    ctrl = spark_connect.SparkConnectCellController()

    with (
        mock.patch.object(socket, "socket") as mock_sock,
        mock.patch.object(urllib.request, "urlopen") as mock_urlopen,
    ):
        ctrl.attach_ipython(ip)

    mock_sock.assert_not_called()
    mock_urlopen.assert_not_called()
    assert ctrl.pre_run_cell_hook in registered["pre_run_cell"]
    assert ctrl.post_run_cell_hook in registered["post_run_cell"]
    ip.register_magic_function.assert_called_once()


def test_lazy_widget_display_only_when_jobs_exist():
    """Pure-Python cells and jobless snapshots never display an empty widget."""
    ip, _ = _make_mock_ipython()
    ctrl = spark_connect.SparkConnectCellController()
    ctrl.attach_ipython(ip)

    display_ctx_var = contextvars.ContextVar("parent_output_tag", default="unset")

    with mock.patch("IPython.display.display") as mock_display:
        # Pure Python cell: zero operations -> zero widgets displayed
        ctrl.pre_run_cell_hook()
        ctrl.post_run_cell_hook()
        mock_display.assert_not_called()

        # Cell with a jobless DDL operation -> still zero widgets displayed
        tok = display_ctx_var.set("cell-output-area-42")
        try:
            ctrl.pre_run_cell_hook()
        finally:
            display_ctx_var.reset(tok)

        cid = ctrl.current_cell_id()
        assert cid is not None
        assert ctrl.register_operation(
            cid,
            "op-ddl",
            spark_ui_url="https://console.cloud.google.com/dataproc/interactive/us-central1/s1",
        )
        assert not ctrl.update_operation(
            cid,
            {
                "operation_id": "op-ddl",
                "operation_state": "OPERATION_STATE_SUCCEEDED",
                "version": 1,
                "jobs": [],
            },
        )
        mock_display.assert_not_called()

        # Subsequent operation in the SAME cell spawns a Spark job -> widget displays once
        seen_ctx_tags = []

        def _fake_display(w):
            seen_ctx_tags.append(display_ctx_var.get())
            w.send = mock.MagicMock()

        mock_display.side_effect = _fake_display

        ctrl.register_operation(cid, "op-job")
        shown = ctrl.update_operation(
            cid,
            {
                "operation_id": "op-job",
                "operation_state": "OPERATION_STATE_RUNNING",
                "version": 1,
                "jobs": [{"job_id": 10, "status": "JOB_STATUS_RUNNING", "num_tasks": 4}],
            },
        )
        assert shown is True
        assert mock_display.call_count == 1
        assert seen_ctx_tags == ["cell-output-area-42"]

        displayed_widget = mock_display.call_args[0][0]
        assert isinstance(displayed_widget, ManagedSparkCellWidget)
        assert displayed_widget.spark_ui_url == (
            "https://console.cloud.google.com/dataproc/interactive/us-central1/s1"
        )
        displayed_widget.send.assert_called_once()

        # Second update reuses the same widget without calling display() again
        ctrl.complete_operation(
            cid,
            "op-job",
            final_snapshot={
                "operation_id": "op-job",
                "operation_state": "OPERATION_STATE_SUCCEEDED",
                "version": 2,
                "final_snapshot": True,
                "jobs": [{"job_id": 10, "status": "JOB_STATUS_SUCCEEDED", "num_tasks": 4}],
            },
        )
        ctrl.complete_operation(cid, "op-ddl")
        assert mock_display.call_count == 1
        assert displayed_widget.send.call_count == 2

        ctrl.post_run_cell_hook()
        assert displayed_widget.cell_finished is True
        assert ctrl.current_cell_id() is None


def test_worker_thread_resolves_active_cell_id():
    """ThreadPoolExecutor workers that did not inherit ContextVar still resolve cell_id."""
    ip, _ = _make_mock_ipython()
    ctrl = spark_connect.SparkConnectCellController()
    ctrl.attach_ipython(ip)

    ctrl.pre_run_cell_hook()
    expected_cid = ctrl.current_cell_id()
    assert expected_cid is not None

    empty_ctx = contextvars.Context()
    with futures.ThreadPoolExecutor(max_workers=1) as pool:
        worker_cid = pool.submit(empty_ctx.run, ctrl.current_cell_id).result()

    assert worker_cid == expected_cid
    ctrl.post_run_cell_hook()


def test_disable_monitoring_freezes_active_cell_and_skips_new_cells():
    """%cellmonitor off / set_enabled(False) freezes live tracking cleanly."""
    ip, _ = _make_mock_ipython()
    ctrl = spark_connect.SparkConnectCellController()
    ctrl.attach_ipython(ip)

    with mock.patch("IPython.display.display") as mock_display:
        ctrl.pre_run_cell_hook()
        cid = ctrl.current_cell_id()
        assert cid is not None
        ctrl.update_operation(
            cid,
            {
                "operation_id": "op-1",
                "operation_state": "OPERATION_STATE_RUNNING",
                "version": 1,
                "jobs": [{"job_id": 1, "status": "JOB_STATUS_RUNNING", "num_tasks": 2}],
            },
        )
        widget = mock_display.call_args[0][0]
        assert widget.cell_finished is False

        # Turn monitoring off mid-cell
        cell_monitoring.set_enabled(False)
        assert widget.cell_finished is True
        assert ctrl.current_cell_id() is None
        assert not ctrl.update_operation(
            cid,
            {
                "operation_id": "op-1",
                "operation_state": "OPERATION_STATE_SUCCEEDED",
                "version": 2,
                "jobs": [{"job_id": 1, "status": "JOB_STATUS_SUCCEEDED", "num_tasks": 2}],
            },
        )
        ctrl.post_run_cell_hook()

        # Next cell while disabled creates no cell tracking
        ctrl.pre_run_cell_hook()
        assert ctrl.current_cell_id() is None
        ctrl.post_run_cell_hook()

        # Re-enable monitoring -> subsequent cell tracks again
        cell_monitoring.set_enabled(True)
        ctrl.pre_run_cell_hook()
        assert ctrl.current_cell_id() is not None
        ctrl.post_run_cell_hook()


def test_widget_ready_replays_merged_operation_snapshots():
    """Snapshots sent before ESM bundle load replay cleanly on widget_ready."""
    widget = ManagedSparkCellWidget(run_id="run-sc", session_id="sess-sc")
    widget.send = mock.MagicMock()

    widget.send_operation_telemetry(
        {
            "operation_id": "op-fast",
            "operation_state": "OPERATION_STATE_RUNNING",
            "version": 3,
            "unchanged": False,
            "final_snapshot": False,
            "jobs": [{"job_id": 5, "status": "JOB_STATUS_SUCCEEDED", "num_tasks": 4}],
        }
    )
    # Terminal poll returned unchanged=True while server state was still TERMINATING
    widget.send_operation_telemetry(
        {
            "operation_id": "op-fast",
            "operation_state": "OPERATION_STATE_TERMINATING",
            "execution_outcome": "SUCCEEDED",
            "version": 3,
            "unchanged": True,
            "final_snapshot": False,
            "closed_at": 5000,
        }
    )
    widget.cleanup()

    widget.send.reset_mock()
    widget._handle_frontend_message(widget, {"type": "widget_ready"})

    widget.send.assert_called_once()
    replay_msg = widget.send.call_args[0][0]
    assert replay_msg["type"] == "replay_snapshots"
    assert len(replay_msg["snapshots"]) == 1
    replayed = replay_msg["snapshots"][0]
    assert replayed["operation_id"] == "op-fast"
    assert replayed["operation_state"] == "OPERATION_STATE_TERMINATING"
    assert replayed["execution_outcome"] == "SUCCEEDED"
    assert replayed["final_snapshot"] is False
    assert replayed["closed_at"] == 5000
    assert replayed["jobs"] == [{"job_id": 5, "status": "JOB_STATUS_SUCCEEDED", "num_tasks": 4}]
