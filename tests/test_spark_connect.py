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
import threading
import types
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
    assert ctrl.pre_execute_hook in registered["pre_execute"]
    assert ctrl.post_execute_hook in registered["post_execute"]
    ip.register_magic_function.assert_called_once()

    ctrl.detach_ipython(ip)
    assert ctrl.pre_run_cell_hook not in registered["pre_run_cell"]
    assert ctrl.post_run_cell_hook not in registered["post_run_cell"]
    assert ctrl.pre_execute_hook not in registered["pre_execute"]
    assert ctrl.post_execute_hook not in registered["post_execute"]


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


def test_comm_msg_lifecycle_and_threadpool_shared_widget():
    """Nested ipykernel comm_msg scopes aggregate ThreadPoolExecutor queries in one widget."""
    ip, _ = _make_mock_ipython()
    ip.parent_header = {"header": {"msg_type": "comm_msg", "msg_id": "comm-batch-1"}}
    ctrl = spark_connect.SparkConnectCellController()
    ctrl.attach_ipython(ip)

    with mock.patch("IPython.display.display") as mock_display:
        sent_messages: list[dict] = []

        def _fake_display(w):
            w.send = mock.MagicMock(side_effect=sent_messages.append)

        mock_display.side_effect = _fake_display

        ctrl.pre_execute_hook()
        comm_cid = ctrl.current_cell_id()
        assert comm_cid is not None

        def _run_worker_query(idx: int) -> str | None:
            # ThreadPoolExecutor workers do not inherit ContextVar; run in empty Context.
            def _worker() -> str | None:
                cid = ctrl.current_cell_id()
                assert cid is not None
                op_id = f"op-{idx}"
                assert ctrl.register_operation(cid, op_id)
                ctrl.update_operation(
                    cid,
                    {
                        "operation_id": op_id,
                        "operation_state": "OPERATION_STATE_RUNNING",
                        "version": 1,
                        "jobs": [
                            {"job_id": idx + 1, "status": "JOB_STATUS_RUNNING", "num_tasks": 2}
                        ],
                    },
                )
                ctrl.complete_operation(
                    cid,
                    op_id,
                    final_snapshot={
                        "operation_id": op_id,
                        "operation_state": "OPERATION_STATE_SUCCEEDED",
                        "version": 2,
                        "final_snapshot": True,
                        "jobs": [
                            {"job_id": idx + 1, "status": "JOB_STATUS_SUCCEEDED", "num_tasks": 2}
                        ],
                    },
                )
                return cid

            return contextvars.Context().run(_worker)

        with futures.ThreadPoolExecutor(max_workers=4) as pool:
            worker_cids = list(pool.map(_run_worker_query, range(4)))

        assert worker_cids == [comm_cid] * 4
        assert mock_display.call_count == 1
        widget = mock_display.call_args[0][0]
        assert widget.cell_finished is False
        assert len(sent_messages) == 8
        assert {msg["operation_id"] for msg in sent_messages} == {f"op-{i}" for i in range(4)}

        ctrl.post_execute_hook()
        assert widget.cell_finished is True
        assert ctrl.current_cell_id() is None


def test_parent_header_extraction_variants():
    """Supports flat mock parent_header dicts and kernel.get_parent() fallback."""
    ip, _ = _make_mock_ipython()
    ctrl = spark_connect.SparkConnectCellController()
    ctrl.attach_ipython(ip)

    # Flat header dict mock
    ip.parent_header = {"msg_type": "comm_msg", "msg_id": "flat-1"}
    ctrl.pre_execute_hook()
    cid_flat = ctrl.current_cell_id()
    assert cid_flat is not None
    ctrl.post_execute_hook()
    assert ctrl.current_cell_id() is None

    # Empty parent_header falling back to kernel.get_parent()
    ip.parent_header = {}
    ip.kernel.get_parent.return_value = {"header": {"msg_type": "comm_msg", "msg_id": "kern-1"}}
    ctrl.pre_execute_hook()
    cid_kern = ctrl.current_cell_id()
    assert cid_kern is not None and cid_kern != cid_flat
    ctrl.post_execute_hook()
    assert ctrl.current_cell_id() is None


def test_zero_job_comm_msg_displays_no_widget():
    """Zero-job comm_msg handlers (widget_ready, PING, metadata-only) display nothing."""
    ip, _ = _make_mock_ipython()
    ip.parent_header = {"header": {"msg_type": "comm_msg", "msg_id": "comm-ping"}}
    ctrl = spark_connect.SparkConnectCellController()
    ctrl.attach_ipython(ip)

    with mock.patch("IPython.display.display") as mock_display:
        ctrl.pre_execute_hook()
        cid = ctrl.current_cell_id()
        assert cid is not None
        assert ctrl.register_operation(cid, "op-meta")
        assert not ctrl.update_operation(
            cid,
            {
                "operation_id": "op-meta",
                "operation_state": "OPERATION_STATE_SUCCEEDED",
                "version": 1,
                "jobs": [],
            },
        )
        ctrl.complete_operation(cid, "op-meta")
        ctrl.post_execute_hook()

        mock_display.assert_not_called()
        assert ctrl.current_cell_id() is None


def test_execute_request_and_silent_cells_unaffected_by_pre_post_execute():
    """execute_request cells (normal, silent, store_history=False) stay solely on run_cell."""
    ip, _ = _make_mock_ipython()
    ctrl = spark_connect.SparkConnectCellController()
    ctrl.attach_ipython(ip)

    # 1. Normal execute_request cell: pre_execute + pre_run_cell + post_execute + post_run_cell
    ip.parent_header = {"header": {"msg_type": "execute_request", "msg_id": "exec-1"}}
    ctrl.pre_execute_hook()
    assert ctrl.current_cell_id() is None

    ctrl.pre_run_cell_hook(types.SimpleNamespace(silent=False, store_history=True))
    cell_cid = ctrl.current_cell_id()
    assert cell_cid is not None

    # Concurrent comm_msg during async cell gets isolated scope and does not pop outer cell
    def _run_concurrent_comm() -> None:
        ip.parent_header = {"header": {"msg_type": "comm_msg", "msg_id": "comm-concurrent"}}
        ctrl.pre_execute_hook()
        comm_cid = ctrl.current_cell_id()
        assert comm_cid is not None and comm_cid != cell_cid
        ctrl.post_execute_hook()

    contextvars.Context().run(_run_concurrent_comm)
    assert ctrl.current_cell_id() == cell_cid

    # post_execute at end of execute_request must NOT pop the pre_run_cell scope
    ip.parent_header = {"header": {"msg_type": "execute_request", "msg_id": "exec-1"}}
    ctrl.post_execute_hook()
    assert ctrl.current_cell_id() == cell_cid

    ctrl.post_run_cell_hook(
        types.SimpleNamespace(info=types.SimpleNamespace(silent=False, store_history=True))
    )
    assert ctrl.current_cell_id() is None

    # 2. silent=True IDE helper cell
    ip.parent_header = {"header": {"msg_type": "execute_request", "msg_id": "exec-silent"}}
    ctrl.pre_execute_hook()
    assert ctrl.current_cell_id() is None
    ctrl.post_execute_hook()
    assert ctrl.current_cell_id() is None

    # 3. store_history=False IDE helper cell
    ip.parent_header = {"header": {"msg_type": "execute_request", "msg_id": "exec-nohist"}}
    ctrl.pre_execute_hook()
    ctrl.pre_run_cell_hook(types.SimpleNamespace(silent=False, store_history=False))
    assert ctrl.current_cell_id() is None
    ctrl.post_execute_hook()
    ctrl.post_run_cell_hook(
        types.SimpleNamespace(info=types.SimpleNamespace(silent=False, store_history=False))
    )
    assert ctrl.current_cell_id() is None


def test_abandoned_comm_scope_cleanup_on_base_exception():
    """Abandoned comm scopes finalize on the next pre_execute or pre_run_cell."""
    ip, _ = _make_mock_ipython()
    ctrl = spark_connect.SparkConnectCellController()
    ctrl.attach_ipython(ip)

    with mock.patch("IPython.display.display") as mock_display:
        mock_display.side_effect = lambda w: setattr(w, "send", mock.MagicMock())

        # Case 1: abandoned comm scope finalized when a new comm_msg (different msg_id) arrives
        ip.parent_header = {"header": {"msg_type": "comm_msg", "msg_id": "comm-abort-1"}}
        ctrl.pre_execute_hook()
        cid1 = ctrl.current_cell_id()
        assert cid1 is not None
        ctrl.update_operation(
            cid1,
            {
                "operation_id": "op-abort-1",
                "operation_state": "OPERATION_STATE_RUNNING",
                "version": 1,
                "jobs": [{"job_id": 1, "status": "JOB_STATUS_RUNNING", "num_tasks": 1}],
            },
        )
        w1 = mock_display.call_args[0][0]
        assert w1.cell_finished is False
        # BaseException unwinds BaseComm.handle_msg -> post_execute_hook NOT called.

        ip.parent_header = {"header": {"msg_type": "comm_msg", "msg_id": "comm-2"}}
        ctrl.pre_execute_hook()
        cid2 = ctrl.current_cell_id()
        assert cid2 is not None and cid2 != cid1
        assert w1.cell_finished is True
        ctrl.post_execute_hook()
        assert ctrl.current_cell_id() is None

        # Case 2: abandoned comm scope finalized when pre_run_cell arrives
        ip.parent_header = {"header": {"msg_type": "comm_msg", "msg_id": "comm-abort-2"}}
        ctrl.pre_execute_hook()
        cid3 = ctrl.current_cell_id()
        assert cid3 is not None
        ctrl.update_operation(
            cid3,
            {
                "operation_id": "op-abort-2",
                "operation_state": "OPERATION_STATE_RUNNING",
                "version": 1,
                "jobs": [{"job_id": 2, "status": "JOB_STATUS_RUNNING", "num_tasks": 1}],
            },
        )
        w2 = mock_display.call_args[0][0]
        assert w2.cell_finished is False

        ip.parent_header = {"header": {"msg_type": "execute_request", "msg_id": "exec-next"}}
        ctrl.pre_run_cell_hook(types.SimpleNamespace(silent=False, store_history=True))
        cid4 = ctrl.current_cell_id()
        assert cid4 is not None and cid4 != cid3
        assert w2.cell_finished is True
        ctrl.post_run_cell_hook()
        assert ctrl.current_cell_id() is None


def test_concurrent_update_and_finish_single_widget_and_ordering():
    """Concurrent update/finish creates one widget with comm_open+display before send."""
    ip, _ = _make_mock_ipython()
    ctrl = spark_connect.SparkConnectCellController()
    ctrl.attach_ipython(ip)

    events_order: list[str] = []
    order_lock = threading.Lock()
    entered_display = threading.Event()
    release_display = threading.Event()
    created_widgets: list[ManagedSparkCellWidget] = []

    class _RecordingWidget(ManagedSparkCellWidget):
        def __init__(self, *args, **kwargs):
            with order_lock:
                events_order.append("comm_open")
            super().__init__(*args, **kwargs)
            self.send = mock.MagicMock(side_effect=self._record_send)
            created_widgets.append(self)

        def _record_send(self, msg):
            with order_lock:
                op_id = msg.get("operation_id")
                ver = msg.get("version")
                events_order.append(f"send:{op_id}:v{ver}:unchanged={bool(msg.get('unchanged'))}")

    def _blocking_display(w):
        with order_lock:
            events_order.append("display")
        entered_display.set()
        assert release_display.wait(timeout=3.0)

    ctrl.pre_run_cell_hook()
    cid = ctrl.current_cell_id()
    assert cid is not None
    assert ctrl.register_operation(cid, "op-race")

    with (
        mock.patch.object(spark_connect, "ManagedSparkCellWidget", _RecordingWidget),
        mock.patch("IPython.display.display", side_effect=_blocking_display) as mock_display,
    ):
        t_poll = threading.Thread(
            target=ctrl.update_operation,
            args=(
                cid,
                {
                    "operation_id": "op-race",
                    "operation_state": "OPERATION_STATE_RUNNING",
                    "version": 1,
                    "jobs": [{"job_id": 1, "status": "JOB_STATUS_RUNNING", "num_tasks": 2}],
                },
            ),
        )
        t_poll.start()
        assert entered_display.wait(timeout=3.0)

        finish_results: list[bool] = []

        def _finish():
            # Synthetic unchanged=True closeout arriving while display() is in flight
            ok = ctrl.update_operation(
                cid,
                {
                    "operation_id": "op-race",
                    "operation_state": "OPERATION_STATE_SUCCEEDED",
                    "execution_outcome": "SUCCEEDED",
                    "version": 1,
                    "unchanged": True,
                    "final_snapshot": False,
                },
            )
            finish_results.append(ok)
            ctrl.complete_operation(cid, "op-race")

        t_finish = threading.Thread(target=_finish)
        t_finish.start()

        # Confirm t_finish is blocked on widget_lock until display() finishes.
        assert not t_finish.join(timeout=0.05) and t_finish.is_alive()

        release_display.set()
        t_poll.join(timeout=3.0)
        t_finish.join(timeout=3.0)

        assert mock_display.call_count == 1
        assert len(created_widgets) == 1
        assert finish_results == [True]
        assert events_order == [
            "comm_open",
            "display",
            "send:op-race:v1:unchanged=False",
            "send:op-race:v1:unchanged=True",
        ]

        ctrl.post_run_cell_hook()
        assert created_widgets[0].cell_finished is True
        replayed = created_widgets[0].operation_snapshots["op-race"]
        assert replayed["execution_outcome"] == "SUCCEEDED"
        assert replayed["jobs"] == [{"job_id": 1, "status": "JOB_STATUS_RUNNING", "num_tasks": 2}]


def test_display_context_propagated_on_background_threads():
    """comm_open, display, telemetry, and trait updates inherit the triggering cell/comm Context."""
    parent_header_var = contextvars.ContextVar("parent_header_var", default="unset")
    ip, _ = _make_mock_ipython()
    ctrl = spark_connect.SparkConnectCellController()
    ctrl.attach_ipython(ip)

    observed_contexts: list[tuple[str, str]] = []

    class _ContextTrackingWidget(ManagedSparkCellWidget):
        def __init__(self, *args, **kwargs):
            observed_contexts.append(("comm_open", parent_header_var.get()))
            super().__init__(*args, **kwargs)
            self.send = mock.MagicMock(
                side_effect=lambda _msg: observed_contexts.append(("send", parent_header_var.get()))
            )
            self.observe(
                lambda _chg: observed_contexts.append(
                    ("trait_spark_ui_url", parent_header_var.get())
                ),
                names="spark_ui_url",
            )
            self.observe(
                lambda _chg: observed_contexts.append(
                    ("trait_cell_finished", parent_header_var.get())
                ),
                names="cell_finished",
            )

    def _tracking_display(_w):
        observed_contexts.append(("display", parent_header_var.get()))

    ip.parent_header = {"header": {"msg_type": "comm_msg", "msg_id": "comm-ctx-7"}}
    tok = parent_header_var.set("comm-ctx-7")
    try:
        ctrl.pre_execute_hook()
    finally:
        parent_header_var.reset(tok)

    cid = ctrl.current_cell_id()
    assert cid is not None

    # Simulate main thread moving on to a different execute_request before background threads run.
    later_tok = parent_header_var.set("later-execute-99")
    try:
        with (
            mock.patch.object(spark_connect, "ManagedSparkCellWidget", _ContextTrackingWidget),
            mock.patch("IPython.display.display", side_effect=_tracking_display),
            futures.ThreadPoolExecutor(max_workers=2) as pool,
        ):

            def _bg_poller_and_worker():
                assert ctrl.register_operation(cid, "op-ctx", spark_ui_url="https://ui/1")
                assert ctrl.update_operation(
                    cid,
                    {
                        "operation_id": "op-ctx",
                        "operation_state": "OPERATION_STATE_RUNNING",
                        "version": 1,
                        "jobs": [{"job_id": 1, "status": "JOB_STATUS_RUNNING", "num_tasks": 1}],
                    },
                )
                ctrl.complete_operation(
                    cid,
                    "op-ctx",
                    final_snapshot={
                        "operation_id": "op-ctx",
                        "operation_state": "OPERATION_STATE_SUCCEEDED",
                        "version": 2,
                        "final_snapshot": True,
                        "jobs": [{"job_id": 1, "status": "JOB_STATUS_SUCCEEDED", "num_tasks": 1}],
                    },
                    spark_ui_url="https://ui/2",
                )

            pool.submit(contextvars.Context().run, _bg_poller_and_worker).result()

        # Even if post_execute runs on a later context or abandoned comm cleanup runs later:
        ctrl.post_execute_hook()
    finally:
        parent_header_var.reset(later_tok)

    assert [name for name, _ in observed_contexts] == [
        "comm_open",
        "display",
        "send",
        "trait_spark_ui_url",
        "send",
        "trait_cell_finished",
    ]
    assert all(ctx_val == "comm-ctx-7" for _, ctx_val in observed_contexts)


def test_abandoned_comm_scope_closed_on_silent_execute_request():
    """Silent execute_request pre_execute cleans up an abandoned comm scope."""
    ip, _ = _make_mock_ipython()
    ctrl = spark_connect.SparkConnectCellController()
    ctrl.attach_ipython(ip)

    ip.parent_header = {"header": {"msg_type": "comm_msg", "msg_id": "comm-abort-silent"}}
    ctrl.pre_execute_hook()
    assert ctrl.current_cell_id() is not None

    ip.parent_header = {"header": {"msg_type": "execute_request", "msg_id": "exec-silent"}}
    ctrl.pre_execute_hook()
    assert ctrl.current_cell_id() is None
    ctrl.post_execute_hook()
    assert ctrl.current_cell_id() is None


def test_freeze_mid_widget_creation_cleans_up_without_sending():
    """Disabling monitoring during display() finalizes created widget and skips telemetry."""
    ip, _ = _make_mock_ipython()
    ctrl = spark_connect.SparkConnectCellController()
    ctrl.attach_ipython(ip)

    entered_display = threading.Event()
    release_display = threading.Event()
    displayed: list[ManagedSparkCellWidget] = []

    def _blocking_display(w):
        w.send = mock.MagicMock()
        displayed.append(w)
        entered_display.set()
        assert release_display.wait(timeout=3.0)

    ctrl.pre_run_cell_hook()
    cid = ctrl.current_cell_id()
    assert cid is not None
    result: list[bool] = []

    with mock.patch("IPython.display.display", side_effect=_blocking_display):
        t_upd = threading.Thread(
            target=lambda: result.append(
                ctrl.update_operation(
                    cid,
                    {
                        "operation_id": "op-freeze",
                        "operation_state": "OPERATION_STATE_RUNNING",
                        "version": 1,
                        "jobs": [{"job_id": 1, "status": "JOB_STATUS_RUNNING", "num_tasks": 1}],
                    },
                )
            )
        )
        t_upd.start()
        assert entered_display.wait(timeout=3.0)

        t_freeze = threading.Thread(target=lambda: cell_monitoring.set_enabled(False))
        t_freeze.start()
        # Give t_freeze a moment to mark state.frozen=True before releasing display().
        t_freeze.join(timeout=0.05)
        release_display.set()
        t_upd.join(timeout=3.0)
        t_freeze.join(timeout=3.0)

    assert result == [False]
    assert len(displayed) == 1
    assert displayed[0].cell_finished is True
    displayed[0].send.assert_not_called()


def test_update_operation_aborts_if_state_evicted_before_widget_lock():
    """Delayed update_operation does not create a widget after complete_operation evicts state."""
    ip, _ = _make_mock_ipython()
    ctrl = spark_connect.SparkConnectCellController()
    ctrl.attach_ipython(ip)

    ctrl.pre_run_cell_hook()
    cid = ctrl.current_cell_id()
    assert cid is not None
    assert ctrl.register_operation(cid, "op-evict")
    ctrl.post_run_cell_hook()

    state = ctrl._cells[cid]
    real_lock = state.widget_lock
    poller_at_lock = threading.Event()
    allow_poller_lock = threading.Event()
    poller_thread_holder: list[threading.Thread] = []

    class _GateLock:
        def __enter__(self):
            if poller_thread_holder and threading.current_thread() is poller_thread_holder[0]:
                poller_at_lock.set()
                assert allow_poller_lock.wait(timeout=3.0)
            return real_lock.__enter__()

        def __exit__(self, exc_type, exc_val, exc_tb):
            return real_lock.__exit__(exc_type, exc_val, exc_tb)

    state.widget_lock = _GateLock()  # type: ignore[assignment]
    result: list[bool] = []

    with mock.patch("IPython.display.display") as mock_display:
        t_poller = threading.Thread(
            target=lambda: result.append(
                ctrl.update_operation(
                    cid,
                    {
                        "operation_id": "op-evict",
                        "operation_state": "OPERATION_STATE_RUNNING",
                        "version": 1,
                        "jobs": [{"job_id": 1, "status": "JOB_STATUS_RUNNING", "num_tasks": 1}],
                    },
                )
            )
        )
        poller_thread_holder.append(t_poller)
        t_poller.start()
        assert poller_at_lock.wait(timeout=3.0)

        ctrl.complete_operation(cid, "op-evict", final_snapshot=None)
        assert cid not in ctrl._cells

        allow_poller_lock.set()
        t_poller.join(timeout=3.0)

        mock_display.assert_not_called()

    assert result == [False]


def test_zmq_shell_and_comm_handle_msg_integration(tmp_path, monkeypatch):
    """Real ZMQInteractiveShell + Comm.handle_msg + ThreadPoolExecutor routes parent_headers."""
    import comm
    import IPython
    import jupyter_client.session as jc_session
    from ipykernel import kernelbase, zmqshell
    from ipykernel.comm import CommManager
    from ipykernel.comm.comm import BaseComm as IpykernelBaseComm
    from traitlets.config import Config

    class _RecordingSession(jc_session.Session):
        def __init__(self) -> None:
            super().__init__(key=b"")
            self.sent: list[dict] = []

        def send(
            self,
            stream,
            msg_or_type,
            content=None,
            parent=None,
            ident=None,
            buffers=None,
            track=False,
            header=None,
            metadata=None,
        ):
            if isinstance(msg_or_type, str):
                msg = self.msg(
                    msg_or_type,
                    content=content,
                    parent=parent,
                    header=header,
                    metadata=metadata,
                )
            else:
                msg = msg_or_type
            self.sent.append(msg)
            return msg

    class _MiniKernel(kernelbase.Kernel):
        def __init__(self, **kwargs) -> None:
            super().__init__(**kwargs)
            self.shell = None
            self.comm_manager = CommManager(kernel=self)

        def set_parent(self, ident, parent, channel="shell") -> None:
            super().set_parent(ident, parent, channel=channel)
            if channel == "shell" and self.shell is not None:
                self.shell.set_parent(parent)

    monkeypatch.setenv("IPYTHONDIR", str(tmp_path))
    cfg = Config()
    cfg.HistoryManager.enabled = False
    cfg.InteractiveShellApp.extensions = []

    session = _RecordingSession()
    pub_sock = object()
    zmqshell.ZMQInteractiveShell.clear_instance()
    _MiniKernel.clear_instance()
    ctrl = spark_connect.SparkConnectCellController()
    shell = None
    try:
        shell = zmqshell.ZMQInteractiveShell.instance(
            config=cfg,
            ipython_dir=str(tmp_path),
            display_pub_class=zmqshell.ZMQDisplayPublisher,
        )
        shell.display_pub.session = session
        shell.display_pub.pub_socket = pub_sock
        shell.display_pub.shell = shell
        kernel = _MiniKernel.instance(session=session, iopub_socket=pub_sock)
        kernel.shell = shell
        shell.kernel = kernel

        monkeypatch.setattr(IPython, "get_ipython", lambda: shell)
        monkeypatch.setattr(comm, "get_comm_manager", lambda: kernel.comm_manager)
        monkeypatch.setattr(comm, "create_comm", IpykernelBaseComm)
        ctrl.attach_ipython(shell)

        comm_parent = {"header": {"msg_type": "comm_msg", "msg_id": "comm-viz-int"}}
        exec_parent = {"header": {"msg_type": "execute_request", "msg_id": "exec-later-int"}}

        viz_comm = comm.create_comm(target_name="test_viz", comm_id="viz-int")
        observed_cids: list[str | None] = []
        bg_ready = threading.Event()
        parent_overwritten = threading.Event()
        bg_done: list[Exception] = []
        bg_thread_holder: list[threading.Thread] = []

        def _on_comm_msg(_msg) -> None:
            def _worker(idx: int) -> str | None:
                cid = ctrl.current_cell_id()
                assert cid is not None
                assert ctrl.register_operation(cid, f"op-int-{idx}")
                return cid

            with futures.ThreadPoolExecutor(max_workers=2) as pool:
                observed_cids.extend(pool.map(_worker, range(2)))

            cid = observed_cids[0]
            assert cid is not None

            def _bg_poller() -> None:
                try:
                    bg_ready.set()
                    assert parent_overwritten.wait(timeout=3.0)
                    assert ctrl.update_operation(
                        cid,
                        {
                            "operation_id": "op-int-0",
                            "operation_state": "OPERATION_STATE_RUNNING",
                            "version": 1,
                            "jobs": [{"job_id": 1, "status": "JOB_STATUS_RUNNING", "num_tasks": 2}],
                        },
                    )
                    ctrl.complete_operation(
                        cid,
                        "op-int-0",
                        final_snapshot={
                            "operation_id": "op-int-0",
                            "operation_state": "OPERATION_STATE_SUCCEEDED",
                            "version": 2,
                            "final_snapshot": True,
                            "jobs": [
                                {"job_id": 1, "status": "JOB_STATUS_SUCCEEDED", "num_tasks": 2}
                            ],
                        },
                    )
                    ctrl.complete_operation(cid, "op-int-1")
                except Exception as exc:
                    bg_done.append(exc)

            t_bg = threading.Thread(target=_bg_poller)
            bg_thread_holder.append(t_bg)
            t_bg.start()
            assert bg_ready.wait(timeout=3.0)

        viz_comm.on_msg(_on_comm_msg)
        session.sent.clear()
        kernel.set_parent([], comm_parent)
        viz_comm.handle_msg({"content": {"data": {"action": "GET_DATA_BATCH"}}})

        # Simulate kernel moving on to a subsequent execute_request before background emission.
        kernel.set_parent([], exec_parent)
        parent_overwritten.set()
        bg_thread_holder[0].join(timeout=3.0)

        assert not bg_done
        assert len(observed_cids) == 2 and len(set(observed_cids)) == 1
        assert ctrl.current_cell_id() is None

        msg_summary = [
            (m["header"]["msg_type"], m["parent_header"].get("msg_id")) for m in session.sent
        ]
        msg_types = [t for t, _ in msg_summary]
        assert "comm_open" in msg_types
        assert msg_types.count("display_data") == 1
        assert "comm_msg" in msg_types
        display_idx = msg_types.index("display_data")
        assert msg_types.index("comm_open") < display_idx
        telemetry_indices = [
            idx
            for idx, m in enumerate(session.sent)
            if m["header"]["msg_type"] == "comm_msg"
            and m.get("content", {}).get("data", {}).get("content", {}).get("type")
            == "operation_telemetry"
        ]
        assert len(telemetry_indices) == 2
        assert all(idx > display_idx for idx in telemetry_indices)
        assert all(pid == "comm-viz-int" for _, pid in msg_summary)
    finally:
        if shell is not None:
            ctrl.detach_ipython(shell)
        _MiniKernel.clear_instance()
        zmqshell.ZMQInteractiveShell.clear_instance()
