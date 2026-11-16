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
"""Unit tests for PS 8 Spark Connect GetStatus `CellMonitorPoller`."""

import contextvars
import threading
from unittest import mock

import grpc
from managed_spark_cell_monitoring._third_party.apache_spark import (
    GRPC_GET_STATUS_METHOD,
    cell_monitor_pb2,
    spark_connect_status_pb2,
)

from managed_spark_cell_monitoring import _spark_connect_poller, spark_connect


class _FakeRpcError(grpc.RpcError):
    def __init__(self, status_code: grpc.StatusCode, details: str = ""):
        super().__init__(details)
        self._code = status_code
        self._details = details

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details


class _FakeController:
    def __init__(self, active_cell_id: str | None = "cell-1"):
        self.active_cell_id = active_cell_id
        self.enabled = True
        self.registered: list[tuple[str, str, str]] = []
        self.updates: list[tuple[str, dict, str]] = []
        self.completed: list[tuple[str, str, dict | None, str]] = []
        self.update_event = threading.Event()

    def is_enabled(self) -> bool:
        return self.enabled

    def current_cell_id(self) -> str | None:
        if not self.enabled:
            return None
        return self.active_cell_id

    def register_operation(self, cell_id: str, operation_id: str, spark_ui_url: str = "") -> bool:
        if not self.enabled:
            return False
        self.registered.append((cell_id, operation_id, spark_ui_url))
        return True

    def update_operation(self, cell_id: str, snapshot: dict, spark_ui_url: str = "") -> bool:
        self.updates.append((cell_id, snapshot, spark_ui_url))
        self.update_event.set()
        return True

    def complete_operation(
        self,
        cell_id: str,
        operation_id: str,
        final_snapshot: dict | None = None,
        spark_ui_url: str = "",
    ) -> None:
        self.completed.append((cell_id, operation_id, final_snapshot, spark_ui_url))


def _pack_telemetry(
    version: int = 1,
    unchanged: bool = False,
    final_snapshot: bool = False,
    jobs: list[cell_monitor_pb2.CellMonitorTelemetry.JobTelemetry] | None = None,
    stages: list[cell_monitor_pb2.CellMonitorTelemetry.StageTelemetry] | None = None,
    running_tasks: cell_monitor_pb2.CellMonitorTelemetry.RunningTasks | None = None,
):
    msg = cell_monitor_pb2.CellMonitorTelemetry(
        version=version,
        unchanged=unchanged,
        final_snapshot=final_snapshot,
        jobs=jobs or [],
        stages=stages or [],
    )
    if running_tasks is not None:
        msg.running_tasks.CopyFrom(running_tasks)
    packed = spark_connect_status_pb2.GetStatusResponse.OperationStatus().extensions.add()
    packed.Pack(msg)
    return packed


def _make_fake_client(rpc_handler):
    client = mock.MagicMock()
    client._closed = False
    client._session_id = "sess-uuid-1"
    client._server_session_id = "srv-sess-1"
    client._user_id = "alice"
    client._builder.userAgent = "managed-spark-test"
    client._builder.metadata.return_value = [("x-goog-user-project", "p1")]

    def _unary_unary(method, request_serializer=None, response_deserializer=None):
        assert method == GRPC_GET_STATUS_METHOD

        def _invoke(req, timeout=None, metadata=None):
            raw_req = request_serializer(req)
            parsed_req = spark_connect_status_pb2.GetStatusRequest.FromString(raw_req)
            resp = rpc_handler(parsed_req, timeout=timeout, metadata=metadata)
            return response_deserializer(resp.SerializeToString())

        return _invoke

    client._channel.unary_unary.side_effect = _unary_unary
    return client


def _unpack_req_telemetry(
    req: spark_connect_status_pb2.GetStatusRequest,
) -> cell_monitor_pb2.CellMonitorTelemetryRequest:
    out = cell_monitor_pb2.CellMonitorTelemetryRequest()
    for ext in req.operation_status.extensions:
        if ext.Is(cell_monitor_pb2.CellMonitorTelemetryRequest.DESCRIPTOR):
            ext.Unpack(out)
            break
    return out


def test_active_polling_since_versions_wait_for_final_and_ps8_fields():
    requests_seen: list[spark_connect_status_pb2.GetStatusRequest] = []
    op_state_enum = spark_connect_status_pb2.GetStatusResponse.OperationStatus

    def _handler(req, timeout=None, metadata=None):
        requests_seen.append(req)
        tel_req = _unpack_req_telemetry(req)
        resp = spark_connect_status_pb2.GetStatusResponse(
            session_id=req.session_id,
            server_side_session_id="srv-sess-1",
        )
        for op_id in req.operation_status.operation_ids:
            since = tel_req.since_versions.get(op_id, 0)
            if since == 0 and not tel_req.wait_for_final:
                ext = _pack_telemetry(
                    version=2,
                    jobs=[
                        cell_monitor_pb2.CellMonitorTelemetry.JobTelemetry(
                            job_id=10,
                            status=cell_monitor_pb2.CellMonitorTelemetry.JOB_STATUS_RUNNING,
                            submission_time_ms=1000,
                            num_tasks=4,
                            num_completed_tasks=2,
                            num_active_tasks=2,
                            num_failed_tasks=0,
                            stage_ids=[1],
                        )
                    ],
                    stages=[
                        cell_monitor_pb2.CellMonitorTelemetry.StageTelemetry(
                            stage_id=1,
                            attempt_id=0,
                            status=cell_monitor_pb2.CellMonitorTelemetry.STAGE_STATUS_RUNNING,
                            num_tasks=4,
                            num_active_tasks=2,
                            num_completed_tasks=2,
                            parent_ids=[],
                            submission_time_ms=1000,
                        )
                    ],
                    running_tasks=cell_monitor_pb2.CellMonitorTelemetry.RunningTasks(
                        slot_width_ms=250,
                        start_time_ms=1000,
                        values=[2],
                    ),
                )
                resp.operation_statuses.append(
                    spark_connect_status_pb2.GetStatusResponse.OperationStatus(
                        operation_id=op_id,
                        state=op_state_enum.OPERATION_STATE_RUNNING,
                        extensions=[ext],
                    )
                )
            elif tel_req.wait_for_final:
                ext = _pack_telemetry(
                    version=3,
                    final_snapshot=True,
                    jobs=[
                        cell_monitor_pb2.CellMonitorTelemetry.JobTelemetry(
                            job_id=10,
                            status=cell_monitor_pb2.CellMonitorTelemetry.JOB_STATUS_SUCCEEDED,
                            submission_time_ms=1000,
                            completion_time_ms=1400,
                            num_tasks=4,
                            num_completed_tasks=4,
                            num_active_tasks=0,
                            num_failed_tasks=0,
                            stage_ids=[1],
                        )
                    ],
                )
                resp.operation_statuses.append(
                    spark_connect_status_pb2.GetStatusResponse.OperationStatus(
                        operation_id=op_id,
                        state=op_state_enum.OPERATION_STATE_TERMINATING,
                        extensions=[ext],
                    )
                )
            else:
                ext = _pack_telemetry(version=2, unchanged=True)
                resp.operation_statuses.append(
                    spark_connect_status_pb2.GetStatusResponse.OperationStatus(
                        operation_id=op_id,
                        state=op_state_enum.OPERATION_STATE_RUNNING,
                        extensions=[ext],
                    )
                )
        return resp

    ctrl = _FakeController(active_cell_id="cell-abc")
    client = _make_fake_client(_handler)
    poller = spark_connect.create_session_poller(
        client,
        spark_ui_url_getter=lambda: "https://console.cloud.google.com/dataproc/s1",
        controller=ctrl,
        poll_interval_s=0.05,
    )
    try:
        assert poller.begin_operation("op-1")
        assert poller.should_suppress_progress_bar("op-1")
        assert ctrl.update_event.wait(timeout=2.0)

        poller.finish_operation("op-1", outcome="SUCCEEDED")
        assert not poller.should_suppress_progress_bar("op-1")

        assert len(ctrl.updates) == 2
        first_snap = ctrl.updates[0][1]
        assert first_snap["version"] == 2
        assert first_snap["operation_state"] == "OPERATION_STATE_RUNNING"
        assert first_snap["jobs"][0]["status"] == "JOB_STATUS_RUNNING"
        assert first_snap["jobs"][0]["num_tasks"] == 4
        assert first_snap["jobs"][0]["submission_time_ms"] == 1000
        assert "name" not in first_snap["jobs"][0]
        assert first_snap["running_tasks"] == {
            "slot_width_ms": 250,
            "start_time_ms": 1000,
            "values": [2],
        }

        last_snap = ctrl.updates[-1][1]
        assert last_snap["version"] == 3
        assert last_snap["final_snapshot"] is True
        assert last_snap["operation_state"] == "OPERATION_STATE_TERMINATING"
        assert last_snap["execution_outcome"] == "SUCCEEDED"
        assert len(ctrl.completed) == 1
        assert ctrl.completed[0][1] == "op-1"

        first_tel_req = _unpack_req_telemetry(requests_seen[0])
        assert first_tel_req.wait_for_final is False
        final_tel_req = _unpack_req_telemetry(requests_seen[-1])
        assert final_tel_req.wait_for_final is True
        assert final_tel_req.since_versions["op-1"] == 2
    finally:
        poller.close()


def test_unchanged_and_rpc_error_on_final_poll_push_terminal_outcome():
    op_state_enum = spark_connect_status_pb2.GetStatusResponse.OperationStatus
    fail_final_rpc = False

    def _handler(req, timeout=None, metadata=None):
        tel_req = _unpack_req_telemetry(req)
        if tel_req.wait_for_final and fail_final_rpc:
            raise _FakeRpcError(grpc.StatusCode.UNAVAILABLE, "transient")
        resp = spark_connect_status_pb2.GetStatusResponse()
        for op_id in req.operation_status.operation_ids:
            since = tel_req.since_versions.get(op_id, 0)
            if since == 0:
                ext = _pack_telemetry(
                    version=4,
                    final_snapshot=False,
                    jobs=[
                        cell_monitor_pb2.CellMonitorTelemetry.JobTelemetry(
                            job_id=1,
                            status=cell_monitor_pb2.CellMonitorTelemetry.JOB_STATUS_RUNNING,
                            num_tasks=8,
                            num_active_tasks=8,
                        )
                    ],
                )
            else:
                # Server state still RUNNING and unchanged=True after wait_for_final timeout
                ext = _pack_telemetry(version=4, unchanged=True, final_snapshot=False)
            resp.operation_statuses.append(
                spark_connect_status_pb2.GetStatusResponse.OperationStatus(
                    operation_id=op_id,
                    state=op_state_enum.OPERATION_STATE_RUNNING,
                    extensions=[ext],
                )
            )
        return resp

    ctrl = _FakeController()
    client = _make_fake_client(_handler)
    poller = _spark_connect_poller.CellMonitorPoller(client, controller=ctrl, poll_interval_s=0.05)
    try:
        # Case 1: unchanged=True + state=RUNNING on final poll -> CANCELLED outcome
        assert poller.begin_operation("op-unchanged")
        assert ctrl.update_event.wait(timeout=2.0)
        poller.finish_operation("op-unchanged", outcome="CANCELLED")

        final_u = [u[1] for u in ctrl.updates if u[1]["operation_id"] == "op-unchanged"][-1]
        assert final_u["unchanged"] is True
        assert final_u["final_snapshot"] is False
        assert final_u["execution_outcome"] == "CANCELLED"

        # Case 2: final poll RPC transiently fails -> synthetic terminal closeout still pushed!
        ctrl.update_event.clear()
        fail_final_rpc = True
        assert poller.begin_operation("op-rpc-fail")
        assert ctrl.update_event.wait(timeout=2.0)
        poller.finish_operation("op-rpc-fail", outcome="FAILED")

        final_f = [u[1] for u in ctrl.updates if u[1]["operation_id"] == "op-rpc-fail"][-1]
        assert final_f["unchanged"] is True
        assert final_f["final_snapshot"] is False
        assert final_f["execution_outcome"] == "FAILED"
    finally:
        poller.close()


def test_multi_operation_request_uses_per_operation_since_versions_map():
    ctrl = _FakeController()
    client = _make_fake_client(lambda req, **kw: spark_connect_status_pb2.GetStatusResponse())
    poller = _spark_connect_poller.CellMonitorPoller(client, controller=ctrl, poll_interval_s=10.0)
    try:
        op_a = _spark_connect_poller._TrackedOperation(
            operation_id="op-a", cell_id="cell-1", last_version=5
        )
        op_b = _spark_connect_poller._TrackedOperation(
            operation_id="op-b", cell_id="cell-1", last_version=0
        )
        req = poller._build_request([op_a, op_b], wait_for_final=False)
        tel_req = _unpack_req_telemetry(req)
        assert dict(tel_req.since_versions) == {"op-a": 5}
        assert tel_req.wait_for_final is False
    finally:
        poller.close()


def test_unimplemented_and_disabled_plugin_fallback():
    ctx_tag = contextvars.ContextVar("cell_ctx_tag", default="wrong")
    fallback_seen: list[tuple[list[str], str]] = []
    done_fallback = threading.Event()

    def _unimplemented_handler(req, timeout=None, metadata=None):
        raise _FakeRpcError(grpc.StatusCode.UNIMPLEMENTED, "Method GetStatus not found")

    def _on_fallback(op_ids: list[str]):
        fallback_seen.append((op_ids, ctx_tag.get()))
        done_fallback.set()

    ctrl = _FakeController()
    poller = _spark_connect_poller.CellMonitorPoller(
        _make_fake_client(_unimplemented_handler),
        on_fallback=_on_fallback,
        controller=ctrl,
        poll_interval_s=0.02,
    )
    try:
        tok = ctx_tag.set("cell-42-ctx")
        try:
            assert poller.begin_operation("op-r30")
        finally:
            ctx_tag.reset(tok)

        assert done_fallback.wait(timeout=2.0)
        assert not poller.is_supported
        assert not poller.should_suppress_progress_bar("op-r30")
        assert fallback_seen == [(["op-r30"], "cell-42-ctx")]
        poller.finish_operation("op-r30")
        assert not poller.begin_operation("op-next")
    finally:
        poller.close()

    # Disabled server plugin even when GetStatus returns OPERATION_STATE_RUNNING on finish
    op_state_enum = spark_connect_status_pb2.GetStatusResponse.OperationStatus

    def _plugin_off_handler(req, timeout=None, metadata=None):
        resp = spark_connect_status_pb2.GetStatusResponse()
        for op_id in req.operation_status.operation_ids:
            resp.operation_statuses.append(
                spark_connect_status_pb2.GetStatusResponse.OperationStatus(
                    operation_id=op_id,
                    state=op_state_enum.OPERATION_STATE_RUNNING,
                    extensions=[],
                )
            )
        return resp

    fallback_ops: list[str] = []
    poller2 = _spark_connect_poller.CellMonitorPoller(
        _make_fake_client(_plugin_off_handler),
        on_fallback=fallback_ops.extend,
        controller=ctrl,
        poll_interval_s=5.0,
    )
    try:
        assert poller2.begin_operation("op-plugin-off")
        poller2.note_spark_tasks_seen("op-plugin-off")
        poller2.finish_operation("op-plugin-off")
        assert not poller2.is_supported
        assert fallback_ops == ["op-plugin-off"]
    finally:
        poller2.close()
