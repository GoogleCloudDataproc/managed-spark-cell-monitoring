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
"""Background Spark Connect `GetStatus` poller for notebook cell monitoring."""

import contextvars
import dataclasses
import logging
import sys
import threading
from collections.abc import Callable
from typing import Any

import grpc

from managed_spark_cell_monitoring._third_party.apache_spark import (
    GRPC_GET_STATUS_METHOD,
    cell_monitor_pb2,
    spark_connect_status_pb2,
)

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL_SECONDS = 0.25
DEFAULT_RPC_TIMEOUT_SECONDS = 3.0

_OpState = spark_connect_status_pb2.GetStatusResponse.OperationStatus.OperationState
_TERMINAL_OP_STATES = frozenset(
    (
        _OpState.OPERATION_STATE_SUCCEEDED,
        _OpState.OPERATION_STATE_FAILED,
        _OpState.OPERATION_STATE_CANCELLED,
    )
)
_VALID_OUTCOMES = frozenset(("SUCCEEDED", "FAILED", "CANCELLED"))


def _normalize_outcome(outcome: str | None) -> str:
    raw = (outcome or "SUCCEEDED").strip().upper()
    if raw.startswith("OPERATION_STATE_"):
        raw = raw[len("OPERATION_STATE_") :]
    if raw == "CANCELED":
        raw = "CANCELLED"
    return raw if raw in _VALID_OUTCOMES else "SUCCEEDED"


@dataclasses.dataclass
class _TrackedOperation:
    operation_id: str
    cell_id: str
    display_context: contextvars.Context = dataclasses.field(
        default_factory=contextvars.copy_context
    )
    last_version: int = 0
    saw_telemetry: bool = False
    saw_spark_tasks: bool = False
    empty_polls_after_tasks: int = 0
    final_reconciled: bool = False
    link_suppressed: bool = True


def _get_active_controller() -> Any:
    """Returns the loaded `SparkConnectCellController`, or `None` if inactive."""
    mod = sys.modules.get("managed_spark_cell_monitoring.spark_connect")
    if mod is None:
        return None
    getter = getattr(mod, "get_spark_connect_controller", None)
    if not callable(getter):
        return None
    try:
        return getter()
    except Exception:
        logger.debug("Failed to access SparkConnectCellController", exc_info=True)
        return None


def _telemetry_to_snapshot(
    operation_id: str,
    op_state_enum: int,
    telemetry: cell_monitor_pb2.CellMonitorTelemetry,
    execution_outcome: str | None = None,
) -> dict[str, Any]:
    """Converts a `CellMonitorTelemetry` protobuf into a widget snapshot dict."""
    state_name = _OpState.Name(op_state_enum)
    snapshot: dict[str, Any] = {
        "operation_id": operation_id,
        "operation_state": state_name,
        "version": int(telemetry.version),
        "unchanged": bool(telemetry.unchanged),
        "final_snapshot": bool(telemetry.final_snapshot),
    }
    if execution_outcome:
        snapshot["execution_outcome"] = _normalize_outcome(execution_outcome)
    if telemetry.unchanged:
        return snapshot

    jobs: list[dict[str, Any]] = []
    for job in telemetry.jobs:
        job_dict: dict[str, Any] = {
            "job_id": int(job.job_id),
            "status": cell_monitor_pb2.CellMonitorTelemetry.JobStatus.Name(job.status),
            "num_tasks": int(job.num_tasks),
            "num_completed_tasks": int(job.num_completed_tasks),
            "num_active_tasks": int(job.num_active_tasks),
            "num_failed_tasks": int(job.num_failed_tasks),
        }
        if job.HasField("submission_time_ms"):
            job_dict["submission_time_ms"] = int(job.submission_time_ms)
        if job.HasField("completion_time_ms"):
            job_dict["completion_time_ms"] = int(job.completion_time_ms)
        jobs.append(job_dict)
    snapshot["jobs"] = jobs

    if telemetry.HasField("running_tasks"):
        rt = telemetry.running_tasks
        snapshot["running_tasks"] = {
            "slot_width_ms": int(rt.slot_width_ms),
            "start_time_ms": int(rt.start_time_ms),
            "values": [int(v) for v in rt.values],
        }

    return snapshot


class CellMonitorPoller:
    """Polls `SparkConnectService/GetStatus` while cell operations are active.

    One poller lives on each `ManagedSparkSession`. Outside IPython (or when
    `%cellmonitor` is off), no background thread is started and no `GetStatus`
    RPCs are sent.
    """

    def __init__(
        self,
        client: Any,
        spark_ui_url_getter: Callable[[], str] | None = None,
        on_fallback: Callable[[list[str]], None] | None = None,
        controller: Any = None,
        poll_interval_s: float = DEFAULT_POLL_INTERVAL_SECONDS,
        rpc_timeout_s: float = DEFAULT_RPC_TIMEOUT_SECONDS,
    ) -> None:
        self._client = client
        self._spark_ui_url_getter = spark_ui_url_getter
        self._on_fallback = on_fallback
        self._explicit_controller = controller
        self._poll_interval_s = max(0.01, float(poll_interval_s))
        self._rpc_timeout_s = max(0.1, float(rpc_timeout_s))

        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._active_ops: dict[str, _TrackedOperation] = {}
        self._closed = False
        self._unsupported = False
        self._confirmed = False
        self._wake_immediately = False
        self._thread: threading.Thread | None = None
        self._rpc_callable: Any = None

    @property
    def is_supported(self) -> bool:
        return not self._unsupported

    def _get_controller(self) -> Any:
        if self._explicit_controller is not None:
            return self._explicit_controller
        return _get_active_controller()

    def _spark_ui_url(self) -> str:
        if self._spark_ui_url_getter is None:
            return ""
        try:
            return self._spark_ui_url_getter() or ""
        except Exception:
            return ""

    def should_suppress_progress_bar(self, operation_id: str | None) -> bool:
        """True when `operation_id` is actively monitored by Cell Monitor."""
        if not operation_id or self._unsupported or self._closed:
            return False
        ctrl = self._get_controller()
        if ctrl is not None:
            is_enabled = getattr(ctrl, "is_enabled", None)
            if callable(is_enabled) and not is_enabled():
                return False
        with self._lock:
            return operation_id in self._active_ops and not self._unsupported

    def note_spark_tasks_seen(self, operation_id: str | None) -> None:
        """Records that PySpark observed Spark stage tasks for `operation_id`."""
        if not operation_id:
            return
        with self._lock:
            tracked = self._active_ops.get(operation_id)
            if tracked is not None:
                tracked.saw_spark_tasks = True

    def begin_operation(self, operation_id: str | None) -> bool:
        """Registers `operation_id` if running in an active monitored cell."""
        if not operation_id or self._unsupported or self._closed:
            return False
        ctrl = self._get_controller()
        if ctrl is None:
            return False
        try:
            cell_id = ctrl.current_cell_id()
        except Exception:
            logger.debug("Failed reading current_cell_id()", exc_info=True)
            return False
        if not cell_id:
            return False

        ui_url = self._spark_ui_url()
        try:
            registered = ctrl.register_operation(cell_id, operation_id, spark_ui_url=ui_url)
        except Exception:
            logger.debug(
                "Failed registering operation %s with cell controller",
                operation_id,
                exc_info=True,
            )
            return False
        if not registered:
            return False

        with self._cond:
            if self._closed or self._unsupported:
                return False
            self._active_ops[operation_id] = _TrackedOperation(
                operation_id=operation_id,
                cell_id=cell_id,
            )
            self._wake_immediately = True
            self._ensure_thread_locked()
            self._cond.notify_all()
        return True

    def finish_operation(
        self,
        operation_id: str | None,
        outcome: str = "SUCCEEDED",
    ) -> None:
        """Performs final `GetStatus` (`wait_for_final`) reconciliation."""
        if not operation_id:
            return
        normalized_outcome = _normalize_outcome(outcome)
        with self._cond:
            tracked = self._active_ops.pop(operation_id, None)
            closed = self._closed
            unsupported = self._unsupported
            self._cond.notify_all()

        if tracked is None:
            return

        ctrl = self._get_controller()
        ui_url = self._spark_ui_url()
        if not closed and not unsupported and not getattr(self._client, "_closed", False):
            self._poll_once(
                [tracked],
                ctrl=ctrl,
                ui_url=ui_url,
                is_final=True,
                execution_outcome=normalized_outcome,
            )

        with self._lock:
            needs_synthetic_closeout = (
                ctrl is not None
                and tracked.saw_telemetry
                and not tracked.final_reconciled
                and not self._unsupported
            )
            last_ver = tracked.last_version
            if needs_synthetic_closeout:
                tracked.final_reconciled = True

        if needs_synthetic_closeout and ctrl is not None:
            synthetic_snapshot = {
                "operation_id": operation_id,
                "operation_state": f"OPERATION_STATE_{normalized_outcome}",
                "execution_outcome": normalized_outcome,
                "version": last_ver,
                "unchanged": True,
                "final_snapshot": False,
            }
            try:
                ctrl.update_operation(tracked.cell_id, synthetic_snapshot, spark_ui_url=ui_url)
            except Exception:
                logger.debug(
                    "Failed pushing synthetic terminal reconciliation for %s",
                    operation_id,
                    exc_info=True,
                )

        if ctrl is not None:
            try:
                ctrl.complete_operation(
                    tracked.cell_id,
                    operation_id,
                    final_snapshot=None,
                    spark_ui_url=ui_url,
                )
            except Exception:
                logger.debug(
                    "Failed completing operation %s on cell controller",
                    operation_id,
                    exc_info=True,
                )

    def close(self) -> None:
        """Stops the background poller thread and releases references."""
        thread: threading.Thread | None = None
        with self._cond:
            if self._closed:
                return
            self._closed = True
            self._active_ops.clear()
            thread = self._thread
            self._thread = None
            self._cond.notify_all()

        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=1.0)
        self._rpc_callable = None
        self._spark_ui_url_getter = None
        self._on_fallback = None

    def _ensure_thread_locked(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        thread = threading.Thread(
            target=self._run_loop,
            name="managed-spark-cell-monitor-poller",
            daemon=True,
        )
        self._thread = thread
        thread.start()

    def _run_loop(self) -> None:
        while True:
            with self._cond:
                while not self._closed and not self._unsupported and not self._active_ops:
                    self._cond.wait()
                if self._closed or self._unsupported:
                    return
                if not self._wake_immediately:
                    self._cond.wait(timeout=self._poll_interval_s)
                    if self._closed or self._unsupported:
                        return
                self._wake_immediately = False
                ops = list(self._active_ops.values())

            if not ops:
                continue
            ctrl = self._get_controller()
            if ctrl is None:
                continue
            self._poll_once(ops, ctrl=ctrl, ui_url=self._spark_ui_url(), is_final=False)

    def _get_rpc_callable(self) -> Any:
        if self._rpc_callable is None:
            channel = getattr(self._client, "_channel", None)
            if channel is None:
                return None
            self._rpc_callable = channel.unary_unary(
                GRPC_GET_STATUS_METHOD,
                request_serializer=spark_connect_status_pb2.GetStatusRequest.SerializeToString,
                response_deserializer=spark_connect_status_pb2.GetStatusResponse.FromString,
            )
        return self._rpc_callable

    def _build_request(
        self,
        ops: list[_TrackedOperation],
        *,
        wait_for_final: bool = False,
    ) -> spark_connect_status_pb2.GetStatusRequest:
        req = spark_connect_status_pb2.GetStatusRequest(
            session_id=str(getattr(self._client, "_session_id", "") or ""),
        )
        builder = getattr(self._client, "_builder", None)
        user_agent = getattr(builder, "userAgent", None)
        if user_agent:
            req.client_type = str(user_agent)
        server_session_id = getattr(self._client, "_server_session_id", None)
        if server_session_id is not None:
            req.client_observed_server_side_session_id = str(server_session_id)
        user_id = getattr(self._client, "_user_id", None)
        if user_id:
            req.user_context.user_id = str(user_id)

        tel_req = cell_monitor_pb2.CellMonitorTelemetryRequest(
            wait_for_final=bool(wait_for_final),
        )
        with self._lock:
            for op in ops:
                req.operation_status.operation_ids.append(op.operation_id)
                if op.last_version > 0:
                    tel_req.since_versions[op.operation_id] = op.last_version

        req.operation_status.extensions.add().Pack(tel_req)
        return req

    def _mark_unsupported(self, extra_ops: list[_TrackedOperation] | None = None) -> None:
        fallback_ops: list[_TrackedOperation] = []
        with self._cond:
            if self._unsupported:
                return
            self._unsupported = True
            seen = set()
            for tracked in list(self._active_ops.values()) + (extra_ops or []):
                if tracked.link_suppressed and tracked.operation_id not in seen:
                    tracked.link_suppressed = False
                    seen.add(tracked.operation_id)
                    fallback_ops.append(tracked)
            self._active_ops.clear()
            self._cond.notify_all()

        cb = self._on_fallback
        if cb is not None:
            for tracked in fallback_ops:
                try:
                    tracked.display_context.run(cb, [tracked.operation_id])
                except Exception:
                    logger.debug(
                        "Error invoking cell monitor fallback callback",
                        exc_info=True,
                    )

    def _poll_once(
        self,
        ops: list[_TrackedOperation],
        ctrl: Any,
        ui_url: str,
        is_final: bool,
        execution_outcome: str | None = None,
    ) -> None:
        if not ops or self._unsupported or self._closed:
            return
        rpc = self._get_rpc_callable()
        if rpc is None:
            return

        req = self._build_request(ops, wait_for_final=is_final)
        builder = getattr(self._client, "_builder", None)
        metadata = None
        if builder is not None and hasattr(builder, "metadata"):
            try:
                metadata = builder.metadata()
            except Exception:
                metadata = None

        try:
            resp = rpc(
                req,
                timeout=self._rpc_timeout_s,
                metadata=metadata,
            )
        except grpc.RpcError as exc:
            code_fn = getattr(exc, "code", None)
            code = code_fn() if callable(code_fn) else None
            if code == grpc.StatusCode.UNIMPLEMENTED:
                logger.debug(
                    "Spark Connect GetStatus is UNIMPLEMENTED; falling back "
                    "to legacy progress bars and operation links for session."
                )
                self._mark_unsupported(extra_ops=ops)
            else:
                logger.debug(
                    "Transient GetStatus poll error (final=%s): %s",
                    is_final,
                    exc,
                )
            return
        except Exception:
            logger.debug(
                "Unexpected error polling Spark Connect GetStatus",
                exc_info=True,
            )
            return

        by_id = {op.operation_id: op for op in ops}
        for op_status in resp.operation_statuses:
            tracked = by_id.get(op_status.operation_id)
            if tracked is None:
                continue

            telemetry: cell_monitor_pb2.CellMonitorTelemetry | None = None
            for ext in op_status.extensions:
                if ext.Is(cell_monitor_pb2.CellMonitorTelemetry.DESCRIPTOR):
                    candidate = cell_monitor_pb2.CellMonitorTelemetry()
                    if ext.Unpack(candidate):
                        telemetry = candidate
                        break

            is_terminal_state = op_status.state in _TERMINAL_OP_STATES
            if telemetry is None:
                if (
                    op_status.state != _OpState.OPERATION_STATE_UNKNOWN
                    and tracked.saw_spark_tasks
                    and not tracked.saw_telemetry
                    and not self._confirmed
                ):
                    with self._lock:
                        tracked.empty_polls_after_tasks += 1
                        trigger_fallback = is_final or tracked.empty_polls_after_tasks >= 2
                    if trigger_fallback:
                        logger.debug(
                            "Server returned GetStatus without "
                            "CellMonitorTelemetry on task-bearing operation %s; "
                            "falling back to legacy UI for session.",
                            tracked.operation_id,
                        )
                        self._mark_unsupported(extra_ops=ops)
                        return
                continue

            is_terminal = is_terminal_state or is_final or bool(telemetry.final_snapshot)
            should_push = False
            with self._lock:
                self._confirmed = True
                tracked.saw_telemetry = True
                ver = int(telemetry.version)
                if tracked.final_reconciled:
                    should_push = False
                elif ver < tracked.last_version:
                    should_push = False
                elif telemetry.unchanged and not is_terminal:
                    should_push = False
                else:
                    if ver > tracked.last_version:
                        tracked.last_version = ver
                    if is_terminal:
                        tracked.final_reconciled = True
                    should_push = True

            if should_push and ctrl is not None:
                snapshot = _telemetry_to_snapshot(
                    tracked.operation_id,
                    op_status.state,
                    telemetry,
                    execution_outcome=execution_outcome if is_final else None,
                )
                try:
                    ctrl.update_operation(tracked.cell_id, snapshot, spark_ui_url=ui_url)
                except Exception:
                    logger.debug(
                        "Failed pushing operation telemetry for %s",
                        tracked.operation_id,
                        exc_info=True,
                    )
