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
"""Spark Connect IPython controller, poller factory, and lazy AnyWidget manager.

Unlike Classic PySpark (`kernelextension.py`), Spark Connect telemetry arrives
in the kernel via `GetStatus` RPC polling rather than a driver-to-kernel TCP
socket, and Spark UI URLs come from the `ManagedSparkSession` itself. Importing
or loading this module therefore starts no local TCP listener, spawns no GCE
metadata probe threads, and never displays a widget until an operation in the
cell actually reports Spark jobs.
"""

import contextvars
import dataclasses
import logging
import sys
import threading
import uuid
from collections.abc import Callable
from typing import Any

import IPython
import IPython.display

import managed_spark_cell_monitoring as cell_monitoring
from managed_spark_cell_monitoring.widget import ManagedSparkCellWidget

logger = logging.getLogger(__name__)

_CURRENT_CELL_VAR: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "managed_spark_cell_monitoring_connect_cell_id", default=None
)


def _is_shell_running_cell(ipython: Any) -> bool:
    """True when called synchronously from inside `ipython.run_cell(...)`."""
    try:
        frame = sys._getframe(1)
    except ValueError:
        return False
    saw_ast_nodes = False
    while frame is not None:
        if frame.f_locals.get("self") is ipython:
            name = frame.f_code.co_name
            if name in ("run_cell", "_run_cell", "run_cell_async"):
                if frame.f_locals.get("silent", False):
                    return False
                if not frame.f_locals.get("store_history", True):
                    return False
                return True
            if name == "run_ast_nodes":
                result = frame.f_locals.get("result")
                info = getattr(result, "info", None)
                if info is not None and (
                    getattr(info, "silent", False)
                    or not getattr(info, "store_history", True)
                ):
                    return False
                saw_ast_nodes = True
        frame = frame.f_back
    return saw_ast_nodes


_register_cellmonitor_magic = cell_monitoring.register_cellmonitor_magic


@dataclasses.dataclass
class _CellExecutionState:
    cell_id: str
    display_context: contextvars.Context
    spark_ui_url: str = ""
    widget: ManagedSparkCellWidget | None = None
    cell_finished: bool = False
    frozen: bool = False
    active_operations: set[str] = dataclasses.field(default_factory=set)


class SparkConnectCellController:
    """Thread-safe controller bridging Spark Connect pollers to AnyWidgets."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._notebook_session_id = str(uuid.uuid4())
        self._active_cell_id: str | None = None
        self._active_token: contextvars.Token[str | None] | None = None
        self._cells: dict[str, _CellExecutionState] = {}
        self._attached_shells: dict[int, Any] = {}
        cell_monitoring.add_state_listener(self._on_monitoring_state_changed)

    def attach_ipython(self, ipython: Any) -> None:
        """Registers `%cellmonitor` and cell lifecycle hooks on `ipython`."""
        if ipython is None:
            return
        shell_id = id(ipython)
        with self._lock:
            if self._attached_shells.get(shell_id) is ipython:
                return
            self._attached_shells[shell_id] = ipython

        _register_cellmonitor_magic(ipython)
        events = getattr(ipython, "events", None)
        if events is not None and hasattr(events, "register"):
            events.register("pre_run_cell", self.pre_run_cell_hook)
            events.register("post_run_cell", self.post_run_cell_hook)
        if self._active_cell_id is None and _is_shell_running_cell(ipython):
            self.pre_run_cell_hook()

    def detach_ipython(self, ipython: Any) -> None:
        """Unregisters cell lifecycle hooks from `ipython`."""
        if ipython is None:
            return
        with self._lock:
            if self._attached_shells.pop(id(ipython), None) is None:
                return

        events = getattr(ipython, "events", None)
        if events is not None and hasattr(events, "unregister"):
            for event_name, hook in (
                ("pre_run_cell", self.pre_run_cell_hook),
                ("post_run_cell", self.post_run_cell_hook),
            ):
                try:
                    events.unregister(event_name, hook)
                except ValueError:
                    pass
        self.freeze_all_widgets()

    def ensure_current_shell_attached(self) -> bool:
        """Lazily attaches hooks to `IPython.get_ipython()` if running in one."""
        ip = IPython.get_ipython()
        if ip is None:
            return False
        if id(ip) not in self._attached_shells:
            self.attach_ipython(ip)
        return True

    def is_enabled(self) -> bool:
        return bool(cell_monitoring.is_enabled())

    def current_cell_id(self) -> str | None:
        """Returns the active IPython cell's `run_id`, or `None` if inactive."""
        if not cell_monitoring.is_enabled():
            return None
        cid = _CURRENT_CELL_VAR.get()
        with self._lock:
            resolved = cid or self._active_cell_id
            if resolved is None or resolved not in self._cells:
                return None
            if self._cells[resolved].frozen:
                return None
            return resolved

    def pre_run_cell_hook(self, *args: Any, **kwargs: Any) -> None:
        cell_info = args[0] if args else kwargs.get("info")
        if cell_info is not None and (
            getattr(cell_info, "silent", False) or not getattr(cell_info, "store_history", True)
        ):
            with self._lock:
                self._active_cell_id = None
            return

        if not cell_monitoring.is_enabled():
            with self._lock:
                self._active_cell_id = None
            return

        cell_id = str(uuid.uuid4())
        display_ctx = contextvars.copy_context()
        token = _CURRENT_CELL_VAR.set(cell_id)
        with self._lock:
            self._active_cell_id = cell_id
            self._active_token = token
            self._cells[cell_id] = _CellExecutionState(
                cell_id=cell_id,
                display_context=display_ctx,
            )

    def post_run_cell_hook(self, result: Any = None) -> None:
        if result is not None and getattr(result, "info", None) is not None:
            if getattr(result.info, "silent", False) or not getattr(
                result.info, "store_history", True
            ):
                return

        widget_to_finish: ManagedSparkCellWidget | None = None
        widget_to_mark_done: ManagedSparkCellWidget | None = None
        with self._lock:
            token = self._active_token
            self._active_token = None
            cell_id = self._active_cell_id
            self._active_cell_id = None
            if token is not None:
                try:
                    _CURRENT_CELL_VAR.reset(token)
                except ValueError:
                    pass
            state = self._cells.get(cell_id) if cell_id else None
            if state is not None:
                state.cell_finished = True
                widget_to_mark_done = state.widget
                if not state.active_operations:
                    self._cells.pop(cell_id, None)
                    widget_to_finish = state.widget

        if widget_to_mark_done is not None:
            widget_to_mark_done.cell_finished = True
        if widget_to_finish is not None:
            widget_to_finish.cleanup()

    def register_operation(self, cell_id: str, operation_id: str, spark_ui_url: str = "") -> bool:
        """Marks `operation_id` as active under `cell_id`."""
        if not cell_id or not operation_id or not cell_monitoring.is_enabled():
            return False
        widget_to_update_url: ManagedSparkCellWidget | None = None
        with self._lock:
            state = self._cells.get(cell_id)
            if state is None or state.frozen:
                return False
            state.active_operations.add(operation_id)
            if spark_ui_url and spark_ui_url != state.spark_ui_url:
                state.spark_ui_url = spark_ui_url
                widget_to_update_url = state.widget
        if widget_to_update_url is not None:
            widget_to_update_url.spark_ui_url = spark_ui_url
        return True

    def update_operation(
        self,
        cell_id: str,
        snapshot: dict[str, Any],
        spark_ui_url: str = "",
    ) -> bool:
        """Lazily displays the cell widget (once jobs exist) and sends snapshot."""
        if not cell_id or not isinstance(snapshot, dict) or not cell_monitoring.is_enabled():
            return False

        widget: ManagedSparkCellWidget | None = None
        display_ctx: contextvars.Context | None = None
        update_url = False

        with self._lock:
            state = self._cells.get(cell_id)
            if state is None or state.frozen:
                return False
            if spark_ui_url and spark_ui_url != state.spark_ui_url:
                state.spark_ui_url = spark_ui_url
                update_url = True

            widget = state.widget
            has_jobs = widget is not None or bool(snapshot.get("jobs"))
            if not has_jobs:
                return False

            if widget is None:
                widget = ManagedSparkCellWidget(
                    run_id=cell_id,
                    session_id=self._notebook_session_id,
                    spark_ui_url=state.spark_ui_url,
                )
                widget.cell_finished = state.cell_finished
                state.widget = widget
                display_ctx = state.display_context
                update_url = False

        if display_ctx is not None and widget is not None:
            try:
                display_ctx.run(IPython.display.display, widget)
            except Exception:
                logger.debug("Failed to display Spark Connect cell widget", exc_info=True)
                return False
        elif update_url and widget is not None:
            widget.spark_ui_url = spark_ui_url

        if widget is not None:
            widget.send_operation_telemetry(snapshot)
            return True
        return False

    def complete_operation(
        self,
        cell_id: str,
        operation_id: str,
        final_snapshot: dict[str, Any] | None = None,
        spark_ui_url: str = "",
    ) -> None:
        """Pushes optional final snapshot and releases operation tracking."""
        if final_snapshot is not None:
            self.update_operation(cell_id, final_snapshot, spark_ui_url=spark_ui_url)

        widget_to_finish: ManagedSparkCellWidget | None = None
        with self._lock:
            state = self._cells.get(cell_id)
            if state is None:
                return
            state.active_operations.discard(operation_id)
            if state.cell_finished and not state.active_operations:
                self._cells.pop(cell_id, None)
                widget_to_finish = state.widget

        if widget_to_finish is not None:
            widget_to_finish.cell_finished = True
            widget_to_finish.cleanup()

    def freeze_all_widgets(self) -> None:
        """Stops tracking live Spark Connect cells while leaving panels intact."""
        with self._lock:
            states = list(self._cells.values())
            for st in states:
                st.frozen = True
            self._cells.clear()
            self._active_cell_id = None
            self._active_token = None
        for st in states:
            if st.widget is not None:
                try:
                    st.widget.cell_finished = True
                    st.widget.cleanup()
                except Exception:
                    logger.debug(
                        "Error freezing Spark Connect cell widget",
                        exc_info=True,
                    )

    def _on_monitoring_state_changed(self, enabled: bool) -> None:
        if not enabled:
            self.freeze_all_widgets()


_CONTROLLER: SparkConnectCellController | None = None
_CONTROLLER_LOCK = threading.Lock()


def get_spark_connect_controller() -> SparkConnectCellController:
    """Returns the process-wide `SparkConnectCellController` singleton."""
    global _CONTROLLER
    if _CONTROLLER is None:
        with _CONTROLLER_LOCK:
            if _CONTROLLER is None:
                _CONTROLLER = SparkConnectCellController()
    return _CONTROLLER


def create_session_poller(
    client: Any,
    *,
    spark_ui_url_getter: Callable[[], str] | None = None,
    on_fallback: Callable[[list[str]], None] | None = None,
    controller: Any = None,
    poll_interval_s: float = 0.25,
    rpc_timeout_s: float = 3.0,
) -> Any:
    """Creates a `CellMonitorPoller` bound to `client`."""
    from managed_spark_cell_monitoring._spark_connect_poller import CellMonitorPoller

    return CellMonitorPoller(
        client,
        spark_ui_url_getter=spark_ui_url_getter,
        on_fallback=on_fallback,
        controller=controller,
        poll_interval_s=poll_interval_s,
        rpc_timeout_s=rpc_timeout_s,
    )


def load_ipython_extension(ipython: Any) -> None:
    """IPython extension entrypoint for Spark Connect notebooks."""
    get_spark_connect_controller().attach_ipython(ipython)


def unload_ipython_extension(ipython: Any) -> None:
    """Unloads the Spark Connect IPython extension."""
    get_spark_connect_controller().detach_ipython(ipython)
