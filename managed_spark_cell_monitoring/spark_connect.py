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


def _extract_parent_header(ipython: Any) -> dict[str, Any]:
    """Extracts the Jupyter message header dict from `ipython`, if available."""
    if ipython is None:
        return {}
    parent = getattr(ipython, "parent_header", None)
    if not isinstance(parent, dict) or not parent:
        kernel = getattr(ipython, "kernel", None)
        get_parent = getattr(kernel, "get_parent", None)
        parent = get_parent() if callable(get_parent) else None
    if not isinstance(parent, dict):
        return {}
    nested = parent.get("header")
    if isinstance(nested, dict):
        return nested
    return parent


def _is_shell_running_cell(ipython: Any) -> bool:
    """True when called synchronously from inside `ipython.run_cell(...)`."""
    try:
        frame = sys._getframe(1)
    except ValueError:
        return False
    while frame is not None:
        if (
            frame.f_code.co_name in ("run_cell", "run_cell_async", "run_ast_nodes")
            and frame.f_locals.get("self") is ipython
        ):
            return True
        frame = frame.f_back
    return False


def _register_cellmonitor_magic(ipython: Any) -> None:
    """Registers `%cellmonitor [on|off|status]` on `ipython`."""

    def cellmonitor(line: str = "") -> None:
        arg = (line or "").strip().lower()
        if arg in ("on", "enable", "enabled", "true", "1"):
            cell_monitoring.set_enabled(True)
        elif arg in ("off", "disable", "disabled", "false", "0"):
            cell_monitoring.set_enabled(False)
        elif arg not in ("", "status"):
            print("Usage: %cellmonitor [on|off|status]")
            return
        state = "on" if cell_monitoring.is_enabled() else "off"
        print(f"Managed Spark cell monitoring is {state}")

    try:
        ipython.register_magic_function(cellmonitor, magic_kind="line", magic_name="cellmonitor")
    except Exception:
        logger.debug("Could not register %%cellmonitor magic", exc_info=True)


@dataclasses.dataclass
class _CellExecutionState:
    cell_id: str
    display_context: contextvars.Context
    token: contextvars.Token[str | None] | None = None
    is_comm_scope: bool = False
    comm_msg_id: str | None = None
    spark_ui_url: str = ""
    widget: ManagedSparkCellWidget | None = None
    cell_finished: bool = False
    frozen: bool = False
    active_operations: set[str] = dataclasses.field(default_factory=set)
    widget_lock: threading.Lock = dataclasses.field(default_factory=threading.Lock)


class SparkConnectCellController:
    """Thread-safe controller bridging Spark Connect pollers to AnyWidgets."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._notebook_session_id = str(uuid.uuid4())
        self._active_stack: list[str] = []
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
            events.register("pre_execute", self.pre_execute_hook)
            events.register("post_execute", self.post_execute_hook)
        with self._lock:
            has_active = bool(self._active_stack)
        if not has_active and _is_shell_running_cell(ipython):
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
                ("pre_execute", self.pre_execute_hook),
                ("post_execute", self.post_execute_hook),
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
        with self._lock:
            already_attached = self._attached_shells.get(id(ip)) is ip
        if not already_attached:
            self.attach_ipython(ip)
        return True

    def _resolve_ipython(self) -> Any:
        ip = IPython.get_ipython()
        if ip is not None:
            return ip
        with self._lock:
            if self._attached_shells:
                return next(reversed(self._attached_shells.values()))
        return None

    def is_enabled(self) -> bool:
        return bool(cell_monitoring.is_enabled())

    def current_cell_id(self) -> str | None:
        """Returns the active IPython cell's `run_id`, or `None` if inactive."""
        if not cell_monitoring.is_enabled():
            return None
        cid = _CURRENT_CELL_VAR.get()
        with self._lock:
            if cid is not None:
                state = self._cells.get(cid)
                if state is not None and not state.frozen and not state.cell_finished:
                    return cid
            for stack_cid in reversed(self._active_stack):
                state = self._cells.get(stack_cid)
                if state is not None and not state.frozen and not state.cell_finished:
                    return stack_cid
            return None

    def _close_states_locked(
        self,
        states: list[_CellExecutionState],
    ) -> list[tuple[_CellExecutionState, contextvars.Token[str | None] | None]]:
        closed: list[tuple[_CellExecutionState, contextvars.Token[str | None] | None]] = []
        for state in states:
            token = state.token
            state.token = None
            state.cell_finished = True
            if not state.active_operations:
                self._cells.pop(state.cell_id, None)
            closed.append((state, token))
        return closed

    def _pop_abandoned_comm_scopes_locked(
        self,
        *,
        keep_msg_id: str | None = None,
    ) -> list[tuple[_CellExecutionState, contextvars.Token[str | None] | None]]:
        to_close: list[_CellExecutionState] = []
        while self._active_stack:
            top_id = self._active_stack[-1]
            top_state = self._cells.get(top_id)
            if top_state is None:
                self._active_stack.pop()
                continue
            if not top_state.is_comm_scope:
                break
            if keep_msg_id is not None and top_state.comm_msg_id == keep_msg_id:
                break
            self._active_stack.pop()
            to_close.append(top_state)
        return self._close_states_locked(to_close)

    def _finalize_closed_states(
        self,
        closed: list[tuple[_CellExecutionState, contextvars.Token[str | None] | None]],
    ) -> None:
        for state, token in closed:
            if token is not None:
                try:
                    _CURRENT_CELL_VAR.reset(token)
                except ValueError:
                    pass
            with state.widget_lock:
                with self._lock:
                    widget = state.widget
                    frozen = state.frozen
                    latest_url = state.spark_ui_url
                    should_cleanup = self._cells.get(state.cell_id) is not state or (
                        state.cell_finished and not state.active_operations
                    )
                if widget is not None and not frozen:

                    def _mark_done(
                        w: ManagedSparkCellWidget = widget,
                        url: str = latest_url,
                        do_cleanup: bool = should_cleanup,
                    ) -> None:
                        if url and w.spark_ui_url != url:
                            w.spark_ui_url = url
                        if not w.cell_finished:
                            w.cell_finished = True
                        if do_cleanup:
                            w.cleanup()

                    try:
                        state.display_context.run(_mark_done)
                    except Exception:
                        logger.debug(
                            "Error finalizing Spark Connect cell widget",
                            exc_info=True,
                        )

    def pre_execute_hook(self, *args: Any, **kwargs: Any) -> None:
        ip = kwargs.get("ipython") or self._resolve_ipython()
        header = _extract_parent_header(ip)
        msg_type = header.get("msg_type")
        comm_msg_id = header.get("msg_id")

        keep_msg_id = comm_msg_id if msg_type == "comm_msg" else None
        with self._lock:
            abandoned = self._pop_abandoned_comm_scopes_locked(keep_msg_id=keep_msg_id)
        if abandoned:
            self._finalize_closed_states(abandoned)

        if msg_type != "comm_msg" or not cell_monitoring.is_enabled():
            return

        cell_id = str(uuid.uuid4())
        display_ctx = contextvars.copy_context()
        token = _CURRENT_CELL_VAR.set(cell_id)
        with self._lock:
            self._active_stack.append(cell_id)
            self._cells[cell_id] = _CellExecutionState(
                cell_id=cell_id,
                display_context=display_ctx,
                token=token,
                is_comm_scope=True,
                comm_msg_id=comm_msg_id,
            )

    def post_execute_hook(self, *args: Any, **kwargs: Any) -> None:
        ip = kwargs.get("ipython") or self._resolve_ipython()
        header = _extract_parent_header(ip)
        msg_type = header.get("msg_type")
        comm_msg_id = header.get("msg_id")

        closed: list[tuple[_CellExecutionState, contextvars.Token[str | None] | None]] = []
        with self._lock:
            if not self._active_stack:
                return
            top_id = self._active_stack[-1]
            top_state = self._cells.get(top_id)
            if top_state is None:
                self._active_stack.pop()
                return
            if not top_state.is_comm_scope:
                return

            if msg_type == "comm_msg" and comm_msg_id is not None:
                match_idx: int | None = None
                for idx in range(len(self._active_stack) - 1, -1, -1):
                    cand = self._cells.get(self._active_stack[idx])
                    if cand is not None and not cand.is_comm_scope:
                        break
                    if cand is not None and cand.comm_msg_id == comm_msg_id:
                        match_idx = idx
                        break
                if match_idx is None:
                    return
                popped_ids = self._active_stack[match_idx:]
                del self._active_stack[match_idx:]
                to_close = [self._cells[cid] for cid in reversed(popped_ids) if cid in self._cells]
                closed = self._close_states_locked(to_close)
            elif msg_type == "comm_msg" or not msg_type:
                self._active_stack.pop()
                closed = self._close_states_locked([top_state])
            else:
                closed = self._pop_abandoned_comm_scopes_locked(keep_msg_id=None)

        if closed:
            self._finalize_closed_states(closed)

    def pre_run_cell_hook(self, *args: Any, **kwargs: Any) -> None:
        with self._lock:
            abandoned = self._pop_abandoned_comm_scopes_locked(keep_msg_id=None)
        if abandoned:
            self._finalize_closed_states(abandoned)

        cell_info = args[0] if args else kwargs.get("info")
        if cell_info is not None and (
            getattr(cell_info, "silent", False) or not getattr(cell_info, "store_history", True)
        ):
            return

        if not cell_monitoring.is_enabled():
            return

        cell_id = str(uuid.uuid4())
        display_ctx = contextvars.copy_context()
        token = _CURRENT_CELL_VAR.set(cell_id)
        with self._lock:
            self._active_stack.append(cell_id)
            self._cells[cell_id] = _CellExecutionState(
                cell_id=cell_id,
                display_context=display_ctx,
                token=token,
                is_comm_scope=False,
            )

    def post_run_cell_hook(self, result: Any = None) -> None:
        if result is not None and getattr(result, "info", None) is not None:
            if getattr(result.info, "silent", False) or not getattr(
                result.info, "store_history", True
            ):
                return

        closed: list[tuple[_CellExecutionState, contextvars.Token[str | None] | None]] = []
        with self._lock:
            match_idx: int | None = None
            for idx in range(len(self._active_stack) - 1, -1, -1):
                cand = self._cells.get(self._active_stack[idx])
                if cand is not None and not cand.is_comm_scope:
                    match_idx = idx
                    break
            if match_idx is None:
                return
            popped_ids = self._active_stack[match_idx:]
            del self._active_stack[match_idx:]
            to_close = [self._cells[cid] for cid in reversed(popped_ids) if cid in self._cells]
            closed = self._close_states_locked(to_close)

        if closed:
            self._finalize_closed_states(closed)

    def register_operation(self, cell_id: str, operation_id: str, spark_ui_url: str = "") -> bool:
        """Marks `operation_id` as active under `cell_id`."""
        if not cell_id or not operation_id or not cell_monitoring.is_enabled():
            return False
        with self._lock:
            state = self._cells.get(cell_id)
            if state is None or state.frozen:
                return False
            state.active_operations.add(operation_id)
            url_changed = bool(spark_ui_url and spark_ui_url != state.spark_ui_url)
            if url_changed:
                state.spark_ui_url = spark_ui_url
            has_widget = state.widget is not None
            widget_lock = state.widget_lock
            display_ctx = state.display_context

        if url_changed and has_widget:
            with widget_lock:
                with self._lock:
                    widget = state.widget
                    frozen = state.frozen
                    latest_url = state.spark_ui_url
                if (
                    widget is not None
                    and not frozen
                    and latest_url
                    and widget.spark_ui_url != latest_url
                ):

                    def _update_url() -> None:
                        widget.spark_ui_url = latest_url

                    try:
                        display_ctx.run(_update_url)
                    except Exception:
                        logger.debug("Failed updating Spark UI URL on widget", exc_info=True)
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

        with self._lock:
            state = self._cells.get(cell_id)
            if state is None or state.frozen:
                return False
            if spark_ui_url and spark_ui_url != state.spark_ui_url:
                state.spark_ui_url = spark_ui_url
            if state.widget is None and not snapshot.get("jobs") and not snapshot.get("unchanged"):
                return False
            widget_lock = state.widget_lock
            display_ctx = state.display_context

        with widget_lock:
            with self._lock:
                cur = self._cells.get(cell_id)
                if cur is not state or state.frozen:
                    return False
                if not cell_monitoring.is_enabled():
                    return False
                if spark_ui_url and spark_ui_url != state.spark_ui_url:
                    state.spark_ui_url = spark_ui_url
                widget = state.widget
                ui_url = state.spark_ui_url
                cell_finished = state.cell_finished

            if widget is None:
                if not snapshot.get("jobs"):
                    return False
                try:
                    created = display_ctx.run(
                        ManagedSparkCellWidget,
                        run_id=cell_id,
                        session_id=self._notebook_session_id,
                        spark_ui_url=ui_url,
                        cell_finished=cell_finished,
                    )
                except Exception:
                    logger.debug("Failed to create Spark Connect cell widget", exc_info=True)
                    return False

                try:
                    display_ctx.run(IPython.display.display, created)
                except Exception:
                    logger.debug("Failed to display Spark Connect cell widget", exc_info=True)

                    def _cleanup_failed_display() -> None:
                        created.cleanup(clear_history=True)
                        if hasattr(created, "close"):
                            created.close()

                    try:
                        display_ctx.run(_cleanup_failed_display)
                    except Exception:
                        pass
                    return False

                with self._lock:
                    if state.frozen or not cell_monitoring.is_enabled():
                        frozen_mid_flight = True
                        latest_url = ""
                        latest_finished = True
                        should_cleanup = True
                    else:
                        frozen_mid_flight = False
                        state.widget = created
                        latest_url = state.spark_ui_url
                        latest_finished = state.cell_finished
                        should_cleanup = self._cells.get(cell_id) is not state or (
                            state.cell_finished and not state.active_operations
                        )

                if frozen_mid_flight:

                    def _cleanup_frozen() -> None:
                        created.cell_finished = True
                        created.cleanup()

                    try:
                        display_ctx.run(_cleanup_frozen)
                    except Exception:
                        logger.debug(
                            "Error cleaning up frozen Spark Connect cell widget",
                            exc_info=True,
                        )
                    return False

                def _finish_created_telemetry() -> None:
                    if latest_url and created.spark_ui_url != latest_url:
                        created.spark_ui_url = latest_url
                    if latest_finished and not created.cell_finished:
                        created.cell_finished = True
                    created.send_operation_telemetry(snapshot)
                    if should_cleanup:
                        created.cleanup()

                display_ctx.run(_finish_created_telemetry)
                return True

            def _send_existing_telemetry() -> None:
                if ui_url and widget.spark_ui_url != ui_url:
                    widget.spark_ui_url = ui_url
                if cell_finished and not widget.cell_finished:
                    widget.cell_finished = True
                widget.send_operation_telemetry(snapshot)

            display_ctx.run(_send_existing_telemetry)
            return True

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

        with self._lock:
            state = self._cells.get(cell_id)
            if state is None:
                return
            state.active_operations.discard(operation_id)
            url_changed = bool(spark_ui_url and spark_ui_url != state.spark_ui_url)
            if url_changed:
                state.spark_ui_url = spark_ui_url
            should_finish = state.cell_finished and not state.active_operations and not state.frozen
            if should_finish:
                self._cells.pop(cell_id, None)
            has_widget = state.widget is not None
            widget_lock = state.widget_lock
            display_ctx = state.display_context

        if not (should_finish or (url_changed and has_widget)):
            return

        with widget_lock:
            with self._lock:
                widget = state.widget
                frozen = state.frozen
                latest_url = state.spark_ui_url
                should_cleanup = self._cells.get(cell_id) is not state or (
                    state.cell_finished and not state.active_operations
                )
            if widget is not None and not frozen:

                def _sync_on_complete() -> None:
                    if latest_url and widget.spark_ui_url != latest_url:
                        widget.spark_ui_url = latest_url
                    if should_finish:
                        if not widget.cell_finished:
                            widget.cell_finished = True
                        if should_cleanup:
                            widget.cleanup()

                try:
                    display_ctx.run(_sync_on_complete)
                except Exception:
                    logger.debug(
                        "Error finalizing Spark Connect cell widget on operation completion",
                        exc_info=True,
                    )

    def freeze_all_widgets(self) -> None:
        """Stops tracking live Spark Connect cells while leaving panels intact."""
        tokens_to_reset: list[contextvars.Token[str | None]] = []
        with self._lock:
            states = list(self._cells.values())
            for st in states:
                st.frozen = True
                if st.token is not None:
                    tokens_to_reset.append(st.token)
                    st.token = None
            self._cells.clear()
            self._active_stack.clear()

        for token in reversed(tokens_to_reset):
            try:
                _CURRENT_CELL_VAR.reset(token)
            except ValueError:
                pass

        for st in states:
            with st.widget_lock:
                with self._lock:
                    widget = st.widget
                if widget is not None:

                    def _freeze_widget(w: ManagedSparkCellWidget = widget) -> None:
                        w.cell_finished = True
                        w.cleanup()

                    try:
                        st.display_context.run(_freeze_widget)
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
