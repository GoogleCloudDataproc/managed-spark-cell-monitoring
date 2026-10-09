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
"""Initialization for Managed Spark Cell Monitoring."""

import importlib.metadata
import logging
import os
import pathlib
import threading

try:
  __version__ = importlib.metadata.version("managed-spark-cell-monitoring")
except importlib.metadata.PackageNotFoundError:
  __version__ = "unknown"

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Runtime on/off switch
#
# Hosts (e.g. an IDE extension with an "in-cell monitoring" setting) can turn
# the monitor off for a kernel without touching the wire protocol: nothing is
# displayed and listener events are dropped at the kernel. The initial value
# comes from the environment so a kernel can start fully off; it can be
# flipped at any time with `set_enabled()` or the `%cellmonitor` magic.
# ---------------------------------------------------------------------------

ENABLED_ENV_VAR = "MANAGED_SPARK_CELL_MONITORING_ENABLED"
_FALSE_VALUES = frozenset({"0", "false", "no", "off"})


def _enabled_from_env(environ=None):
  """Returns False only for an explicit opt-out value; anything else is on."""
  if environ is None:
    environ = os.environ
  return environ.get(ENABLED_ENV_VAR, "").strip().lower() not in _FALSE_VALUES


_enabled = _enabled_from_env()
_enabled_lock = threading.Lock()
_disable_listeners = []


def is_enabled():
  """Returns True when in-cell monitoring is active for this kernel."""
  return _enabled


def set_enabled(enabled):
  """Turns in-cell monitoring on or off for this kernel.

  Switching off removes any monitors currently displayed and stops new cells
  from showing one; switching back on takes effect from the next cell. The
  listener connection itself is left alone so the toggle is reversible.

  Args:
    enabled: True to monitor cells, False to run them without a monitor.

  Returns:
    The new state.
  """
  global _enabled
  enabled = bool(enabled)
  with _enabled_lock:
    changed = enabled != _enabled
    _enabled = enabled
  if changed and not enabled:
    for listener in list(_disable_listeners):
      try:
        listener()
      except Exception:  # pylint: disable=broad-exception-caught
        logger.debug("Error while disabling cell monitoring", exc_info=True)
  return enabled


def add_disable_listener(callback):
  """Registers a callable invoked whenever monitoring is switched off."""
  if callback not in _disable_listeners:
    _disable_listeners.append(callback)


def remove_disable_listener(callback):
  """Unregisters a callable added with `add_disable_listener`."""
  if callback in _disable_listeners:
    _disable_listeners.remove(callback)


def get_jar_path(spark_version: str = "3") -> str:
  """Returns the path to the bundled Scala listener JAR.

  Args:
    spark_version: '3' for Spark 3.5 or '4' for Spark 4.x.

  Returns:
    Absolute path to the listener JAR file as a string.

  Raises:
    FileNotFoundError: If the listener JAR file does not exist.
  """
  listeners_dir = pathlib.Path(__file__).parent / "static" / "listeners"
  version_str = __version__ if __version__ != "unknown" else "0.1.0"
  target_name = f"managed-spark-cell-monitoring-spark{spark_version}-assembly-{version_str}.jar"
  target_path = listeners_dir / target_name
  if not target_path.is_file():
    raise FileNotFoundError(
        f"Listener JAR for Spark {spark_version} not found at '{target_path}'. "
        "Ensure the package is properly installed or run 'sbt assembly'."
    )

  return str(target_path)


def load_ipython_extension(ipython):
  """Load the kernel extension."""
  from . import kernelextension

  kernelextension.load_ipython_extension(ipython)


def unload_ipython_extension(ipython):
  """Unload the kernel extension."""
  pass
