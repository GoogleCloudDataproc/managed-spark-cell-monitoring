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
import pathlib

try:
  __version__ = importlib.metadata.version("managed-spark-cell-monitoring")
except importlib.metadata.PackageNotFoundError:
  __version__ = "unknown"


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
  version_str = __version__ if __version__ != "unknown" else "1.0.0"
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
