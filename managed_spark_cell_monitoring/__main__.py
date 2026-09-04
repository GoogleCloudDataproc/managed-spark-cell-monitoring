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
"""CLI configuration helper for Managed Spark Cell Monitoring."""

import sys
from managed_spark_cell_monitoring import get_jar_path, __version__


def main():
  spark_version = sys.argv[1] if len(sys.argv) > 1 else "3"
  jar = get_jar_path(spark_version)
  print(f"Managed Spark Cell Monitoring v{__version__}")
  print("=" * 60)
  print(f"Target Spark Version: {spark_version}")
  print(f"Listener Class: managed_spark_cell_monitoring.listener.JupyterManagedSparkCellMonitoringListener")
  print("\nTo configure PySpark / spark-defaults.conf, add:")
  print(f"  spark.extraListeners managed_spark_cell_monitoring.listener.JupyterManagedSparkCellMonitoringListener")
  print(f"  spark.driver.extraClassPath {jar}")
  print("=" * 60)


if __name__ == "__main__":
  main()
