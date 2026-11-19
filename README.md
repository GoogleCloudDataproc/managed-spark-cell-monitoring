# Managed Spark Cell Monitoring (`managed-spark-cell-monitoring`)

**Managed Spark Cell Monitoring** is a Jupyter kernel extension designed to monitor Apache Spark jobs directly from your notebook cells in real time.

Leveraging [AnyWidget](https://anywidget.dev/), it enables platform-agnostic, zero-installation frontend rendering across JupyterLab, Jupyter Classic, Colab Enterprise, and VS Code notebooks.

---

## Key Features

- 📊 **Real-time Progress Tracking:** Visual progress bars for active Apache Spark jobs and their tasks directly in notebook cells.
- ⚡ **AnyWidget Architecture:** Precompiled ESM widgets rendered natively without requiring custom JupyterLab extension binaries.
- ⏱️ **Task Chart:** Live chart of running and scheduled tasks against available executor cores.

---

## Architecture Overview

1. **Classic PySpark Mode:**
   - **Scala Listener (`scalalistener`):** Plugs into the Apache Spark driver's `SparkListener` bus to capture fine-grained task and stage metrics with lock-free queueing and delta-change metric caching.
   - **Python Kernel Extension (`managed_spark_cell_monitoring.kernelextension`):** Intercepts cell execution hooks (`pre_run_cell`, `post_run_cell`) and routes telemetry events to active widget instances via Jupyter Comms.
2. **Spark Connect Snapshot Mode:**
   - **Python Controller & Poller (`managed_spark_cell_monitoring.spark_connect`):** Tracks the active notebook cell across worker threads (`contextvars`), polls `GetStatus` for `CellMonitorTelemetry` protobuf extensions, and reconciles terminal snapshots per operation.
   - **Automatic Fallback:** If a Spark Connect server does not implement `GetStatus` (`UNIMPLEMENTED`) or omits `CellMonitorTelemetry` on task-bearing operations, the poller falls back cleanly for that session without disrupting execution.
3. **AnyWidget React Frontend (`managed_spark_cell_monitoring/static/widget.js`):** Modular React 18 / MobX frontend compiled via `esbuild`, providing isolated UI components for the job table, header summary, and task chart in both classic event-stream mode and Spark Connect snapshot mode.

---

## Installation & Usage

### 1. Installation

Install the pre-built package from PyPI (no Node.js, Yarn, Java, or SBT required on client machines):

```bash
# Classic PySpark notebooks
pip install managed-spark-cell-monitoring

# With Spark Connect polling dependencies (grpcio, protobuf)
pip install "managed-spark-cell-monitoring[connect]"
```

### 2. Enable in Notebook

In your IPython environment or notebook cell, load the kernel extension:

```python
%load_ext managed_spark_cell_monitoring
```

Alternatively, configure it to load automatically in all notebook sessions by adding it to your IPython configuration (e.g. `~/.ipython/profile_default/ipython_kernel_config.py`):

```python
c.InteractiveShellApp.extensions.append('managed_spark_cell_monitoring')
```

### 3. Configure PySpark Session

Create your Apache Spark `SparkSession` with the extra configurations to activate the Scala Listener. Use `managed_spark_cell_monitoring.get_jar_path(...)` to automatically resolve the bundled listener JAR. Pass `"3"` for Apache Spark 3.5 or `"4"` for Apache Spark 4.x:

```python
import managed_spark_cell_monitoring
from pyspark.sql import SparkSession

# Specify your target major Apache Spark version:
# • Pass "3" for Apache Spark 3.5 (e.g., Google Cloud Dataproc 2.2)
# • Pass "4" for Apache Spark 4.x
spark_major_version = "3"  # Change to "4" for Apache Spark 4.x clusters

# Automatically resolves the bundled listener JAR inside site-packages
jar_path = managed_spark_cell_monitoring.get_jar_path(spark_major_version)

spark = (
    SparkSession.builder
    .config(
        'spark.extraListeners',
        'managed_spark_cell_monitoring.listener.JupyterManagedSparkCellMonitoringListener',
    )
    .config('spark.driver.extraClassPath', jar_path)
    .getOrCreate()
)
```

### 4. Spark Connect Usage & Toggling

For remote Spark Connect sessions (such as Google Cloud Dataproc Serverless Interactive via `managed-spark-connect-python`), load the Spark Connect IPython extension or let your client session call `get_spark_connect_controller().ensure_current_shell_attached()` and create a `CellMonitorPoller` via `create_session_poller(client, ...)`:

```python
%load_ext managed_spark_cell_monitoring.spark_connect
```

You can also toggle in-cell monitoring at runtime in either mode using the line magic or environment variable:

```python
%cellmonitor status   # or on / off
```

Setting `MANAGED_SPARK_CELL_MONITORING_ENABLED=0` (or `false`, `off`, `no`) starts the kernel with monitoring disabled.

#### Spark Connect Notes & Differences vs. Classic PySpark

- **Server Requirement & Fallback:** Snapshot mode requires a Spark Connect server that exposes `CellMonitorTelemetry` in `GetStatusResponse` extensions. Servers without that support trigger automatic per-session fallback (`on_fallback`) so built-in progress reporting can take over.
- **Lazy Widget Display:** In Spark Connect mode, a cell widget is displayed only after at least one Spark job is observed for the cell; pure-Python cells and jobless SQL/DDL operations leave no empty widget behind.
- **Per-Operation Snapshots:** Telemetry is aggregated and deduplicated per `(operation_id, version)` over `GetStatus` RPCs rather than streamed over a local driver socket, so Spark driver JAR configuration (`get_jar_path` / `spark.extraListeners`) is not required on the notebook kernel.

---

## Development & Building

### Prerequisites
- **Node.js** (>= 18.0) & **Yarn**
- **Java JDK** (8, 11, or 17) & **SBT**
- **Python** (>= 3.10)

### Building from Source

#### 1. React UI Frontend
The frontend uses React 18 and MobX, bundled via `esbuild` into standalone assets in `managed_spark_cell_monitoring/static/`.

```bash
# Install frontend dependencies
yarn --cwd js install

# Bundle frontend assets into static/widget.js and static/widget.css
yarn --cwd js build

# Run frontend tests and linter
yarn --cwd js test
yarn --cwd js lint
```

#### 2. Scala Listener Fat JARs
The Scala listener is compiled using `sbt` and supports Apache Spark 3.5 (Scala 2.12) and Apache Spark 4.x (Scala 2.13). We use `sbt-assembly` to build shaded Fat JARs directly into `managed_spark_cell_monitoring/static/listeners/`.

```bash
# Run Scala unit tests
cd scala && sbt test && cd ..

# Build shaded Fat JARs for Apache Spark 3.5 and Apache Spark 4.x
cd scala && sbt assembly && cd ..
```

#### 3. Python Kernel Package
Install in editable mode and run Python test suites:

```bash
# Install package in editable mode with test dependencies
pip install -e ".[test]"

# Run Python unit tests
pytest tests/

# Build distribution wheel (.whl) and source archive (.tar.gz)
python -m build
```

---

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

---

## Trademarks

Apache®, Apache Spark™, Spark™, and the Apache feather logo are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.