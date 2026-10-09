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

1. **Scala Listener (`scalalistener`):** Plugs into the Apache Spark driver's `SparkListener` bus to capture fine-grained task and stage metrics with lock-free queueing and delta-change metric caching.
2. **Python Kernel Extension (`managed_spark_cell_monitoring.kernelextension`):** Intercepts cell execution hooks (`pre_run_cell`, `post_run_cell`) and routes telemetry events to active widget instances via Jupyter Comms.
3. **AnyWidget React Frontend (`managed_spark_cell_monitoring/static/widget.js`):** Modular React 18 / MobX frontend compiled via `esbuild`, providing isolated UI components for the job table and task chart.

---

## Installation & Usage

### 1. Installation

Install the pre-built package from PyPI (no Node.js, Yarn, Java, or SBT required on client machines):

```bash
pip install managed-spark-cell-monitoring
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

### 4. Turning monitoring on or off

Monitoring is on by default. It can be switched off for a kernel without unloading the extension, which is useful for hosts that expose an "in-cell monitoring" setting or for notebooks where the monitor is not wanted:

```python
%cellmonitor off      # also: on | status
```

or, from code (for example a host sending a silent execution):

```python
import managed_spark_cell_monitoring
managed_spark_cell_monitoring.set_enabled(False)   # returns the new state
managed_spark_cell_monitoring.is_enabled()
```

While off, any monitors currently displayed are removed, new cells show nothing, and listener events are dropped at the kernel so no widget traffic is produced. Switching back on takes effect from the next cell. To start a kernel with monitoring off, set `MANAGED_SPARK_CELL_MONITORING_ENABLED=0` (also accepts `false`, `no`, `off`) in the kernel's environment; any other value, or an unset variable, means on.

---

## Development & Building

### Prerequisites
- **Node.js** (>= 18.0) & **Yarn**
- **Java JDK** (8, 11, or 17) & **SBT**
- **Python** (>= 3.8)

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