# Managed Spark Cell Monitoring (`managed-spark-cell-monitoring`)

**Managed Spark Cell Monitoring** is a Jupyter kernel extension designed to monitor Apache Spark jobs directly from your notebook cells in real time.

Leveraging [AnyWidget](https://anywidget.dev/), it enables platform-agnostic, zero-installation frontend rendering across JupyterLab, Jupyter Classic, Colab Enterprise, and VS Code notebooks.

---

## Key Features

- 📊 **Real-time Progress Tracking:** Visual progress bars for active Spark jobs, stages, and tasks directly in notebook cells.
- ⚡ **AnyWidget Architecture:** Precompiled ESM widgets rendered natively without requiring custom JupyterLab extension binaries.
- ⏱️ **Task Timeline & Metrics:** Detailed timeline graphs for task execution, GC time, serialization overhead, and active executor core utilization.

---

## Architecture Overview

1. **Scala Listener (`scalalistener`):** Plugs into the Spark Driver's `SparkListener` bus to capture fine-grained task and stage metrics with lock-free queueing and delta-change metric caching.
2. **Python Kernel Extension (`managed_spark_cell_monitoring.kernelextension`):** Intercepts cell execution hooks (`pre_run_cell`, `post_run_cell`) and routes telemetry events to active widget instances via Jupyter Comms.
3. **AnyWidget React Frontend (`managed_spark_cell_monitoring/static/widget.js`):** Modular React 18 / MobX frontend compiled via `esbuild`, providing isolated UI components for job tables, stage bars, and task timelines.

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
c.InteractiveShellApp.extensions.append(
    'managed_spark_cell_monitoring.kernelextension'
)
```

### 3. Configure PySpark Session

Create your Spark session with the extra configurations to activate the Scala Listener. Use `managed_spark_cell_monitoring.get_jar_path(...)` to automatically resolve the bundled listener JAR. Pass `"3"` for Apache Spark 3.x or `"4"` for Apache Spark 4.x:

```python
import managed_spark_cell_monitoring
from pyspark.sql import SparkSession

# Specify your target major Apache Spark version:
# • Pass "3" for Apache Spark 3.x (e.g., Spark 3.4 / 3.5 on Google Cloud Dataproc 2.2)
# • Pass "4" for Apache Spark 4.x
spark_major_version = "3"  # Change to "4" for Spark 4.x clusters

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

### 4. Cluster / Dataproc Configuration (`spark-defaults.conf`)

For Dataproc initialization actions, cluster startup scripts, or `spark-defaults.conf`, you can use the CLI entrypoint to print the exact configuration:

* **For Spark 3.x:**
  ```bash
  python -m managed_spark_cell_monitoring 3
  ```
* **For Spark 4.x:**
  ```bash
  python -m managed_spark_cell_monitoring 4
  ```

This outputs:
```properties
spark.extraListeners managed_spark_cell_monitoring.listener.JupyterManagedSparkCellMonitoringListener
spark.driver.extraClassPath /path/to/managed-spark-cell-monitoring-spark3-assembly-1.0.0.jar
```

---

## Development & Building

### Prerequisites
- **Node.js** (>= 18.0) & **Yarn**
- **Java JDK** (8, 11, or 17) & **SBT**
- **Python** (>= 3.9)

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
The Scala listener is compiled using `sbt` and supports Spark 3 (Scala 2.12) and Spark 4 (Scala 2.13). We use `sbt-assembly` to build shaded Fat JARs directly into `managed_spark_cell_monitoring/static/listeners/`.

```bash
# Run Scala unit tests
cd scala && sbt test && cd ..

# Build shaded Fat JARs for Spark 3 and Spark 4
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