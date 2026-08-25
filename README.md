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
2. **Python Kernel Extension (`managed_spark_cell_monitoring.kernelextension`):** Intercepts cell execution hooks (`pre_run_cell`, `post_run_cell`) and routes telemetry events to active widget instances via Jupyter Comms or gRPC streams.
3. **AnyWidget React Frontend (`managed_spark_cell_monitoring/static/widget.js`):** Modular React 18 / MobX frontend compiled via `esbuild`, providing isolated UI components for job tables, stage bars, and task timelines.

---

## Installation & Usage

### 1. Installation

```bash
pip install managed-spark-cell-monitoring
```

### 2. Enable in Notebook

In your IPython environment or notebook cell, load the kernel extension:

```python
%load_ext managed_spark_cell_monitoring.kernelextension
```

Alternatively, you can configure it to load automatically in all notebooks by adding it to your `ipython_config.py`:

```python
c.InteractiveShellApp.extensions = [
    'managed_spark_cell_monitoring.kernelextension'
]
```

### 3. Configure Spark Session

Create your Spark session with the extra configurations to activate the Scala Listener. You must set `spark.extraListeners` to the fully qualified class name and `spark.driver.extraClassPath` to the physical path of the compiled JAR.

```python
from pyspark.sql import SparkSession

# Replace with the path to the correct JAR for your Spark version
JAR_PATH = "path/to/managed-spark-cell-monitoring-spark4-assembly-1.0.0.jar"

spark = SparkSession.builder \
        .config('spark.extraListeners', 'managed_spark_cell_monitoring.listener.JupyterManagedSparkCellMonitoringListener') \
        .config('spark.driver.extraClassPath', JAR_PATH) \
        .getOrCreate()
```

---

## Development & Building

### Prerequisites
- Node.js (>= 18.0)
- Python (>= 3.9)
- sbt (for compiling Scala listener JARs)

### Building from Source

#### 1. React UI Frontend
The frontend uses React and is bundled into static assets for AnyWidget.
```bash
# Install frontend dependencies
npm install

# Bundle React AnyWidget assets into static/widget.js
npm run build:widgets
```

#### 2. Python Kernel Backend
The Python extension is packaged as a standard Python wheel (`.whl`).
```bash
# Install the build tool (if not already installed)
pip install build

# Build the Python kernel extension wheel
python -m build --wheel

# Install testing dependencies
pip install pytest pytest-mock pytest-cov

# Run the Python unit tests with coverage
python3 -m pytest --cov=.
```

#### 3. Scala Listener JARs
The Scala listener is compiled using `sbt` and supports Spark 3 (Scala 2.12) and Spark 4 (Scala 2.13). We use the `sbt-assembly` plugin to build shaded Fat JARs.
```bash
# Navigate to the scala directory
cd scala

# Build shaded Fat JARs for both Spark 3 and Spark 4 simultaneously
sbt "project spark3" assembly "project spark4" assembly

# Run the test suite and generate a coverage report
sbt coverage "project core" test coverageReport
```

---

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.