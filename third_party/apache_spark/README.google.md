# Apache Spark Connect `GetStatus` & Dataproc Cell Monitor Protos

This directory contains minimal protocol buffer definitions and generated Python
stubs derived from Apache Spark 4.2 (`dataproc-branch-4.2.0`), following
Google's non-google3 third-party policy (`go/thirdparty/non-google3`).

At wheel build time (`pyproject.toml` `[tool.hatch.build.targets.wheel.sources]`),
this directory is remapped into the private package namespace
`managed_spark_cell_monitoring._third_party.apache_spark` so no top-level
`third_party` package is ever installed into `site-packages/`.

## Source Provenance

- **Upstream Apache Spark Base Release:** [`v4.2.0`](https://github.com/apache/spark/tree/v4.2.0) (`32f7299601108917fb01920a54e084595b7b3bf8`)
- **Pinned Spark Source Commit:** `7384499f0307c8309686fdc7450face915853c8e` (`PINNED_SPARK_COMMIT` in `scripts/regen_protos.py`)

## Included Files & Local Modifications

- `LICENSE` & `NOTICE`: Apache License 2.0 and upstream Apache Spark `NOTICE`
  attribution file (also packaged into the wheel under
  `managed_spark_cell_monitoring/_third_party/apache_spark/`).
- `METADATA`: Third-party compliance metadata (`devtools.metadata.Metadata`).
- `spark_connect_status.proto`: Extracted subset of
  `sql/connect/common/src/main/protobuf/spark/connect/base.proto` containing
  only `UserContext`, `GetStatusRequest`, `GetStatusResponse`, and
  `SparkConnectService.GetStatus` (`package spark.connect;`). Extracting only
  these messages avoids bundling ~15,000 lines of Spark Connect plan/expression
  protos already provided by `pyspark-client` / `pyspark`.
- `cell_monitor.proto`: Verbatim copy of
  `sql/connect/common/src/main/protobuf/dataproc/cellmonitor/v1/cell_monitor.proto`
  (`package dataproc.cellmonitor.v1;`) defining `CellMonitorTelemetryRequest`
  and `CellMonitorTelemetry`.
- `spark_connect_status_pb2.py` & `cell_monitor_pb2.py`: Generated Python
  protobuf stubs produced by `scripts/regen_protos.py`.
  `spark_connect_status_pb2.py` loads its `spark.connect.*` descriptors into an
  isolated `DescriptorPool` and `SymbolDatabase` so importing it alongside
  `pyspark.sql.connect.proto.base_pb2` never causes duplicate-symbol collisions
  across PySpark 4.0, 4.1, or 4.2+.

## Regenerating Stubs

From a virtual environment with `grpcio-tools` and `pyink` installed:

```sh
# Regenerate *_pb2.py from local .proto files:
python scripts/regen_protos.py

# Verify checked-in *_pb2.py files are up to date:
python scripts/regen_protos.py --check

# Refresh .proto files, NOTICE, METADATA, and stubs from PINNED_SPARK_COMMIT
# (or pass --ref <sha-or-tag> to bump PINNED_SPARK_COMMIT):
python scripts/regen_protos.py --remote <spark-git-url-or-path>
```
