# Jairaj Jagarapu Data Engineering Portfolio

Responsive personal portfolio with two original synthetic data engineering demonstrations. Website files are served from the repository root by GitHub Pages.

# Data Engineering Portfolio Projects

Complete source for two original educational demonstrations:

1. Config-driven PySpark / Delta medallion lakehouse with CDC, tombstones, quarantine, and quality reporting.
2. Kafka / Spark Structured Streaming transaction quality monitor with watermark-based deduplication, audit output, and reporting datasets.

These are newly created portfolio demos using synthetic data. They contain no employer code or data and do not substantiate historical employment performance claims.

## Repository structure

```text
projects/
  lakehouse/
    pipeline.py
    config.json
    data/cdc.jsonl
  streaming/
    stream.py
    producer.py
    data/transactions.jsonl
tests/test_projects.py
requirements.txt
compose.yaml
.github/workflows/tests.yml
VALIDATION.md
```

Run every command below from this repository's root. Runtime output and checkpoints are ignored by Git.

## Environment

Use Python 3.10 or 3.11, Java 17 with JAVA_HOME set, and a Spark-compatible environment (Linux, macOS, WSL2, or Docker recommended). Create a virtual environment, activate it, then run:

```sh
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
RUN_SPARK_TESTS=1 python -m unittest discover -s tests -v
```

On PowerShell set `$env:RUN_SPARK_TESTS='1'` before the last command. Initial Delta and Kafka runs download JVM packages and need network access. Do not mix incompatible Spark, Scala, or Delta versions. The lightweight tests check fixture contracts; the opt-in tests execute actual Delta and Spark streaming. See `VALIDATION.md` for the checks actually run here.

## Demo 1 Config-driven medallion lakehouse

```sh
python -m projects.lakehouse.pipeline --config projects/lakehouse/config.json
```

`source → Bronze Delta → schema/key/op validation → quarantine + Silver Delta MERGE → Gold region counts`

The JSON configuration selects source, storage root, primary key, sequence column, and allowed operations. This example models customer inserts, updates, and deletes. The input schema is explicit; malformed values become null and key/sequence/op failures enter quarantine. Schema drift is not automatically evolved. Bronze is a replaceable demo snapshot rather than a production ingestion ledger. Conflicting payloads at an identical key and sequence fail the run. Silver keeps only the highest source sequence per key, retaining delete tombstones. Updates require a newer sequence, so reruns are idempotent and old inserts cannot resurrect deleted customers. Gold excludes tombstones. `quality.json` records input, rejection, and active-customer counts; uniqueness and non-null assertions fail the run if violated. Expected fixture output: 6 input rows, 1 quarantined row, 2 active customers, one each in East and West.

Sequence is a per-key monotonic source version, not ingestion time. Do not replace it with an unreliable arrival timestamp. This sample re-reads all supplied events; production incremental source cursors, transactional ingestion offsets, retention, schema contracts, orchestration, and access controls need separate implementation. Retain tombstones until upstream replay bounds permit removal. For Databricks, use an appropriate runtime with Delta included, remove local session/package setup, place paths in governed cloud storage, and schedule as a job. Add secret-managed source access, alerts, compaction, and realistic scale/load tests before production use.

## Demo 2 Streaming transaction quality monitor

File mode requires Spark but no broker:

```sh
spark-submit --master local[2] projects/streaming/stream.py --source files --seconds 30
```

Kafka mode, in separate terminals:

```sh
docker compose up -d
spark-submit --master local[2] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 projects/streaming/stream.py --source kafka --seconds 120
python -m projects.streaming.producer
```

Run the producer on the host, matching Kafka's advertised localhost address. Allow the broker time to start. The topic is automatically created in this development configuration. Stop with `docker compose down`. For broker errors inspect `docker compose logs kafka`.

`Kafka JSON → parse → raw audit → event-time watermark → transaction-ID deduplication → quality labels → reporting Parquet + JSON metrics`

A ten-minute event-time watermark bounds deduplication state. Duplicate IDs within retained state are removed. Events older than the watermark may be dropped; this is not a global forever-unique guarantee. Null IDs/timestamps stay in the raw audit but do not enter stateful processing. Positive finite amounts and USD are accepted; invalid business records remain in deduplicated quality rows. Malformed payloads appear as null parsed fields in audit; this sample does not retain the original Kafka bytes.

Each checkpointed batch writes `batches/<batch_id>/rows`, `metrics`, and valid-only `reporting`. Deterministic overwrite paths handle retry of the same batch; they are not an atomic transaction across all three outputs. The separate audit query has its own checkpoint. Consumers must avoid reading partially written batches. Spark progress is saved to `spark-progress.json` at graceful shutdown and contains state/watermark drop metrics; quality metrics count only retained deduplicated rows. Kafka offsets advance through checkpoints; preserve checkpoint and output together. Use a new output directory for a fresh demonstration. Do not delete checkpoints while retaining batch outputs.

The producer pauses between fixtures so advancing event times can establish a watermark before the old event arrives. File mode loads all six rows in one microbatch, so it demonstrates deduplication and quality but cannot establish a prior-batch watermark for the late record. To observe late drops, run Kafka mode and inspect progress `stateOperators[].numRowsDroppedByWatermark`. Arrival scheduling and trigger execution affect this outcome. A production late-event recovery process should replay audited records through an explicit correction path.

Reporting Parquet uses transaction ID, UTC event timestamp, amount, and currency: a Snowflake-compatible staging shape, not a deployed Snowflake connection. Load with an external stage and COPY INTO after configuring your own account/storage integration. Use decimal currency, schema registry, dead-letter retention, multi-broker TLS/SASL, durable object storage, alerts, and reconciliation for production. This monitor detects data quality issues; it does not infer fraud.

