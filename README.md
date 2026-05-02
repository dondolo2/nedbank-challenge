# Nedbank Data Engineering Challenge — Stage 1/2/3 Pipeline

Medallion pipeline (Bronze → Silver → Gold) using PySpark and Delta Lake, matching the challenge Docker contract and `output_schema_spec.md`. Includes Stage 2 multi-format date parsing, DQ reporting, and Stage 3 streaming ingestion.

## Layout

- `pipeline/run_all.py` — entry point (ingest → transform → provision → dq_report → stream_ingest if `/data/stream` exists)
- `pipeline/ingest.py` — Bronze: raw CSV/JSONL as strings (CSV), `mergeSchema` JSON, deterministic row ids for dedup
- `pipeline/transform.py` — Silver: dedupe, typing, DQ flags from `config/dq_rules.yaml`, multi-format date parsing
- `pipeline/provision.py` — Gold: surrogate keys, `dim_*` and `fact_transactions` (fact `province` = customer province for linkage consistency)
- `pipeline/dq_report.py` — DQ report writer for Stage 2 compliance
- `pipeline/stream_ingest.py` — Stage 3 streaming ingestion with current_balances and recent_transactions
- `config/pipeline_config.yaml` — paths and Spark settings
- `config/dq_rules.yaml` — DQ rules with handling actions
- `adr/stage3_adr.md` — Stage 3 architectural decisions

## Stage 2 Features

- **Multi-format date parsing**: Supports `yyyy-MM-dd`, `dd/MM/yyyy`, and Unix epoch seconds in date fields
- **merchant_subcategory**: Optional field in transactions (nullable STRING)
- **DQ reporting**: Generates `/data/output/dq_report.json` with issue counts and handling actions
- **Updated fact_transactions**: Now 15 fields including merchant_subcategory at position 9

## Stage 3 Features

- **Streaming ingestion**: Processes `/data/stream/*.jsonl` files in chronological order
- **current_balances**: Upsert table with account balances and last transaction timestamps
- **recent_transactions**: Rolling window of 50 most recent transactions per account
- **Production-like**: Includes quiesce logic (30s poll after no new files)

## Local quick test (Docker)

From this directory, with data under `./test_data`:

```text
test_data/
  input/accounts.csv
  input/transactions.jsonl
  input/customers.csv
  config/pipeline_config.yaml
  config/dq_rules.yaml
```

Copy `config/` files into `test_data/config/`, then:

```bash
docker build -t nedbank-submission:test .
docker run --rm --network=none --memory=2g --memory-swap=2g --cpus=2 ^
  --read-only --tmpfs /tmp:rw,size=512m ^
  -v "%CD%\test_data\input:/data/input:ro" ^
  -v "%CD%\test_data\config:/data/config:ro" ^
  -v "%CD%\test_data\output:/data/output:rw" ^
  nedbank-submission:test
```

For Stage 3 testing, add stream data:

```text
test_data/
  stream/stream_20260320_143000_0001.jsonl
  stream/stream_20260320_143500_0002.jsonl
  ...
```

And mount it:

```bash
docker run --rm --network=none --memory=2g --memory-swap=2g --cpus=2 ^
  --read-only --tmpfs /tmp:rw,size=512m ^
  -v "%CD%\test_data\input:/data/input:ro" ^
  -v "%CD%\test_data\config:/data/config:ro" ^
  -v "%CD%\test_data\output:/data/output:rw" ^
  -v "%CD%\test_data\stream:/data/stream:ro" ^
  nedbank-submission:test
```

## Scoring-equivalent Docker flags

The contract uses `--network=none`, `--read-only`, and a 512 MB `/tmp` tmpfs. This repo:

- Writes a minimal JVM hosts file under `/tmp` before Spark starts (see `pipeline/run_all.py`).
- Bundles `delta-spark` + `delta-storage` JARs at **image build** time (PyPI `delta-spark` is Python-only; `configure_spark_with_delta_pip` needs Ivy and fails offline).
- Sets `spark.sql.parquet.compression.codec=gzip` so Parquet does not load Snappy native code from `/tmp` (often broken under hardened Docker).

Build the official base image if you cannot pull it:

`docker build -t nedbank-de-challenge/base:1.0 -f stage1/infrastructure/Dockerfile.base stage1/infrastructure`

## References

Challenge docs live under `stage1/docs/` in this repo (data dictionary, validation SQL, Docker contract).
