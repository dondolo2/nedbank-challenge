# Nedbank Data Engineering Challenge — Stage 1 Pipeline

Medallion pipeline (Bronze → Silver → Gold) using PySpark and Delta Lake, matching the challenge Docker contract and `output_schema_spec.md`.

## Layout

- `pipeline/run_all.py` — entry point (ingest → transform → provision)
- `pipeline/ingest.py` — Bronze: raw CSV/JSONL as strings (CSV), `mergeSchema` JSON, deterministic row ids for dedup
- `pipeline/transform.py` — Silver: dedupe, typing, DQ flags from `config/dq_rules.yaml`
- `pipeline/provision.py` — Gold: surrogate keys, `dim_*` and `fact_transactions` (fact `province` = customer province for linkage consistency)
- `config/pipeline_config.yaml` — paths and Spark settings

## Local quick test (Docker)

From this directory, with data under `./test_data`:

```text
test_data/
  input/accounts.csv
  input/transactions.jsonl
  input/customers.csv
  config/pipeline_config.yaml
```

Copy `config/pipeline_config.yaml` into `test_data/config/`, then:

```bash
docker build -t nedbank-submission:test .
docker run --rm --network=none --memory=2g --memory-swap=2g --cpus=2 ^
  --read-only --tmpfs /tmp:rw,size=512m ^
  -v "%CD%\test_data\input:/data/input:ro" ^
  -v "%CD%\test_data\config:/data/config:ro" ^
  -v "%CD%\test_data\output:/data/output:rw" ^
  nedbank-submission:test
```

(On Linux/macOS, use the volume syntax from `stage1/docs/docker_interface_contract.md`.)

## Scoring-equivalent Docker flags

The contract uses `--network=none`, `--read-only`, and a 512 MB `/tmp` tmpfs. This repo:

- Writes a minimal JVM hosts file under `/tmp` before Spark starts (see `pipeline/run_all.py`).
- Bundles `delta-spark` + `delta-storage` JARs at **image build** time (PyPI `delta-spark` is Python-only; `configure_spark_with_delta_pip` needs Ivy and fails offline).
- Sets `spark.sql.parquet.compression.codec=gzip` so Parquet does not load Snappy native code from `/tmp` (often broken under hardened Docker).

Build the official base image if you cannot pull it:

`docker build -t nedbank-de-challenge/base:1.0 -f stage1/infrastructure/Dockerfile.base stage1/infrastructure`

## References

Challenge docs live under `stage1/docs/` in this repo (data dictionary, validation SQL, Docker contract).
