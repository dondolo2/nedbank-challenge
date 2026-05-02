# Stage 3 Architecture Decision Record

## Decision 1: How did the Stage 1 architecture facilitate or hinder streaming?

Stage 1 helped a lot in several concrete ways. The shared `pipeline/common.py` utilities such as `get_spark_session()` and `write_delta()` were reusable directly in the new Stage 3 module, so I did not need to invent a separate Spark bootstrap or Delta write pattern. The existing config-driven structure in `config/pipeline_config.yaml` also meant the core batch pipeline already had a clean separation between code and mounted paths, so adding a stream-aware module did not require touching ingestion or transformation path resolution. Finally, the batch split into `pipeline/ingest.py`, `pipeline/transform.py`, and `pipeline/provision.py` made it easy to add `pipeline/stream_ingest.py` as a standalone extension without breaking the working Stage 1 flow.

At the same time, the Stage 1 architecture introduced some practical friction. `pipeline/run_all.py` was designed as a simple sequential runner, and it did not keep a persistent Spark session across stages. That meant Stage 3 had to start its own session in `pipeline/stream_ingest.py`, adding JVM startup overhead if the stream loop were more active. Also, the multi-format date parsing and amount casting logic lived in `pipeline/transform.py` rather than a shared helper module, so Stage 3 had to import `parse_flexible_date()` from the batch transform module and reapply the same normalization patterns.

Overall, existing batch code survival was strong: about 80% of `common.py` stayed reusable, around 60% of the transformation logic was reused or mirrored, and the entire `provision.py` remained intact except for the `merchant_subcategory` field addition.

## Decision 2: What would you change in hindsight?

In hindsight, I would have extracted shared normalization helpers into a dedicated utilities module from the start. A `pipeline/transform_utils.py` containing date parsing, amount casting, and currency normalization would have been imported by both batch transform and stream ingest, avoiding the split between shared and duplicate logic. This would have made Stage 3 cleaner and reduced the risk of drift between batch and stream behavior.

I also wish the entry point had been designed to accept a mode argument such as `--mode=batch|stream|all`. That would have made `run_all.py` more flexible and more explicit about whether Stage 3 should run, instead of inferring it from the existence of `/data/stream`. A mode flag would also help with unit tests and local evaluation scenarios.

Finally, I would have kept a single long-lived SparkSession across the whole pipeline rather than stopping and starting a new session in each stage. The current design wastes JVM startup time, which is especially important for Stage 3 processing and any repeated micro-batch loop under a 30-minute limit.

## Decision 3: If I had full Stage 3 visibility on Day 1, what would I have built?

If Stage 3 had been visible on Day 1, I would have designed the pipeline around a single SparkSession passed through all stages. That would eliminate repeated session creation and make stream and batch stages feel like variations of the same workflow. I would have also created a shared transform utilities package from the start so both `pipeline/transform.py` and `pipeline/stream_ingest.py` could call the same `parse_flexible_date()`, amount casting, and normalization logic.

The configuration would have included a dedicated streaming section in `pipeline_config.yaml` such as `stream_path`, `quiesce_seconds`, and `recent_tx_window`. That would keep path and behavior parameters out of hardcoded defaults and make the stream extension easier to test. I would also have designed the initial data model around Delta MERGE rather than overwrite for batch output, so the same upsert semantics used in Stage 3 would be consistent with the batch pipeline.

Finally, the entry point would have been built as `run_all.py --mode=batch|stream|all`, with support for a stream-only evaluation path. That would make Stage 3 opt-in and avoid any surprise behavior when only Stage 1/2 inputs are present.
