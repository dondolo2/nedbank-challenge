# Stage 3 Stream Data Release

## Overview

This release contains 12 streaming micro-batch files. For local testing: mount the bundled `data/stream/` at `/data/stream/` when running your container locally. At evaluation time the harness mounts the directory automatically — do not bundle the stream files in your Docker image.

## Data Summary

| Metric | Value |
|---|---|
| Total events | 2,415 |
| Number of batches | 12 |
| Events per batch | 60 -- 450 (varying) |

## File Processing Order

All 12 stream files are present at `/data/stream/` when your container starts. Process them in **filename order** — lexicographic ordering matches chronological ordering. The 30-minute container timeout applies to the full run (batch + stream).

```
stream_20260320_143000_0001.jsonl
stream_20260320_143500_0002.jsonl
...
stream_20260320_152500_0012.jsonl
```

## Mount Point

Mount the stream directory **read-only** at:

```
/data/stream/
```

## What to Read

The following documents (included in this Stage 3 pack) contain the full specification:

- **`stage3_spec_addendum.md`** -- Stage 3 requirements and acceptance criteria
- **`stream_interface_spec.md`** -- stream file format, polling contract, and idempotency rules

## ADR Requirement

You must complete an Architecture Decision Record for your Stage 3 design. Use the template provided in this Stage 3 pack:

```
starter_kit/adr/stage3_adr.md
```

Alternatively, refer to `adr_template.md` for the blank template.

## New Output Tables

Stage 3 introduces two new output directories under `/data/output/stream_gold/`:

- **`current_balances/`** -- latest computed balance per account
- **`recent_transactions/`** -- recent transaction window for downstream consumers
