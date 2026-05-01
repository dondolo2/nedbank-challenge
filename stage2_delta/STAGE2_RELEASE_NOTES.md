# Stage 2 Data Release

## Overview

This data release replaces your Stage 1 data entirely. Remove or overwrite the Stage 1 files and mount the new data at `/data/input/` (the same mount point as Stage 1).

## Data Volume

| Dataset | Records |
|---|---|
| Customers | 240,000 |
| Accounts | 300,000 |
| Transactions | ~3,150,000 |
| **Total on disk** | **~1.5 GB** |

## Key Changes from Stage 1

- **3x volume increase** across all three datasets.
- **6 data-quality issue types injected** -- your pipeline must now detect and report these.
- **New field**: `merchant_subcategory` has been added to transactions.

## What to Read

Refer to **`stage2_spec_addendum.md`** (included in this pack) for the full specification of Stage 2 requirements, DQ issue definitions, and updated acceptance criteria.

## DQ Report Requirement

The `dq_report_template.json` (in your Stage 1 `docs/` folder) must now be populated correctly. Every field is scored -- incomplete or missing reports will lose marks.

## Resource Constraints Reminder

Your container will be evaluated under the following limits:

| Resource | Limit |
|---|---|
| Memory | 2 GB |
| CPU | 2 vCPU |
| Wall-clock time | 30 minutes |

Test locally with:

```bash
docker run -m 2g --cpus=2 your-image
```

Refer to `docs/resource_constraints.md` in your Stage 1 pack for full details.
