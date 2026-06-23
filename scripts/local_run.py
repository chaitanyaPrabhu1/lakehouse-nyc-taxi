#!/usr/bin/env python3
"""Run ingestion locally against a filesystem "lake".

Downloads a sample of the real (free, public) NYC TLC data and lands it in the
bronze zone under ./.local_lake so you can inspect the Parquet without an AWS
account:

    make local-run
    # then look at .local_lake/bronze/yellow_trips/... and .../taxi_zones/...

Set SAMPLE_ROWS=0 to ingest the full month (~3M rows, ~50MB download).
"""
from __future__ import annotations

import datetime as dt
import os
import sys
from pathlib import Path

# Make the ingestion/ package importable when run from the repo root.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "ingestion"))

import common  # noqa: E402
import ingest  # noqa: E402


def main() -> int:
    os.environ.setdefault("STORAGE_BACKEND", "local")
    os.environ.setdefault("LOCAL_LAKE_DIR", str(ROOT / ".local_lake"))
    backend = common.get_backend()
    now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)

    sample_env = os.environ.get("SAMPLE_ROWS")
    sample_rows = None if sample_env in (None, "", "0") else int(sample_env)

    print(f"== ingest ==  (lake: {os.environ['LOCAL_LAKE_DIR']})")
    result = ingest.run(backend=backend, now=now, sample_rows=sample_rows)
    print(f"   months: {result['months_ingested']}  "
          f"trip rows: {result['rows_ingested']}  zones: {result['zones_ingested']}")

    # Quick read-back to prove the bronze Parquet is valid + schema-stable.
    if result["trips"]:
        table = common.read_parquet_bytes(backend.get_bytes(result["trips"][0]["key"]))
        print(f"   trips parquet verified: {table.num_rows} rows x {table.num_columns} cols")
        print(f"   columns: {table.column_names}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
