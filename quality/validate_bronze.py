#!/usr/bin/env python3
"""Bronze data-quality gate — run BEFORE dbt.

A lightweight, dependency-minimal expectations suite (pyarrow only) that checks
the *shape and sanity* of the bronze trip data the moment it lands, before a
single dbt model runs. Think of it as a slimmed-down Great Expectations
checkpoint: structural guarantees here, business-rule/quality tests in dbt.

    make quality          # validate ./.local_lake bronze
    # or, against S3:
    DATA_BUCKET=my-lake python quality/validate_bronze.py

Two tiers:
  - schema expectations  -> a violation is a HARD failure (exit 1): the contract
    dbt sources depend on is broken.
  - distribution expectations -> reported as WARN: messy values are expected and
    are dbt's job to quarantine, but a *huge* bad fraction signals an upstream
    problem worth a human look.

For a heavier setup, swap this for the `great_expectations` library (a documented
expectation suite + data docs); the design here mirrors that flow intentionally.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "ingestion"))

import common  # noqa: E402

# Canonical columns the contract guarantees exist (data cols + metadata).
EXPECTED_COLUMNS = [name for name, _aliases, _kind in common.TRIP_COLUMNS] + [
    name for name, _kind in common.META_COLUMNS
]

# Fraction of rows that may violate a distribution check before we warn loudly.
WARN_FRACTION = 0.20


def _load_bronze_trips(backend) -> "list":
    import pyarrow as pa  # type: ignore
    import pyarrow.parquet as pq  # type: ignore

    keys = [k for k in backend.list_keys(common.BRONZE_TRIPS_PREFIX) if k.endswith(".parquet")]
    if not keys:
        raise RuntimeError(
            f"no bronze trip parquet under {common.BRONZE_TRIPS_PREFIX} "
            f"(run `make local-run` first)"
        )
    tables = [pq.read_table(pa.BufferReader(backend.get_bytes(k))) for k in keys]
    return keys, pa.concat_tables(tables, promote_options="default")


def main() -> int:
    import pyarrow.compute as pc  # type: ignore

    backend = common.get_backend()
    keys, table = _load_bronze_trips(backend)
    n = table.num_rows
    print(f"== bronze quality gate ==  ({len(keys)} file(s), {n} rows)")

    hard_failures: list[str] = []
    warnings: list[str] = []

    # --- schema expectations (HARD) --- #
    missing = [c for c in EXPECTED_COLUMNS if c not in table.column_names]
    if missing:
        hard_failures.append(f"missing expected columns: {missing}")
    if n == 0:
        hard_failures.append("bronze table is empty")

    # Required identity columns must never be null.
    for col in ("vendor_id", "tpep_pickup_datetime", "tpep_dropoff_datetime",
                "pu_location_id", "do_location_id"):
        if col in table.column_names:
            nulls = pc.sum(pc.is_null(table.column(col))).as_py() or 0
            if nulls:
                hard_failures.append(f"{col} has {nulls} nulls (expected 0)")

    # --- distribution expectations (WARN) --- #
    def warn_fraction(label: str, bad: int) -> None:
        frac = (bad / n) if n else 0
        line = f"{label}: {bad}/{n} ({frac:.1%})"
        (warnings.append(line) if frac > WARN_FRACTION else print(f"   ok   {line}"))

    if "fare_amount" in table.column_names and n:
        bad = pc.sum(pc.less(table.column("fare_amount"), 0)).as_py() or 0
        warn_fraction("negative fare_amount", bad)
    if "trip_distance" in table.column_names and n:
        bad = pc.sum(pc.less_equal(table.column("trip_distance"), 0)).as_py() or 0
        warn_fraction("non-positive trip_distance", bad)
    if "passenger_count" in table.column_names and n:
        zero = pc.sum(pc.equal(table.column("passenger_count"), 0)).as_py() or 0
        nul = pc.sum(pc.is_null(table.column("passenger_count"))).as_py() or 0
        warn_fraction("zero/null passenger_count", zero + nul)

    # --- report --- #
    for w in warnings:
        print(f"   WARN {w}")
    if hard_failures:
        print("\nFAILED — schema contract violated:")
        for f in hard_failures:
            print(f"   FAIL {f}")
        return 1
    print(f"\nPASSED — bronze schema contract holds ({len(warnings)} warning(s)).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
