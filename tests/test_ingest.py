import pytest

import common
import ingest
from conftest import NOW


def test_run_lands_bronze_trips_and_zones(local_backend, fake_fetch):
    result = ingest.run(
        months=["2024-01"], backend=local_backend, fetch=fake_fetch, now=NOW
    )

    assert result["months_ingested"] == 1
    assert result["rows_ingested"] == 3
    assert result["zones_ingested"] == 3

    trips_key = common.trip_object_key("2024-01")
    zones_key = common.zones_object_key()
    assert local_backend.list_keys("bronze/yellow_trips/") == [trips_key]
    assert local_backend.list_keys("bronze/taxi_zones/") == [zones_key]

    # Bronze trips carry the normalized schema.
    table = common.read_parquet_bytes(local_backend.get_bytes(trips_key))
    assert table.num_rows == 3
    assert {"vendor_id", "pu_location_id", "airport_fee", "ingested_at"} <= set(table.column_names)


def test_sample_rows_caps_output(local_backend, fake_fetch):
    result = ingest.run(
        months=["2024-01"], backend=local_backend, fetch=fake_fetch, now=NOW, sample_rows=2
    )
    assert result["rows_ingested"] == 2


def test_rerun_is_idempotent(local_backend, fake_fetch):
    ingest.run(months=["2024-01"], backend=local_backend, fetch=fake_fetch, now=NOW)
    ingest.run(months=["2024-01"], backend=local_backend, fetch=fake_fetch, now=NOW)

    # Same deterministic key -> overwritten in place, no duplicate objects.
    keys = local_backend.list_keys("bronze/yellow_trips/")
    assert keys == [common.trip_object_key("2024-01")]


def test_failed_month_does_not_kill_run(local_backend):
    def flaky_fetch(url: str) -> bytes:
        if url.endswith(".csv"):
            from conftest import make_zones_csv

            return make_zones_csv()
        if "2024-01" in url:
            raise RuntimeError("boom")  # this month fails
        from conftest import make_raw_trips_parquet

        return make_raw_trips_parquet()

    result = ingest.run(
        months=["2024-01", "2024-02"], backend=local_backend, fetch=flaky_fetch, now=NOW
    )
    assert result["months_ingested"] == 1
    assert len(result["failures"]) == 1
    assert result["failures"][0]["month"] == "2024-01"


def test_all_months_fail_raises(local_backend):
    def dead_fetch(url: str) -> bytes:
        if url.endswith(".csv"):
            from conftest import make_zones_csv

            return make_zones_csv()
        raise RuntimeError("down")

    with pytest.raises(RuntimeError, match="0 trip files"):
        ingest.run(months=["2024-01"], backend=local_backend, fetch=dead_fetch, now=NOW)
