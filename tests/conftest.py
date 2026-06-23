import datetime as dt
import io
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "ingestion"))

import common  # noqa: E402

NOW = dt.datetime(2024, 2, 1, 0, 0, 0)


def make_raw_trips_parquet(rows: int = 3) -> bytes:
    """A tiny TLC-shaped trip file using the ORIGINAL source column names.

    Deliberately exercises the normalizer:
    - mixed-case source names (``VendorID``, ``Airport_fee``)
    - ``passenger_count`` / ``RatecodeID`` as doubles (must cast to int64)
    - ``congestion_surcharge`` entirely absent (must be filled with nulls)
    """
    pick = [dt.datetime(2024, 1, 1, 8, h) for h in range(rows)]
    drop = [dt.datetime(2024, 1, 1, 8, h + 15) for h in range(rows)]
    table = pa.table(
        {
            "VendorID": pa.array([1, 2, 1][:rows], type=pa.int64()),
            "tpep_pickup_datetime": pa.array(pick, type=pa.timestamp("us")),
            "tpep_dropoff_datetime": pa.array(drop, type=pa.timestamp("us")),
            "passenger_count": pa.array([1.0, 2.0, 1.0][:rows], type=pa.float64()),
            "trip_distance": pa.array([1.2, 3.4, 0.8][:rows], type=pa.float64()),
            "RatecodeID": pa.array([1.0, 1.0, 2.0][:rows], type=pa.float64()),
            "store_and_fwd_flag": pa.array(["N", "N", "Y"][:rows], type=pa.string()),
            "PULocationID": pa.array([142, 236, 41][:rows], type=pa.int64()),
            "DOLocationID": pa.array([238, 141, 42][:rows], type=pa.int64()),
            "payment_type": pa.array([1, 2, 1][:rows], type=pa.int64()),
            "fare_amount": pa.array([9.5, 14.0, 6.0][:rows], type=pa.float64()),
            "extra": pa.array([0.5, 0.0, 0.5][:rows], type=pa.float64()),
            "mta_tax": pa.array([0.5, 0.5, 0.5][:rows], type=pa.float64()),
            "tip_amount": pa.array([2.0, 0.0, 1.0][:rows], type=pa.float64()),
            "tolls_amount": pa.array([0.0, 0.0, 0.0][:rows], type=pa.float64()),
            "improvement_surcharge": pa.array([1.0, 1.0, 1.0][:rows], type=pa.float64()),
            "total_amount": pa.array([14.0, 16.0, 9.5][:rows], type=pa.float64()),
            "Airport_fee": pa.array([0.0, 1.75, 0.0][:rows], type=pa.float64()),
        }
    )
    sink = io.BytesIO()
    pq.write_table(table, sink)
    return sink.getvalue()


def make_zones_csv() -> bytes:
    return (
        b"LocationID,Borough,Zone,service_zone\n"
        b"1,EWR,Newark Airport,EWR\n"
        b"142,Manhattan,Lincoln Square East,Yellow Zone\n"
        b"236,Manhattan,Upper East Side North,Yellow Zone\n"
    )


@pytest.fixture
def fake_fetch():
    """Stub for ingest's URL fetcher: routes by URL to parquet or CSV bytes."""
    def _fetch(url: str) -> bytes:
        return make_zones_csv() if url.endswith(".csv") else make_raw_trips_parquet()

    return _fetch


@pytest.fixture
def local_backend(tmp_path):
    return common.LocalBackend(str(tmp_path / "lake"))
