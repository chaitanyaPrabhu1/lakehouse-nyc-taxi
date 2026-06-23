# Lakehouse + Data Modeling on AWS (NYC Taxi)

> Take a famously messy public dataset — the NYC TLC yellow-taxi trips — and turn it into a clean,
> well-modeled, tested, documented **medallion lakehouse** on S3 + Athena with dbt. Lighter on
> infrastructure than a streaming pipeline, heavy on the *modeling and data-quality* skills DE teams
> screen hardest for. Pairs perfectly with the serverless batch ELT project.

**Resume line**

> Designed a medallion-architecture lakehouse on AWS (S3 + Athena): modeled raw NYC-taxi data into
> bronze/silver/gold layers with dbt, added incremental models, a surrogate-keyed star schema, 30+
> data-quality tests, source-freshness checks, and auto-generated documentation.

---

## Architecture

```
 NYC TLC open data (yellow_tripdata_*.parquet  +  taxi_zone_lookup.csv)
     │  ingestion job (Python, on demand)  ── normalize schema ──┐
     ▼                                                           ▼
 ┌─────────────────────────── S3 data lake ───────────────────────────┐
 │                                                                     │
 │  BRONZE  bronze/yellow_trips/year=YYYY/month=MM/*.parquet           │  ◀── Glue Crawler
 │          bronze/taxi_zones/taxi_zone_lookup.parquet                 │      (catalogs bronze)
 │             │   raw, immutable, schema-stable                        │
 │             ▼   dbt staging  (type, dedupe, quality-flag)            │
 │  SILVER  stg_yellow_trips · stg_taxi_zones                          │
 │          int_trips_valid  ─┬─►  (passed every quality check)         │
 │          int_trips_quarantined  (failed — with reason, kept queryable)
 │             │   dbt marts  (dimensional model)                       │
 │             ▼                                                        │
 │  GOLD    fct_trips (incremental, partitioned by pickup_date)        │
 │          dim_zone · dim_date · dim_payment_type · dim_rate_code ·    │
 │          dim_vendor · agg_trips_daily · agg_revenue_by_zone         │
 └─────────────────────────────────────────────────────────────────────┘
     │                                   │
     ▼ Athena (serverless SQL)           ▼ dbt tests + source freshness + dbt docs (lineage)
 BI / analyst queries              data-quality gate on every build

 Bronze quality gate (pyarrow) runs BEFORE dbt — a structural contract check on landed data.
```

**Medallion → dbt mapping:** *bronze* = the raw TLC data landed in S3 and cataloged by Glue (dbt
**sources**); *silver* = `staging` + `intermediate` (typed, conformed, quality-flagged); *gold* =
`marts` (the star schema: facts, dimensions, aggregates).

## What this lakehouse does and why

An ingestion job downloads NYC TLC trip data and the zone lookup, **normalizes them to one stable
schema** (TLC renames/retypes columns between years), and lands them in the S3 **bronze** zone as
partitioned Parquet — an immutable, replayable copy. A Glue crawler catalogs bronze so Athena can
query it. dbt then builds the lakehouse on top:

- **silver** types and de-duplicates every trip, computes derived fields (duration, speed, tip %),
  and runs a battery of data-quality checks — *without dropping rows*. Trips that pass land in
  `int_trips_valid`; trips that fail land in `int_trips_quarantined` **with the reason attached**, so
  bad data is visible and trendable instead of silently discarded.
- **gold** is a classic **star schema**: a surrogate-keyed `fct_trips` fact (incremental, partitioned
  by `pickup_date`) surrounded by conformed dimensions (`dim_zone`, `dim_date`, `dim_payment_type`,
  `dim_rate_code`, `dim_vendor`) and ready-to-chart aggregates.

Every build is gated by **100+ dbt tests** (uniqueness, not-null, referential integrity, accepted
ranges/values, custom reconciliation) plus **source-freshness** checks. `dbt docs` renders the full
lineage graph.

## Design choices (own these in an interview)

- **Why medallion (bronze/silver/gold)?** Each layer has one job and a clear contract. Bronze is the
  cheap insurance copy you can always rebuild from. Silver is where cleaning/conforming/quality lives.
  Gold is the modeled, business-facing layer. Failures and changes stay localized to a layer.
- **Why normalize the schema at the bronze boundary?** TLC changes column names (`Airport_fee` vs
  `airport_fee`) and types (`passenger_count` as int vs double) between releases. Pinning a canonical
  schema at ingest means the Glue catalog never flip-flops and every downstream model has a dependable
  contract. Bad *values* are left for dbt; only the *shape* is fixed here.
- **Why quarantine instead of filtering?** Real taxi data is messy: negative fares, zero passengers,
  impossible distances, pickups after dropoffs, unknown zones. Dropping them hides problems. Modeling
  them as a first-class `int_trips_quarantined` table (with `quality_reason`) makes data quality
  observable — you can alert when the bad fraction spikes.
- **Why a surrogate key on trips?** TLC records have no primary key, so `trip_id` is a deterministic
  hash of the immutable trip attributes. That gives a stable grain, a real uniqueness test, and
  idempotent de-duplication.
- **Why partition `fct_trips` by `pickup_date` + incremental?** Athena bills by data scanned. Date
  partitions let queries prune to the days they need, and `insert_overwrite` means a re-run replaces
  whole day-partitions cleanly — idempotent, no append drift, and only new days get reprocessed.
- **Why a `dim_date` spine?** Building the calendar independently (not from dates that happen to
  appear) means zero-trip days still show up in time series — no silent gaps in a dashboard.
- **Two-tier data quality.** A lightweight **bronze gate** (pyarrow) checks the structural contract
  the instant data lands — *before* dbt. **dbt tests** then enforce business rules on the modeled
  layers. (Swap the bronze gate for Great Expectations if you want data-docs; the flow is the same.)

## Repository layout

```
lakehouse-nyc-taxi/
├── README.md
├── Makefile                       # one-liners: local-run, quality, dbt-build, docs, apply
├── requirements-dev.txt
├── config/
│   └── datasets.json              # which months + source URLs to ingest
├── ingestion/
│   ├── ingest.py                  # TLC parquet/CSV → bronze (normalized, partitioned)
│   ├── common.py                  # storage backends, canonical schema, normalization
│   └── requirements.txt
├── quality/
│   └── validate_bronze.py         # pre-dbt structural quality gate (GE-style)
├── dbt/
│   ├── dbt_project.yml            # medallion schemas (silver/gold), vars
│   ├── profiles.yml               # dbt-athena (env-var driven)
│   ├── packages.yml               # dbt_utils
│   ├── seeds/                     # payment_type / rate_code / vendor lookups
│   ├── macros/                    # trip_duration_minutes
│   ├── models/
│   │   ├── staging/               # silver: stg_* (+ bronze sources, freshness)
│   │   ├── intermediate/          # silver: int_trips_valid / int_trips_quarantined
│   │   └── marts/                 # gold: fct_trips, dims, aggregates
│   └── tests/                     # singular tests (reconciliation, no-future-pickup)
├── scripts/
│   └── local_run.py               # ingest a sample into ./.local_lake (no AWS)
├── terraform/                     # S3, Glue db + crawler, Athena workgroup, IAM, billing alarm
└── tests/                         # pytest: schema-normalization + ingestion (no AWS)
```

## Quickstart

### 1. Run ingestion locally (no AWS needed)

```bash
make venv          # create .venv and install dev deps
make local-run     # download a 50k-row sample + zones into ./.local_lake (real TLC data)
make quality       # run the bronze quality gate over the local lake
make test          # unit tests (synthetic parquet, moto-mocked S3)
```

`make local-run` downloads a sample of the real (free, public) TLC data and writes normalized bronze
Parquet to a local filesystem "lake" so you can inspect it without an AWS account. Set `SAMPLE_ROWS=0`
to ingest a full month.

### 2. Deploy the lake to AWS

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # optional: set alarm_email
terraform init
terraform apply
eval "$(terraform output -raw env_exports)"     # export DATA_BUCKET, GLUE_DATABASE, etc.
```

This provisions the S3 lake bucket, the Glue database + bronze crawler, an Athena workgroup, the
crawler IAM role, and an optional billing alarm.

### 3. Land data + catalog it

```bash
DATA_BUCKET=$DATA_BUCKET python ingestion/ingest.py        # land bronze in S3
aws glue start-crawler --name "$GLUE_CRAWLER"              # catalog bronze tables
```

### 4. Build the lakehouse with dbt

```bash
cd dbt
dbt deps
dbt seed   --target prod      # load the reference dimensions
dbt run    --target prod      # silver → gold
dbt test   --target prod      # 100+ data-quality tests
dbt source freshness          # heartbeat: has new data landed?
dbt docs generate             # lineage graph for the README screenshot
```

(`make dbt-build` runs deps + seed + run + test in one go.)

## Cost & teardown

Everything is serverless and sized for the **Free Tier**: S3 (pennies), Athena (pay-per-scan, kept
tiny by partitioning + columnar Parquet), Glue crawler (on-demand). A billing alarm is provisioned by
Terraform. **Tear down when done:**

```bash
cd terraform && terraform destroy
```

## Headline numbers (fill in after a build)

- Models a full month of NYC taxi trips (**~3M rows**) into a tested star schema.
- **100+ dbt data-quality tests** + source-freshness gate every build.
- **Bronze → silver → gold** medallion; `fct_trips` is incremental + date-partitioned.
- Messy rows aren't dropped — they're **quarantined with a reason** (observable data quality).
