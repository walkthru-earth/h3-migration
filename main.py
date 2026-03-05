"""Migrate S3 Parquet files to optimized H3 int64 schema under v2/ prefix.

Reads current v1 files from Source Cooperative S3, writes optimized v2:
  - h3_index: VARCHAR hex -> BIGINT (via h3_string_to_h3())
  - Drops: geometry, lat, lon, area_km2
  - Single sorted data.parquet per partition (no part files)
  - ZSTD level 3, ROW_GROUP_SIZE 1M, ORDER BY h3_index

Output layout (v2/ prefix, same partitioning after):
  dem:        .../dem-terrain/v2/h3/h3_res={1..10}/data.parquet
  building:   .../indices/building/v2/h3/h3_res={3..8}/data.parquet
  population: .../indices/population/v2/scenario=SSP2/h3_res={1..8}/data.parquet
  weather:    .../indices/weather/v2/model=GraphCast_GFS/date=.../hour=.../h3_res=.../data.parquet

All transforms are pure DuckDB SQL — no Python data handling.

Usage:
  uv run python main.py                             # Dry run (all datasets)
  uv run python main.py --execute                   # Write all to S3
  uv run python main.py --dataset dem --execute      # DEM only
  uv run python main.py --dataset weather --execute --weather-dates 2026-03-01
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time

import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


# ── Load .env if present ─────────────────────────────────────────────────────

def _load_dotenv() -> None:
    from pathlib import Path

    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, val = line.partition("=")
        if key and val and key not in os.environ:
            os.environ[key] = val


_load_dotenv()

S3_BUCKET = os.environ.get("S3_BUCKET", "us-west-2.opendata.source.coop")
S3_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-west-2")


# ── Dataset definitions (v1 source → v2 destination) ────────────────────────

DATASETS = {
    "dem": {
        "src": f"{S3_BUCKET}/walkthru-earth/dem-terrain/h3",
        "dst": f"{S3_BUCKET}/walkthru-earth/dem-terrain/v2/h3",
        "partitions": [f"h3_res={r}" for r in range(1, 11)],
        "drop_cols": {"geometry", "lat", "lon", "h3_res"},
    },
    "building": {
        "src": f"{S3_BUCKET}/walkthru-earth/indices/building/h3",
        "dst": f"{S3_BUCKET}/walkthru-earth/indices/building/v2/h3",
        "partitions": [f"h3_res={r}" for r in range(3, 9)],
        "drop_cols": {"geometry", "lat", "lon", "area_km2", "h3_res"},
    },
    "population": {
        "src": f"{S3_BUCKET}/walkthru-earth/indices/population/scenario=SSP2",
        "dst": f"{S3_BUCKET}/walkthru-earth/indices/population/v2/scenario=SSP2",
        "partitions": [f"h3_res={r}" for r in range(1, 9)],
        "drop_cols": {"geometry", "lat", "lon", "area_km2", "h3_res"},
    },
    "weather": {
        "src": f"{S3_BUCKET}/walkthru-earth/indices/weather",
        "dst": f"{S3_BUCKET}/walkthru-earth/indices/weather/v2",
        "partitions": None,  # discovered dynamically
        "drop_cols": {"geometry", "lat", "lon", "area_km2", "h3_res"},
    },
}


# ── DuckDB setup ────────────────────────────────────────────────────────────

def fmt_duration(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def setup_connection() -> duckdb.DuckDBPyConnection:
    """Create DuckDB connection with httpfs + h3 extensions and S3 credentials."""
    log.info("Initializing DuckDB %s", duckdb.__version__)
    con = duckdb.connect()

    # httpfs for S3 access
    try:
        con.execute("LOAD httpfs")
    except Exception:
        con.execute("INSTALL httpfs; LOAD httpfs")
    log.info("  Extension 'httpfs' loaded")

    # h3 from community for h3_string_to_h3()
    try:
        con.execute("LOAD h3")
        log.info("  Extension 'h3' loaded")
    except Exception:
        try:
            con.execute("INSTALL h3 FROM community; LOAD h3")
            log.info("  Extension 'h3' loaded (community)")
        except Exception as e:
            log.warning("  h3 extension unavailable (%s) — needed for --execute", e)

    # S3 credentials
    aws_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

    if aws_key and aws_secret:
        con.execute(f"SET s3_region='{S3_REGION}'")
        con.execute(f"SET s3_access_key_id='{aws_key}'")
        con.execute(f"SET s3_secret_access_key='{aws_secret}'")
        con.execute("SET s3_url_style='path'")
        log.info("  S3 configured: region=%s", S3_REGION)
    else:
        log.warning("  No AWS credentials — reads will use anonymous access")

    # Performance
    con.execute("SET preserve_insertion_order=false")

    return con


# ── Schema introspection (pure SQL) ─────────────────────────────────────────

def get_columns(con: duckdb.DuckDBPyConnection, s3_uri: str) -> list[str]:
    """Get column names from a Parquet file via DESCRIBE."""
    rows = con.execute(f"""
        SELECT column_name
        FROM (DESCRIBE SELECT * FROM read_parquet('{s3_uri}'))
    """).fetchall()
    return [r[0] for r in rows]


def get_h3_type(con: duckdb.DuckDBPyConnection, s3_uri: str) -> str:
    """Return the DuckDB type name of h3_index column."""
    try:
        row = con.execute(f"""
            SELECT typeof(h3_index) FROM read_parquet('{s3_uri}') LIMIT 1
        """).fetchone()
        return row[0] if row else "UNKNOWN"
    except Exception:
        return "ERROR"


def build_select_sql(
    con: duckdb.DuckDBPyConnection, s3_uri: str, drop_cols: set[str]
) -> str:
    """Build SELECT clause: convert h3_index VARCHAR->BIGINT, drop unwanted cols."""
    columns = get_columns(con, s3_uri)
    h3_type = get_h3_type(con, s3_uri)

    parts = []
    for col in columns:
        if col in drop_cols:
            continue
        if col == "h3_index" and "VARCHAR" in h3_type:
            parts.append("h3_string_to_h3(h3_index) AS h3_index")
        else:
            parts.append(col)

    return ",\n               ".join(parts)


def is_already_migrated(
    con: duckdb.DuckDBPyConnection, s3_uri: str, drop_cols: set[str]
) -> bool:
    """Check if a v2 file already exists and looks correct."""
    h3_type = get_h3_type(con, s3_uri)
    if h3_type == "ERROR":
        return False
    columns = get_columns(con, s3_uri)
    has_redundant = bool(drop_cols & set(columns))
    is_int = "BIGINT" in h3_type or "INTEGER" in h3_type
    return is_int and not has_redundant


# ── Static dataset migration (DEM, building, population) ────────────────────

def migrate_static(
    con: duckdb.DuckDBPyConnection,
    name: str,
    config: dict,
    execute: bool,
) -> None:
    """Migrate a static dataset: read v1, write v2/h3/same partitioning."""
    log.info("=" * 60)
    log.info("Dataset: %s", name.upper())
    log.info("  Source: s3://%s", config["src"])
    log.info("  Dest:   s3://%s", config["dst"])

    for partition in config["partitions"]:
        src_uri = f"s3://{config['src']}/{partition}/data.parquet"
        dst_uri = f"s3://{config['dst']}/{partition}/data.parquet"

        log.info("  %s", partition)

        # Skip if v2 already exists and looks good
        if is_already_migrated(con, dst_uri, config["drop_cols"]):
            log.info("    v2 already exists — skipping")
            continue

        # Verify source is readable
        h3_type = get_h3_type(con, src_uri)
        if h3_type == "ERROR":
            log.warning("    Source not readable — skipping")
            continue

        select_sql = build_select_sql(con, src_uri, config["drop_cols"])

        query = f"""
            COPY (
                SELECT {select_sql}
                FROM read_parquet('{src_uri}')
                ORDER BY h3_index
            ) TO '{dst_uri}'
            (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3,
             ROW_GROUP_SIZE 1000000)
        """

        if not execute:
            log.info("    [DRY RUN] %s -> %s", partition, dst_uri)
            log.info("    h3_index: %s", h3_type)
            log.info("    Columns: %s", get_columns(con, src_uri))
            continue

        log.info("    Writing -> %s", dst_uri)
        t0 = time.time()
        con.execute(query)

        # Verify output
        row_count = con.execute(f"""
            SELECT count(*) FROM read_parquet('{dst_uri}')
        """).fetchone()[0]

        log.info(
            "    Done: %s rows in %s",
            f"{row_count:,}",
            fmt_duration(time.time() - t0),
        )


# ── Weather migration (Hive-partitioned, may have part-*.parquet) ────────────

def discover_weather_partitions(date_filter: list[str] | None = None) -> list[str]:
    """Discover weather partition paths on S3 via boto3 list."""
    import boto3

    base = DATASETS["weather"]["src"]
    bucket = base.split("/", 1)[0]
    prefix = base.split("/", 1)[1] + "/model=GraphCast_GFS/"

    s3 = boto3.client("s3", region_name=S3_REGION)
    paginator = s3.get_paginator("list_objects_v2")

    partitions = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            date_dir = cp["Prefix"].rstrip("/").rsplit("/", 1)[-1]
            if date_filter:
                date_val = date_dir.replace("date=", "")
                if date_val not in date_filter:
                    continue

            for hour_page in paginator.paginate(
                Bucket=bucket, Prefix=cp["Prefix"], Delimiter="/"
            ):
                for hour_cp in hour_page.get("CommonPrefixes", []):
                    for res_page in paginator.paginate(
                        Bucket=bucket, Prefix=hour_cp["Prefix"], Delimiter="/"
                    ):
                        for res_cp in res_page.get("CommonPrefixes", []):
                            rel = res_cp["Prefix"][len(prefix) :].rstrip("/")
                            partitions.append(rel)

    log.info("Discovered %d weather partitions", len(partitions))
    return partitions


def migrate_weather(
    con: duckdb.DuckDBPyConnection,
    execute: bool,
    date_filter: list[str] | None = None,
) -> None:
    """Migrate weather: read v1 (may be part files), write single v2 data.parquet."""
    config = DATASETS["weather"]
    src_model = f"s3://{config['src']}/model=GraphCast_GFS"
    dst_model = f"s3://{config['dst']}/model=GraphCast_GFS"

    log.info("=" * 60)
    log.info("Dataset: WEATHER")
    log.info("  Source: %s", src_model)
    log.info("  Dest:   %s", dst_model)

    partitions = discover_weather_partitions(date_filter)

    for partition in partitions:
        dst_uri = f"{dst_model}/{partition}/data.parquet"

        log.info("  %s", partition)

        # Skip if v2 already done
        if is_already_migrated(con, dst_uri, config["drop_cols"]):
            log.info("    v2 already exists — skipping")
            continue

        # Try data.parquet first, then part-*.parquet glob
        src_data = f"{src_model}/{partition}/data.parquet"
        src_parts = f"{src_model}/{partition}/part-*.parquet"

        h3_type = get_h3_type(con, src_data)
        if h3_type != "ERROR":
            read_uri = src_data
        else:
            # Check part files
            part0 = f"{src_model}/{partition}/part-0.parquet"
            h3_type = get_h3_type(con, part0)
            if h3_type == "ERROR":
                log.info("    No readable source files — skipping")
                continue
            read_uri = src_parts

        select_sql = build_select_sql(con, read_uri.replace("*", "0") if "*" in read_uri else read_uri, config["drop_cols"])

        # Always produce a single sorted data.parquet
        query = f"""
            COPY (
                SELECT {select_sql}
                FROM read_parquet('{read_uri}', hive_partitioning=false)
                ORDER BY h3_index
            ) TO '{dst_uri}'
            (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3,
             ROW_GROUP_SIZE 1000000)
        """

        if not execute:
            log.info("    [DRY RUN] -> %s", dst_uri)
            log.info("    h3_index: %s, source: %s", h3_type, "part files" if "*" in read_uri else "data.parquet")
            continue

        log.info("    Writing -> %s", dst_uri)
        t0 = time.time()
        con.execute(query)

        row_count = con.execute(f"""
            SELECT count(*) FROM read_parquet('{dst_uri}')
        """).fetchone()[0]

        log.info(
            "    Done: %s rows in %s",
            f"{row_count:,}",
            fmt_duration(time.time() - t0),
        )


# ── Validation ───────────────────────────────────────────────────────────────

def validate(con: duckdb.DuckDBPyConnection, dataset: str) -> None:
    """Validate a migrated v2 dataset."""
    config = DATASETS[dataset]

    log.info("=" * 60)
    log.info("Validating: %s", dataset.upper())

    if dataset == "weather":
        log.info("  (weather validation requires --weather-dates, skipping)")
        return

    for partition in config["partitions"]:
        uri = f"s3://{config['dst']}/{partition}/data.parquet"
        h3_type = get_h3_type(con, uri)

        if h3_type == "ERROR":
            log.warning("  %s: NOT FOUND", partition)
            continue

        cols = get_columns(con, uri)
        has_redundant = bool(config["drop_cols"] & set(cols))

        stats = con.execute(f"""
            SELECT count(*) AS rows,
                   typeof(h3_index) AS h3_type,
                   min(h3_index) AS h3_min,
                   max(h3_index) AS h3_max
            FROM read_parquet('{uri}')
        """).fetchone()

        status = "OK" if ("BIGINT" in h3_type and not has_redundant) else "NEEDS MIGRATION"
        log.info(
            "  %s: %s | %s rows | h3=%s | cols=%d %s",
            partition,
            status,
            f"{stats[0]:,}",
            stats[1],
            len(cols),
            cols,
        )


# ── CLI ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate walkthru-earth Parquet to v2 H3 int64 schema"
    )
    parser.add_argument(
        "--dataset",
        choices=["dem", "building", "population", "weather", "all"],
        default="all",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Write to S3 (default: dry run)",
    )
    parser.add_argument(
        "--weather-dates",
        type=str,
        default=None,
        help="Comma-separated dates for weather (e.g. 2026-03-01,2026-03-02)",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate v2 output (no writes)",
    )
    args = parser.parse_args()

    con = setup_connection()

    datasets = (
        ["dem", "building", "population", "weather"]
        if args.dataset == "all"
        else [args.dataset]
    )
    date_filter = args.weather_dates.split(",") if args.weather_dates else None

    if args.validate:
        for name in datasets:
            validate(con, name)
        return

    mode = "EXECUTE" if args.execute else "DRY RUN"
    log.info("Mode: %s | Datasets: %s", mode, datasets)

    t_total = time.time()

    for name in datasets:
        if name == "weather":
            migrate_weather(con, args.execute, date_filter)
        else:
            migrate_static(con, name, DATASETS[name], args.execute)

    log.info("=" * 60)
    log.info("Total: %s", fmt_duration(time.time() - t_total))

    if not args.execute:
        log.info("DRY RUN complete. Add --execute to write to S3.")


if __name__ == "__main__":
    main()
