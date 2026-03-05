# h3-migration

One-shot migration tool: rewrite all walkthru-earth S3 Parquet files from v1 (H3 hex strings + geometry) to v2 (H3 int64, no redundant columns).

Part of the [walkthru-earth](https://github.com/walkthru-earth) index family alongside `dem-terrain`, `walkthru-building-index`, `walkthru-pop-index`, and `walkthru-weather-index`.

## What this does

Reads existing v1 Parquet files from Source Cooperative S3, transforms them via pure DuckDB SQL, and writes optimized v2 files to a `v2/` prefix on S3. Original v1 files are untouched.

### Transforms applied

| Change | Before (v1) | After (v2) |
|--------|------------|------------|
| h3_index type | VARCHAR hex string (`"88271c69cdfffff"`) | BIGINT int64 via `h3_string_to_h3()` |
| geometry column | POINT WKB (native Parquet 2.11+ GEOMETRY) | Dropped — derivable via `h3_cell_to_boundary_wkt()` |
| lat, lon columns | float32 center coordinates | Dropped — derivable via `h3_cell_to_lat()`, `h3_cell_to_lng()` |
| area_km2 column | float32 cell area | Dropped — derivable via `h3_cell_area()` |
| h3_res column | uint8 (baked-in Hive partition metadata) | Dropped — redundant with partition path |
| Part files | Weather has `part-0.parquet .. part-84.parquet` | Single `data.parquet` per partition |
| Sort order | ORDER BY h3_index (string) | ORDER BY h3_index (int64, delta-encodes well) |
| Compression | ZSTD level 3 | ZSTD level 3 (unchanged, optimal for browser decompression) |

### Why these changes

- **int64 H3**: Delta encoding on sorted int64 compresses dramatically better than string. Parquet row group min/max statistics enable spatial pushdown via range queries.
- **Drop geometry/lat/lon/area_km2**: All derivable from H3 index at query time using DuckDB h3 extension (`h3_cell_to_lat`, `h3_cell_to_lng`, `h3_cell_to_boundary_wkt`, `h3_cell_area`). The geometry column alone was the largest column per file.
- **Single data.parquet**: Browser consumers (DuckDB-WASM) perform one HTTP metadata fetch instead of 85 separate fetches for part files.
- **Inspired by**: [crim-ca/ogc-dggs notebook](https://github.com/crim-ca/ogc-dggs/blob/main/canada-population/data_preparation.ipynb) — demonstrated ~87% size reduction by dropping geometry, ~10% more by converting H3 string to int64.

## Commands

```bash
# Setup
uv sync

# Dry run (shows what would be migrated, no writes)
uv run python main.py                              # All datasets
uv run python main.py --dataset building            # Building only
uv run python main.py --dataset dem                 # DEM only

# Execute migration (writes to S3 under v2/ prefix)
uv run python main.py --dataset building --execute
uv run python main.py --dataset population --execute
uv run python main.py --dataset dem --execute       # ~287 GB, run on cloud
uv run python main.py --dataset weather --execute --weather-dates 2026-03-01,2026-03-02

# Validate v2 output
uv run python main.py --dataset building --validate
```

## S3 layout: v1 vs v2

```
v1 (current, untouched):
  dem-terrain/h3/h3_res={1..10}/data.parquet
  indices/building/h3/h3_res={3..8}/data.parquet
  indices/population/scenario=SSP2/h3_res={1..8}/data.parquet
  indices/weather/model=GraphCast_GFS/date=.../hour=.../h3_res=.../part-*.parquet

v2 (new, optimized):
  dem-terrain/v2/h3/h3_res={1..10}/data.parquet
  indices/building/v2/h3/h3_res={3..8}/data.parquet
  indices/population/v2/scenario=SSP2/h3_res={1..8}/data.parquet
  indices/weather/v2/model=GraphCast_GFS/date=.../hour=.../h3_res=.../data.parquet
```

All on `s3://us-west-2.opendata.source.coop/walkthru-earth/`.

## v2 schema per dataset

### DEM (res 1-10)
`h3_index` (BIGINT), `elev` (FLOAT), `slope` (FLOAT), `aspect` (FLOAT), `tri` (FLOAT), `tpi` (FLOAT)

### Building (res 3-8)
`h3_index` (BIGINT), `building_count` (INT), `building_density` (FLOAT), `total_footprint_m2` (FLOAT), `coverage_ratio` (FLOAT), `avg_height_m` (FLOAT), `max_height_m` (FLOAT), `height_std_m` (FLOAT), `total_volume_m3` (FLOAT), `volume_density_m3_per_km2` (FLOAT), `avg_footprint_m2` (FLOAT)

### Population (res 1-8)
`h3_index` (BIGINT), `pop_2025` (FLOAT), `pop_2030` (FLOAT), ..., `pop_2100` (FLOAT)

### Weather (res 0-5, daily)
`h3_index` (BIGINT), `timestamp` (TIMESTAMP), `temperature_2m_C` (FLOAT), `wind_speed_10m_ms` (FLOAT), ... (27 weather columns)

## Browser consumption pattern

v2 files are optimized for DuckDB-WASM + h3 extension in the browser:

```sql
LOAD h3;

-- Query by viewport: convert bounds to H3 cells, query sorted int64 index
SELECT h3_index,
       h3_cell_to_lat(h3_index) AS lat,
       h3_cell_to_lng(h3_index) AS lng,
       elev, slope
FROM read_parquet('https://...dem-terrain/v2/h3/h3_res=5/data.parquet')
WHERE h3_index BETWEEN ? AND ?   -- row group min/max statistics prune efficiently

-- For deck.gl H3HexagonLayer (needs hex string):
SELECT h3_h3_to_string(h3_index) AS h3_hex, elev
FROM read_parquet('https://...')
```

Sorted int64 H3 indices share base-cell prefixes in the high bits, so geographically nearby cells cluster in value ranges. Parquet row group statistics prune effectively without needing a geometry column or lat/lon.

## Key design decisions

- **Pure DuckDB SQL**: All transforms run as `COPY (SELECT ... FROM read_parquet(...) ORDER BY h3_index) TO ...`. No Python data handling, no pandas, no pyarrow manipulation. DuckDB streams from S3 and writes back.
- **v2/ prefix**: Writes to new paths, never modifies v1 originals. Safe to run and re-run.
- **Idempotent**: Checks if v2 file already exists with correct schema before writing. Re-running skips completed partitions.
- **h3 extension**: Uses `h3_string_to_h3()` from the DuckDB h3 community extension for VARCHAR-to-BIGINT conversion. Extension is optional for dry runs.
- **Weather discovery**: Uses boto3 `list_objects_v2` to discover Hive partition paths dynamically. Supports `--weather-dates` filter.
- **DEM is 287 GB**: The DEM res 10 file alone is ~245 GB. Run on a cloud instance near us-west-2 for network throughput.

## Sibling project changes

All four source projects were also updated in the same effort:

- **duckdb upgraded**: All projects `duckdb==1.5.0.dev332` -> `duckdb==1.5.0.dev334`
- **Pipeline code updated**: h3_index now written as int64 in new pipeline runs, geometry/lat/lon/area_km2 dropped from output, spatial extension removed, `GEOPARQUET_VERSION 'BOTH'` removed
- **Weather `dem.py`**: Updated `_aggregate_dem_to_parent()` to handle int64 H3 values with `h3.int_to_str()`/`h3.str_to_int()` wrappers
- **Weather `h3_grid.py`**: H3 cells stored as int64 in DataFrame (lat/lon kept for interpolation, not written to output)
- **Weather `export.py`**: Removed geometry, lat, lon, area_km2 from PyArrow arrays and DuckDB merge

## Environment

- `.env` file with S3 credentials (gitignored, copied from dem-terrain)
- AWS profile `sc-iam` for Source Cooperative S3
- S3 bucket: `us-west-2.opendata.source.coop`

## File layout

```
main.py              Migration script (pure DuckDB SQL)
pyproject.toml       Dependencies: duckdb==1.5.0.dev334, boto3, pyarrow
.env                 S3 credentials (gitignored)
CLAUDE.md            This file
```

## License

CC BY 4.0 by walkthru-earth.
