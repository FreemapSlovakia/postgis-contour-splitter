# Countour splitter

Tool reads contours from a GeoPackage and writes them to a PostGIS table, evenly splitting lines so slices have at most 1000 (configurable) coordinates. Optional simplification is available before splitting. Source geometries are reprojected to EPSG:3857 in PostGIS with `ST_Transform` based on GeoPackage metadata.

## Rust version (faster, streaming)

Requirements: Rust toolchain, system build tools. The binary streams rows from the GeoPackage via SQLite, simplifies with `geo`, and inserts into Postgres. No features are loaded into memory at once.

```bash
cargo build --release
./target/release/splitter-rs \
  --source-gpkg /path/to/contours.gpkg \
  --source-table contours_gedtm30 \
  --dest-table cont_dmr_split \
  --source-epsg 25833 \
  --split-max-points 1000 \
  --simplify-tolerance 0 \
  --simplify-high-quality false \
  --commit-interval 1000
```

Flags also read matching env vars (e.g. `SOURCE_GPKG`, `SOURCE_TABLE`, `DEST_TABLE`, `SOURCE_EPSG`, `SPLIT_MAX_POINTS`, `SIMPLIFY_TOLERANCE`, `SIMPLIFY_HIGH_QUALITY`, `COMMIT_INTERVAL`, `DATABASE_URL` or `PG_CONNECTION_STRING`).
