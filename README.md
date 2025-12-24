# Countour splitter

Tool reads contours from a GeoPackage (`contours.gpkg`, table `contours_gedtm30` by default) and writes them to a PostGIS table (`cont_dmr_split` by default), evenly splitting lines so slices have at most 1000 coordinates. Optional simplification is available before splitting.

To update source path/table or split limit please directly modify `index.js` or set env vars (`SOURCE_GPKG`, `SOURCE_TABLE`, `DEST_TABLE`, `SPLIT_MAX_POINTS`, `COMMIT_INTERVAL`, `SIMPLIFY_TOLERANCE`, `SIMPLIFY_HIGH_QUALITY`).

## Rust version (faster, streaming)

Requirements: Rust toolchain, system build tools. The binary streams rows from the GeoPackage via SQLite, simplifies with `geo`, and inserts into Postgres. No features are loaded into memory at once.

```bash
cargo build --release
./target/release/splitter-rs \
  --source-gpkg /path/to/contours.gpkg \
  --source-table contours_gedtm30 \
  --dest-table cont_dmr_split \
  --split-max-points 1000 \
  --simplify-tolerance 0 \
  --simplify-high-quality false \
  --commit-interval 1000
```

Flags also read matching env vars (e.g. `SOURCE_GPKG`, `SOURCE_TABLE`, `DEST_TABLE`, `SPLIT_MAX_POINTS`, `SIMPLIFY_TOLERANCE`, `SIMPLIFY_HIGH_QUALITY`, `COMMIT_INTERVAL`, `DATABASE_URL` or `PG_CONNECTION_STRING`).

## Usage

```bash
npm i
node .
```
