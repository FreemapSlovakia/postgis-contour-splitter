use anyhow::{anyhow, Context, Result};
use clap::Parser;
use geo::{Coord, Geometry, LineString, Simplify, SimplifyVw};
use geozero::wkb::GpkgWkb;
use geozero::{CoordDimensions, ToGeo, ToWkb};
use postgres::{Client, NoTls};
use rusqlite::{Connection, OpenFlags};
use std::env;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "splitter-rs",
    version,
    about = "Stream contours from GeoPackage to Postgres"
)]
struct Args {
    /// Path to GeoPackage file
    #[arg(long, env = "SOURCE_GPKG", default_value = "contours.gpkg")]
    source_gpkg: String,

    /// Source table name in GeoPackage
    #[arg(long, env = "SOURCE_TABLE", default_value = "contours_gedtm30")]
    source_table: String,

    /// Destination Postgres table name
    #[arg(long, env = "DEST_TABLE", default_value = "cont_dmr_split")]
    dest_table: String,

    /// Postgres connection string (or use PG_CONNECTION_STRING)
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,

    /// Max coordinates per slice
    #[arg(long, env = "SPLIT_MAX_POINTS", default_value_t = 1000)]
    split_max_points: usize,

    /// Rows per transaction commit
    #[arg(long, env = "COMMIT_INTERVAL", default_value_t = 1000)]
    commit_interval: usize,

    /// Simplification tolerance (0 disables)
    #[arg(long, env = "SIMPLIFY_TOLERANCE", default_value_t = 0.0)]
    simplify_tolerance: f64,

    /// Use high-quality (Visvalingam) simplification
    #[arg(long, env = "SIMPLIFY_HIGH_QUALITY", default_value_t = false)]
    simplify_high_quality: bool,
}

fn parse_gpkg_linestring(buffer: &[u8]) -> Result<LineString<f64>> {
    let geom: Geometry<f64> = GpkgWkb(buffer)
        .to_geo()
        .map_err(|e| anyhow!("Failed to decode GeoPackage geometry: {e}"))?;

    match geom {
        Geometry::LineString(ls) => Ok(LineString(
            ls.0.into_iter().map(|c| Coord { x: c.x, y: c.y }).collect(),
        )),
        other => Err(anyhow!("Unexpected geometry type: {:?}", other)),
    }
}

fn linestring_to_wkb(ls: &LineString<f64>) -> Result<Vec<u8>> {
    Ok(Geometry::LineString(ls.clone())
        .to_ewkb(CoordDimensions::xy(), Some(3857))
        .map_err(|e| anyhow!("Failed to encode WKB: {e}"))?)
}

fn split_line(line: &LineString<f64>, max_coords: usize) -> Vec<LineString<f64>> {
    if max_coords == 0 {
        return vec![line.clone()];
    }

    let coords: Vec<_> = line.coords().collect();

    if coords.len() <= max_coords {
        return vec![line.clone()];
    }

    let parts = (coords.len() + max_coords - 1) / max_coords;
    let size = (coords.len() as f64) / (parts as f64);
    let mut from = 0f64;
    let mut pieces = Vec::with_capacity(parts);

    for _ in 0..parts {
        let start = (from.round() as isize - if from > 0.0 { 1 } else { 0 }).max(0) as usize;

        let end = ((from + size).round() as usize).min(coords.len());

        let slice = coords[start..end]
            .iter()
            .map(|c| Coord { x: c.x, y: c.y })
            .collect::<Vec<_>>();

        if slice.len() >= 2 {
            pieces.push(LineString::from(slice));
        }

        from += size;
    }

    pieces
}

fn simplify_line(line: &LineString<f64>, tolerance: f64, high_quality: bool) -> LineString<f64> {
    if tolerance <= 0.0 || line.0.len() < 3 {
        return line.clone();
    }

    let simplified = if high_quality {
        line.simplify_vw(tolerance)
    } else {
        line.simplify(tolerance)
    };

    if simplified.0.len() >= 2 {
        simplified
    } else {
        line.clone()
    }
}

fn main() -> Result<()> {
    let args = Args::parse();

    let db_url = args
        .database_url
        .or_else(|| env::var("PG_CONNECTION_STRING").ok())
        .context("DATABASE_URL or PG_CONNECTION_STRING must be set for Postgres")?;

    let mut pg_client = Client::connect(&db_url, NoTls)
        .with_context(|| format!("Failed to connect to Postgres at {}", db_url))?;

    let gpkg = Connection::open_with_flags(
        &args.source_gpkg,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .with_context(|| format!("Failed to open GeoPackage file {}", args.source_gpkg))?;

    let mut stmt = gpkg
        .prepare(&format!(
            "SELECT ID as id, height, geom FROM {}",
            args.source_table
        ))
        .with_context(|| format!("Failed to prepare SELECT on {}", args.source_table))?;

    let insert_sql = format!(
        "INSERT INTO {} (id, height, wkb_geometry) VALUES ($1, $2, ST_GeomFromWKB($3, 3857))",
        args.dest_table
    );

    let mut rows = stmt
        .query([])
        .with_context(|| format!("Failed to query rows from {}", args.source_table))?;

    let mut processed = 0usize;
    let mut tx = pg_client.transaction()?;

    while let Some(row) = rows.next()? {
        let id: i64 = row.get("id")?;

        let height: Option<f64> = row.get("height")?;

        let geom: Vec<u8> = row.get("geom")?;

        if geom.is_empty() {
            continue;
        }

        let mut line = parse_gpkg_linestring(&geom)
            .with_context(|| format!("Failed to parse WKB for id {id}"))?;

        line = simplify_line(&line, args.simplify_tolerance, args.simplify_high_quality);

        for slice in split_line(&line, args.split_max_points) {
            let wkb = linestring_to_wkb(&slice)?;

            tx.execute(&insert_sql, &[&id, &height, &wkb])?;
        }

        processed += 1;

        if processed % args.commit_interval == 0 {
            tx.commit()?;
            println!("Processed {}", processed);
            tx = pg_client.transaction()?;
        }
    }

    tx.commit()?;
    println!("Finished. Total processed: {}", processed);
    Ok(())
}
