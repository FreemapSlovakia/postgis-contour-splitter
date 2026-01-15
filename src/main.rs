use anyhow::{anyhow, Context, Result};
use clap::Parser;
use geo::{Coord, Geometry, LineString, Simplify, SimplifyVw};
use geozero::wkb::GpkgWkb;
use geozero::{CoordDimensions, ToGeo, ToWkb};
use postgres::{Client, NoTls};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use std::env;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "splitter-rs",
    version,
    about = "Stream contours from GeoPackage to Postgres"
)]
struct Args {
    /// Path to GeoPackage file
    #[arg(long, env = "SOURCE_GPKG")]
    source_gpkg: String,

    /// Source table name in GeoPackage
    #[arg(long, env = "SOURCE_TABLE")]
    source_table: String,

    /// Destination Postgres table name
    #[arg(long, env = "DEST_TABLE")]
    dest_table: String,

    /// Source EPSG (overrides GeoPackage metadata)
    #[arg(long, env = "SOURCE_EPSG")]
    source_epsg: Option<i32>,

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

const DEST_EPSG: i32 = 3857;

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

fn linestring_to_wkb(ls: LineString<f64>) -> Result<Vec<u8>> {
    Ok(Geometry::LineString(ls)
        .to_ewkb(CoordDimensions::xy(), None)
        .map_err(|e| anyhow!("Failed to encode WKB: {e}"))?)
}

fn lookup_gpkg_srid(conn: &Connection, table: &str) -> Result<i32> {
    let mut stmt = conn
        .prepare("SELECT srs_id FROM gpkg_geometry_columns WHERE table_name = ?1 LIMIT 1")
        .context("Failed to prepare GeoPackage SRID lookup")?;

    let srs_id: Option<i32> = stmt
        .query_row([table], |row| row.get(0))
        .optional()
        .context("Failed to query GeoPackage SRID")?;

    let srs_id = srs_id.ok_or_else(|| {
        anyhow!(
            "Missing GeoPackage SRID metadata for table {} (gpkg_geometry_columns)",
            table
        )
    })?;

    if srs_id <= 0 {
        return Err(anyhow!("Unsupported GeoPackage SRID {}", srs_id));
    }

    Ok(srs_id)
}

fn split_line(line: LineString<f64>, max_coords: usize) -> Vec<LineString<f64>> {
    if max_coords < 2 {
        return vec![line];
    }

    let coords: Vec<_> = line.coords().collect();

    if coords.len() <= max_coords {
        return vec![line];
    }

    // Aim for near-equal parts with a single shared vertex between slices, without rounding drift.
    let parts = (coords.len() + max_coords - 1) / max_coords;
    let base = coords.len() / parts;
    let extra = coords.len() % parts; // distribute remainders to the first slices

    let mut pieces = Vec::with_capacity(parts);
    let mut cursor = 0usize;

    let mut idx = 0usize;
    while cursor < coords.len() {
        let len_this = base + if idx < extra { 1 } else { 0 };

        let start_idx = if cursor > 0 {
            cursor.saturating_sub(1)
        } else {
            0
        };

        let end_idx = (cursor + len_this).min(coords.len());

        let slice = coords[start_idx..end_idx]
            .iter()
            .map(|c| Coord { x: c.x, y: c.y })
            .collect::<Vec<_>>();

        if slice.len() >= 2 {
            pieces.push(LineString::from(slice));
        }

        cursor = end_idx;
        idx += 1;
    }

    pieces
}

fn simplify_line(line: LineString<f64>, tolerance: f64, high_quality: bool) -> LineString<f64> {
    if tolerance <= 0.0 || line.0.len() < 3 {
        return line;
    }

    let simplified = if high_quality {
        line.simplify_vw(tolerance)
    } else {
        line.simplify(tolerance)
    };

    if simplified.0.len() >= 2 {
        simplified
    } else {
        line
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

    let source_epsg = match args.source_epsg {
        Some(epsg) => epsg,
        None => lookup_gpkg_srid(&gpkg, &args.source_table)?,
    };

    let mut stmt = gpkg
        .prepare(&format!(
            "SELECT ID as id, height, geom FROM {}",
            args.source_table
        ))
        .with_context(|| format!("Failed to prepare SELECT on {}", args.source_table))?;

    let insert_sql = format!(
        "INSERT INTO {} (id, height, wkb_geometry) VALUES ($1, $2, ST_Transform(ST_GeomFromWKB($3, $4), {}))",
        args.dest_table, DEST_EPSG
    );

    println!(
        "Reprojecting with PostGIS ST_Transform EPSG:{} -> EPSG:{}",
        source_epsg, DEST_EPSG
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

        let line = parse_gpkg_linestring(&geom)
            .with_context(|| format!("Failed to parse WKB for id {id}"))?;

        let line = simplify_line(line, args.simplify_tolerance, args.simplify_high_quality);

        for slice in split_line(line, args.split_max_points) {
            let wkb = linestring_to_wkb(slice)?;

            tx.execute(&insert_sql, &[&id, &height, &wkb, &source_epsg])?;
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
