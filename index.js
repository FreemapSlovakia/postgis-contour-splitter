const { Client } = require('pg');
const wkx = require('wkx');
const QueryStream = require('pg-query-stream');

const client = new Client();
const client2 = new Client();

let num = 0;

const sourceTable = 'cont_dmr';
const destTable = 'cont_dmr_split';

const fn = async () => {
  await Promise.all([
    client.connect(),
    client2.connect(),
  ]);

  const qs = new QueryStream(`
    SELECT
      ${sourceTable}.id AS id,
      ${sourceTable}.height AS height,
      st_asbinary(st_simplify(${sourceTable}.wkb_geometry, 0.1, true)) AS wkb_geometry
    FROM ${sourceTable} LEFT JOIN ${destTable} ON ${destTable}.id = ${sourceTable}.id
    WHERE ${destTable}.id IS NULL
  `);

  const stream = client.query(qs);

  for await (const row of stream) {
    if (num % 1000 === 0) {
      if (num > 0) {
        await client2.query('COMMIT');
      }

      console.log('ROW', num);

      await client2.query('BEGIN');
    }

    num++;

    const gj = wkx.Geometry.parse(row.wkb_geometry).toGeoJSON();
    // console.log(gj);
    const len = gj.coordinates.length;
    const n = Math.ceil(len / 1000);
    const size = len / n;
    let from = 0;

    for (let i = 0; i < n; i++) {
      const sliceCoords = gj.coordinates.slice(Math.round(from) - (from > 0 ? 1 : 0), Math.round(from + size));
      const sliceGeom = new wkx.LineString(sliceCoords.map(([x, y]) => (new wkx.Point(x, y)))).toWkb();

      await client2.query(
        `INSERT INTO ${destTable} (id, height, wkb_geometry) VALUES ($1, $2, ST_GeomFromWKB($3, 3857))`,
        [
          row.id,
          row.height,
          sliceGeom,
        ],
      );

      from += size;
    }
  }

  await client2.query('COMMIT');

  client.end();
  client2.end();
};

fn().catch(err => {
  console.log(err);
});
