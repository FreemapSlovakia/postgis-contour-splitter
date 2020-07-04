const { Client } = require('pg');
const wkx = require('wkx');
const QueryStream = require('pg-query-stream');

const client = new Client();
const client2 = new Client();

let num = 0;

const fn = async () => {
  await Promise.all([
    client.connect(),
    client2.connect(),
  ]);

  const qs = new QueryStream(`
    SELECT
      cont.id AS id,
      cont.height AS height,
      st_asbinary(cont.wkb_geometry) AS wkb_geometry
    FROM cont LEFT JOIN cont_split ON cont_split.id = cont.id
    WHERE cont_split.id IS NULL
  `);

  const stream = client.query(qs);

  for await (const row of stream) {
    if (num % 1000 === 0) {
      console.log('ROW', num);
    }

    num++;

    const gj = wkx.Geometry.parse(row.wkb_geometry).toGeoJSON();
    // console.log(gj);
    const len = gj.coordinates.length;
    const n = Math.ceil(len / 200);
    const size = len / n;
    let from = 0;

    for (let i = 0; i < n; i++) {
      const sliceCoords = gj.coordinates.slice(Math.round(from) - (from > 0 ? 1 : 0), Math.round(from + size));
      const sliceGeom = new wkx.LineString(sliceCoords.map(([x, y]) => (new wkx.Point(x, y)))).toWkb();

      await client2.query(
        'INSERT INTO cont_split (id, height, wkb_geometry) VALUES ($1, $2, ST_GeomFromWKB($3, 900914))',
        [
          row.id,
          row.height,
          sliceGeom,
        ],
      );

      from += size;
    }
  }

  client.end();
  client2.end();
};

fn().catch(err => {
  console.log(err);
});
