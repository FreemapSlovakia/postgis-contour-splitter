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

  const qs = new QueryStream('SELECT ogc_fid, height, st_asbinary(wkb_geometry) AS wkb_geometry FROM contour');
  const stream = client.query(qs);

  for await (const row of stream) {
    num++;
    if (num % 1000 === 0) {
      console.log('ROW', num);
    }
    const gj = wkx.Geometry.parse(row.wkb_geometry).toGeoJSON();
    // console.log(gj);
    const len = gj.coordinates.length;
    const n = Math.ceil(len / 1000);
    const size = len / n;
    let from = 0;

    for (let i = 0; i < n; i++) {
      const rSize = Math.round(size);
      const sliceCoords = gj.coordinates.slice(from, from + rSize);
      const sliceGeom = new wkx.LineString(sliceCoords.map(([x, y]) => (new wkx.Point(x, y)))).toWkb();
      await client2.query(
        'INSERT INTO contour_split (cid, height, geom) VALUES ($1, $2, ST_GeomFromWKB($3, 900914))',
        [
          row.ogc_fid,
          row.height,
          sliceGeom,
        ],
      );

      from += rSize - 1;
    }
  }

  client.end();
  client2.end();
};

fn().catch(err => {
  console.log(err);
});
