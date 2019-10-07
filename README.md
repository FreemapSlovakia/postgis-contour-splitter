# Countour splitter

Tool uses PostGIS, reads contours from `contour` table and writes them to `contour_split` table evenly splitting countours to have max 1000 coordinates.

To update table and column names or split limit please directly modify `index.js` file.

## Usage

```bash
npm i
node .
```
