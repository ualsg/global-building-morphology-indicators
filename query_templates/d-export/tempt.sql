SET postgis.gdal_enabled_drivers = 'GTiff';

CREATE TABLE gbmi.rast_agg_bgi_by_cell_worldpop2020_1km_blvl_mpr AS
    SELECT
        st_union(st_asraster(a."c_geom", b.rast, '32BF', a."blvl_mpr", -9999)) AS rast
    FROM
        (SELECT "cell_geom" AS "c_geom", "building:levels_mean_pct_rnk"::float AS "blvl_mpr" FROM gbmi.agg_bgi_by_cell_worldpop2020_1km) AS a,
        public.raw_tiles_worldpop2020_1km AS b;


CREATE TABLE gbmi.blvl_mpr_out AS
    SELECT
        lo_from_bytea(0, st_asgdalraster(st_union(rast), 'GTiff', ARRAY ['COMPRESS=DEFLATE', 'PREDICTOR=2', 'PZLEVEL=9'])) AS loid
    FROM
        gbmi.rast_agg_bgi_by_cell_worldpop2020_1km_blvl_mpr;


SELECT
    lo_export(loid, '/data/gbmi_export/amsterdam/agg_cell/rast_agg_bgi_by_cell_worldpop2020_1km_blvl_mpr.tiff')
FROM
    gbmi.blvl_mpr_out;


SELECT
    lo_unlink(loid)
FROM
    gbmi.blvl_mpr_out;

DROP TABLE IF EXISTS gbmi.blvl_mpr_out;





