SET postgis.enable_outdb_rasters = True;
SET postgis.gdal_enabled_drivers = 'GTiff';

-- SELECT st_gdaldrivers();
DROP TABLE IF EXISTS gbmi.rast_agg_bgi_by_cell_{{raster_name}}_attr_abbr;


CREATE TABLE gbmi.rast_agg_bgi_by_cell_{{raster_name}}_attr_abbr AS
    SELECT
        st_union(st_asraster(a.\"c_geom\", b.rast, 'band_pix_type', a.\"attr_abbr\", -9999)) AS rast
    FROM
        (SELECT \"cell_geom\" AS \"c_geom\", \"attr\"::psql_dtype AS \"attr_abbr\" FROM gbmi.agg_bgi_by_cell_{{raster_name}}) AS a,
        public.raw_tiles_{{raster_name}} AS b;


CREATE TABLE gbmi.attr_abbr_out AS
    SELECT
        lo_from_bytea(0, st_asgdalraster(st_union(rast), 'GTiff', ARRAY ['COMPRESS=DEFLATE', 'PREDICTOR=2', 'PZLEVEL=9'])) AS loid
    FROM
        gbmi.rast_agg_bgi_by_cell_{{raster_name}}_attr_abbr;


SELECT
    lo_export(loid, 'export_dir/rast_agg_bgi_by_cell_{{raster_name}}_attr_abbr.tiff')
FROM
    gbmi.attr_abbr_out;


SELECT
    lo_unlink(loid)
FROM
    gbmi.attr_abbr_out;

DROP TABLE IF EXISTS gbmi.attr_abbr_out;


DROP TABLE IF EXISTS gbmi.rast_agg_bgi_by_cell_{{raster_name}}_attr_abbr;






