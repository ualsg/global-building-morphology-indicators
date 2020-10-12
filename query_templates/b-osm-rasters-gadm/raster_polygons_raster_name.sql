DROP TABLE IF EXISTS {{public_schema}}.raster_polygons_{{raster_name}} CASCADE;


CREATE TABLE {{public_schema}}.raster_polygons_{{raster_name}} AS (
                                    SELECT
                                        (gv).x,
                                        (gv).y,
                                        (gv).val,
                                        st_transform((gv).geom, 4326) AS geom
                                    FROM
                                        (
                                        SELECT ST_PixelAsPolygons(rast) AS gv
                                        FROM {{public_schema}}.raw_tiles_{{raster_name}}
                                        ) AS tiles
                                    );


--adding a primary key and index onto the new polygon table
ALTER TABLE {{public_schema}}.raster_polygons_{{raster_name}}
    ADD COLUMN id SERIAL
        PRIMARY KEY;


CREATE INDEX raster_polygons_{{raster_name}}_gidx ON {{public_schema}}.raster_polygons_{{raster_name}} USING gist(geom);