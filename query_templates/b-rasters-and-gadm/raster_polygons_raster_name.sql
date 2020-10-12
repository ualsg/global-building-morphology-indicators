DROP TABLE IF EXISTS {{db_schema}}.raster_polygons_{{raster_name}} CASCADE;


CREATE TABLE {{db_schema}}.raster_polygons_{{raster_name}} AS (
                                    SELECT
                                        (gv).x,
                                        (gv).y,
                                        (gv).val,
                                        st_transform((gv).geom, 4326) AS geom
                                    FROM
                                        (
                                        SELECT ST_PixelAsPolygons(rast) AS gv
                                        FROM {{db_schema}}.raw_tiles_{{raster_name}}
                                        ) AS gvv
                                    );


--adding a primary key and index onto the new polygon table
ALTER TABLE {{db_schema}}.raster_polygons_{{raster_name}}
    ADD COLUMN id SERIAL
        PRIMARY KEY;


CREATE INDEX raster_polygons_{{raster_name}}_gidx ON {{db_schema}}.raster_polygons_{{raster_name}} USING gist(geom);