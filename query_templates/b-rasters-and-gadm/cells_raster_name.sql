DROP TABLE IF EXISTS {{db_schema}}.cells_{{raster_name}} CASCADE;


CREATE TABLE {{db_schema}}.cells_{{raster_name}} AS (
                          SELECT
                              raster.id AS cell_id,
                              st_centroid(raster.geom) AS cell_centroid,
                              raster.geom AS cell_geom,
                              ST_Area(ST_Transform(raster.geom, 4326), TRUE) AS cell_area,
                              gadm.name_0 AS country,
                              gadm.name_1 AS admin_div1,
                              gadm.name_2 AS admin_div2,
                              gadm.name_3 AS admin_div3,
                              gadm.name_4 AS admin_div4,
                              gadm.name_5 AS admin_div5,{% if raster_population %}
                              raster.val AS {{raster_population}},{% endif %}
                              cc.country_name_english AS country_official_name,
                              cc.alpha2_code AS country_code2,
                              cc.alpha3_code AS country_code3
                          FROM
                              {{db_schema}}.raster_polygons_{{raster_name}} AS raster
                              LEFT JOIN {{db_schema}}.gadm36 AS gadm ON ST_Contains(gadm.geom, ST_Centroid(raster.geom))
                              LEFT JOIN {{db_schema}}.country_codes AS cc ON gadm.name_0 = cc.gadm_country
                          );


--adding another column to denote whether centroid falls on continental land or not
ALTER TABLE {{db_schema}}.cells_{{raster_name}}
    ADD COLUMN centroid_on_land BOOLEAN;


UPDATE {{db_schema}}.cells_{{raster_name}} SET
    centroid_on_land = CASE WHEN country IS NULL THEN FALSE WHEN country IS NOT NULL THEN TRUE END;


-- creating the spatial index for cell table
CREATE INDEX cells_{{raster_name}}_gidx ON {{db_schema}}.cells_{{raster_name}} USING gist(cell_geom);
CREATE INDEX cells_{{raster_name}}_centroid_gidx ON {{db_schema}}.cells_{{raster_name}} USING gist(cell_centroid);


VACUUM ANALYZE {{db_schema}}.cells_{{raster_name}};