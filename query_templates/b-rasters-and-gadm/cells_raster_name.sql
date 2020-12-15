DROP TABLE IF EXISTS {{db_schema}}.cells_{{raster_name}} CASCADE;


CREATE TABLE {{db_schema}}.cells_{{raster_name}} AS (
                          SELECT
                              cc.cell_id AS cell_id,
                              cc.centroid_geom AS centroid_geom,
                              ST_Point(ST_XMin(ST_Extent(raster.geom)),
                                       ST_YMax(ST_Extent(raster.geom))) AS upper_left_geom,
                              ST_Area(ST_Transform(raster.geom, 4326), TRUE) AS area,
                              cc.admin_div2 AS admin_div2,
                              cc.admin_div1 AS admin_div1,
                              cc.country AS country,
                              cc.country_official_name AS country_official_name,
                              cc.country_code2 AS country_code2,
                              cc.country_code3 AS country_code3,{% if raster_population  %}
                              raster.val AS {{raster_population}},{% endif %}
                              raster.geom AS raster_geom
                          FROM
                              {{db_schema}}.raster_polygons_{{raster_name}} AS raster
                              JOIN {{db_schema}}.cell_centroids_{{raster_name}} AS cc
                                    ON ST_Contains(raster.geom, cc.centroid_geom)
                          GROUP BY
                              cell_id,
                              centroid_geom,
                              area,
                              admin_div2,
                              admin_div1,
                              country,
                              country_official_name,
                              country_code2,
                              country_code3,{% if raster_population  %}
                              {{raster_population}},{% endif %}
                              raster_geom
                          );


--adding another column to denote whether centroid falls on continental land or not
ALTER TABLE {{db_schema}}.cells_{{raster_name}}
    ADD COLUMN centroid_on_land BOOLEAN;


UPDATE {{db_schema}}.cells_{{raster_name}} SET
    centroid_on_land = CASE WHEN country IS NULL THEN FALSE WHEN country IS NOT NULL THEN TRUE END;


-- creating the spatial index for cell table
CREATE INDEX cells_{{raster_name}}_gidx ON {{db_schema}}.cells_{{raster_name}} USING gist(raster_geom);


VACUUM ANALYZE {{db_schema}}.cells_{{raster_name}};