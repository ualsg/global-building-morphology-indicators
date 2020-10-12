DROP TABLE IF EXISTS {{db_schema}}.cell_centroids_{{raster_name}} CASCADE;


CREATE TABLE {{db_schema}}.cell_centroids_{{raster_name}} AS (
    SELECT
        raster.id AS cell_id,
        raster.val AS cell_population,
        gadm.name_2 AS admin_div2,
        gadm.name_1 AS admin_div1,
        gadm.name_0 AS country,
        st_centroid(raster.geom) AS centroid_geom,
        cc.country_name_english AS "country_official_name",
        cc.alpha2_code AS "country_code2",
        cc.alpha3_code AS "country_code3"
    FROM
        {{db_schema}}.raster_polygons_{{raster_name}} AS raster
        LEFT JOIN {{db_schema}}.gadm36 AS gadm ON ST_Contains(gadm.geom, ST_Centroid(raster.geom))
        LEFT JOIN {{db_schema}}.country_codes AS cc ON gadm.name_0 = cc.gadm_country
);
