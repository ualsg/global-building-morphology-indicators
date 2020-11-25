DROP TABLE IF EXISTS {{db_schema}}.agg_gadm36_areas;


CREATE TABLE {{db_schema}}.agg_gadm36_areas AS (
    WITH gadm36_areas AS (
        SELECT name_0, name_1, name_2, name_3, name_4, name_5, geom, ST_AREA(geom::geography) AS area_sqm
        FROM {{db_schema}}.gadm36)
    SELECT
        name_0 AS country,
        name_1 AS admin_level1,
        name_2 AS admin_level2,
        sum(area_sqm) OVER (PARTITION BY name_0) AS agg_area_country,
        sum(area_sqm) OVER (PARTITION BY name_0, name_1) AS agg_area_admin_level1,
        sum(area_sqm) OVER (PARTITION BY name_0, name_1, name_2) AS agg_area_admin_level2
    FROM
        gadm36_areas
    ORDER BY
        country, admin_level1, admin_level2);
