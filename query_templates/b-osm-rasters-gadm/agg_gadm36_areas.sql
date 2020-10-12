DROP TABLE IF EXISTS {{public_schema}}.agg_gadm36_areas CASCADE;


CREATE TABLE {{public_schema}}.agg_gadm36_areas AS (
    WITH gadm36_areas AS (
        SELECT name_0, name_1, name_2, name_3, name_4, name_5, geom, ST_AREA(geom::geography) AS area_sqm
        FROM {{public_schema}}.gadm36)
    SELECT
        name_0 AS country,
        name_1 AS admin_div1,
        name_2 AS admin_div2,
        name_3 AS admin_div3,
        name_4 AS admin_div4,
        sum(area_sqm) OVER (PARTITION BY name_0) AS agg_area_country,
        sum(area_sqm) OVER (PARTITION BY name_0, name_1) AS agg_area_admin_div1,
        sum(area_sqm) OVER (PARTITION BY name_0, name_1, name_2) AS agg_area_admin_div2,
        sum(area_sqm) OVER (PARTITION BY name_0, name_1, name_2, name_3) AS agg_area_admin_div3,
        sum(area_sqm) OVER (PARTITION BY name_0, name_1, name_2, name_3, name_4) AS agg_area_admin_div4
    FROM
        gadm36_areas
    ORDER BY
        country, admin_div1, admin_div2, admin_div3, admin_div4);