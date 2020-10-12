CREATE TABLE {{db_schema}}.agg_gadm36_country_admin_level_areas AS (
	SELECT
		name_0 AS country, name_1 AS admin_level1, name_2 AS admin_level2, sum(area_sqm) AS "area_sqm"
	FROM
		{{db_schema}}.gadm36_areas
	GROUP BY
		country, admin_level1, admin_level2
	ORDER BY
		country, admin_level1, admin_level2
);

