CREATE TABLE {{db_schema}}.agg_gadm36_areas AS (
	SELECT
		name_0 AS admin_level0,
		name_1 AS admin_level1,
		name_2 AS admin_level2,
		sum(st_area(geom::geography)) OVER ( PARTITION BY name_0, name_1, name_2 ) AS agg_area_admin_level2,
		sum(st_area(geom::geography)) OVER ( PARTITION BY name_0, name_1 ) AS agg_area_admin_level1,
		sum(st_area(geom::geography)) OVER ( PARTITION BY name_0 ) AS agg_area_admin_level0
	FROM
		{{db_schema}}.gadm36
	ORDER BY
		name_0, name_1, name_2
);

