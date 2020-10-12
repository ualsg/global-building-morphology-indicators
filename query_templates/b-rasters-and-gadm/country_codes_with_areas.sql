CREATE TABLE {{db_schema}}.country_codes_with_areas AS (
	WITH agg_gadm36_country_areas AS (
		SELECT
			country, sum(area_sqm) AS gadm_area_sqm
		FROM
			{{db_schema}}.agg_gadm36_country_admin_level_areas
		GROUP BY
			country
		ORDER BY
			country
	)
	SELECT
		cc.*, agca.gadm_area_sqm
	FROM
		{{db_schema}}.country_codes cc
	LEFT JOIN agg_gadm36_country_areas agca ON
		cc.gadm_country = agca.country
);