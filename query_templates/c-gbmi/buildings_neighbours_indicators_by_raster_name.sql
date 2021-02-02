DROP TABLE IF EXISTS {{gbmi_schema}}.buildings_neighbours_indicators_by_{{raster_name}} CASCADE;



CREATE TABLE {{gbmi_schema}}.buildings_neighbours_indicators_by_{{raster_name}} AS (
                                                    WITH bn_25 AS (
                                                                   SELECT *,
                                                                       CASE
                                                                           WHEN distance_25_mean IS NOT NULL
                                                                               THEN percent_rank() OVER (ORDER BY distance_25_mean)
                                                                       END AS distance_25_mean_pct_rnk
                                                                   FROM
                                                                       {{gbmi_schema}}.buildings_neighbours_25_by_{{raster_name}}
                                                                   ),
                                                        bn_50 AS (
                                                                  SELECT *,
                                                                      CASE
                                                                          WHEN distance_50_mean IS NOT NULL
                                                                              THEN percent_rank() OVER (ORDER BY distance_50_mean)
                                                                      END AS distance_50_mean_pct_rnk
                                                                  FROM
                                                                      {{gbmi_schema}}.buildings_neighbours_50_by_{{raster_name}}
                                                                  ),
                                                        bn_100 AS (
                                                                  SELECT *,
                                                                      CASE
                                                                          WHEN distance_100_mean IS NOT NULL
                                                                              THEN percent_rank() OVER (ORDER BY distance_100_mean)
                                                                      END AS distance_100_mean_pct_rnk
                                                                  FROM
                                                                      {{gbmi_schema}}.buildings_neighbours_100_by_{{raster_name}}
                                                                  )
                                                    SELECT
                                                        bn_100.osm_id AS osm_id,
                                                        bn_100.way AS way,
                                                        bn_100.way_centroid AS way_centroid,
                                                        bn_100.cell_id AS cell_id,
                                                        bn_100.cell_centroid AS cell_centroid,
                                                        bn_100.cell_geom AS cell_geom,
                                                        bn_100.cell_area AS cell_area,
                                                        bn_100.cell_country AS cell_country,
                                                        bn_100.cell_admin_div1 AS cell_admin_div1,
                                                        bn_100.cell_admin_div2 AS cell_admin_div2,
                                                        bn_100.cell_admin_div3 AS cell_admin_div3,
                                                        bn_100.cell_admin_div4 AS cell_admin_div4,
                                                        bn_100.cell_admin_div5 AS cell_admin_div5,{% if raster_population %}
                                                        bn_100.{{raster_population}} AS "cell_population",{% endif %}
                                                        bn_100.cell_country_official_name AS cell_country_official_name,
                                                        bn_100.cell_country_code2 AS cell_country_code2,
                                                        bn_100.cell_country_code3 AS cell_country_code3,
                                                        neighbour_100_count,
                                                        distance_100_min,
                                                        distance_100_median,
                                                        distance_100_max,
                                                        distance_100_mean,
                                                        distance_100_sd,
                                                        distance_100_d,
                                                        distance_100_mean_pct_rnk,
                                                        neighbour_50_count,
                                                        distance_50_min,
                                                        distance_50_median,
                                                        distance_50_max,
                                                        distance_50_mean,
                                                        distance_50_sd,
                                                        distance_50_d,
                                                        distance_50_mean_pct_rnk,
                                                        neighbour_25_count,
                                                        distance_25_min,
                                                        distance_25_median,
                                                        distance_25_max,
                                                        distance_25_mean,
                                                        distance_25_sd,
                                                        distance_25_d,
                                                        distance_25_mean_pct_rnk
                                                    FROM
                                                        bn_100
                                                        LEFT JOIN bn_50 ON bn_100.osm_id = bn_50.osm_id AND bn_100.way = bn_50.way
                                                        LEFT JOIN bn_25 ON bn_50.osm_id = bn_25.osm_id AND bn_50.way = bn_25.way
                                                    );




CREATE INDEX buildings_neighbours_indicators_by_{{raster_name}}_osm_id ON {{gbmi_schema}}.buildings_neighbours_indicators_by_{{raster_name}}(osm_id);

CREATE INDEX buildings_neighbours_indicators_by_{{raster_name}}_centroid_spgist ON {{gbmi_schema}}.buildings_neighbours_indicators_by_{{raster_name}} USING SPGIST (way_centroid);

CREATE INDEX buildings_neighbours_indicators_by_{{raster_name}}_spgist ON {{gbmi_schema}}.buildings_neighbours_indicators_by_{{raster_name}} USING SPGIST (way);

VACUUM ANALYZE {{gbmi_schema}}.buildings_neighbours_indicators_by_{{raster_name}};