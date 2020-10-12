-- MATERIALIZED VIEW FOR DEBUGGING
DROP MATERIALIZED VIEW IF EXISTS {{gbmi_schema}}.bni_by_{{raster_name}}_duplicates CASCADE;
DROP TABLE IF EXISTS {{gbmi_schema}}.bni_by_{{raster_name}} CASCADE;



CREATE TABLE {{gbmi_schema}}.bni_by_{{raster_name}} AS (
                                                    WITH bn_25 AS (
                                                                   SELECT *,
                                                                       CASE
                                                                           WHEN distance_25_mean IS NOT NULL
                                                                               THEN percent_rank() OVER (ORDER BY distance_25_mean)
                                                                       END AS distance_25_mean_pct_rnk,
                                                                       CASE
                                                                           WHEN neighbour_footprint_area_25_mean IS NOT NULL
                                                                               THEN percent_rank() OVER (ORDER BY neighbour_footprint_area_25_mean)
                                                                       END AS neighbour_footprint_area_25_mean_pct_rnk,
                                                                       CASE
                                                                           WHEN ratio_neighbour_height_to_distance_25_mean IS NOT NULL
                                                                               THEN percent_rank() OVER (ORDER BY ratio_neighbour_height_to_distance_25_mean)
                                                                       END AS ratio_neighbour_height_to_distance_25_mean_pct_rnk
                                                                   FROM
                                                                       {{gbmi_schema}}.bn_25_by_{{raster_name}}
                                                                   ),
                                                        bn_50 AS (
                                                                  SELECT *,
                                                                      CASE
                                                                          WHEN distance_50_mean IS NOT NULL
                                                                              THEN percent_rank() OVER (ORDER BY distance_50_mean)
                                                                      END AS distance_50_mean_pct_rnk,
                                                                      CASE
                                                                          WHEN neighbour_footprint_area_50_mean IS NOT NULL
                                                                              THEN percent_rank() OVER (ORDER BY neighbour_footprint_area_50_mean)
                                                                      END AS neighbour_footprint_area_50_mean_pct_rnk,
                                                                      CASE
                                                                          WHEN ratio_neighbour_height_to_distance_50_mean IS NOT NULL
                                                                              THEN percent_rank() OVER (ORDER BY ratio_neighbour_height_to_distance_50_mean)
                                                                      END AS ratio_neighbour_height_to_distance_50_mean_pct_rnk
                                                                  FROM
                                                                      {{gbmi_schema}}.bn_50_by_{{raster_name}}
                                                                  ){% if not limit_buffer %},
                                                        bn_100 AS (
                                                                  SELECT *,
                                                                      CASE
                                                                          WHEN distance_100_mean IS NOT NULL
                                                                              THEN percent_rank() OVER (ORDER BY distance_100_mean)
                                                                      END AS distance_100_mean_pct_rnk,
                                                                      CASE
                                                                           WHEN neighbour_footprint_area_100_mean IS NOT NULL
                                                                               THEN percent_rank() OVER (ORDER BY neighbour_footprint_area_100_mean)
                                                                      END AS neighbour_footprint_area_100_mean_pct_rnk,
                                                                      CASE
                                                                           WHEN ratio_neighbour_height_to_distance_100_mean IS NOT NULL
                                                                               THEN percent_rank() OVER (ORDER BY ratio_neighbour_height_to_distance_100_mean)
                                                                      END AS ratio_neighbour_height_to_distance_100_mean_pct_rnk
                                                                  FROM
                                                                      {{gbmi_schema}}.bn_100_by_{{raster_name}}
                                                                  ){% endif %}
                                                    SELECT{% if not limit_buffer %}
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
                                                        buffer_area_100,
                                                        neighbour_100_count,
                                                        distance_100_min,
                                                        distance_100_median,
                                                        distance_100_max,
                                                        distance_100_mean,
                                                        distance_100_sd,
                                                        distance_100_d,
                                                        distance_100_cv,
                                                        distance_100_mean_pct_rnk,
                                                        neighbour_footprint_area_100_sum,
                                                        neighbour_footprint_area_100_min,
                                                        neighbour_footprint_area_100_max,
                                                        neighbour_footprint_area_100_median,
                                                        neighbour_footprint_area_100_mean,
                                                        neighbour_footprint_area_100_sd,
                                                        neighbour_footprint_area_100_d,
                                                        neighbour_footprint_area_100_cv,
                                                        neighbour_footprint_area_100_mean_pct_rnk,
                                                        ratio_neighbour_footprint_sum_to_buffer_100,
                                                        ratio_neighbour_height_to_distance_100_min,
                                                        ratio_neighbour_height_to_distance_100_max,
                                                        ratio_neighbour_height_to_distance_100_median,
                                                        ratio_neighbour_height_to_distance_100_mean,
                                                        ratio_neighbour_height_to_distance_100_sd,
                                                        ratio_neighbour_height_to_distance_100_d,
                                                        ratio_neighbour_height_to_distance_100_cv,
                                                        ratio_neighbour_height_to_distance_100_mean_pct_rnk,{% else %}
                                                        bn_50.osm_id AS osm_id,
                                                        bn_50.way AS way,
                                                        bn_50.way_centroid AS way_centroid,
                                                        bn_50.cell_id AS cell_id,
                                                        bn_50.cell_centroid AS cell_centroid,
                                                        bn_50.cell_geom AS cell_geom,
                                                        bn_50.cell_area AS cell_area,
                                                        bn_50.cell_country AS cell_country,
                                                        bn_50.cell_admin_div1 AS cell_admin_div1,
                                                        bn_50.cell_admin_div2 AS cell_admin_div2,
                                                        bn_50.cell_admin_div3 AS cell_admin_div3,
                                                        bn_50.cell_admin_div4 AS cell_admin_div4,
                                                        bn_50.cell_admin_div5 AS cell_admin_div5,{% if raster_population %}
                                                        bn_50.{{raster_population}} AS "cell_population",{% endif %}
                                                        bn_50.cell_country_official_name AS cell_country_official_name,
                                                        bn_50.cell_country_code2 AS cell_country_code2,
                                                        bn_50.cell_country_code3 AS cell_country_code3,{% endif %}
                                                        buffer_area_50,
                                                        neighbour_50_count,
                                                        distance_50_min,
                                                        distance_50_median,
                                                        distance_50_max,
                                                        distance_50_mean,
                                                        distance_50_sd,
                                                        distance_50_d,
                                                        distance_50_cv,
                                                        distance_50_mean_pct_rnk,
                                                        neighbour_footprint_area_50_sum,
                                                        neighbour_footprint_area_50_min,
                                                        neighbour_footprint_area_50_max,
                                                        neighbour_footprint_area_50_median,
                                                        neighbour_footprint_area_50_mean,
                                                        neighbour_footprint_area_50_sd,
                                                        neighbour_footprint_area_50_d,
                                                        neighbour_footprint_area_50_cv,
                                                        neighbour_footprint_area_50_mean_pct_rnk,
                                                        ratio_neighbour_footprint_sum_to_buffer_50,
                                                        ratio_neighbour_height_to_distance_50_min,
                                                        ratio_neighbour_height_to_distance_50_max,
                                                        ratio_neighbour_height_to_distance_50_median,
                                                        ratio_neighbour_height_to_distance_50_mean,
                                                        ratio_neighbour_height_to_distance_50_sd,
                                                        ratio_neighbour_height_to_distance_50_d,
                                                        ratio_neighbour_height_to_distance_50_cv,
                                                        ratio_neighbour_height_to_distance_50_mean_pct_rnk,
                                                        buffer_area_25,
                                                        neighbour_25_count,
                                                        distance_25_min,
                                                        distance_25_median,
                                                        distance_25_max,
                                                        distance_25_mean,
                                                        distance_25_sd,
                                                        distance_25_d,
                                                        distance_25_cv,
                                                        distance_25_mean_pct_rnk,
                                                        neighbour_footprint_area_25_sum,
                                                        neighbour_footprint_area_25_min,
                                                        neighbour_footprint_area_25_max,
                                                        neighbour_footprint_area_25_median,
                                                        neighbour_footprint_area_25_mean,
                                                        neighbour_footprint_area_25_sd,
                                                        neighbour_footprint_area_25_d,
                                                        neighbour_footprint_area_25_cv,
                                                        neighbour_footprint_area_25_mean_pct_rnk,
                                                        ratio_neighbour_footprint_sum_to_buffer_25,
                                                        ratio_neighbour_height_to_distance_25_min,
                                                        ratio_neighbour_height_to_distance_25_max,
                                                        ratio_neighbour_height_to_distance_25_median,
                                                        ratio_neighbour_height_to_distance_25_mean,
                                                        ratio_neighbour_height_to_distance_25_sd,
                                                        ratio_neighbour_height_to_distance_25_d,
                                                        ratio_neighbour_height_to_distance_25_cv,
                                                        ratio_neighbour_height_to_distance_25_mean_pct_rnk
                                                    FROM{% if not limit_buffer %}
                                                        bn_100
                                                        LEFT JOIN bn_50 ON bn_100.osm_id = bn_50.osm_id AND ST_Equals(bn_100.way::geometry, bn_50.way::geometry)
                                                        LEFT JOIN bn_25 ON bn_50.osm_id = bn_25.osm_id AND ST_Equals(bn_50.way::geometry, bn_25.way::geometry){% else %}
                                                        bn_50 LEFT JOIN bn_25 ON bn_50.osm_id = bn_25.osm_id AND ST_Equals(bn_50.way::geometry, bn_25.way::geometry){% endif %}
                                                    );




CREATE INDEX bni_by_{{raster_name}}_osm_id ON {{gbmi_schema}}.bni_by_{{raster_name}}(osm_id);

CREATE INDEX bni_by_{{raster_name}}_centroid_spgist ON {{gbmi_schema}}.bni_by_{{raster_name}} USING SPGIST (way_centroid);

CREATE INDEX bni_by_{{raster_name}}_spgist ON {{gbmi_schema}}.bni_by_{{raster_name}} USING SPGIST (way);

VACUUM ANALYZE {{gbmi_schema}}.bni_by_{{raster_name}};



/*
-- This is to troubleshoot whether joints and building data are created correctly

CREATE MATERIALIZED VIEW {{gbmi_schema}}.bni_by_{{raster_name}}_duplicates AS
    SELECT
        ({{gbmi_schema}}.bni_by_{{raster_name}}.*)::text,
        count(*)
    FROM
        {{gbmi_schema}}.bni_by_{{raster_name}}
    GROUP BY
        {{gbmi_schema}}.bni_by_{{raster_name}}.*
    HAVING count(*) > 1;

 */