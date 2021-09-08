-- MATERIALIZED VIEW FOR DEBUGGING
-- DROP MATERIALIZED VIEW IF EXISTS {{gbmi_schema}}.agg_bni_by_{{agg_level}}_{{raster_name}}_centroid_duplicates CASCADE;
-- DROP TABLE IF EXISTS {{gbmi_schema}}.agg_bni_by_{{agg_level}}_{{raster_name}}_centroid CASCADE;



CREATE TABLE {{gbmi_schema}}.agg_bni_by_{{agg_level}}_{{raster_name}}_centroid AS (
                                                                        WITH agg0_neighbours AS (
                                                                            SELECT
                                                                                {{agg_columns}},
                                                                                {{agg_geom}},
                                                                                AVG("buffer_area_25") AS "buffer_area_25_mean",
                                                                                MIN("neighbour_25_count") AS "neighbour_25_count_min",
                                                                                (PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY "neighbour_25_count" )) AS "neighbour_25_count_median",
                                                                                AVG("neighbour_25_count") AS "neighbour_25_count_mean",
                                                                                MAX("neighbour_25_count") AS "neighbour_25_count_max",
                                                                                STDDEV_POP("neighbour_25_count") AS "neighbour_25_count_sd",
                                                                                VAR_POP("neighbour_25_count") AS "neighbour_25_count_var",
                                                                                MIN("distance_25_min") AS "distance_25_min",
                                                                                PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY "distance_25_median" ) AS "distance_25_median",
                                                                                AVG("distance_25_mean") AS "distance_25_mean",
                                                                                MAX("distance_25_max") AS "distance_25_max",
                                                                                STDDEV_POP("distance_25_mean") AS "distance_25_sd",
                                                                                VAR_POP("distance_25_mean") AS "distance_25_var",
                                                                                MIN("neighbour_footprint_area_25_sum") AS "neighbour_footprint_area_25_sum_min",
                                                                                (PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY "neighbour_footprint_area_25_sum" )) AS "neighbour_footprint_area_25_sum_median",
                                                                                AVG("neighbour_footprint_area_25_sum") AS "neighbour_footprint_area_25_sum_mean",
                                                                                MAX("neighbour_footprint_area_25_sum") AS "neighbour_footprint_area_25_sum_max",
                                                                                STDDEV_POP("neighbour_footprint_area_25_sum") AS "neighbour_footprint_area_25_sum_sd",
                                                                                VAR_POP("neighbour_footprint_area_25_sum") AS "neighbour_footprint_area_25_sum_var",
                                                                                MIN("neighbour_footprint_area_25_min") AS "neighbour_footprint_area_25_min",
                                                                                PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY "neighbour_footprint_area_25_median" ) AS "neighbour_footprint_area_25_median",
                                                                                AVG("neighbour_footprint_area_25_mean") AS "neighbour_footprint_area_25_mean",
                                                                                MAX("neighbour_footprint_area_25_max") AS "neighbour_footprint_area_25_max",
                                                                                STDDEV_POP("neighbour_footprint_area_25_mean") AS "neighbour_footprint_area_25_sd",
                                                                                VAR_POP("neighbour_footprint_area_25_mean") AS "neighbour_footprint_area_25_var",
                                                                                MIN("ratio_neighbour_footprint_sum_to_buffer_25") AS "ratio_neighbour_footprint_sum_to_buffer_25_min",
                                                                                (PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY "ratio_neighbour_footprint_sum_to_buffer_25" )) AS "ratio_neighbour_footprint_sum_to_buffer_25_median",
                                                                                AVG("ratio_neighbour_footprint_sum_to_buffer_25") AS "ratio_neighbour_footprint_sum_to_buffer_25_mean",
                                                                                MAX("ratio_neighbour_footprint_sum_to_buffer_25") AS "ratio_neighbour_footprint_sum_to_buffer_25_max",
                                                                                STDDEV_POP("ratio_neighbour_footprint_sum_to_buffer_25") AS "ratio_neighbour_footprint_sum_to_buffer_25_sd",
                                                                                VAR_POP("ratio_neighbour_footprint_sum_to_buffer_25") AS "ratio_neighbour_footprint_sum_to_buffer_25_var",
                                                                                MIN("ratio_neighbour_height_to_distance_25_min") AS "ratio_neighbour_height_to_distance_25_min",
                                                                                PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY "ratio_neighbour_height_to_distance_25_median" ) AS "ratio_neighbour_height_to_distance_25_median",
                                                                                AVG("ratio_neighbour_height_to_distance_25_mean") AS "ratio_neighbour_height_to_distance_25_mean",
                                                                                MAX("ratio_neighbour_height_to_distance_25_max") AS "ratio_neighbour_height_to_distance_25_max",
                                                                                STDDEV_POP("ratio_neighbour_height_to_distance_25_mean") AS "ratio_neighbour_height_to_distance_25_sd",
                                                                                VAR_POP("ratio_neighbour_height_to_distance_25_mean") AS "ratio_neighbour_height_to_distance_25_var",
                                                                                AVG("buffer_area_50") AS "buffer_area_50_mean",
                                                                                MIN("neighbour_50_count") AS "neighbour_50_count_min",
                                                                                (PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY "neighbour_50_count" )) AS "neighbour_50_count_median",
                                                                                AVG("neighbour_50_count") AS "neighbour_50_count_mean",
                                                                                MAX("neighbour_50_count") AS "neighbour_50_count_max",
                                                                                STDDEV_POP("neighbour_50_count") AS "neighbour_50_count_sd",
                                                                                VAR_POP("neighbour_50_count") AS "neighbour_50_count_var",
                                                                                MIN("distance_50_min") AS "distance_50_min",
                                                                                PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY "distance_50_median" ) AS "distance_50_median",
                                                                                AVG("distance_50_mean") AS "distance_50_mean",
                                                                                MAX("distance_50_max") AS "distance_50_max",
                                                                                STDDEV_POP("distance_50_mean") AS "distance_50_sd",
                                                                                VAR_POP("distance_50_mean") AS "distance_50_var",
                                                                                MIN("neighbour_footprint_area_50_sum") AS "neighbour_footprint_area_50_sum_min",
                                                                                (PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY "neighbour_footprint_area_50_sum" )) AS "neighbour_footprint_area_50_sum_median",
                                                                                AVG("neighbour_footprint_area_50_sum") AS "neighbour_footprint_area_50_sum_mean",
                                                                                MAX("neighbour_footprint_area_50_sum") AS "neighbour_footprint_area_50_sum_max",
                                                                                STDDEV_POP("neighbour_footprint_area_50_sum") AS "neighbour_footprint_area_50_sum_sd",
                                                                                VAR_POP("neighbour_footprint_area_50_sum") AS "neighbour_footprint_area_50_sum_var",
                                                                                MIN("neighbour_footprint_area_50_min") AS "neighbour_footprint_area_50_min",
                                                                                PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY "neighbour_footprint_area_50_median" ) AS "neighbour_footprint_area_50_median",
                                                                                AVG("neighbour_footprint_area_50_mean") AS "neighbour_footprint_area_50_mean",
                                                                                MAX("neighbour_footprint_area_50_max") AS "neighbour_footprint_area_50_max",
                                                                                STDDEV_POP("neighbour_footprint_area_50_mean") AS "neighbour_footprint_area_50_sd",
                                                                                VAR_POP("neighbour_footprint_area_50_mean") AS "neighbour_footprint_area_50_var",
                                                                                MIN("ratio_neighbour_footprint_sum_to_buffer_50") AS "ratio_neighbour_footprint_sum_to_buffer_50_min",
                                                                                (PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY "ratio_neighbour_footprint_sum_to_buffer_50" )) AS "ratio_neighbour_footprint_sum_to_buffer_50_median",
                                                                                AVG("ratio_neighbour_footprint_sum_to_buffer_50") AS "ratio_neighbour_footprint_sum_to_buffer_50_mean",
                                                                                MAX("ratio_neighbour_footprint_sum_to_buffer_50") AS "ratio_neighbour_footprint_sum_to_buffer_50_max",
                                                                                STDDEV_POP("ratio_neighbour_footprint_sum_to_buffer_50") AS "ratio_neighbour_footprint_sum_to_buffer_50_sd",
                                                                                VAR_POP("ratio_neighbour_footprint_sum_to_buffer_50") AS "ratio_neighbour_footprint_sum_to_buffer_50_var",
                                                                                MIN("ratio_neighbour_height_to_distance_50_min") AS "ratio_neighbour_height_to_distance_50_min",
                                                                                PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY "ratio_neighbour_height_to_distance_50_median" ) AS ratio_neighbour_height_to_distance_50_median,
                                                                                AVG("ratio_neighbour_height_to_distance_50_mean") AS "ratio_neighbour_height_to_distance_50_mean",
                                                                                MAX("ratio_neighbour_height_to_distance_50_max") AS "ratio_neighbour_height_to_distance_50_max",
                                                                                STDDEV_POP("ratio_neighbour_height_to_distance_50_mean") AS "ratio_neighbour_height_to_distance_50_sd",
                                                                                VAR_POP("ratio_neighbour_height_to_distance_50_mean") AS "ratio_neighbour_height_to_distance_50_var"{% if not limit_buffer %},
                                                                                AVG("buffer_area_100") AS "buffer_area_100_mean",
                                                                                MIN("neighbour_100_count") AS "neighbour_100_count_min",
                                                                                (PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY "neighbour_100_count" )) AS "neighbour_100_count_median",
                                                                                AVG("neighbour_100_count") AS "neighbour_100_count_mean",
                                                                                MAX("neighbour_100_count") AS "neighbour_100_count_max",
                                                                                STDDEV_POP("neighbour_100_count") AS "neighbour_100_count_sd",
                                                                                VAR_POP("neighbour_100_count") AS "neighbour_100_count_var",
                                                                                MIN("distance_100_min") AS distance_100_min,
                                                                                PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY "distance_100_median" ) AS "distance_100_median",
                                                                                AVG("distance_100_mean") AS "distance_100_mean",
                                                                                MAX("distance_100_max") AS "distance_100_max",
                                                                                STDDEV_POP("distance_100_mean") AS "distance_100_sd",
                                                                                VAR_POP("distance_100_mean") AS "distance_100_var",
                                                                                MIN("neighbour_footprint_area_100_sum") AS "neighbour_footprint_area_100_sum_min",
                                                                                (PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY "neighbour_footprint_area_100_sum" )) AS "neighbour_footprint_area_100_sum_median",
                                                                                AVG("neighbour_footprint_area_100_sum") AS "neighbour_footprint_area_100_sum_mean",
                                                                                MAX("neighbour_footprint_area_100_sum") AS "neighbour_footprint_area_100_sum_max",
                                                                                STDDEV_POP("neighbour_footprint_area_100_sum") AS "neighbour_footprint_area_100_sum_sd",
                                                                                VAR_POP("neighbour_footprint_area_100_sum") AS "neighbour_footprint_area_100_sum_var",
                                                                                MIN("neighbour_footprint_area_100_min") AS "neighbour_footprint_area_100_min",
                                                                                PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY "neighbour_footprint_area_100_median" ) AS "neighbour_footprint_area_100_median",
                                                                                AVG("neighbour_footprint_area_100_mean") AS "neighbour_footprint_area_100_mean",
                                                                                MAX("neighbour_footprint_area_100_max") AS "neighbour_footprint_area_100_max",
                                                                                STDDEV_POP("neighbour_footprint_area_100_mean") AS "neighbour_footprint_area_100_sd",
                                                                                VAR_POP("neighbour_footprint_area_100_mean") AS "neighbour_footprint_area_100_var",
                                                                                MIN("ratio_neighbour_footprint_sum_to_buffer_100") AS "ratio_neighbour_footprint_sum_to_buffer_100_min",
                                                                                (PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY "ratio_neighbour_footprint_sum_to_buffer_100" )) AS "ratio_neighbour_footprint_sum_to_buffer_100_median",
                                                                                AVG("ratio_neighbour_footprint_sum_to_buffer_100") AS "ratio_neighbour_footprint_sum_to_buffer_100_mean",
                                                                                MAX("ratio_neighbour_footprint_sum_to_buffer_100") AS "ratio_neighbour_footprint_sum_to_buffer_100_max",
                                                                                STDDEV_POP("ratio_neighbour_footprint_sum_to_buffer_100") AS "ratio_neighbour_footprint_sum_to_buffer_100_sd",
                                                                                VAR_POP("ratio_neighbour_footprint_sum_to_buffer_100") AS "ratio_neighbour_footprint_sum_to_buffer_100_var",
                                                                                MIN("ratio_neighbour_height_to_distance_100_min") AS "ratio_neighbour_height_to_distance_100_min",
                                                                                PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY "ratio_neighbour_height_to_distance_100_median" ) AS "ratio_neighbour_height_to_distance_100_median",
                                                                                AVG("ratio_neighbour_height_to_distance_100_mean") AS "ratio_neighbour_height_to_distance_100_mean",
                                                                                MAX("ratio_neighbour_height_to_distance_100_max") AS "ratio_neighbour_height_to_distance_100_max",
                                                                                STDDEV_POP("ratio_neighbour_height_to_distance_100_mean") AS "ratio_neighbour_height_to_distance_100_sd",
                                                                                VAR_POP("ratio_neighbour_height_to_distance_100_mean") AS "ratio_neighbour_height_to_distance_100_var"{% endif %}
                                                                            FROM
                                                                                {{gbmi_schema}}.buildings_indicators_by_{{raster_name}}_centroid bga {{join_clause}}
                                                                            WHERE cell_country IS NOT NULL
                                                                            GROUP BY
                                                                                {{agg_columns}},
                                                                                {{agg_geom}},
                                                                                {{agg_area}})
                                                                        SELECT
                                                                            {{agg_columns}},
                                                                            {{agg_geom}},
                                                                            "buffer_area_25_mean",
                                                                            "neighbour_25_count_min",
                                                                            "neighbour_25_count_median",
                                                                            "neighbour_25_count_mean",
                                                                            "neighbour_25_count_max",
                                                                            "neighbour_25_count_sd",
                                                                            CASE
                                                                                WHEN "neighbour_25_count_mean" > 0
                                                                                    THEN "neighbour_25_count_var" / "neighbour_25_count_mean"
                                                                            END AS "neighbour_25_count_d",
                                                                            CASE
                                                                                WHEN "neighbour_25_count_mean" > 0
                                                                                    THEN "neighbour_25_count_sd" / "neighbour_25_count_mean"
                                                                            END AS "neighbour_25_count_cv",
                                                                            CASE
                                                                                WHEN "neighbour_25_count_mean" IS NOT NULL
                                                                                    THEN PERCENT_RANK() OVER ( ORDER BY "neighbour_25_count_mean" )
                                                                            END AS "neighbour_25_count_mean_pct_rnk",
                                                                            "distance_25_min",
                                                                            "distance_25_median",
                                                                            "distance_25_mean",
                                                                            "distance_25_max",
                                                                            "distance_25_sd",
                                                                            CASE
                                                                                WHEN "distance_25_mean" > 0
                                                                                    THEN "distance_25_var" / "distance_25_mean"
                                                                            END AS "distance_25_d",
                                                                            CASE
                                                                                WHEN "distance_25_mean" > 0
                                                                                    THEN "distance_25_sd" / "distance_25_mean"
                                                                            END AS "distance_25_cv",
                                                                            CASE
                                                                                WHEN "distance_25_mean" IS NOT NULL
                                                                                    THEN PERCENT_RANK() OVER ( ORDER BY "distance_25_mean" )
                                                                            END AS "distance_25_mean_pct_rnk",
                                                                            "neighbour_footprint_area_25_sum_min",
                                                                            "neighbour_footprint_area_25_sum_median",
                                                                            "neighbour_footprint_area_25_sum_mean",
                                                                            "neighbour_footprint_area_25_sum_max",
                                                                            "neighbour_footprint_area_25_sum_sd",
                                                                            CASE
                                                                                WHEN "neighbour_footprint_area_25_sum_mean" > 0
                                                                                    THEN "neighbour_footprint_area_25_sum_var" / "neighbour_footprint_area_25_sum_mean"
                                                                            END AS "neighbour_footprint_area_25_sum_d",
                                                                            CASE
                                                                                WHEN "neighbour_footprint_area_25_sum_mean" > 0
                                                                                    THEN "neighbour_footprint_area_25_sum_sd" / "neighbour_footprint_area_25_sum_mean"
                                                                            END AS "neighbour_footprint_area_25_sum_cv",
                                                                            CASE
                                                                                WHEN "neighbour_footprint_area_25_sum_mean" IS NOT NULL
                                                                                    THEN PERCENT_RANK() OVER ( ORDER BY "neighbour_footprint_area_25_sum_mean" )
                                                                            END AS "neighbour_footprint_area_25_sum_mean_pct_rnk",
                                                                            "neighbour_footprint_area_25_min",
                                                                            "neighbour_footprint_area_25_median",
                                                                            "neighbour_footprint_area_25_mean",
                                                                            "neighbour_footprint_area_25_max",
                                                                            "neighbour_footprint_area_25_sd",
                                                                            CASE
                                                                                WHEN "neighbour_footprint_area_25_mean" > 0
                                                                                    THEN "neighbour_footprint_area_25_var" / "neighbour_footprint_area_25_mean"
                                                                            END AS "neighbour_footprint_area_25_d",
                                                                            CASE
                                                                                WHEN "neighbour_footprint_area_25_mean" > 0
                                                                                    THEN "neighbour_footprint_area_25_sd" / "neighbour_footprint_area_25_mean"
                                                                            END AS "neighbour_footprint_area_25_cv",
                                                                            CASE
                                                                                WHEN "neighbour_footprint_area_25_mean" IS NOT NULL
                                                                                    THEN PERCENT_RANK() OVER ( ORDER BY "neighbour_footprint_area_25_mean" )
                                                                            END AS "neighbour_footprint_area_25_mean_pct_rnk",
                                                                            "ratio_neighbour_footprint_sum_to_buffer_25_min",
                                                                            "ratio_neighbour_footprint_sum_to_buffer_25_median",
                                                                            "ratio_neighbour_footprint_sum_to_buffer_25_mean",
                                                                            "ratio_neighbour_footprint_sum_to_buffer_25_max",
                                                                            "ratio_neighbour_footprint_sum_to_buffer_25_sd",
                                                                            CASE
                                                                                WHEN "ratio_neighbour_footprint_sum_to_buffer_25_mean" > 0
                                                                                    THEN "ratio_neighbour_footprint_sum_to_buffer_25_var" / "ratio_neighbour_footprint_sum_to_buffer_25_mean"
                                                                            END AS "ratio_neighbour_footprint_sum_to_buffer_25_d",
                                                                            CASE
                                                                                WHEN "ratio_neighbour_footprint_sum_to_buffer_25_mean" > 0
                                                                                    THEN "ratio_neighbour_footprint_sum_to_buffer_25_sd" / "ratio_neighbour_footprint_sum_to_buffer_25_mean"
                                                                            END AS "ratio_neighbour_footprint_sum_to_buffer_25_cv",
                                                                            CASE
                                                                                WHEN "ratio_neighbour_footprint_sum_to_buffer_25_mean" IS NOT NULL
                                                                                    THEN PERCENT_RANK() OVER ( ORDER BY "ratio_neighbour_footprint_sum_to_buffer_25_mean" )
                                                                            END AS "ratio_neighbour_footprint_sum_to_buffer_25_mean_pct_rnk",
                                                                            "ratio_neighbour_height_to_distance_25_min",
                                                                            "ratio_neighbour_height_to_distance_25_median",
                                                                            "ratio_neighbour_height_to_distance_25_mean",
                                                                            "ratio_neighbour_height_to_distance_25_max",
                                                                            "ratio_neighbour_height_to_distance_25_sd",
                                                                            CASE
                                                                                WHEN "ratio_neighbour_height_to_distance_25_mean" > 0
                                                                                    THEN "ratio_neighbour_height_to_distance_25_var" / "ratio_neighbour_height_to_distance_25_mean"
                                                                            END AS "ratio_neighbour_height_to_distance_25_d",
                                                                            CASE
                                                                                WHEN "ratio_neighbour_height_to_distance_25_mean" > 0
                                                                                    THEN "ratio_neighbour_height_to_distance_25_sd" / "ratio_neighbour_height_to_distance_25_mean"
                                                                            END AS "ratio_neighbour_height_to_distance_25_cv",
                                                                            CASE
                                                                                WHEN "ratio_neighbour_height_to_distance_25_mean" IS NOT NULL
                                                                                    THEN PERCENT_RANK() OVER ( ORDER BY "ratio_neighbour_height_to_distance_25_mean" )
                                                                            END AS "ratio_neighbour_height_to_distance_25_mean_pct_rnk",
                                                                            "buffer_area_50_mean",
                                                                            "neighbour_50_count_min",
                                                                            "neighbour_50_count_median",
                                                                            "neighbour_50_count_mean",
                                                                            "neighbour_50_count_max",
                                                                            "neighbour_50_count_sd",
                                                                            CASE
                                                                                WHEN "neighbour_50_count_mean" > 0
                                                                                    THEN "neighbour_50_count_var" / "neighbour_50_count_mean"
                                                                            END AS "neighbour_50_count_d",
                                                                            CASE
                                                                                WHEN "neighbour_50_count_mean" > 0
                                                                                    THEN "neighbour_50_count_sd" / "neighbour_50_count_mean"
                                                                            END AS "neighbour_50_count_cv",
                                                                            CASE
                                                                                WHEN "neighbour_50_count_mean" IS NOT NULL
                                                                                    THEN PERCENT_RANK() OVER ( ORDER BY "neighbour_50_count_mean" )
                                                                            END AS "neighbour_50_count_mean_pct_rnk",
                                                                            "distance_50_min",
                                                                            "distance_50_median",
                                                                            "distance_50_mean",
                                                                            "distance_50_max",
                                                                            "distance_50_sd",
                                                                            CASE
                                                                                WHEN "distance_50_mean" > 0
                                                                                    THEN "distance_50_var" / "distance_50_mean"
                                                                            END AS "distance_50_d",
                                                                            CASE
                                                                                WHEN "distance_50_mean" > 0
                                                                                    THEN "distance_50_sd" / "distance_50_mean"
                                                                            END AS "distance_50_cv",
                                                                            CASE
                                                                                WHEN "distance_50_mean" IS NOT NULL
                                                                                    THEN PERCENT_RANK() OVER ( ORDER BY "distance_50_mean" )
                                                                            END AS "distance_50_mean_pct_rnk",
                                                                            "neighbour_footprint_area_50_sum_min",
                                                                            "neighbour_footprint_area_50_sum_median",
                                                                            "neighbour_footprint_area_50_sum_mean",
                                                                            "neighbour_footprint_area_50_sum_max",
                                                                            "neighbour_footprint_area_50_sum_sd",
                                                                            CASE
                                                                                WHEN "neighbour_footprint_area_50_sum_mean" > 0
                                                                                    THEN "neighbour_footprint_area_50_sum_var" / "neighbour_footprint_area_50_sum_mean"
                                                                            END AS "neighbour_footprint_area_50_sum_d",
                                                                            CASE
                                                                                WHEN "neighbour_footprint_area_50_sum_mean" > 0
                                                                                    THEN "neighbour_footprint_area_50_sum_sd" / "neighbour_footprint_area_50_sum_mean"
                                                                            END AS "neighbour_footprint_area_50_sum_cv",
                                                                            CASE
                                                                                WHEN "neighbour_footprint_area_50_sum_mean" IS NOT NULL
                                                                                    THEN PERCENT_RANK() OVER ( ORDER BY "neighbour_footprint_area_50_sum_mean" )
                                                                            END AS "neighbour_footprint_area_50_sum_mean_pct_rnk",
                                                                            "neighbour_footprint_area_50_min",
                                                                            "neighbour_footprint_area_50_median",
                                                                            "neighbour_footprint_area_50_mean",
                                                                            "neighbour_footprint_area_50_max",
                                                                            "neighbour_footprint_area_50_sd",
                                                                            CASE
                                                                                WHEN "neighbour_footprint_area_50_mean" > 0
                                                                                    THEN "neighbour_footprint_area_50_var" / "neighbour_footprint_area_50_mean"
                                                                            END AS "neighbour_footprint_area_50_d",
                                                                            CASE
                                                                                WHEN "neighbour_footprint_area_50_mean" > 0
                                                                                    THEN "neighbour_footprint_area_50_sd" / "neighbour_footprint_area_50_mean"
                                                                            END AS "neighbour_footprint_area_50_cv",
                                                                            CASE
                                                                                WHEN "neighbour_footprint_area_50_mean" IS NOT NULL
                                                                                    THEN PERCENT_RANK() OVER ( ORDER BY "neighbour_footprint_area_50_mean" )
                                                                            END AS "neighbour_footprint_area_50_mean_pct_rnk",
                                                                            "ratio_neighbour_footprint_sum_to_buffer_50_min",
                                                                            "ratio_neighbour_footprint_sum_to_buffer_50_median",
                                                                            "ratio_neighbour_footprint_sum_to_buffer_50_mean",
                                                                            "ratio_neighbour_footprint_sum_to_buffer_50_max",
                                                                            "ratio_neighbour_footprint_sum_to_buffer_50_sd",
                                                                            CASE
                                                                                WHEN "ratio_neighbour_footprint_sum_to_buffer_50_mean" > 0
                                                                                    THEN "ratio_neighbour_footprint_sum_to_buffer_50_var" / "ratio_neighbour_footprint_sum_to_buffer_50_mean"
                                                                            END AS "ratio_neighbour_footprint_sum_to_buffer_50_d",
                                                                            CASE
                                                                                WHEN "ratio_neighbour_footprint_sum_to_buffer_50_mean" > 0
                                                                                    THEN "ratio_neighbour_footprint_sum_to_buffer_50_sd" / "ratio_neighbour_footprint_sum_to_buffer_50_mean"
                                                                            END AS "ratio_neighbour_footprint_sum_to_buffer_50_cv",
                                                                            CASE
                                                                                WHEN "ratio_neighbour_footprint_sum_to_buffer_50_mean" IS NOT NULL
                                                                                    THEN PERCENT_RANK() OVER ( ORDER BY "ratio_neighbour_footprint_sum_to_buffer_50_mean" )
                                                                            END AS "ratio_neighbour_footprint_sum_to_buffer_50_mean_pct_rnk",
                                                                            "ratio_neighbour_height_to_distance_50_min",
                                                                            "ratio_neighbour_height_to_distance_50_median",
                                                                            "ratio_neighbour_height_to_distance_50_mean",
                                                                            "ratio_neighbour_height_to_distance_50_max",
                                                                            "ratio_neighbour_height_to_distance_50_sd",
                                                                            CASE
                                                                                WHEN "ratio_neighbour_height_to_distance_50_mean" > 0
                                                                                    THEN "ratio_neighbour_height_to_distance_50_var" / "ratio_neighbour_height_to_distance_50_mean"
                                                                            END AS "ratio_neighbour_height_to_distance_50_d",
                                                                            CASE
                                                                                WHEN "ratio_neighbour_height_to_distance_50_mean" > 0
                                                                                    THEN "ratio_neighbour_height_to_distance_50_sd" / "ratio_neighbour_height_to_distance_50_mean"
                                                                            END AS "ratio_neighbour_height_to_distance_50_cv",
                                                                            CASE
                                                                                WHEN "ratio_neighbour_height_to_distance_50_mean" IS NOT NULL
                                                                                    THEN PERCENT_RANK() OVER ( ORDER BY "ratio_neighbour_height_to_distance_50_mean" )
                                                                            END AS "ratio_neighbour_height_to_distance_50_mean_pct_rnk"{% if not limit_buffer %},
                                                                            "buffer_area_100_mean",
                                                                            "neighbour_100_count_min",
                                                                            "neighbour_100_count_median",
                                                                            "neighbour_100_count_mean",
                                                                            "neighbour_100_count_max",
                                                                            "neighbour_100_count_sd",
                                                                            CASE
                                                                                WHEN "neighbour_100_count_mean" > 0
                                                                                    THEN "neighbour_100_count_var" / "neighbour_100_count_mean"
                                                                            END AS "neighbour_100_count_d",
                                                                            CASE
                                                                                WHEN "neighbour_100_count_mean" > 0
                                                                                    THEN "neighbour_100_count_sd" / "neighbour_100_count_mean"
                                                                            END AS "neighbour_100_count_cv",
                                                                            CASE
                                                                                WHEN "neighbour_100_count_mean" IS NOT NULL
                                                                                    THEN PERCENT_RANK() OVER ( ORDER BY "neighbour_100_count_mean" )
                                                                            END AS "neighbour_100_count_mean_pct_rnk",
                                                                            "distance_100_min",
                                                                            "distance_100_median",
                                                                            "distance_100_mean",
                                                                            "distance_100_max",
                                                                            "distance_100_sd",
                                                                            CASE
                                                                                WHEN "distance_100_mean" > 0
                                                                                    THEN "distance_100_var" / "distance_100_mean"
                                                                            END AS "distance_100_d",
                                                                            CASE
                                                                                WHEN "distance_100_mean" > 0
                                                                                    THEN "distance_100_sd" / "distance_100_mean"
                                                                            END AS "distance_100_cv",
                                                                            CASE
                                                                                WHEN "distance_100_mean" IS NOT NULL
                                                                                    THEN PERCENT_RANK() OVER ( ORDER BY "distance_100_mean" )
                                                                            END AS "distance_100_mean_pct_rnk",
                                                                            "neighbour_footprint_area_100_sum_min",
                                                                            "neighbour_footprint_area_100_sum_median",
                                                                            "neighbour_footprint_area_100_sum_mean",
                                                                            "neighbour_footprint_area_100_sum_max",
                                                                            "neighbour_footprint_area_100_sum_sd",
                                                                            CASE
                                                                                WHEN "neighbour_footprint_area_100_sum_mean" > 0
                                                                                    THEN "neighbour_footprint_area_100_sum_var" / "neighbour_footprint_area_100_sum_mean"
                                                                            END AS "neighbour_footprint_area_100_sum_d",
                                                                            CASE
                                                                                WHEN "neighbour_footprint_area_100_sum_mean" > 0
                                                                                    THEN "neighbour_footprint_area_100_sum_sd" / "neighbour_footprint_area_100_sum_mean"
                                                                            END AS "neighbour_footprint_area_100_sum_cv",
                                                                            CASE
                                                                                WHEN "neighbour_footprint_area_100_sum_mean" IS NOT NULL
                                                                                    THEN PERCENT_RANK() OVER ( ORDER BY "neighbour_footprint_area_100_sum_mean" )
                                                                            END AS "neighbour_footprint_area_100_sum_mean_pct_rnk",
                                                                            "neighbour_footprint_area_100_min",
                                                                            "neighbour_footprint_area_100_median",
                                                                            "neighbour_footprint_area_100_mean",
                                                                            "neighbour_footprint_area_100_max",
                                                                            "neighbour_footprint_area_100_sd",
                                                                            CASE
                                                                                WHEN "neighbour_footprint_area_100_mean" > 0
                                                                                    THEN "neighbour_footprint_area_100_var" / "neighbour_footprint_area_100_mean"
                                                                            END AS "neighbour_footprint_area_100_d",
                                                                            CASE
                                                                                WHEN "neighbour_footprint_area_100_mean" > 0
                                                                                    THEN "neighbour_footprint_area_100_sd" / "neighbour_footprint_area_100_mean"
                                                                            END AS "neighbour_footprint_area_100_cv",
                                                                            CASE
                                                                                WHEN "neighbour_footprint_area_100_mean" IS NOT NULL
                                                                                    THEN PERCENT_RANK() OVER ( ORDER BY "neighbour_footprint_area_100_mean" )
                                                                            END AS "neighbour_footprint_area_100_mean_pct_rnk",
                                                                            "ratio_neighbour_footprint_sum_to_buffer_100_min",
                                                                            "ratio_neighbour_footprint_sum_to_buffer_100_median",
                                                                            "ratio_neighbour_footprint_sum_to_buffer_100_mean",
                                                                            "ratio_neighbour_footprint_sum_to_buffer_100_max",
                                                                            "ratio_neighbour_footprint_sum_to_buffer_100_sd",
                                                                            CASE
                                                                                WHEN "ratio_neighbour_footprint_sum_to_buffer_100_mean" > 0
                                                                                    THEN "ratio_neighbour_footprint_sum_to_buffer_100_var" / "ratio_neighbour_footprint_sum_to_buffer_100_mean"
                                                                            END AS "ratio_neighbour_footprint_sum_to_buffer_100_d",
                                                                            CASE
                                                                                WHEN "ratio_neighbour_footprint_sum_to_buffer_100_mean" > 0
                                                                                    THEN "ratio_neighbour_footprint_sum_to_buffer_100_sd" / "ratio_neighbour_footprint_sum_to_buffer_100_mean"
                                                                            END AS "ratio_neighbour_footprint_sum_to_buffer_100_cv",
                                                                            CASE
                                                                                WHEN "ratio_neighbour_footprint_sum_to_buffer_100_mean" IS NOT NULL
                                                                                    THEN PERCENT_RANK() OVER ( ORDER BY "ratio_neighbour_footprint_sum_to_buffer_100_mean" )
                                                                            END AS "ratio_neighbour_footprint_sum_to_buffer_100_mean_pct_rnk",
                                                                            "ratio_neighbour_height_to_distance_100_min",
                                                                            "ratio_neighbour_height_to_distance_100_median",
                                                                            "ratio_neighbour_height_to_distance_100_mean",
                                                                            "ratio_neighbour_height_to_distance_100_max",
                                                                            "ratio_neighbour_height_to_distance_100_sd",
                                                                            CASE
                                                                                WHEN "ratio_neighbour_height_to_distance_100_mean" > 0
                                                                                    THEN "ratio_neighbour_height_to_distance_100_var" / "ratio_neighbour_height_to_distance_100_mean"
                                                                            END AS "ratio_neighbour_height_to_distance_100_d",
                                                                            CASE
                                                                                WHEN "ratio_neighbour_height_to_distance_100_mean" > 0
                                                                                    THEN "ratio_neighbour_height_to_distance_100_sd" / "ratio_neighbour_height_to_distance_100_mean"
                                                                            END AS "ratio_neighbour_height_to_distance_100_cv",
                                                                            CASE
                                                                                WHEN "ratio_neighbour_height_to_distance_100_mean" IS NOT NULL
                                                                                    THEN PERCENT_RANK() OVER ( ORDER BY "ratio_neighbour_height_to_distance_100_mean" )
                                                                            END AS "ratio_neighbour_height_to_distance_100_mean_pct_rnk"{% endif %}
                                                                        FROM
                                                                            agg0_neighbours
                                                                        ORDER BY {{order_columns}});



/*
-- This is to troubleshoot whether joints and building data are aggregated correctly

CREATE MATERIALIZED VIEW {{gbmi_schema}}.agg_bni_by_{{agg_level}}_{{raster_name}}_centroid_duplicates AS
    SELECT
        ({{gbmi_schema}}.agg_bni_by_{{agg_level}}_{{raster_name}}_centroid.*)::text,
        count(*)
    FROM
        {{gbmi_schema}}.agg_bni_by_{{agg_level}}_{{raster_name}}_centroid
    GROUP BY
        {{gbmi_schema}}.agg_bni_by_{{agg_level}}_{{raster_name}}_centroid.*
    HAVING count(*) > 1;

 */