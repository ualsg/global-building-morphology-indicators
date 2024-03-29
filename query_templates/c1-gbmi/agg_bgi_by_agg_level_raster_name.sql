-- MATERIALIZED VIEW FOR DEBUGGING
-- DROP MATERIALIZED VIEW IF EXISTS {{gbmi_schema}}.agg_bgi_by_{{agg_level}}_{{raster_name}}_duplicates CASCADE;
-- DROP TABLE IF EXISTS {{gbmi_schema}}.agg_bgi_by_{{agg_level}}_{{raster_name}} CASCADE;


CREATE TABLE {{gbmi_schema}}.agg_bgi_by_{{agg_level}}_{{raster_name}} AS (
                                                                            WITH agg0 AS (
                                                                                     SELECT
                                                                                         {{agg_columns}},
                                                                                         {{agg_geom}},
                                                                                         {{agg_area}},
                                                                                         count(DISTINCT osm_id) AS buildings_count,
                                                                                         count(DISTINCT osm_id) / {{agg_area}} AS buildings_count_normalised,
                                                                                         min(clipped_bldg_area) AS footprint_area_min,
                                                                                         (percentile_cont(0.5) WITHIN GROUP ( ORDER BY clipped_bldg_area )) AS footprint_area_median,
                                                                                         avg(clipped_bldg_area) AS footprint_area_mean,
                                                                                         max(clipped_bldg_area) AS footprint_area_max,
                                                                                         sum(clipped_bldg_area) AS footprint_area_sum,
                                                                                         sum(clipped_bldg_area) / {{agg_area}} AS footprint_area_sum_normalised,
                                                                                         stddev_pop(clipped_bldg_area) AS footprint_area_sd,
                                                                                         var_pop(clipped_bldg_area) AS footprint_area_var,
                                                                                         min(clipped_bldg_perimeter) AS perimeter_min,
                                                                                         (percentile_cont(0.5) WITHIN GROUP ( ORDER BY clipped_bldg_perimeter )) AS perimeter_median,
                                                                                         avg(clipped_bldg_perimeter) AS perimeter_mean,
                                                                                         max(clipped_bldg_perimeter) AS perimeter_max,
                                                                                         sum(clipped_bldg_perimeter) AS perimeter_sum,
                                                                                         sum(clipped_bldg_perimeter) / {{agg_area}} AS perimeter_sum_normalised,
                                                                                         stddev_pop(clipped_bldg_perimeter) AS perimeter_sd,
                                                                                         var_pop(clipped_bldg_perimeter) AS perimeter_var,
                                                                                         min(height) AS height_min,
                                                                                         (percentile_cont(0.5) WITHIN GROUP ( ORDER BY height )) AS height_median,
                                                                                         avg(height) AS height_mean,
                                                                                         max(height) AS height_max,
                                                                                         stddev_pop(height) AS height_sd,
                                                                                         var_pop(height) AS height_var,
                                                                                         CASE
                                                                                             WHEN sum(clipped_bldg_area) = 0 THEN 0
                                                                                             ELSE sum(height * clipped_bldg_area) / sum(clipped_bldg_area)
                                                                                         END AS height_weighted_mean,
                                                                                         min(ratio_height_to_footprint_area) AS ratio_height_to_footprint_area_min,
                                                                                         (percentile_cont(0.5) WITHIN GROUP ( ORDER BY ratio_height_to_footprint_area )) AS ratio_height_to_footprint_area_median,
                                                                                         avg(ratio_height_to_footprint_area) AS ratio_height_to_footprint_area_mean,
                                                                                         max(ratio_height_to_footprint_area) AS ratio_height_to_footprint_area_max,
                                                                                         stddev_pop(ratio_height_to_footprint_area) AS ratio_height_to_footprint_area_sd,
                                                                                         var_pop(ratio_height_to_footprint_area) AS ratio_height_to_footprint_area_var,
                                                                                         min(volume) AS volume_min,
                                                                                         (percentile_cont(0.5) WITHIN GROUP ( ORDER BY volume )) AS volume_median,
                                                                                         avg(volume) AS volume_mean,
                                                                                         max(volume) AS volume_max,
                                                                                         sum(volume) AS volume_sum,
                                                                                         sum(volume) / {{agg_area}} AS volume_sum_normalised,
                                                                                         stddev_pop(volume) AS volume_sd,
                                                                                         var_pop(volume) AS volume_var,
                                                                                         min(wall_area) AS wall_area_min,
                                                                                         (percentile_cont(0.5) WITHIN GROUP ( ORDER BY wall_area )) AS wall_area_median,
                                                                                         avg(wall_area) AS wall_area_mean,
                                                                                         max(wall_area) AS wall_area_max,
                                                                                         sum(wall_area) AS wall_area_sum,
                                                                                         sum(wall_area) / {{agg_area}} AS wall_area_sum_normalised,
                                                                                         stddev_pop(wall_area) AS wall_area_sd,
                                                                                         var_pop(wall_area) AS wall_area_var,
                                                                                         min(envelope_area) AS envelope_area_min,
                                                                                         (percentile_cont(0.5) WITHIN GROUP ( ORDER BY envelope_area )) AS envelope_area_median,
                                                                                         avg(envelope_area) AS envelope_area_mean,
                                                                                         max(envelope_area) AS envelope_area_max,
                                                                                         sum(envelope_area) AS envelope_area_sum,
                                                                                         sum(envelope_area) / {{agg_area}} AS envelope_area_sum_normalised,
                                                                                         stddev_pop(envelope_area) AS envelope_area_sd,
                                                                                         var_pop(envelope_area) AS envelope_area_var,
                                                                                         min(vertices_count) AS vertices_count_min,
                                                                                         (percentile_cont(0.5) WITHIN GROUP ( ORDER BY vertices_count )) AS vertices_count_median,
                                                                                         avg(clipped_vertices_count) AS vertices_count_mean,
                                                                                         max(clipped_vertices_count) AS vertices_count_max,
                                                                                         sum(clipped_vertices_count) AS vertices_count_sum,
                                                                                         sum(clipped_vertices_count) / {{agg_area}} AS vertices_count_sum_normalised,
                                                                                         stddev_pop(clipped_vertices_count) AS vertices_count_sd,
                                                                                         var_pop(clipped_vertices_count) AS vertices_count_var,
                                                                                         min(complexity) AS complexity_min,
                                                                                         percentile_cont(0.5) WITHIN GROUP ( ORDER BY complexity ) AS complexity_median,
                                                                                         avg(complexity) AS complexity_mean,
                                                                                         max(complexity) AS complexity_max,
                                                                                         stddev_pop(complexity) AS complexity_sd,
                                                                                         var_pop(complexity) AS complexity_var,
                                                                                         min(compactness) AS compactness_min,
                                                                                         percentile_cont(0.5) WITHIN GROUP ( ORDER BY compactness ) AS compactness_median,
                                                                                         avg(compactness) AS compactness_mean,
                                                                                         max(compactness) AS compactness_max,
                                                                                         stddev_pop(compactness) AS compactness_sd,
                                                                                         var_pop(compactness) AS compactness_var,
                                                                                         min(equivalent_rectangular_index) AS equivalent_rectangular_index_min,
                                                                                         percentile_cont(0.5) WITHIN GROUP ( ORDER BY equivalent_rectangular_index ) AS equivalent_rectangular_index_median,
                                                                                         avg(equivalent_rectangular_index) AS equivalent_rectangular_index_mean,
                                                                                         max(equivalent_rectangular_index) AS equivalent_rectangular_index_max,
                                                                                         stddev_pop(equivalent_rectangular_index) AS equivalent_rectangular_index_sd,
                                                                                         var_pop(equivalent_rectangular_index) AS equivalent_rectangular_index_var,
                                                                                         min(azimuth) AS azimuth_min,
                                                                                         percentile_cont(0.5) WITHIN GROUP ( ORDER BY azimuth ) AS azimuth_median,
                                                                                         CASE
                                                                                             WHEN avg(azimuth) >= 90 AND avg(azimuth) < 180
                                                                                                 THEN avg(azimuth) + 180
                                                                                             WHEN avg(azimuth) >= 180 AND avg(azimuth) < 270
                                                                                                 THEN avg(azimuth) - 180
                                                                                             ELSE avg(azimuth)
                                                                                         END AS azimuth_mean,
                                                                                         max(azimuth) AS azimuth_max,
                                                                                         stddev_pop(azimuth) AS azimuth_sd,
                                                                                         var_pop(azimuth) AS azimuth_var,
                                                                                         min(mbr_length) AS mbr_length_min,
                                                                                         percentile_cont(0.5) WITHIN GROUP ( ORDER BY mbr_length ) AS mbr_length_median,
                                                                                         avg(mbr_length) AS mbr_length_mean,
                                                                                         max(mbr_length) AS mbr_length_max,
                                                                                         stddev_pop(mbr_length) AS mbr_length_sd,
                                                                                         var_pop(mbr_length) AS mbr_length_var,
                                                                                         min(mbr_width) AS mbr_width_min,
                                                                                         percentile_cont(0.5) WITHIN GROUP ( ORDER BY mbr_width ) AS mbr_width_median,
                                                                                         avg(mbr_width) AS mbr_width_mean,
                                                                                         max(mbr_width) AS mbr_width_max,
                                                                                         stddev_pop(mbr_width) AS mbr_width_sd,
                                                                                         var_pop(mbr_width) AS mbr_width_var,
                                                                                         min(clipped_oriented_mbr_area) AS mbr_area_min,
                                                                                         percentile_cont(0.5) WITHIN GROUP ( ORDER BY clipped_oriented_mbr_area ) AS mbr_area_median,
                                                                                         avg(clipped_oriented_mbr_area) AS mbr_area_mean,
                                                                                         max(clipped_oriented_mbr_area) AS mbr_area_max,
                                                                                         sum(clipped_oriented_mbr_area) AS mbr_area_sum,
                                                                                         sum(clipped_oriented_mbr_area) / {{agg_area}} AS mbr_area_sum_normalised,
                                                                                         stddev_pop(clipped_oriented_mbr_area) AS mbr_area_sd,
                                                                                         var_pop(clipped_oriented_mbr_area) AS mbr_area_var,
                                                                                         min("building:levels") AS "building:levels_min",
                                                                                         percentile_cont(0.5) WITHIN GROUP ( ORDER BY "building:levels" ) AS "building:levels_median",
                                                                                         avg("building:levels") AS "building:levels_mean",
                                                                                         max("building:levels") AS "building:levels_max",
                                                                                         stddev_pop("building:levels") AS "building:levels_sd",
                                                                                         var_pop("building:levels") AS "building:levels_var",
                                                                                         min(floor_area) AS floor_area_min,
                                                                                         (percentile_cont(0.5) WITHIN GROUP ( ORDER BY floor_area )) AS floor_area_median,
                                                                                         avg(floor_area) AS floor_area_mean,
                                                                                         max(floor_area) AS floor_area_max,
                                                                                         sum(floor_area) AS floor_area_sum,
                                                                                         sum(floor_area) / {{agg_area}} AS floor_area_sum_normalised,
                                                                                         stddev_pop(floor_area) AS floor_area_sd,
                                                                                         var_pop(floor_area) AS floor_area_var,
                                                                                         count(is_residential) FILTER ( WHERE is_residential IS TRUE ) AS residential_count,
                                                                                         count(is_residential) / {{agg_area}} AS residential_count_normalised,
                                                                                         min(floor_area) FILTER ( WHERE is_residential IS TRUE ) AS residential_floor_area_min,
                                                                                         (percentile_cont(0.5) WITHIN GROUP ( ORDER BY floor_area ) FILTER ( WHERE is_residential IS TRUE )) AS residential_floor_area_median,
                                                                                         avg(floor_area) FILTER ( WHERE is_residential IS TRUE ) AS residential_floor_area_mean,
                                                                                         max(floor_area) FILTER ( WHERE is_residential IS TRUE ) AS residential_floor_area_max,
                                                                                         sum(floor_area) FILTER ( WHERE is_residential IS TRUE ) AS residential_floor_area_sum,
                                                                                         sum(floor_area) FILTER ( WHERE is_residential IS TRUE ) / {{agg_area}} AS residential_floor_area_sum_normalised,
                                                                                         stddev_pop(floor_area) FILTER ( WHERE is_residential IS TRUE ) AS residential_floor_area_sd,
                                                                                         var_pop(floor_area) FILTER ( WHERE is_residential IS TRUE ) AS residential_floor_area_var
                                                                                     FROM
                                                                                         {{gbmi_schema}}.bgi_clipped_by_{{raster_name}} bga {{join_clause}}
                                                                                     WHERE cell_country IS NOT NULL
                                                                                     GROUP BY
                                                                                         {{agg_columns}},
                                                                                         {{agg_geom}},
                                                                                         {{agg_area}}
                                                                                     )
                                                                        SELECT
                                                                            {{agg_columns}},
                                                                            {{agg_geom}},
                                                                            {{agg_area}},
                                                                            buildings_count,
                                                                            buildings_count_normalised,
                                                                            footprint_area_min,
                                                                            footprint_area_median,
                                                                            footprint_area_mean,
                                                                            footprint_area_max,
                                                                            footprint_area_sum,
                                                                            footprint_area_sum_normalised,
                                                                            footprint_area_sd,
                                                                            footprint_area_var / footprint_area_mean AS footprint_area_d,
                                                                            footprint_area_sd / footprint_area_mean AS footprint_area_cv,
                                                                            CASE
                                                                                WHEN footprint_area_mean IS NOT NULL
                                                                                    THEN percent_rank() OVER ( ORDER BY footprint_area_mean )
                                                                            END AS footprint_area_mean_pct_rnk,
                                                                            perimeter_min,
                                                                            perimeter_median,
                                                                            perimeter_mean,
                                                                            perimeter_max,
                                                                            perimeter_sum,
                                                                            perimeter_sum_normalised,
                                                                            perimeter_sd,
                                                                            perimeter_var / perimeter_mean AS perimeter_d,
                                                                            perimeter_sd / perimeter_mean AS perimeter_cv,
                                                                            CASE
                                                                                WHEN perimeter_mean IS NOT NULL
                                                                                    THEN percent_rank() OVER ( ORDER BY perimeter_mean )
                                                                            END AS perimeter_mean_pct_rnk,
                                                                            height_min,
                                                                            height_median,
                                                                            height_mean,
                                                                            height_max,
                                                                            height_sd,
                                                                            CASE WHEN height_mean > 0 THEN height_var / height_mean END AS height_d,
                                                                            CASE WHEN height_mean > 0 THEN height_sd / height_mean END AS height_cv,
                                                                            CASE
                                                                                WHEN height_mean IS NOT NULL
                                                                                    THEN percent_rank() OVER ( ORDER BY height_mean )
                                                                            END AS height_mean_pct_rnk,
                                                                            height_weighted_mean,
                                                                            ratio_height_to_footprint_area_min,
                                                                            ratio_height_to_footprint_area_median,
                                                                            ratio_height_to_footprint_area_mean,
                                                                            ratio_height_to_footprint_area_max,
                                                                            ratio_height_to_footprint_area_sd,
                                                                            CASE
                                                                                WHEN ratio_height_to_footprint_area_mean > 0 THEN ratio_height_to_footprint_area_var / ratio_height_to_footprint_area_mean
                                                                            END AS ratio_height_to_footprint_area_d,
                                                                            CASE
                                                                                WHEN ratio_height_to_footprint_area_mean > 0 THEN ratio_height_to_footprint_area_sd / ratio_height_to_footprint_area_mean
                                                                            END AS ratio_height_to_footprint_area_cv,
                                                                            CASE
                                                                                WHEN ratio_height_to_footprint_area_mean IS NOT NULL
                                                                                    THEN percent_rank() OVER ( ORDER BY ratio_height_to_footprint_area_mean )
                                                                            END AS ratio_height_to_footprint_area_mean_pct_rnk,
                                                                            volume_min,
                                                                            volume_median,
                                                                            volume_mean,
                                                                            volume_max,
                                                                            volume_sum,
                                                                            volume_sum_normalised,
                                                                            volume_sd,
                                                                            CASE WHEN volume_mean > 0 THEN volume_var / volume_mean END AS volume_d,
                                                                            CASE WHEN volume_mean > 0 THEN volume_sd / volume_mean END AS volume_cv,
                                                                            CASE
                                                                                WHEN volume_mean IS NOT NULL
                                                                                    THEN percent_rank() OVER ( ORDER BY volume_mean )
                                                                            END AS volume_mean_pct_rnk,
                                                                            wall_area_min,
                                                                            wall_area_median,
                                                                            wall_area_mean,
                                                                            wall_area_max,
                                                                            wall_area_sum,
                                                                            wall_area_sum_normalised,
                                                                            wall_area_sd,
                                                                            CASE
                                                                                WHEN wall_area_mean > 0
                                                                                    THEN wall_area_var / wall_area_mean
                                                                            END AS wall_area_d,
                                                                            CASE
                                                                                WHEN wall_area_mean > 0
                                                                                    THEN wall_area_sd / wall_area_mean
                                                                            END AS wall_area_cv,
                                                                            CASE
                                                                                WHEN wall_area_mean IS NOT NULL
                                                                                    THEN percent_rank() OVER ( ORDER BY wall_area_mean )
                                                                            END AS wall_area_mean_pct_rnk,
                                                                            envelope_area_min,
                                                                            envelope_area_median,
                                                                            envelope_area_mean,
                                                                            envelope_area_max,
                                                                            envelope_area_sum,
                                                                            envelope_area_sum_normalised,
                                                                            envelope_area_sd,
                                                                            CASE
                                                                                WHEN envelope_area_mean > 0
                                                                                    THEN envelope_area_var / envelope_area_mean
                                                                            END AS envelope_area_d,
                                                                            CASE
                                                                                WHEN envelope_area_mean > 0
                                                                                    THEN envelope_area_sd / envelope_area_mean
                                                                            END AS envelope_area_cv,
                                                                            CASE
                                                                                WHEN envelope_area_mean IS NOT NULL
                                                                                    THEN percent_rank() OVER ( ORDER BY envelope_area_mean )
                                                                            END AS envelope_area_mean_pct_rnk,
                                                                            vertices_count_min,
                                                                            vertices_count_median,
                                                                            vertices_count_mean,
                                                                            vertices_count_max,
                                                                            vertices_count_sum,
                                                                            vertices_count_sum_normalised,
                                                                            vertices_count_sd,
                                                                            CASE
                                                                                WHEN vertices_count_mean > 0
                                                                                    THEN vertices_count_var / vertices_count_mean
                                                                            END AS vertices_count_d,
                                                                            CASE
                                                                                WHEN vertices_count_mean > 0
                                                                                    THEN vertices_count_sd / vertices_count_mean
                                                                            END AS vertices_count_cv,
                                                                            CASE
                                                                                WHEN vertices_count_mean IS NOT NULL
                                                                                    THEN percent_rank() OVER ( ORDER BY vertices_count_mean )
                                                                            END AS vertices_count_mean_pct_rnk,
                                                                            complexity_min,
                                                                            complexity_median,
                                                                            complexity_mean,
                                                                            complexity_max,
                                                                            complexity_sd,
                                                                            CASE WHEN complexity_mean > 0 THEN complexity_var / complexity_mean END AS complexity_d,
                                                                            CASE WHEN complexity_mean > 0 THEN complexity_sd / complexity_mean END AS complexity_cv,
                                                                            CASE
                                                                                WHEN complexity_mean IS NOT NULL
                                                                                    THEN percent_rank() OVER ( ORDER BY complexity_mean )
                                                                            END AS complexity_mean_pct_rnk,
                                                                            compactness_min,
                                                                            compactness_median,
                                                                            compactness_mean,
                                                                            compactness_max,
                                                                            compactness_sd,
                                                                            CASE
                                                                                WHEN compactness_mean > 0
                                                                                    THEN compactness_var / compactness_mean
                                                                            END AS compactness_d,
                                                                            CASE
                                                                                WHEN compactness_mean > 0
                                                                                    THEN compactness_sd / compactness_mean
                                                                            END AS compactness_cv,
                                                                            CASE
                                                                                WHEN compactness_mean IS NOT NULL
                                                                                    THEN percent_rank() OVER ( ORDER BY compactness_mean )
                                                                            END AS compactness_mean_pct_rnk,
                                                                            equivalent_rectangular_index_min,
                                                                            equivalent_rectangular_index_median,
                                                                            equivalent_rectangular_index_mean,
                                                                            equivalent_rectangular_index_max,
                                                                            equivalent_rectangular_index_sd,
                                                                            CASE
                                                                                WHEN equivalent_rectangular_index_mean > 0
                                                                                    THEN equivalent_rectangular_index_var / equivalent_rectangular_index_mean
                                                                            END AS equivalent_rectangular_index_d,
                                                                            CASE
                                                                                WHEN equivalent_rectangular_index_mean > 0
                                                                                    THEN equivalent_rectangular_index_sd / equivalent_rectangular_index_mean
                                                                            END AS equivalent_rectangular_index_cv,
                                                                            CASE
                                                                                WHEN equivalent_rectangular_index_mean IS NOT NULL
                                                                                    THEN percent_rank() OVER ( ORDER BY equivalent_rectangular_index_mean )
                                                                            END AS equivalent_rectangular_index_mean_pct_rnk,
                                                                            azimuth_min,
                                                                            azimuth_median,
                                                                            azimuth_mean,
                                                                            azimuth_max,
                                                                            azimuth_sd,
                                                                            CASE WHEN azimuth_mean > 0 THEN azimuth_var / azimuth_mean END AS azimuth_d,
                                                                            CASE WHEN azimuth_mean > 0 THEN azimuth_sd / azimuth_mean END AS azimuth_cv,
                                                                            CASE
                                                                                WHEN azimuth_mean IS NOT NULL
                                                                                    THEN percent_rank() OVER ( ORDER BY azimuth_mean )
                                                                            END AS azimuth_mean_pct_rnk,
                                                                            mbr_length_min,
                                                                            mbr_length_median,
                                                                            mbr_length_mean,
                                                                            mbr_length_max,
                                                                            mbr_length_sd,
                                                                            CASE WHEN mbr_length_mean > 0 THEN mbr_length_var / mbr_length_mean END AS mbr_length_d,
                                                                            CASE WHEN mbr_length_mean > 0 THEN mbr_length_sd / mbr_length_mean END AS mbr_length_cv,
                                                                            CASE
                                                                                WHEN mbr_length_mean IS NOT NULL
                                                                                    THEN percent_rank() OVER ( ORDER BY mbr_length_mean )
                                                                            END AS mbr_length_mean_pct_rnk,
                                                                            mbr_width_min,
                                                                            mbr_width_median,
                                                                            mbr_width_mean,
                                                                            mbr_width_max,
                                                                            mbr_width_sd,
                                                                            CASE WHEN mbr_width_mean > 0 THEN mbr_width_var / mbr_width_mean END AS mbr_width_d,
                                                                            CASE WHEN mbr_width_mean > 0 THEN mbr_width_sd / mbr_width_mean END AS mbr_width_cv,
                                                                            CASE
                                                                                WHEN mbr_width_mean IS NOT NULL
                                                                                    THEN percent_rank() OVER ( ORDER BY mbr_width_mean )
                                                                            END AS mbr_width_mean_pct_rnk,
                                                                            mbr_area_min,
                                                                            mbr_area_median,
                                                                            mbr_area_mean,
                                                                            mbr_area_max,
                                                                            mbr_area_sum,
                                                                            mbr_area_sum_normalised,
                                                                            mbr_area_sd,
                                                                            CASE WHEN mbr_area_mean > 0 THEN mbr_area_var / mbr_area_mean END AS mbr_area_d,
                                                                            CASE WHEN mbr_area_mean > 0 THEN mbr_area_sd / mbr_area_mean END AS mbr_area_cv,
                                                                            CASE
                                                                                WHEN mbr_area_mean IS NOT NULL
                                                                                    THEN percent_rank() OVER ( ORDER BY mbr_area_mean )
                                                                            END AS mbr_area_mean_pct_rnk,
                                                                            "building:levels_min",
                                                                            "building:levels_median",
                                                                            "building:levels_mean",
                                                                            "building:levels_max",
                                                                            "building:levels_sd",
                                                                            CASE
                                                                                WHEN "building:levels_mean" > 0
                                                                                    THEN "building:levels_var" / "building:levels_mean"
                                                                            END AS "building:levels_d",
                                                                            CASE
                                                                                WHEN "building:levels_mean" > 0
                                                                                    THEN "building:levels_sd" / "building:levels_mean"
                                                                            END AS "building:levels_cv",
                                                                            CASE
                                                                                WHEN "building:levels_mean" IS NOT NULL
                                                                                    THEN percent_rank() OVER ( ORDER BY "building:levels_mean" )
                                                                            END AS "building:levels_mean_pct_rnk",
                                                                            floor_area_min,
                                                                            floor_area_median,
                                                                            floor_area_mean,
                                                                            floor_area_max,
                                                                            floor_area_sum,
                                                                            floor_area_sum_normalised,
                                                                            floor_area_sd,
                                                                            CASE WHEN floor_area_mean > 0 THEN (floor_area_sd) ^ 2 / floor_area_mean END AS floor_area_d,
                                                                            CASE WHEN floor_area_mean > 0 THEN floor_area_sd / floor_area_mean END AS floor_area_cv,
                                                                            CASE
                                                                                WHEN floor_area_mean IS NOT NULL
                                                                                    THEN percent_rank() OVER ( ORDER BY floor_area_mean )
                                                                            END AS floor_area_mean_pct_rnk,
                                                                            residential_count,
                                                                            residential_count_normalised,
                                                                            residential_floor_area_min,
                                                                            residential_floor_area_median,
                                                                            residential_floor_area_mean,
                                                                            residential_floor_area_max,
                                                                            residential_floor_area_sum,
                                                                            residential_floor_area_sum_normalised,
                                                                            residential_floor_area_sd,
                                                                            CASE
                                                                                WHEN residential_floor_area_mean > 0
                                                                                    THEN residential_floor_area_var / residential_floor_area_mean
                                                                            END AS residential_floor_area_d,
                                                                            CASE
                                                                                WHEN residential_floor_area_mean > 0
                                                                                    THEN residential_floor_area_sd / residential_floor_area_mean
                                                                            END AS residential_floor_area_cv,
                                                                            CASE
                                                                                WHEN residential_floor_area_mean IS NOT NULL
                                                                                    THEN percent_rank() OVER ( ORDER BY residential_floor_area_mean )
                                                                            END AS residential_floor_area_mean_pct_rnk
                                                                        FROM
                                                                            agg0
                                                                        ORDER BY {{order_columns}}
                                                                        );



/*
-- This is to troubleshoot whether joints and building data are aggregated correctly

CREATE MATERIALIZED VIEW {{gbmi_schema}}.agg_bgi_by_{{agg_level}}_{{raster_name}}_duplicates AS
    SELECT
        ({{gbmi_schema}}.agg_bgi_by_{{agg_level}}_{{raster_name}}.*)::text,
        count(*)
    FROM
        {{gbmi_schema}}.agg_bgi_by_{{agg_level}}_{{raster_name}}
    GROUP BY
        {{gbmi_schema}}.agg_bgi_by_{{agg_level}}_{{raster_name}}.*
    HAVING count(*) > 1;

*/