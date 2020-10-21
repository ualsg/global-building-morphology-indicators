DROP TABLE IF EXISTS {{gbmi_schema}}.agg_buildings_geom_indicators_by_cell_{{raster_name}};

CREATE TABLE {{gbmi_schema}}.agg_buildings_geom_indicators_by_cell_{{raster_name}} AS (
                                                                  WITH agg1 AS (
                                                                               SELECT
                                                                                   cell_id,
                                                                                   cell_geom,
                                                                                   cell_country,
                                                                                   cell_admin_div1,
                                                                                   cell_admin_div2,
                                                                                   count(DISTINCT osm_id) AS buildings_count,
                                                                                   count(DISTINCT osm_id) / cell_area AS buildings_count_normalised,
                                                                                   min(clipped_bldg_area) AS footprint_area_min,
                                                                                   (percentile_cont(0.5) WITHIN GROUP ( ORDER BY clipped_bldg_area )) AS footprint_area_median,
                                                                                   avg(clipped_bldg_area) AS footprint_area_mean,
                                                                                   max(clipped_bldg_area) AS footprint_area_max,
                                                                                   sum(clipped_bldg_area) AS footprint_area_sum,
                                                                                   sum(clipped_bldg_area) / cell_area AS footprint_area_sum_normalised,
                                                                                   stddev_pop(clipped_bldg_area) AS footprint_area_sd,
                                                                                   min(clipped_bldg_perimeter) AS perimeter_min,
                                                                                   (percentile_cont(0.5) WITHIN GROUP ( ORDER BY clipped_bldg_perimeter )) AS perimeter_median,
                                                                                   avg(clipped_bldg_perimeter) AS perimeter_mean,
                                                                                   max(clipped_bldg_perimeter) AS perimeter_max,
                                                                                   sum(clipped_bldg_perimeter) AS perimeter_sum,
                                                                                   sum(clipped_bldg_perimeter) / cell_area AS perimeter_sum_normalised,
                                                                                   stddev_pop(clipped_bldg_perimeter) AS perimeter_sd,
                                                                                   min(height) AS height_min,
                                                                                   (percentile_cont(0.5) WITHIN GROUP ( ORDER BY height )) AS height_median,
                                                                                   avg(height) AS height_mean,
                                                                                   max(height) AS height_max,
                                                                                   stddev_pop(height) AS height_sd,
                                                                                   min(est_volume) AS volume_min,
                                                                                   (percentile_cont(0.5) WITHIN GROUP ( ORDER BY est_volume )) AS volume_median,
                                                                                   avg(est_volume) AS volume_mean,
                                                                                   max(est_volume) AS volume_max,
                                                                                   sum(est_volume) AS volume_sum,
                                                                                   sum(est_volume) / cell_area AS volume_sum_normalised,
                                                                                   stddev_pop(est_volume) AS volume_sd,
                                                                                   min(est_wall_area) AS wall_area_min,
                                                                                   (percentile_cont(0.5) WITHIN GROUP ( ORDER BY est_wall_area )) AS wall_area_median,
                                                                                   avg(est_wall_area) AS wall_area_mean,
                                                                                   max(est_wall_area) AS wall_area_max,
                                                                                   sum(est_wall_area) AS wall_area_sum,
                                                                                   sum(est_wall_area) / cell_area AS wall_area_sum_normalised,
                                                                                   stddev_pop(est_wall_area) AS wall_area_sd,
                                                                                   min(est_envelope_area) AS envelope_area_min,
                                                                                   (percentile_cont(0.5) WITHIN GROUP ( ORDER BY est_envelope_area )) AS envelope_area_median,
                                                                                   avg(est_envelope_area) AS envelope_area_mean,
                                                                                   max(est_envelope_area) AS envelope_area_max,
                                                                                   sum(est_envelope_area) AS envelope_area_sum,
                                                                                   sum(est_envelope_area) / cell_area AS envelope_area_sum_normalised,
                                                                                   stddev_pop(est_envelope_area) AS envelope_area_sd,
                                                                                   min(vertices_count) AS vertices_count_min,
                                                                                   (percentile_cont(0.5) WITHIN GROUP ( ORDER BY vertices_count )) AS vertices_count_median,
                                                                                   avg(vertices_count) AS vertices_count_mean,
                                                                                   max(vertices_count) AS vertices_count_max,
                                                                                   sum(vertices_count) AS vertices_count_sum,
                                                                                   sum(vertices_count) / cell_area AS vertices_count_sum_normalised,
                                                                                   stddev_pop(vertices_count) AS vertices_count_sd,
                                                                                   min(complexity) AS complexity_min,
                                                                                   percentile_cont(0.5) WITHIN GROUP ( ORDER BY complexity ) AS complexity_median,
                                                                                   avg(complexity) AS complexity_mean,
                                                                                   max(complexity) AS complexity_max,
                                                                                   stddev_pop(complexity) AS complexity_sd,
                                                                                   min(compactness) AS compactness_min,
                                                                                   percentile_cont(0.5) WITHIN GROUP ( ORDER BY compactness ) AS compactness_median,
                                                                                   avg(compactness) AS compactness_mean,
                                                                                   max(compactness) AS compactness_max,
                                                                                   stddev_pop(compactness) AS compactness_sd,
                                                                                   min(equivalent_rectangular_index) AS equivalent_rectangular_index_min,
                                                                                   percentile_cont(0.5) WITHIN GROUP ( ORDER BY equivalent_rectangular_index ) AS equivalent_rectangular_index_median,
                                                                                   avg(equivalent_rectangular_index) AS equivalent_rectangular_index_mean,
                                                                                   max(equivalent_rectangular_index) AS equivalent_rectangular_index_max,
                                                                                   stddev_pop(equivalent_rectangular_index) AS equivalent_rectangular_index_sd,
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
                                                                                   min(oriented_mbr_length) AS mbr_length_min,
                                                                                   percentile_cont(0.5) WITHIN GROUP ( ORDER BY oriented_mbr_length ) AS mbr_length_median,
                                                                                   avg(oriented_mbr_length) AS mbr_length_mean,
                                                                                   max(oriented_mbr_length) AS mbr_length_max,
                                                                                   stddev_pop(oriented_mbr_length) AS mbr_length_sd,
                                                                                   min(oriented_mbr_width) AS mbr_width_min,
                                                                                   percentile_cont(0.5) WITHIN GROUP ( ORDER BY oriented_mbr_width ) AS mbr_width_median,
                                                                                   avg(oriented_mbr_width) AS mbr_width_mean,
                                                                                   max(oriented_mbr_width) AS mbr_width_max,
                                                                                   stddev_pop(oriented_mbr_width) AS mbr_width_sd,
                                                                                   min(oriented_mbr_area) AS mbr_area_min,
                                                                                   percentile_cont(0.5) WITHIN GROUP ( ORDER BY oriented_mbr_area ) AS mbr_area_median,
                                                                                   avg(oriented_mbr_area) AS mbr_area_mean,
                                                                                   max(oriented_mbr_area) AS mbr_area_max,
                                                                                   sum(oriented_mbr_area) AS mbr_area_sum,
                                                                                   stddev_pop(oriented_mbr_area) AS mbr_area_sd,
                                                                                   min("building:levels") AS "building:levels_min",
                                                                                   percentile_cont(0.5) WITHIN GROUP ( ORDER BY "building:levels" ) AS "building:levels_median",
                                                                                   avg("building:levels") AS "building:levels_mean",
                                                                                   max("building:levels") AS "building:levels_max",
                                                                                   stddev_pop("building:levels") AS "building:levels_sd",
                                                                                   min(est_floor_area) AS floor_area_min,
                                                                                   (percentile_cont(0.5) WITHIN GROUP ( ORDER BY est_floor_area )) AS floor_area_median,
                                                                                   avg(est_floor_area) AS floor_area_mean,
                                                                                   max(est_floor_area) AS floor_area_max,
                                                                                   sum(est_floor_area) AS floor_area_sum,
                                                                                   sum(est_floor_area) / cell_area AS floor_area_sum_normalised,
                                                                                   stddev_pop(est_floor_area) AS floor_area_sd,
                                                                                   count(is_residential) FILTER ( WHERE is_residential IS TRUE ) AS residential_count,
                                                                                   count(is_residential) / cell_area AS residential_count_normalised,
                                                                                   min(est_floor_area) FILTER ( WHERE is_residential IS TRUE ) AS residential_floor_area_min,
                                                                                   (percentile_cont(0.5) WITHIN GROUP ( ORDER BY est_floor_area ) FILTER ( WHERE is_residential IS TRUE )) AS residential_floor_area_median,
                                                                                   avg(est_floor_area) FILTER ( WHERE is_residential IS TRUE ) AS residential_floor_area_mean,
                                                                                   max(est_floor_area) FILTER ( WHERE is_residential IS TRUE ) AS residential_floor_area_max,
                                                                                   sum(est_floor_area) FILTER ( WHERE is_residential IS TRUE ) AS residential_floor_area_sum,
                                                                                   sum(est_floor_area) FILTER ( WHERE is_residential IS TRUE ) / cell_area AS residential_floor_area_sum_normalised,
                                                                                   stddev_pop(est_floor_area) FILTER ( WHERE is_residential IS TRUE ) AS residential_floor_area_sd,
                                                                                   min(year_of_construction) AS year_of_construction_min,
                                                                                   percentile_cont(0.5) WITHIN GROUP ( ORDER BY year_of_construction ) AS year_of_construction_median,
                                                                                   avg(year_of_construction) AS year_of_construction_mean,
                                                                                   max(year_of_construction) AS year_of_construction_max,
                                                                                   stddev_pop(year_of_construction) AS year_of_construction_sd
                                                                               FROM {{gbmi_schema}}.buildings_geom_attributes_by_{{raster_name}}
                                                                               GROUP BY
                                                                                   cell_id,
                                                                                   cell_country,
                                                                                   cell_admin_div1,
                                                                                   cell_admin_div2,
                                                                                   cell_area
                                                                               )
                                                                  SELECT
                                                                      cell_id,
                                                                      cell_geom,
                                                                      cell_country,
                                                                      cell_admin_div1,
                                                                      cell_admin_div2,
                                                                      buildings_count,
                                                                      buildings_count_normalised,
                                                                      footprint_area_min,
                                                                      footprint_area_median,
                                                                      footprint_area_mean,
                                                                      footprint_area_max,
                                                                      footprint_area_sum,
                                                                      footprint_area_sum_normalised,
                                                                      footprint_area_sd,
                                                                      (footprint_area_sd) ^ 2 / footprint_area_mean AS footprint_area_d,
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
                                                                      (perimeter_sd) ^ 2 / perimeter_mean AS perimeter_d,
                                                                      CASE
                                                                          WHEN perimeter_mean IS NOT NULL
                                                                              THEN percent_rank() OVER ( ORDER BY perimeter_mean )
                                                                      END AS perimeter_mean_pct_rnk,
                                                                      height_min,
                                                                      height_median,
                                                                      height_mean,
                                                                      height_max,
                                                                      height_sd,
                                                                      CASE WHEN height_mean > 0 THEN (height_sd) ^ 2 / height_mean END AS height_d,
                                                                      CASE
                                                                          WHEN height_mean IS NOT NULL
                                                                              THEN percent_rank() OVER ( ORDER BY height_mean )
                                                                      END AS height_mean_pct_rnk,
                                                                      volume_min,
                                                                      volume_median,
                                                                      volume_mean,
                                                                      volume_max,
                                                                      volume_sum,
                                                                      volume_sum_normalised,
                                                                      volume_sd,
                                                                      CASE WHEN volume_mean > 0 THEN (volume_sd) ^ 2 / volume_mean END AS volume_d,
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
                                                                              THEN (wall_area_sd) ^ 2 / wall_area_mean
                                                                          ELSE NULL
                                                                      END AS wall_area_d,
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
                                                                              THEN (envelope_area_sd) ^ 2 / envelope_area_mean
                                                                      END AS envelope_area_d,
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
                                                                              THEN (vertices_count_sd) ^ 2 / vertices_count_mean
                                                                      END AS vertices_count_d,
                                                                      CASE
                                                                          WHEN vertices_count_mean IS NOT NULL
                                                                              THEN percent_rank() OVER ( ORDER BY vertices_count_mean )
                                                                      END AS vertices_count_mean_pct_rnk,
                                                                      complexity_min,
                                                                      complexity_median,
                                                                      complexity_mean,
                                                                      complexity_max,
                                                                      complexity_sd,
                                                                      CASE WHEN complexity_mean > 0 THEN (complexity_sd) ^ 2 / complexity_mean END AS complexity_d,
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
                                                                              THEN (compactness_sd) ^ 2 / compactness_mean
                                                                      END AS compactness_d,
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
                                                                              THEN (equivalent_rectangular_index_sd) ^ 2 / equivalent_rectangular_index_mean
                                                                      END AS equivalent_rectangular_index_d,
                                                                      CASE
                                                                          WHEN equivalent_rectangular_index_mean IS NOT NULL
                                                                              THEN percent_rank() OVER ( ORDER BY equivalent_rectangular_index_mean )
                                                                      END AS equivalent_rectangular_index_mean_pct_rnk,
                                                                      azimuth_min,
                                                                      azimuth_median,
                                                                      azimuth_mean,
                                                                      azimuth_max,
                                                                      azimuth_sd,
                                                                      CASE WHEN azimuth_mean > 0 THEN (azimuth_sd) ^ 2 / azimuth_mean END AS azimuth_d,
                                                                      CASE
                                                                          WHEN azimuth_mean IS NOT NULL
                                                                              THEN percent_rank() OVER ( ORDER BY azimuth_mean )
                                                                      END AS azimuth_mean_pct_rnk,
                                                                      mbr_length_min,
                                                                      mbr_length_median,
                                                                      mbr_length_mean,
                                                                      mbr_length_max,
                                                                      mbr_length_sd,
                                                                      CASE WHEN mbr_length_mean > 0 THEN (mbr_length_sd) ^ 2 / mbr_length_mean END AS mbr_length_d,
                                                                      CASE
                                                                          WHEN mbr_length_mean IS NOT NULL
                                                                              THEN percent_rank() OVER ( ORDER BY mbr_length_mean )
                                                                      END AS mbr_length_mean_pct_rnk,
                                                                      mbr_width_min,
                                                                      mbr_width_median,
                                                                      mbr_width_mean,
                                                                      mbr_width_max,
                                                                      mbr_width_sd,
                                                                      CASE WHEN mbr_width_mean > 0 THEN (mbr_width_sd) ^ 2 / mbr_width_mean END AS mbr_width_d,
                                                                      CASE
                                                                          WHEN mbr_width_mean IS NOT NULL
                                                                              THEN percent_rank() OVER ( ORDER BY mbr_width_mean )
                                                                      END AS mbr_width_mean_pct_rnk,
                                                                      mbr_area_min,
                                                                      mbr_area_median,
                                                                      mbr_area_mean,
                                                                      mbr_area_max,
                                                                      mbr_area_sum,
                                                                      mbr_area_sd,
                                                                      CASE WHEN mbr_area_mean > 0 THEN (mbr_area_sd) ^ 2 / mbr_area_mean END AS mbr_area_d,
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
                                                                              THEN ("building:levels_sd") ^ 2 / "building:levels_mean"
                                                                      END AS "building:levels_d",
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
                                                                              THEN (residential_floor_area_sd) ^ 2 / residential_floor_area_mean
                                                                      END AS residential_floor_area_d,
                                                                      CASE
                                                                          WHEN residential_floor_area_mean IS NOT NULL
                                                                              THEN percent_rank() OVER ( ORDER BY residential_floor_area_mean )
                                                                      END AS residential_floor_area_mean_pct_rnk,
                                                                      year_of_construction_min,
                                                                      year_of_construction_median,
                                                                      year_of_construction_mean,
                                                                      year_of_construction_max,
                                                                      year_of_construction_sd,
                                                                      CASE
                                                                          WHEN year_of_construction_mean > 0
                                                                              THEN (year_of_construction_sd) ^ 2 / year_of_construction_mean
                                                                      END AS year_of_construction_d,
                                                                      CASE
                                                                          WHEN year_of_construction_mean IS NOT NULL
                                                                              THEN percent_rank() OVER ( ORDER BY year_of_construction_mean )
                                                                      END AS year_of_construction_mean_pct_rnk
                                                                  FROM
                                                                      agg1
                                                                  ORDER BY
                                                                      cell_country,
                                                                      cell_admin_div1,
                                                                      cell_admin_div2,
                                                                      cell_id
                                                                  );
