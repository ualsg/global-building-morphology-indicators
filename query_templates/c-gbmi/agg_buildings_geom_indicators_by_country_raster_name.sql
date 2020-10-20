DROP TABLE IF EXISTS {{gbmi_schema}}.agg_buildings_geom_indicators_by_country_{{raster_name}};

CREATE TABLE {{gbmi_schema}}.agg_buildings_geom_indicators_by_country_{{raster_name}} AS (
                                                                     WITH agg1 AS (
                                                                                  SELECT
                                                                                      cell_country,
                                                                                      count(osm_id) / ccwa.gadm_area_sqm AS count_buildings,
                                                                                      min(clipped_bldg_area) / ccwa.gadm_area_sqm AS min_footprint_area,
                                                                                          (percentile_cont(0.5) WITHIN GROUP (ORDER BY clipped_bldg_area)) /
                                                                                          ccwa.gadm_area_sqm AS median_footprint_area,
                                                                                      avg(clipped_bldg_area) / ccwa.gadm_area_sqm AS mean_footprint_area,
                                                                                      max(clipped_bldg_area) / ccwa.gadm_area_sqm AS max_footprint_area,
                                                                                      sum(clipped_bldg_area) / ccwa.gadm_area_sqm AS sum_footprint_area,
                                                                                      stddev_pop(clipped_bldg_area) / ccwa.gadm_area_sqm AS sd_footprint_area,
                                                                                      min(clipped_bldg_perimeter) / ccwa.gadm_area_sqm AS min_perimeter,
                                                                                          (percentile_cont(0.5) WITHIN GROUP (ORDER BY clipped_bldg_perimeter)) /
                                                                                          ccwa.gadm_area_sqm AS median_perimeter,
                                                                                      avg(clipped_bldg_perimeter) / ccwa.gadm_area_sqm AS mean_perimeter,
                                                                                      max(clipped_bldg_perimeter) / ccwa.gadm_area_sqm AS max_perimeter,
                                                                                      sum(clipped_bldg_perimeter) / ccwa.gadm_area_sqm AS sum_perimeter,
                                                                                      stddev_pop(clipped_bldg_perimeter) / ccwa.gadm_area_sqm AS sd_perimeter,
                                                                                      min(height) / ccwa.gadm_area_sqm AS min_height,
                                                                                          (percentile_cont(0.5) WITHIN GROUP (ORDER BY height)) /
                                                                                          ccwa.gadm_area_sqm AS median_height,
                                                                                      avg(height) / ccwa.gadm_area_sqm AS mean_height,
                                                                                      max(height) / ccwa.gadm_area_sqm AS max_height,
                                                                                      sum(height) / ccwa.gadm_area_sqm AS sum_height,
                                                                                      stddev_pop(height) / ccwa.gadm_area_sqm AS sd_height,
                                                                                      min(est_volume) / ccwa.gadm_area_sqm AS min_volume,
                                                                                          (percentile_cont(0.5) WITHIN GROUP (ORDER BY est_volume)) /
                                                                                          ccwa.gadm_area_sqm AS median_volume,
                                                                                      avg(est_volume) / ccwa.gadm_area_sqm AS mean_volume,
                                                                                      max(est_volume) / ccwa.gadm_area_sqm AS max_volume,
                                                                                      sum(est_volume) / ccwa.gadm_area_sqm AS sum_volume,
                                                                                      stddev_pop(est_volume) / ccwa.gadm_area_sqm AS sd_volume,
                                                                                      min(est_wall_area) / ccwa.gadm_area_sqm AS min_wall_area,
                                                                                          (percentile_cont(0.5) WITHIN GROUP (ORDER BY est_wall_area)) /
                                                                                          ccwa.gadm_area_sqm AS median_wall_area,
                                                                                      avg(est_wall_area) / ccwa.gadm_area_sqm AS mean_wall_area,
                                                                                      max(est_wall_area) / ccwa.gadm_area_sqm AS max_wall_area,
                                                                                      sum(est_wall_area) / ccwa.gadm_area_sqm AS sum_wall_area,
                                                                                      stddev_pop(est_wall_area) / ccwa.gadm_area_sqm AS sd_wall_area,
                                                                                      min(est_envelope_area) / ccwa.gadm_area_sqm AS min_envelope_area,
                                                                                          (percentile_cont(0.5) WITHIN GROUP (ORDER BY est_envelope_area)) /
                                                                                          ccwa.gadm_area_sqm AS median_envelope_area,
                                                                                      avg(est_envelope_area) / ccwa.gadm_area_sqm AS mean_envelope_area,
                                                                                      max(est_envelope_area) / ccwa.gadm_area_sqm AS max_envelope_area,
                                                                                      sum(est_envelope_area) / ccwa.gadm_area_sqm AS sum_envelope_area,
                                                                                      stddev_pop(est_envelope_area) / ccwa.gadm_area_sqm AS sd_envelope_area,
                                                                                      min(est_floor_area) / ccwa.gadm_area_sqm AS min_floor_area,
                                                                                          (percentile_cont(0.5) WITHIN GROUP (ORDER BY est_floor_area)) /
                                                                                          ccwa.gadm_area_sqm AS median_floor_area,
                                                                                      avg(est_floor_area) / ccwa.gadm_area_sqm AS mean_floor_area,
                                                                                      max(est_floor_area) / ccwa.gadm_area_sqm AS max_floor_area,
                                                                                      sum(est_floor_area) / ccwa.gadm_area_sqm AS sum_floor_area,
                                                                                      stddev_pop(est_floor_area) / ccwa.gadm_area_sqm AS sd_floor_area,
                                                                                      min(count_vertices) / ccwa.gadm_area_sqm AS min_count_vertices,
                                                                                          (percentile_cont(0.5) WITHIN GROUP (ORDER BY count_vertices)) /
                                                                                          ccwa.gadm_area_sqm AS median_count_vertices,
                                                                                      avg(count_vertices) / ccwa.gadm_area_sqm AS mean_count_vertices,
                                                                                      max(count_vertices) / ccwa.gadm_area_sqm AS max_count_vertices,
                                                                                      sum(count_vertices) / ccwa.gadm_area_sqm AS sum_count_vertices,
                                                                                      stddev_pop(count_vertices) / ccwa.gadm_area_sqm AS sd_count_vertices,
                                                                                      min(complexity) AS min_complexity,
                                                                                      percentile_cont(0.5) WITHIN GROUP (ORDER BY complexity) AS median_complexity,
                                                                                      avg(complexity) AS mean_complexity,
                                                                                      max(complexity) AS max_complexity,
                                                                                      stddev_pop(complexity) AS sd_complexity,
                                                                                      min(compactness) AS min_compactness,
                                                                                      percentile_cont(0.5) WITHIN GROUP (ORDER BY compactness) AS median_compactness,
                                                                                      avg(compactness) AS mean_compactness,
                                                                                      max(compactness) AS max_compactness,
                                                                                      stddev_pop(compactness) AS sd_compactness,
                                                                                      min(equivalent_rectangular_index) AS min_equivalent_rectangular_index,
                                                                                      percentile_cont(0.5) WITHIN GROUP ( ORDER BY equivalent_rectangular_index ) AS median_equivalent_rectangular_index,
                                                                                      avg(equivalent_rectangular_index) AS mean_equivalent_rectangular_index,
                                                                                      max(equivalent_rectangular_index) AS max_equivalent_rectangular_index,
                                                                                      stddev_pop(equivalent_rectangular_index) AS sd_equivalent_rectangular_index,
                                                                                      min(azimuth) AS min_azimuth,
                                                                                      percentile_cont(0.5) WITHIN GROUP (ORDER BY azimuth) AS median_azimuth,
                                                                                      CASE
                                                                                          WHEN avg(azimuth) >= 90 AND avg(azimuth) < 180
                                                                                              THEN avg(azimuth) + 180
                                                                                          WHEN avg(azimuth) >= 180 AND avg(azimuth) < 270
                                                                                              THEN avg(azimuth) - 180
                                                                                          ELSE avg(azimuth)
                                                                                      END AS mean_azimuth,
                                                                                      max(azimuth) AS max_azimuth,
                                                                                      stddev_pop(azimuth) AS sd_azimuth,
                                                                                      min(oriented_mbr_length) AS min_mbr_length,
                                                                                      percentile_cont(0.5) WITHIN GROUP (ORDER BY oriented_mbr_length) AS median_mbr_length,
                                                                                      avg(oriented_mbr_length) AS mean_mbr_length,
                                                                                      max(oriented_mbr_length) AS max_mbr_length,
                                                                                      stddev_pop(oriented_mbr_length) AS sd_mbr_length,
                                                                                      min(oriented_mbr_width) AS min_mbr_width,
                                                                                      percentile_cont(0.5) WITHIN GROUP (ORDER BY oriented_mbr_width) AS median_mbr_width,
                                                                                      avg(oriented_mbr_width) AS mean_mbr_width,
                                                                                      max(oriented_mbr_width) AS max_mbr_width,
                                                                                      stddev_pop(oriented_mbr_width) AS sd_mbr_width,
                                                                                      min(oriented_mbr_area) AS min_mbr_area,
                                                                                      percentile_cont(0.5) WITHIN GROUP (ORDER BY oriented_mbr_area) AS median_mbr_area,
                                                                                      avg(oriented_mbr_area) AS mean_mbr_area,
                                                                                      max(oriented_mbr_area) AS max_mbr_area,
                                                                                      sum(oriented_mbr_area) AS sum_mbr_area,
                                                                                      stddev_pop(oriented_mbr_area) AS sd_mbr_area,
                                                                                      min("building:levels") AS "min_building:levels",
                                                                                      percentile_cont(0.5) WITHIN GROUP (ORDER BY "building:levels") AS "median_building:levels",
                                                                                      avg("building:levels") AS "mean_building:levels",
                                                                                      max("building:levels") AS "max_building:levels",
                                                                                      stddev_pop("building:levels") AS "sd_building:levels",
                                                                                      count(is_residential) FILTER (WHERE is_residential IS TRUE) AS count_residential,
                                                                                          min(est_floor_area) FILTER (WHERE is_residential IS TRUE) /
                                                                                          ccwa.gadm_area_sqm AS min_residential_floor_area,
                                                                                          (percentile_cont(0.5)
                                                                                           WITHIN GROUP (ORDER BY est_floor_area)
                                                                                           FILTER (WHERE is_residential IS TRUE)) /
                                                                                          ccwa.gadm_area_sqm AS median_residential_floor_area,
                                                                                          avg(est_floor_area) FILTER (WHERE is_residential IS TRUE) /
                                                                                          ccwa.gadm_area_sqm AS mean_residential_floor_area,
                                                                                          max(est_floor_area) FILTER (WHERE is_residential IS TRUE) /
                                                                                          ccwa.gadm_area_sqm AS max_residential_floor_area,
                                                                                          sum(est_floor_area) FILTER (WHERE is_residential IS TRUE) /
                                                                                          ccwa.gadm_area_sqm AS sum_residential_floor_area,
                                                                                          stddev_pop(est_floor_area) FILTER (WHERE is_residential IS TRUE) /
                                                                                          ccwa.gadm_area_sqm AS sd_residential_floor_area,
                                                                                      min(year_of_construction) AS min_year_of_construction,
                                                                                      percentile_cont(0.5) WITHIN GROUP (ORDER BY year_of_construction) AS median_year_of_construction,
                                                                                      avg(year_of_construction) AS mean_year_of_construction,
                                                                                      max(year_of_construction) AS max_year_of_construction,
                                                                                      stddev_pop(year_of_construction) AS sd_year_of_construction,
                                                                                      min(start_date) AS min_start_date,
                                                                                      percentile_cont(0.5) WITHIN GROUP (ORDER BY start_date) AS median_start_date,
                                                                                      avg(start_date) AS mean_start_date,
                                                                                      max(start_date) AS max_start_date,
                                                                                      stddev_pop(start_date) AS sd_start_date
                                                                                  FROM
                                                                                      {{gbmi_schema}}.buildings_geom_attributes_by_{{raster_name}} bga
                                                                                      LEFT JOIN {{db_schema}}.country_codes_with_areas ccwa
                                                                                      ON bga.cell_country = ccwa.gadm_country
                                                                                  GROUP BY bga.cell_country, ccwa.gadm_area_sqm
                                                                                  )
                                                                     SELECT
                                                                         cell_country,
                                                                         count_buildings,
                                                                         min_footprint_area,
                                                                         median_footprint_area,
                                                                         mean_footprint_area,
                                                                         max_footprint_area,
                                                                         sum_footprint_area,
                                                                         sd_footprint_area,
                                                                         (sd_footprint_area) ^ 2 / mean_footprint_area AS d_footprint_area,
                                                                         CASE
                                                                             WHEN mean_footprint_area IS NOT NULL
                                                                                 THEN percent_rank() OVER (ORDER BY mean_footprint_area)
                                                                         END AS mean_footprint_area_pct_rnk,
                                                                         min_perimeter,
                                                                         median_perimeter,
                                                                         mean_perimeter,
                                                                         max_perimeter,
                                                                         sum_perimeter,
                                                                         sd_perimeter,
                                                                         (sd_perimeter) ^ 2 / mean_perimeter AS d_perimeter,
                                                                         CASE
                                                                             WHEN mean_perimeter IS NOT NULL
                                                                                 THEN percent_rank() OVER (ORDER BY mean_perimeter)
                                                                         END AS mean_perimeter_pct_rnk,
                                                                         min_height,
                                                                         median_height,
                                                                         mean_height,
                                                                         max_height,
                                                                         sd_height,
                                                                         CASE WHEN mean_height > 0 THEN (sd_height) ^ 2 / mean_height END AS d_height,
                                                                         CASE
                                                                             WHEN mean_height IS NOT NULL
                                                                                 THEN percent_rank() OVER (ORDER BY mean_height)
                                                                         END AS mean_height_pct_rnk,
                                                                         min_volume,
                                                                         median_volume,
                                                                         mean_volume,
                                                                         max_volume,
                                                                         sum_volume,
                                                                         sd_volume,
                                                                         CASE WHEN mean_volume > 0 THEN (sd_volume) ^ 2 / mean_volume END AS d_volume,
                                                                         CASE
                                                                             WHEN mean_volume IS NOT NULL
                                                                                 THEN percent_rank() OVER (ORDER BY mean_volume)
                                                                         END AS mean_volume_pct_rnk,
                                                                         min_wall_area,
                                                                         median_wall_area,
                                                                         mean_wall_area,
                                                                         max_wall_area,
                                                                         sum_wall_area,
                                                                         sd_wall_area,
                                                                         CASE
                                                                             WHEN mean_wall_area > 0
                                                                                 THEN (sd_wall_area) ^ 2 / mean_wall_area
                                                                             ELSE NULL
                                                                         END AS d_wall_area,
                                                                         CASE
                                                                             WHEN mean_wall_area IS NOT NULL
                                                                                 THEN percent_rank() OVER (ORDER BY mean_wall_area)
                                                                         END AS mean_wall_area_pct_rnk,
                                                                         min_envelope_area,
                                                                         median_envelope_area,
                                                                         mean_envelope_area,
                                                                         max_envelope_area,
                                                                         sum_envelope_area,
                                                                         sd_envelope_area,
                                                                         CASE
                                                                             WHEN mean_envelope_area > 0
                                                                                 THEN (sd_envelope_area) ^ 2 / mean_envelope_area
                                                                         END AS d_envelope_area,
                                                                         CASE
                                                                             WHEN mean_envelope_area IS NOT NULL
                                                                                 THEN percent_rank() OVER (ORDER BY mean_envelope_area)
                                                                         END AS mean_envelope_area_pct_rnk,
                                                                         min_floor_area,
                                                                         median_floor_area,
                                                                         mean_floor_area,
                                                                         max_floor_area,
                                                                         sum_floor_area,
                                                                         sd_floor_area,
                                                                         CASE WHEN mean_floor_area > 0 THEN (sd_floor_area) ^ 2 / mean_floor_area END AS d_floor_area,
                                                                         CASE
                                                                             WHEN mean_floor_area IS NOT NULL
                                                                                 THEN percent_rank() OVER (ORDER BY mean_floor_area)
                                                                         END AS mean_floor_area_pct_rnk,
                                                                         min_complexity,
                                                                         median_complexity,
                                                                         mean_complexity,
                                                                         max_complexity,
                                                                         sd_complexity,
                                                                         CASE WHEN mean_complexity > 0 THEN (sd_complexity) ^ 2 / mean_complexity END AS d_complexity,
                                                                         CASE
                                                                             WHEN mean_complexity IS NOT NULL
                                                                                 THEN percent_rank() OVER (ORDER BY mean_complexity)
                                                                         END AS mean_complexity_pct_rnk,
                                                                         min_compactness,
                                                                         median_compactness,
                                                                         mean_compactness,
                                                                         max_compactness,
                                                                         sd_compactness,
                                                                         CASE
                                                                             WHEN mean_compactness > 0
                                                                                 THEN (sd_compactness) ^ 2 / mean_compactness
                                                                         END AS d_compactness,
                                                                         CASE
                                                                             WHEN mean_compactness IS NOT NULL
                                                                                 THEN percent_rank() OVER (ORDER BY mean_compactness)
                                                                         END AS mean_compactness_pct_rnk,
                                                                         min_equivalent_rectangular_index,
                                                                         median_equivalent_rectangular_index,
                                                                         mean_equivalent_rectangular_index,
                                                                         max_equivalent_rectangular_index,
                                                                         sd_equivalent_rectangular_index,
                                                                         CASE
                                                                             WHEN mean_equivalent_rectangular_index > 0
                                                                                 THEN (sd_equivalent_rectangular_index) ^ 2 / mean_equivalent_rectangular_index
                                                                         END AS d_equivalent_rectangular_index,
                                                                         CASE
                                                                             WHEN equivalent_rectangular_index IS NOT NULL
                                                                                 THEN percent_rank() OVER ( ORDER BY equivalent_rectangular_index )
                                                                         END AS mean_equivalent_rectangular_index_pct_rnk,
                                                                         min_azimuth,
                                                                         median_azimuth,
                                                                         mean_azimuth,
                                                                         max_azimuth,
                                                                         sd_azimuth,
                                                                         CASE WHEN mean_azimuth > 0 THEN (sd_azimuth) ^ 2 / mean_azimuth END AS d_azimuth,
                                                                         CASE
                                                                             WHEN mean_azimuth IS NOT NULL
                                                                                 THEN percent_rank() OVER (ORDER BY mean_azimuth)
                                                                         END AS mean_azimuth_pct_rnk,
                                                                         min_mbr_length,
                                                                         median_mbr_length,
                                                                         mean_mbr_length,
                                                                         max_mbr_length,
                                                                         sd_mbr_length,
                                                                         CASE WHEN mean_mbr_length > 0 THEN (sd_mbr_length) ^ 2 / mean_mbr_length END AS d_mbr_length,
                                                                         CASE
                                                                             WHEN mean_mbr_length IS NOT NULL
                                                                                 THEN percent_rank() OVER (ORDER BY mean_mbr_length)
                                                                         END AS mean_mbr_length_pct_rnk,
                                                                         min_mbr_width,
                                                                         median_mbr_width,
                                                                         mean_mbr_width,
                                                                         max_mbr_width,
                                                                         sd_mbr_width,
                                                                         CASE WHEN mean_mbr_width > 0 THEN (sd_mbr_width) ^ 2 / mean_mbr_width END AS d_mbr_width,
                                                                         CASE
                                                                             WHEN mean_mbr_width IS NOT NULL
                                                                                 THEN percent_rank() OVER (ORDER BY mean_mbr_width)
                                                                         END AS mean_mbr_width_pct_rnk,
                                                                         min_mbr_area,
                                                                         median_mbr_area,
                                                                         mean_mbr_area,
                                                                         max_mbr_area,
                                                                         sum_mbr_area,
                                                                         sd_mbr_area,
                                                                         CASE WHEN mean_mbr_area > 0 THEN (sd_mbr_area) ^ 2 / mean_mbr_area END AS d_mbr_area,
                                                                         CASE
                                                                             WHEN mean_mbr_area IS NOT NULL
                                                                                 THEN percent_rank() OVER (ORDER BY mean_mbr_area)
                                                                         END AS mean_mbr_area_pct_rnk,
                                                                         "min_building:levels",
                                                                         "median_building:levels",
                                                                         "mean_building:levels",
                                                                         "max_building:levels",
                                                                         "sd_building:levels",
                                                                         CASE
                                                                             WHEN "mean_building:levels" > 0
                                                                                 THEN ("sd_building:levels") ^ 2 / "mean_building:levels"
                                                                         END AS "d_building:levels",
                                                                         CASE
                                                                             WHEN "mean_building:levels" IS NOT NULL
                                                                                 THEN percent_rank() OVER (ORDER BY "mean_building:levels")
                                                                         END AS "mean_building:levels_pct_rnk",
                                                                         count_residential,
                                                                         min_residential_floor_area,
                                                                         median_residential_floor_area,
                                                                         mean_residential_floor_area,
                                                                         max_residential_floor_area,
                                                                         sum_residential_floor_area,
                                                                         sd_residential_floor_area,
                                                                         CASE
                                                                             WHEN mean_residential_floor_area > 0
                                                                                 THEN (sd_residential_floor_area) ^ 2 / mean_residential_floor_area
                                                                         END AS d_residential_floor_area,
                                                                         CASE
                                                                             WHEN mean_residential_floor_area IS NOT NULL
                                                                                 THEN percent_rank() OVER (ORDER BY mean_residential_floor_area)
                                                                         END AS mean_residential_floor_area_pct_rnk,
                                                                         min_year_of_construction,
                                                                         median_year_of_construction,
                                                                         mean_year_of_construction,
                                                                         max_year_of_construction,
                                                                         sd_year_of_construction,
                                                                         CASE
                                                                             WHEN mean_year_of_construction > 0
                                                                                 THEN (sd_year_of_construction) ^ 2 / mean_year_of_construction
                                                                         END AS d_year_of_construction,
                                                                         CASE
                                                                             WHEN mean_year_of_construction IS NOT NULL
                                                                                 THEN percent_rank() OVER (ORDER BY mean_year_of_construction)
                                                                         END AS mean_year_of_construction_pct_rnk,
                                                                         min_start_date,
                                                                         median_start_date,
                                                                         mean_start_date,
                                                                         max_start_date,
                                                                         sd_start_date,
                                                                         CASE WHEN mean_start_date > 0 THEN (sd_start_date) ^ 2 / mean_start_date END AS d_start_date,
                                                                         CASE
                                                                             WHEN mean_start_date IS NOT NULL
                                                                                 THEN percent_rank() OVER (ORDER BY mean_start_date)
                                                                         END AS mean_start_date_pct_rnk
                                                                     FROM
                                                                         agg1
                                                                     ORDER BY
                                                                      cell_country
                                                                     );
