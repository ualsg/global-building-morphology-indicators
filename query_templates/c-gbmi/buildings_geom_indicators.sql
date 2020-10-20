DROP TABLE IF EXISTS {{gbmi_schema}}.buildings_geom_indicators CASCADE;

CREATE TABLE {{gbmi_schema}}.buildings_geom_indicators AS (
                                     SELECT
                                         osm_id,
                                         tags,
                                         way,
                                         way_centroid,
                                         count_vertices,
                                         percent_rank() OVER (ORDER BY count_vertices) AS count_vertices_pct_rnk,
                                         footprint_area,
                                         percent_rank() OVER (ORDER BY footprint_area) AS footprint_area_pct_rnk,
                                         perimeter,
                                         percent_rank() OVER (ORDER BY perimeter) AS perimeter_pct_rnk,
                                         oriented_mbr,
                                         oriented_mbr_area,
                                         percent_rank() OVER (ORDER BY oriented_mbr_area) AS oriented_mbr_area_pct_rnk,
                                         oriented_mbr_length,
                                         percent_rank() OVER (ORDER BY oriented_mbr_length) AS oriented_mbr_length_pct_rnk,
                                         oriented_mbr_width,
                                         percent_rank() OVER (ORDER BY oriented_mbr_width) AS oriented_mbr_width_pct_rnk,
                                         azimuth,
                                         is_residential,
                                         "height",
                                         CASE
                                             WHEN "height" IS NOT NULL THEN percent_rank() OVER (ORDER BY "height")
                                         END AS height_pct_rnk,
                                         "building:levels",
                                         CASE
                                             WHEN "building:levels" IS NOT NULL
                                                 THEN percent_rank() OVER (ORDER BY "building:levels")
                                         END AS "building:levels_pct_rnk",
                                         est_floor_area,
                                         CASE
                                             WHEN est_floor_area IS NOT NULL
                                                 THEN percent_rank() OVER (ORDER BY est_floor_area)
                                         END AS est_floor_area_pct_rnk,
                                         est_wall_area,
                                         CASE
                                             WHEN est_wall_area IS NOT NULL
                                                 THEN percent_rank() OVER (ORDER BY est_wall_area)
                                         END AS est_wall_area_pct_rnk,
                                         est_volume,
                                         CASE
                                             WHEN est_volume IS NOT NULL THEN percent_rank() OVER (ORDER BY est_volume)
                                         END AS est_volume_pct_rnk,
                                         compactness,
                                         CASE
                                             WHEN compactness IS NOT NULL THEN percent_rank() OVER (ORDER BY compactness)
                                         END AS compactness_pct_rnk,
                                         complexity,
                                         CASE
                                             WHEN complexity IS NOT NULL THEN percent_rank() OVER (ORDER BY complexity)
                                         END AS complexity_pct_rnk,
                                         equivalent_rectangular_index,
                                         CASE
                                             WHEN equivalent_rectangular_index IS NOT NULL THEN percent_rank() OVER (ORDER BY equivalent_rectangular_index)
                                         END AS cequivalent_rectangular_index_pct_rnk,
                                         year_of_construction,
                                         CASE
                                             WHEN year_of_construction IS NOT NULL
                                                 THEN percent_rank() OVER (ORDER BY year_of_construction)
                                         END AS year_of_construction_pct_rnk,
                                         start_date,
                                         CASE
                                             WHEN start_date IS NOT NULL THEN percent_rank() OVER (ORDER BY start_date)
                                         END AS start_date_pct_rnk
                                     FROM
                                         {{gbmi_schema}}.buildings_geom_attributes
                                     );