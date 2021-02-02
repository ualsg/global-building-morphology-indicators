DROP TABLE IF EXISTS {{gbmi_schema}}.buildings_geom_indicators_by_{{raster_name}} CASCADE;

CREATE TABLE {{gbmi_schema}}.buildings_geom_indicators_by_{{raster_name}} AS (
                                     SELECT
                                         osm_id,
                                         tags,
                                         way,
                                         way_centroid,
                                         vertices_count,
                                         percent_rank() OVER (ORDER BY vertices_count) AS vertices_count_pct_rnk,
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
                                             ELSE NULL
                                         END AS height_pct_rnk,
                                         "building:levels",
                                         CASE
                                             WHEN "building:levels" IS NOT NULL
                                                 THEN percent_rank() OVER (ORDER BY "building:levels")
                                             ELSE NULL
                                         END AS "building:levels_pct_rnk",
                                         est_floor_area,
                                         CASE
                                             WHEN est_floor_area IS NOT NULL
                                                 THEN percent_rank() OVER (ORDER BY est_floor_area)
                                             ELSE NULL
                                         END AS est_floor_area_pct_rnk,
                                         est_wall_area,
                                         CASE
                                             WHEN est_wall_area IS NOT NULL
                                                 THEN percent_rank() OVER (ORDER BY est_wall_area)
                                             ELSE NULL
                                         END AS est_wall_area_pct_rnk,
                                         est_envelope_area,
                                         CASE
                                             WHEN est_envelope_area IS NOT NULL
                                                 THEN percent_rank() OVER (ORDER BY est_wall_area)
                                             ELSE NULL
                                         END AS est_envelope_area_pct_rnk,
                                         est_volume,
                                         CASE
                                             WHEN est_volume IS NOT NULL THEN percent_rank() OVER (ORDER BY est_volume)
                                             ELSE NULL
                                         END AS est_volume_pct_rnk,
                                         compactness,
                                         CASE
                                             WHEN compactness IS NOT NULL THEN percent_rank() OVER (ORDER BY compactness)
                                             ELSE NULL
                                         END AS compactness_pct_rnk,
                                         complexity,
                                         CASE
                                             WHEN complexity IS NOT NULL THEN percent_rank() OVER (ORDER BY complexity)
                                             ELSE NULL
                                         END AS complexity_pct_rnk,
                                         equivalent_rectangular_index,
                                         CASE
                                             WHEN equivalent_rectangular_index IS NOT NULL THEN percent_rank() OVER (ORDER BY equivalent_rectangular_index)
                                             ELSE NULL
                                         END AS cequivalent_rectangular_index_pct_rnk,
                                         year_of_construction,
                                         CASE
                                             WHEN year_of_construction IS NOT NULL
                                                 THEN percent_rank() OVER (ORDER BY year_of_construction)
                                             ELSE NULL
                                         END AS year_of_construction_pct_rnk,
                                         start_date,
                                         CASE
                                             WHEN start_date IS NOT NULL THEN percent_rank() OVER (ORDER BY start_date)
                                             ELSE NULL
                                         END AS start_date_pct_rnk,
                                         "cell_id",
                                         "cell_centroid",
                                         "cell_geom",
                                         "cell_area",
                                         "cell_country",
                                         "cell_admin_div1",
                                         "cell_admin_div2",
                                         "cell_admin_div3",
                                         "cell_admin_div4",
                                         "cell_admin_div5",{% if raster_population %}
                                         "cell_population",{% endif %}
                                         "cell_country_official_name",
                                         "cell_country_code2",
                                         "cell_country_code3",
                                         "clipped_way"
                                     FROM
                                         {{gbmi_schema}}.buildings_geom_attributes_by_{{raster_name}}
                                     );




CREATE INDEX buildings_geom_indicators_by_{{raster_name}}_osm_id ON {{gbmi_schema}}.buildings_geom_indicators_by_{{raster_name}}(osm_id);

CREATE INDEX buildings_geom_indicators_by_{{raster_name}}_centroid_spgist ON {{gbmi_schema}}.buildings_geom_indicators_by_{{raster_name}} USING SPGIST (way_centroid);

CREATE INDEX buildings_geom_indicators_by_{{raster_name}}_spgist ON {{gbmi_schema}}.buildings_geom_indicators_by_{{raster_name}} USING SPGIST (way);

VACUUM ANALYZE {{gbmi_schema}}.buildings_geom_indicators_by_{{raster_name}};