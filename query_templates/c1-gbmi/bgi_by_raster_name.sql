-- MATERIALIZED VIEW FOR DEBUGGING
DROP MATERIALIZED VIEW IF EXISTS {{gbmi_schema}}.bgi_by_{{raster_name}}_duplicates CASCADE;
DROP TABLE IF EXISTS {{gbmi_schema}}.bgi_by_{{raster_name}} CASCADE;


CREATE TABLE {{gbmi_schema}}.bgi_by_{{raster_name}} AS (
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
                                         mbr,
                                         mbr_area,
                                         percent_rank() OVER (ORDER BY mbr_area) AS mbr_area_pct_rnk,
                                         "mbr_length",
                                         percent_rank() OVER (ORDER BY "mbr_length") AS mbr_length_pct_rnk,
                                         mbr_width,
                                         percent_rank() OVER (ORDER BY "mbr_width") AS mbr_width_pct_rnk,
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
                                         "ratio_height_to_footprint_area",
                                         CASE
                                             WHEN "ratio_height_to_footprint_area" IS NOT NULL THEN percent_rank() OVER (ORDER BY "ratio_height_to_footprint_area")
                                         END AS ratio_height_to_footprint_area_pct_rnk,
                                         floor_area,
                                         CASE
                                             WHEN floor_area IS NOT NULL
                                                 THEN percent_rank() OVER (ORDER BY floor_area)
                                         END AS floor_area_pct_rnk,
                                         wall_area,
                                         CASE
                                             WHEN wall_area IS NOT NULL
                                                 THEN percent_rank() OVER (ORDER BY wall_area)
                                         END AS wall_area_pct_rnk,
                                         envelope_area,
                                         CASE
                                             WHEN envelope_area IS NOT NULL
                                                 THEN percent_rank() OVER (ORDER BY envelope_area)
                                         END AS envelope_area_pct_rnk,
                                         volume,
                                         CASE
                                             WHEN volume IS NOT NULL THEN percent_rank() OVER (ORDER BY volume)
                                         END AS volume_pct_rnk,
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
                                         END AS equivalent_rectangular_index_pct_rnk,
                                         start_date,
                                         CASE
                                             WHEN start_date IS NOT NULL THEN percent_rank() OVER (ORDER BY start_date)
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
                                         "cell_country_code3"
                                     FROM
                                         {{gbmi_schema}}.bga_by_{{raster_name}}
                                     );




CREATE INDEX bgi_by_{{raster_name}}_osm_id ON {{gbmi_schema}}.bgi_by_{{raster_name}}(osm_id);

CREATE INDEX bgi_by_{{raster_name}}_centroid_spgist ON {{gbmi_schema}}.bgi_by_{{raster_name}} USING SPGIST (way_centroid);

CREATE INDEX bgi_by_{{raster_name}}_spgist ON {{gbmi_schema}}.bgi_by_{{raster_name}} USING SPGIST (way);

VACUUM ANALYZE {{gbmi_schema}}.bgi_by_{{raster_name}};



/*
-- This is to troubleshoot whether joints and building data are created correctly

CREATE MATERIALIZED VIEW {{gbmi_schema}}.bgi_by_{{raster_name}}_duplicates AS
    SELECT
        ({{gbmi_schema}}.bgi_by_{{raster_name}}.*)::text,
        count(*)
    FROM
        {{gbmi_schema}}.bgi_by_{{raster_name}}
    GROUP BY
        {{gbmi_schema}}.bgi_by_{{raster_name}}.*
    HAVING count(*) > 1;

 */