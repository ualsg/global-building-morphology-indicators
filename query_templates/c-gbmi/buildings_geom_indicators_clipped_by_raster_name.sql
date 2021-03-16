DROP TABLE IF EXISTS {{gbmi_schema}}.buildings_geom_indicators_clipped_by_{{raster_name}} CASCADE;

CREATE TABLE {{gbmi_schema}}.buildings_geom_indicators_clipped_by_{{raster_name}} AS (
                                    WITH dumped_mbr AS (
                                                        SELECT
                                                            osm_id,
                                                            way,
                                                            way_centroid,
                                                            oriented_mbr,
                                                            (st_dump(st_boundary(oriented_mbr))).geom AS geom,
                                                            (st_dump(st_boundary(oriented_mbr))).path AS path -- To identify the polygon
                                                        FROM
                                                            {{gbmi_schema}}.buildings),
                                         pointsets AS (
                                                      SELECT
                                                          osm_id,
                                                          way,
                                                          way_centroid,
                                                          oriented_mbr,
                                                          COALESCE(path[1], 0) AS polygon_num,
                                                          generate_series(1, st_npoints(geom) - 1) AS point_order,
                                                          st_pointn(geom, generate_series(1, st_npoints(geom) - 1)) AS sp,
                                                          st_pointn(geom, generate_series(2, st_npoints(geom))) AS ep
                                                      FROM
                                                          dumped_mbr
                                                      ORDER BY path),
                                         linesets AS (
                                                     SELECT
                                                         osm_id,
                                                         way,
                                                         way_centroid,
                                                         oriented_mbr,
                                                         polygon_num,
                                                         point_order AS line_order,
                                                         sp,
                                                         ep,
                                                         st_makeline(sp, ep) AS line,
                                                         st_length(st_makeline(sp, ep)::geography) AS line_length,
                                                         max(st_length(st_makeline(sp, ep)::geography)) OVER (PARTITION BY osm_id, way, way_centroid) AS "max_length",
                                                         min(st_length(st_makeline(sp, ep)::geography)) OVER (PARTITION BY osm_id, way, way_centroid) AS "min_length",
                                                         degrees(st_azimuth(sp, ep)) AS azimuth
                                                     FROM
                                                         pointsets),
                                         buildings_mbr_attributes AS (
                                                                     SELECT DISTINCT
                                                                         osm_id,
                                                                         way,
                                                                         way_centroid,
                                                                         max_length AS "mbr_length",
                                                                         min_length AS "mbr_width",
                                                                         CASE
                                                                             WHEN azimuth > 90 AND azimuth < 180
                                                                                 THEN azimuth + 180
                                                                             WHEN azimuth > 180 AND azimuth < 270
                                                                                 THEN azimuth - 180
                                                                             WHEN azimuth = 180 OR azimuth = 360
                                                                                 THEN 0
                                                                             ELSE azimuth
                                                                         END AS azimuth
                                                                     FROM
                                                                         linesets
                                                                     WHERE max_length = line_length),
                                         buildings_height_levels AS (
                                                                     SELECT DISTINCT
                                                                         osm_id,
                                                                         tags,
                                                                         way,
                                                                         way_centroid,
                                                                         calc_count_vertices AS vertices_count,
                                                                         calc_way_area AS footprint_area,
                                                                         calc_perimeter AS perimeter,
                                                                         oriented_mbr AS mbr,
                                                                         oriented_mbr_area AS mbr_area,
                                                                         is_residential,
                                                                         CASE
                                                                             WHEN "height" IS NULL AND "building:levels" IS NOT NULL AND "building:levels" > 0
                                                                                 THEN "building:levels" * 3.0
                                                                             ELSE "height"
                                                                         END AS "height",
                                                                         CASE
                                                                             WHEN "building:levels" IS NULL AND "height" IS NOT NULL AND "height" > 0
                                                                                 THEN round("height" / 3.0)
                                                                             ELSE "building:levels"
                                                                         END AS "building:levels",
                                                                         2.0 * sqrt(pi() * calc_way_area) / calc_perimeter AS compactness,
                                                                         calc_perimeter / sqrt(sqrt(calc_way_area)) AS complexity,
                                                                         year_of_construction,
                                                                         start_date,
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
                                                                         "clipped_way",
                                                                         "clipped_bldg_area",
                                                                         "clipped_bldg_perimeter",
                                                                         ST_NPOINTS("clipped_way") AS "clipped_vertices_count",
                                                                         ST_ENVELOPE("clipped_way") AS "clipped_mbr",
                                                                         ST_AREA(st_envelope("clipped_way")::geography) AS "clipped_mbr_area",
                                                                         ST_ORIENTEDENVELOPE("clipped_way") AS "clipped_oriented_mbr",
                                                                         ST_AREA(ST_ORIENTEDENVELOPE("clipped_way")::geography) AS "clipped_oriented_mbr_area"
                                                                     FROM
                                                                         {{gbmi_schema}}.buildings_by_{{raster_name}}),
                                         bldg_attributes AS (
                                              SELECT
                                                  bldg.osm_id,
                                                  bldg.tags,
                                                  bldg.way,
                                                  bldg.way_centroid,
                                                  bldg.vertices_count,
                                                  bldg.footprint_area,
                                                  bldg.perimeter,
                                                  bldg.mbr,
                                                  bldg.mbr_area,
                                                  bma.mbr_length,
                                                  bma.mbr_width,
                                                  bma.azimuth,
                                                  bldg.is_residential,
                                                  bldg."height",
                                                  bldg."building:levels",
                                                  CASE
                                                      WHEN bldg."height" IS NOT NULL AND bldg.footprint_area > 0
                                                          THEN bldg."height" * 1.0 / bldg.footprint_area
                                                  END AS "ratio_height_to_footprint_area",
                                                  bldg.clipped_bldg_area * bldg."building:levels" AS floor_area,
                                                  bldg.clipped_bldg_perimeter * bldg."height" AS wall_area,
                                                  (bldg.clipped_bldg_perimeter * bldg."height") + bldg.clipped_bldg_area AS envelope_area,
                                                  bldg.clipped_bldg_area * bldg."height" AS volume,
                                                  bldg.compactness,
                                                  bldg.complexity,
                                                  sqrt( bldg.footprint_area / bldg.mbr_area ) * ( bma.mbr_length / bldg.perimeter ) AS equivalent_rectangular_index,
                                                  bldg.year_of_construction,
                                                  bldg.start_date,
                                                  bldg."cell_id",
                                                  bldg."cell_centroid",
                                                  bldg."cell_geom",
                                                  bldg."cell_area",
                                                  bldg."cell_country",
                                                  bldg."cell_admin_div1",
                                                  bldg."cell_admin_div2",
                                                  bldg."cell_admin_div3",
                                                  bldg."cell_admin_div4",
                                                  bldg."cell_admin_div5",{% if raster_population %}
                                                  bldg."cell_population",{% endif %}
                                                  bldg."cell_country_official_name",
                                                  bldg."cell_country_code2",
                                                  bldg."cell_country_code3",
                                                  bldg."clipped_way",
                                                  bldg."clipped_bldg_area",
                                                  bldg."clipped_bldg_perimeter",
                                                  bldg."clipped_vertices_count",
                                                  bldg."clipped_mbr",
                                                  bldg."clipped_mbr_area",
                                                  bldg."clipped_oriented_mbr",
                                                  bldg."clipped_oriented_mbr_area"
                                              FROM
                                                  buildings_height_levels bldg
                                                  LEFT JOIN buildings_mbr_attributes bma
                                                      ON bldg.osm_id = bma.osm_id AND bldg.way = bma.way)
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
                                        percent_rank() OVER (ORDER BY mbr_width) AS mbr_width_pct_rnk,
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
                                        END AS cequivalent_rectangular_index_pct_rnk,
                                        year_of_construction,
                                        CASE
                                            WHEN year_of_construction IS NOT NULL
                                                THEN percent_rank() OVER (ORDER BY year_of_construction)
                                        END AS year_of_construction_pct_rnk,
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
                                        "cell_country_code3",
                                        "clipped_way",
                                        "clipped_bldg_area",
                                        "clipped_bldg_perimeter",
                                        "clipped_vertices_count",
                                        "clipped_mbr",
                                        "clipped_mbr_area",
                                        "clipped_oriented_mbr",
                                        "clipped_oriented_mbr_area"
                                    FROM
                                        bldg_attributes);




CREATE INDEX buildings_geom_indicators_clipped_by_{{raster_name}}_osm_id ON {{gbmi_schema}}.buildings_geom_indicators_clipped_by_{{raster_name}}(osm_id);

CREATE INDEX buildings_geom_indicators_clipped_by_{{raster_name}}_centroid_spgist ON {{gbmi_schema}}.buildings_geom_indicators_clipped_by_{{raster_name}} USING SPGIST (way_centroid);

CREATE INDEX buildings_geom_indicators_clipped_by_{{raster_name}}_spgist ON {{gbmi_schema}}.buildings_geom_indicators_clipped_by_{{raster_name}} USING SPGIST (way);

VACUUM ANALYZE {{gbmi_schema}}.buildings_geom_indicators_clipped_by_{{raster_name}};


-- MATERIALIZED VIEW FOR DEBUGGING
DROP MATERIALIZED VIEW IF EXISTS {{gbmi_schema}}.buildings_geom_indicators_clipped_by_{{raster_name}}_duplicates CASCADE;

CREATE MATERIALIZED VIEW {{gbmi_schema}}.buildings_geom_indicators_clipped_by_{{raster_name}}_duplicates AS
    SELECT
        ({{gbmi_schema}}.buildings_geom_indicators_clipped_by_{{raster_name}}.*)::text,
        count(*)
    FROM
        {{gbmi_schema}}.buildings_geom_indicators_clipped_by_{{raster_name}}
    GROUP BY
        {{gbmi_schema}}.buildings_geom_indicators_clipped_by_{{raster_name}}.*
    HAVING count(*) > 1;