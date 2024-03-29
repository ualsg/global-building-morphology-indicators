-- MATERIALIZED VIEW FOR DEBUGGING
DROP MATERIALIZED VIEW IF EXISTS {{gbmi_schema}}.bga_by_{{raster_name}}_duplicates CASCADE;
DROP TABLE IF EXISTS {{gbmi_schema}}.bga_by_{{raster_name}} CASCADE;

CREATE TABLE {{gbmi_schema}}.bga_by_{{raster_name}} AS (
                                     WITH dumped_mbr AS (
                                                        SELECT
                                                            osm_id,
                                                            way,
                                                            way_centroid,
                                                            oriented_mbr,
                                                            (st_dump(st_boundary(oriented_mbr))).geom AS geom,
                                                            (st_dump(st_boundary(oriented_mbr))).path AS path -- To identify the polygon
                                                        FROM
                                                            {{gbmi_schema}}.buildings
                                                        ),
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
                                                      ORDER BY path
                                                      ),
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
                                                         MAX(st_length(st_makeline(sp, ep)::geography)) OVER (PARTITION BY osm_id, way, way_centroid) AS "max_length",
                                                         MIN(st_length(st_makeline(sp, ep)::geography)) OVER (PARTITION BY osm_id, way, way_centroid) AS "min_length",
                                                         degrees(st_azimuth(sp, ep)) AS azimuth
                                                     FROM
                                                         pointsets
                                                     ),
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
                                                                     WHERE max_length = line_length
                                                                     ),
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
                                                                         "cell_country_code3"
                                                                     FROM
                                                                         {{gbmi_schema}}.buildings_by_{{raster_name}}
                                                                    )
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
                                         bldg.footprint_area * bldg."building:levels" AS floor_area,
                                         bldg.perimeter * bldg."height" AS wall_area,
                                         (bldg.perimeter * bldg."height") + bldg.footprint_area AS envelope_area,
                                         bldg.footprint_area * bldg."height" AS volume,
                                         bldg.compactness,
                                         bldg.complexity,
                                         sqrt( bldg.footprint_area / bldg.mbr_area ) * ( (bma.mbr_length + bma.mbr_width) * 2 / bldg.perimeter ) AS equivalent_rectangular_index,
                                         bldg.start_date,
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
                                         buildings_height_levels bldg
                                         LEFT JOIN buildings_mbr_attributes bma
                                             ON bldg.osm_id = bma.osm_id AND bldg.way = bma.way
                                     );



/*
-- This is to troubleshoot whether joints and building data are created correctly

CREATE MATERIALIZED VIEW {{gbmi_schema}}.bga_by_{{raster_name}}_duplicates AS
    SELECT
        ({{gbmi_schema}}.bga_by_{{raster_name}}.*)::text,
        count(*)
    FROM
        {{gbmi_schema}}.bga_by_{{raster_name}}
    GROUP BY
        {{gbmi_schema}}.bga_by_{{raster_name}}.*
    HAVING count(*) > 1;

 */