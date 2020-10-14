DROP TABLE IF EXISTS {{gbmi_schema}}.buildings_geom_attributes CASCADE;

CREATE TABLE {{gbmi_schema}}.buildings_geom_attributes AS (
                                     WITH dumped_mbr AS (
                                                        SELECT
                                                            osm_id,
                                                            way,
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
                                                         oriented_mbr,
                                                         polygon_num,
                                                         point_order AS line_order,
                                                         sp,
                                                         ep,
                                                         st_makeline(sp, ep) AS line,
                                                         st_length(st_makeline(sp, ep)::geography) AS line_length,
                                                                 max(st_length(st_makeline(sp, ep)::geography))
                                                                 OVER (PARTITION BY osm_id) AS "max_length",
                                                                 min(st_length(st_makeline(sp, ep)::geography))
                                                                 OVER (PARTITION BY osm_id) AS "min_length",
                                                         degrees(st_azimuth(sp, ep)) AS azimuth
                                                     FROM
                                                         pointsets
                                                     ),
                                         buildings_mbr_attributes AS (
                                                                     SELECT
                                                                         osm_id,
                                                                         max_length AS oriented_mbr_length,
                                                                         min_length AS oriented_mbr_width,
                                                                         CASE
                                                                             WHEN azimuth > 90 AND azimuth < 180
                                                                                 THEN azimuth + 180
                                                                             WHEN azimuth > 180 AND azimuth < 270
                                                                                 THEN azimuth - 180
                                                                             ELSE azimuth
                                                                         END AS azimuth
                                                                     FROM
                                                                         linesets
                                                                     WHERE max_length = line_length
                                                                     )
                                     SELECT
                                         bldg.osm_id,
                                         bldg.tags,
                                         bldg.way,
                                         bldg.way_centroid,
                                         calc_count_vertices AS count_vertices,
                                         calc_way_area AS footprint_area,
                                         calc_perimeter AS perimeter,
                                         oriented_mbr,
                                         oriented_mbr_area,
                                         oriented_mbr_length,
                                         oriented_mbr_width,
                                         azimuth,
                                         is_residential,
                                         CASE
                                             WHEN "height" IS NULL AND "building:levels" IS NOT NULL AND "building:levels" > 0
                                                 THEN "building:levels" * 3.0
                                             ELSE "height"
                                         END AS "height",
                                         CASE
                                             WHEN "building:levels" IS NULL AND height IS NOT NULL AND height > 0
                                                 THEN round(height / 3.0)
                                             ELSE "building:levels"
                                         END AS "building:levels",
                                         calc_way_area * "building:levels" AS est_floor_area,
                                         calc_perimeter * "height" AS est_wall_area,
                                         (calc_perimeter * "height") + calc_way_area AS est_envelope_area,
                                         calc_way_area * "height" AS est_volume,
                                         2.0 * sqrt(pi() * calc_way_area) / calc_perimeter AS compactness,
                                         calc_perimeter / sqrt(sqrt(calc_way_area)) AS complexity,
                                         year_of_construction,
                                         start_date
                                     FROM
                                         {{gbmi_schema}}.buildings bldg
                                         LEFT JOIN buildings_mbr_attributes bma ON bldg.osm_id = bma.osm_id
                                     );

