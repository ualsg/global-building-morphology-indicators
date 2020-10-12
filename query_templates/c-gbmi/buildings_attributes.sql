CREATE TABLE {{gbmi_schema}}.buildings AS (
                               WITH bldgs_0 AS (
                                               SELECT
                                                   osm_id,
                                                   building,
                                                   tags,
                                                   way,
                                                   st_centroid(way::geography) AS way_centroid,
                                                   st_area(way::geography) AS area,
                                                   st_perimeter(way::geography) AS perimeter,
                                                   st_npoints(way) AS n_vertices,
                                                   COALESCE(SUBSTRING(tags -> 'height' FROM '\d+\.?\d*'),
                                                            SUBSTRING(tags -> 'building:height' FROM '\d+\.?\d*'))::numeric AS height,
                                                   COALESCE(SUBSTRING(tags -> 'building:levels' FROM '\d+\.?\d*'),
                                                            SUBSTRING(tags -> 'levels' FROM '\d+\.?\d*'))::numeric AS "building:levels",
                                                   SUBSTRING(tags -> 'year_of_construction' FROM '\d{4}')::SMALLINT AS "year_of_construction",
                                                   SUBSTRING(tags -> 'start_date' FROM '\d{4}')::SMALLINT AS "start_date"
                                               FROM
                                                   {{gbmi_schema}}.planet_osm_polygon pop
                                               WHERE building IS NOT NULL
                                                 AND lower(building) NOT IN ('no', 'not', 'non', 'none')
                                               ),
                                   bldgs_1 AS (
                                              SELECT
                                                  osm_id,
                                                  building,
                                                  tags,
                                                  way,
                                                  way_centroid,
                                                  area,
                                                  perimeter,
                                                  n_vertices,
                                                  height,
                                                  "building:levels",
                                                  year_of_construction,
                                                  start_date,
                                                  area * "building:levels" AS est_floor_area,
                                                  area * "height" AS est_volume,
                                                  2.0 * sqrt(pi() * area) / perimeter AS compactness,
                                                  perimeter / sqrt(sqrt(area)) AS complexity
                                              FROM
                                                  bldgs_0
                                              )
                               SELECT
                                   osm_id,
                                   building,
                                   tags,
                                   way,
                                   way_centroid,
                                   area,
                                   percent_rank() OVER (ORDER BY area) AS area_pct_rnk,
                                   perimeter,
                                   percent_rank() OVER (ORDER BY perimeter) AS perimeter_pct_rnk,
                                   n_vertices,
                                   percent_rank() OVER (ORDER BY n_vertices) AS n_vertices_pct_rnk,
                                   height,
                                   CASE
                                       WHEN height IS NOT NULL THEN percent_rank() OVER (ORDER BY height)
                                   END AS height_pct_rnk,
                                   "building:levels",
                                   CASE
                                       WHEN height IS NOT NULL THEN percent_rank() OVER (ORDER BY "building:levels")
                                   END AS "building:levels_pct_rnk",
                                   year_of_construction,
                                   CASE
                                       WHEN height IS NOT NULL THEN percent_rank() OVER (ORDER BY year_of_construction)
                                   END AS year_of_construction_pct_rnk,
                                   start_date,
                                   CASE
                                       WHEN height IS NOT NULL THEN percent_rank() OVER (ORDER BY start_date)
                                   END AS start_date_pct_rnk,
                                   est_floor_area,
                                   CASE
                                       WHEN height IS NOT NULL THEN percent_rank() OVER (ORDER BY est_floor_area)
                                   END AS est_floor_area_pct_rnk,
                                   est_volume,
                                   CASE
                                       WHEN height IS NOT NULL THEN percent_rank() OVER (ORDER BY est_volume)
                                   END AS est_volume_pct_rnk,
                                   compactness,
                                   CASE
                                       WHEN height IS NOT NULL THEN percent_rank() OVER (ORDER BY compactness)
                                   END AS compactness_pct_rnk,
                                   complexity,
                                   CASE
                                       WHEN height IS NOT NULL THEN percent_rank() OVER (ORDER BY complexity)
                                   END AS complexity_pct_rnk
                               FROM
                                   bldgs_1
                               );

