DROP TABLE IF EXISTS {{gbmi_schema}}.buildings_neighbours_{{buffer}}_by_{{raster_name}};

CREATE TABLE {{gbmi_schema}}.buildings_neighbours_{{buffer}}_by_{{raster_name}} AS (
                                                              WITH bldgs_within_{{buffer}} AS (
                                                                                       SELECT
                                                                                            *,
                                                                                            CASE
                                                                                                WHEN height2 IS NOT NULL AND distance > 0 THEN height2 * 1.0 / distance
                                                                                            END AS ratio_neighbour_height_to_distance
                                                                                       FROM
                                                                                           {{gbmi_schema}}.buildings_neighbours_by_{{raster_name}}
                                                                                       WHERE distance > 0 AND distance <= {{buffer}}
                                                                                       ),
                                                                  count_neighbours_{{buffer}} AS (
                                                                                          SELECT
                                                                                              osm_id1,
                                                                                              way1,
                                                                                              way_centroid1,
                                                                                              COUNT(DISTINCT way2) AS neighbour_{{buffer}}_count
                                                                                          FROM
                                                                                              bldgs_within_{{buffer}}
                                                                                          GROUP BY osm_id1, way1, way_centroid1
                                                                                          ),
                                                                  median_distance_{{buffer}} AS (
                                                                                         SELECT
                                                                                             osm_id1,
                                                                                             way1,
                                                                                             way_centroid1,
                                                                                             percentile_cont(0.5) WITHIN GROUP (ORDER BY distance) AS distance_{{buffer}}_median,
                                                                                             percentile_cont(0.5) WITHIN GROUP (ORDER BY way_area2) AS neighbour_footprint_area_{{buffer}}_median,
                                                                                             percentile_cont(0.5) WITHIN GROUP (ORDER BY ratio_neighbour_height_to_distance) AS ratio_neighbour_height_to_distance_{{buffer}}_median
                                                                                         FROM
                                                                                             bldgs_within_{{buffer}}
                                                                                         GROUP BY osm_id1, way1, way_centroid1
                                                                                         )
                                                              SELECT
                                                                  bn_{{buffer}}.osm_id1 AS osm_id,
                                                                  bn_{{buffer}}.way1 AS way,
                                                                  bn_{{buffer}}.way_centroid1 AS way_centroid,
                                                                  bn_{{buffer}}.cell_id,
                                                                  bn_{{buffer}}.cell_centroid,
                                                                  bn_{{buffer}}.cell_geom,
                                                                  bn_{{buffer}}.cell_area,
                                                                  bn_{{buffer}}.cell_country,
                                                                  bn_{{buffer}}.cell_admin_div1,
                                                                  bn_{{buffer}}.cell_admin_div2,
                                                                  bn_{{buffer}}.cell_admin_div3,
                                                                  bn_{{buffer}}.cell_admin_div4,
                                                                  bn_{{buffer}}.cell_admin_div5,
                                                                  bn_{{buffer}}.cell_population AS cell_population,
                                                                  bn_{{buffer}}.cell_country_official_name,
                                                                  bn_{{buffer}}.cell_country_code2,
                                                                  bn_{{buffer}}.cell_country_code3,
                                                                  cn_{{buffer}}.neighbour_{{buffer}}_count,
                                                                  MIN(bn_{{buffer}}.distance)
                                                                    OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) AS distance_{{buffer}}_min,
                                                                  MAX(bn_{{buffer}}.distance)
                                                                    OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) AS distance_{{buffer}}_max,
                                                                  md_{{buffer}}.distance_{{buffer}}_median,
                                                                  AVG(bn_{{buffer}}.distance)
                                                                    OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) AS distance_{{buffer}}_mean,
                                                                  STDDEV_POP(bn_{{buffer}}.distance)
                                                                    OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) AS distance_{{buffer}}_sd,
                                                                  CASE
                                                                      WHEN AVG(bn_{{buffer}}.distance) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) > 0
                                                                          THEN VAR_POP(bn_{{buffer}}.distance) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) / AVG(bn_{{buffer}}.distance) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1)
                                                                  END AS distance_{{buffer}}_d,
                                                                  CASE
                                                                      WHEN AVG(bn_{{buffer}}.distance) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) > 0
                                                                          THEN STDDEV_POP(bn_{{buffer}}.distance) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) / AVG(bn_{{buffer}}.distance) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1)
                                                                  END AS distance_{{buffer}}_cv,
                                                                  SUM(bn_{{buffer}}.way_area2)
                                                                    OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) AS neighbour_footprint_area_{{buffer}}_sum,
                                                                  MIN(bn_{{buffer}}.way_area2)
                                                                    OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) AS neighbour_footprint_area_{{buffer}}_min,
                                                                  MAX(bn_{{buffer}}.way_area2)
                                                                    OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) AS neighbour_footprint_area_{{buffer}}_max,
                                                                  md_{{buffer}}.neighbour_footprint_area_{{buffer}}_median,
                                                                  AVG(bn_{{buffer}}.way_area2)
                                                                    OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) AS neighbour_footprint_area_{{buffer}}_mean,
                                                                  STDDEV_POP(bn_{{buffer}}.way_area2)
                                                                    OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) AS neighbour_footprint_area_{{buffer}}_sd,
                                                                  CASE
                                                                      WHEN AVG(bn_{{buffer}}.way_area2) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) > 0
                                                                          THEN VAR_POP(bn_{{buffer}}.way_area2) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) / AVG(bn_{{buffer}}.distance) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1)
                                                                  END AS neighbour_footprint_area_{{buffer}}_d,
                                                                  CASE
                                                                      WHEN AVG(bn_{{buffer}}.way_area2) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) > 0
                                                                          THEN STDDEV_POP(bn_{{buffer}}.way_area2) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) / AVG(bn_{{buffer}}.distance) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1)
                                                                  END AS neighbour_footprint_area_{{buffer}}_cv,
                                                                  MIN(bn_{{buffer}}.ratio_neighbour_height_to_distance)
                                                                    OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) AS ratio_neighbour_height_to_distance_{{buffer}}_min,
                                                                  MAX(bn_{{buffer}}.ratio_neighbour_height_to_distance)
                                                                    OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) AS ratio_neighbour_height_to_distance_{{buffer}}_max,
                                                                  md_{{buffer}}.ratio_neighbour_height_to_distance_{{buffer}}_median,
                                                                  AVG(bn_{{buffer}}.ratio_neighbour_height_to_distance)
                                                                    OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) AS ratio_neighbour_height_to_distance_{{buffer}}_mean,
                                                                  STDDEV_POP(bn_{{buffer}}.ratio_neighbour_height_to_distance)
                                                                    OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) AS ratio_neighbour_height_to_distance_{{buffer}}_sd,
                                                                  CASE
                                                                      WHEN AVG(bn_{{buffer}}.ratio_neighbour_height_to_distance) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) > 0
                                                                          THEN VAR_POP(bn_{{buffer}}.way_area2) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) / AVG(bn_{{buffer}}.distance) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1)
                                                                  END AS ratio_neighbour_height_to_distance_{{buffer}}_d,
                                                                  CASE
                                                                      WHEN AVG(bn_{{buffer}}.ratio_neighbour_height_to_distance) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) > 0
                                                                          THEN STDDEV_POP(bn_{{buffer}}.way_area2) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) / AVG(bn_{{buffer}}.distance) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1)
                                                                  END AS ratio_neighbour_height_to_distance_{{buffer}}_cv
                                                              FROM
                                                                  bldgs_within_{{buffer}} bn_{{buffer}}
                                                                  LEFT JOIN count_neighbours_{{buffer}} cn_{{buffer}}
                                                                  ON bn_{{buffer}}.osm_id1 = cn_{{buffer}}.osm_id1 AND
                                                                     ST_Equals(bn_{{buffer}}.way1::geometry, cn_{{buffer}}.way1::geometry) AND
                                                                     ST_Equals(bn_{{buffer}}.way_centroid1::geometry, cn_{{buffer}}.way_centroid1::geometry)
                                                                  LEFT JOIN median_distance_{{buffer}} md_{{buffer}}
                                                                  ON bn_{{buffer}}.osm_id1 = md_{{buffer}}.osm_id1 AND
                                                                     ST_Equals(bn_{{buffer}}.way1::geometry, md_{{buffer}}.way1::geometry) AND
                                                                     ST_Equals(bn_{{buffer}}.way_centroid1::geometry, md_{{buffer}}.way_centroid1::geometry)
                                                              );



CREATE INDEX buildings_neighbours_{{buffer}}_by_{{raster_name}}_osm_id ON {{gbmi_schema}}.buildings_neighbours_{{buffer}}_by_{{raster_name}}(osm_id);

CREATE INDEX buildings_neighbours_{{buffer}}_by_{{raster_name}}_centroid_spgist ON {{gbmi_schema}}.buildings_neighbours_{{buffer}}_by_{{raster_name}} USING spgist(way_centroid);

CREATE INDEX buildings_neighbours_{{buffer}}_by_{{raster_name}}_spgist ON {{gbmi_schema}}.buildings_neighbours_{{buffer}}_by_{{raster_name}} USING spgist(way);

VACUUM ANALYZE {{gbmi_schema}}.buildings_neighbours_{{buffer}}_by_{{raster_name}};