-- MATERIALIZED VIEW FOR DEBUGGING
DROP MATERIALIZED VIEW IF EXISTS {{gbmi_schema}}.bn_{{buffer}}_by_{{raster_name}}_duplicates CASCADE;
DROP TABLE IF EXISTS {{gbmi_schema}}.bn_{{buffer}}_by_{{raster_name}} CASCADE;



CREATE TABLE {{gbmi_schema}}.bn_{{buffer}}_by_{{raster_name}} AS (
                                                              WITH bldgs_within_{{buffer}} AS (
                                                                                       SELECT
                                                                                            *,
                                                                                            ST_AREA(ST_Buffer("way1"::geography, {{buffer}})) AS buffer_area_{{buffer}},
                                                                                            ST_AREA(ST_INTERSECTION(ST_Buffer("way1"::geography, {{buffer}}), "way2"::geography)) AS clipped_way_area2_{{buffer}},
                                                                                            CASE
                                                                                                WHEN height2 IS NOT NULL AND distance > 0 THEN height2 * 1.0 / distance
                                                                                                ELSE NULL
                                                                                            END AS ratio_neighbour_height_to_distance
                                                                                       FROM
                                                                                           {{gbmi_schema}}.bn_by_{{raster_name}}
                                                                                       WHERE distance >= 0 AND distance <= {{buffer}}
                                                                                           AND osm_id1 != osm_id2
                                                                                           AND NOT ST_Equals(way1::geometry, way2::geometry)
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
                                                                                             percentile_cont(0.5) WITHIN GROUP (ORDER BY clipped_way_area2_{{buffer}}) AS neighbour_footprint_area_{{buffer}}_median,
                                                                                             percentile_cont(0.5) WITHIN GROUP (ORDER BY ratio_neighbour_height_to_distance) AS ratio_neighbour_height_to_distance_{{buffer}}_median
                                                                                         FROM
                                                                                             bldgs_within_{{buffer}}
                                                                                         GROUP BY osm_id1, way1, way_centroid1
                                                                                         )
                                                              SELECT DISTINCT
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
                                                                  bn_{{buffer}}.buffer_area_{{buffer}},
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
                                                                  SUM(bn_{{buffer}}.clipped_way_area2_{{buffer}})
                                                                    OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) AS neighbour_footprint_area_{{buffer}}_sum,
                                                                  MIN(bn_{{buffer}}.clipped_way_area2_{{buffer}})
                                                                    OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) AS neighbour_footprint_area_{{buffer}}_min,
                                                                  MAX(bn_{{buffer}}.clipped_way_area2_{{buffer}})
                                                                    OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) AS neighbour_footprint_area_{{buffer}}_max,
                                                                  md_{{buffer}}.neighbour_footprint_area_{{buffer}}_median,
                                                                  AVG(bn_{{buffer}}.clipped_way_area2_{{buffer}})
                                                                    OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) AS neighbour_footprint_area_{{buffer}}_mean,
                                                                  STDDEV_POP(bn_{{buffer}}.clipped_way_area2_{{buffer}})
                                                                    OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) AS neighbour_footprint_area_{{buffer}}_sd,
                                                                  CASE
                                                                      WHEN AVG(bn_{{buffer}}.clipped_way_area2_{{buffer}}) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) > 0
                                                                          THEN VAR_POP(bn_{{buffer}}.clipped_way_area2_{{buffer}}) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) / AVG(bn_{{buffer}}.clipped_way_area2_{{buffer}}) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1)
                                                                  END AS neighbour_footprint_area_{{buffer}}_d,
                                                                  CASE
                                                                      WHEN AVG(bn_{{buffer}}.clipped_way_area2_{{buffer}}) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) > 0
                                                                          THEN STDDEV_POP(bn_{{buffer}}.clipped_way_area2_{{buffer}}) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) / AVG(bn_{{buffer}}.clipped_way_area2_{{buffer}}) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1)
                                                                  END AS neighbour_footprint_area_{{buffer}}_cv,
                                                                  (SUM(bn_{{buffer}}.clipped_way_area2_{{buffer}})
                                                                    OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) / bn_{{buffer}}.buffer_area_{{buffer}}) AS ratio_neighbour_footprint_sum_to_buffer_{{buffer}},
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
                                                                          THEN VAR_POP(bn_{{buffer}}.ratio_neighbour_height_to_distance) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) / AVG(bn_{{buffer}}.distance) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1)
                                                                  END AS ratio_neighbour_height_to_distance_{{buffer}}_d,
                                                                  CASE
                                                                      WHEN AVG(bn_{{buffer}}.ratio_neighbour_height_to_distance) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) > 0
                                                                          THEN STDDEV_POP(bn_{{buffer}}.ratio_neighbour_height_to_distance) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1) / AVG(bn_{{buffer}}.distance) OVER (PARTITION BY bn_{{buffer}}.osm_id1, bn_{{buffer}}.way1, bn_{{buffer}}.way_centroid1)
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



CREATE INDEX bn_{{buffer}}_by_{{raster_name}}_osm_id ON {{gbmi_schema}}.bn_{{buffer}}_by_{{raster_name}}(osm_id);

CREATE INDEX bn_{{buffer}}_by_{{raster_name}}_centroid_spgist ON {{gbmi_schema}}.bn_{{buffer}}_by_{{raster_name}} USING spgist(way_centroid);

CREATE INDEX bn_{{buffer}}_by_{{raster_name}}_spgist ON {{gbmi_schema}}.bn_{{buffer}}_by_{{raster_name}} USING spgist(way);

VACUUM ANALYZE {{gbmi_schema}}.bn_{{buffer}}_by_{{raster_name}};



/*
-- This is to troubleshoot whether joints and building data are created correctly

CREATE MATERIALIZED VIEW {{gbmi_schema}}.bn_{{buffer}}_by_{{raster_name}}_duplicates AS
    SELECT
        ({{gbmi_schema}}.bn_{{buffer}}_by_{{raster_name}}.*)::text,
        count(*)
    FROM
        {{gbmi_schema}}.bn_{{buffer}}_by_{{raster_name}}
    GROUP BY
        {{gbmi_schema}}.bn_{{buffer}}_by_{{raster_name}}.*
    HAVING count(*) > 1;

 */