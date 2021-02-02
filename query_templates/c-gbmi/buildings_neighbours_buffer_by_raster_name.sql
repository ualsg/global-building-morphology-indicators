DROP TABLE IF EXISTS {{gbmi_schema}}.buildings_neighbours_{{buffer}}_by_{{raster_name}};

CREATE TABLE {{gbmi_schema}}.buildings_neighbours_{{buffer}}_by_{{raster_name}} AS (
                                                              WITH bldgs_within_{{buffer}} AS (
                                                                                       SELECT *
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
                                                                                             percentile_cont(0.25) WITHIN GROUP (ORDER BY distance) AS distance_{{buffer}}_25th_percentile,
                                                                                             percentile_cont(0.5) WITHIN GROUP (ORDER BY distance) AS distance_{{buffer}}_median,
                                                                                             percentile_cont(0.75) WITHIN GROUP (ORDER BY distance) AS distance_{{buffer}}_75th_percentile
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
                                                                  END AS distance_{{buffer}}_cv
                                                              FROM
                                                                  bldgs_within_{{buffer}} bn_{{buffer}}
                                                                  LEFT JOIN count_neighbours_{{buffer}} cn_{{buffer}}
                                                                  ON bn_{{buffer}}.osm_id1 = cn_{{buffer}}.osm_id1 AND
                                                                     bn_{{buffer}}.way1 = cn_{{buffer}}.way1 AND
                                                                     bn_{{buffer}}.way_centroid1 = cn_{{buffer}}.way_centroid1
                                                                  LEFT JOIN median_distance_{{buffer}} md_{{buffer}}
                                                                  ON bn_{{buffer}}.osm_id1 = md_{{buffer}}.osm_id1 AND
                                                                     bn_{{buffer}}.way1 = md_{{buffer}}.way1 AND
                                                                     bn_{{buffer}}.way_centroid1 = md_{{buffer}}.way_centroid1
                                                              );



CREATE INDEX buildings_neighbours_{{buffer}}_by_{{raster_name}}_osm_id ON {{gbmi_schema}}.buildings_neighbours_{{buffer}}_by_{{raster_name}}(osm_id);

CREATE INDEX buildings_neighbours_{{buffer}}_by_{{raster_name}}_centroid_spgist ON {{gbmi_schema}}.buildings_neighbours_{{buffer}}_by_{{raster_name}} USING spgist(way_centroid1);

CREATE INDEX buildings_neighbours_{{buffer}}_by_{{raster_name}}_spgist ON {{gbmi_schema}}.buildings_neighbours_{{buffer}}_by_{{raster_name}} USING spgist(way);

VACUUM ANALYZE {{gbmi_schema}}.buildings_neighbours_{{buffer}}_by_{{raster_name}};