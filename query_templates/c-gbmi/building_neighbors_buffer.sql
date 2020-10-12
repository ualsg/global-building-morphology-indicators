DROP TABLE IF EXISTS {{gbmi_schema}}.building_neighbors_{{buffer}};

CREATE TABLE {{gbmi_schema}}.building_neighbors_{{buffer}} AS (
                                   WITH neighbors AS (
                                                     SELECT
                                                         bldg1.osm_id AS osm_id1,
                                                         bldg2.osm_id AS osm_id2,
                                                         st_distance(bldg1.way_centroid, bldg2.way_centroid) AS distance
                                                     FROM
                                                         {{gbmi_schema}}.buildings AS bldg1
                                                         CROSS JOIN (
                                                                    SELECT osm_id, way_centroid
                                                                    FROM {{gbmi_schema}}.buildings
                                                                    ) AS bldg2
                                                     WHERE st_dwithin(bldg1.way_centroid, bldg2.way_centroid, 250)
                                                     ORDER BY
                                                         osm_id1,
                                                         osm_id2,
                                                         st_distance(bldg1.way_centroid, bldg2.way_centroid)
                                                     )
                                   SELECT
                                       osm_id1 AS osm_id,
                                       COUNT(DISTINCT osm_id2) AS count_neighbor_{{buffer}},
                                       MIN(distance) AS distance_{{buffer}}_mn,
                                       percentile_cont(0.5) WITHIN GROUP (ORDER BY distance) AS distance_{{buffer}}_md,
                                       MAX(distance) AS distance_{{buffer}}_mx,
                                       AVG(distance) AS distance_{{buffer}}_avg,
                                       STDDEV_POP(distance) AS distance_{{buffer}}_sd,
                                       VAR_POP(distance) / AVG(distance) AS distance_{{buffer}}_d
                                   FROM
                                       neighbors
                                   WHERE osm_id1 != osm_id2 AND distance <= {{buffer}}
                                   GROUP BY
                                       osm_id1
                                   );
