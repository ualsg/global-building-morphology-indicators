DROP TABLE IF EXISTS {{gbmi_schema}}.buildings_neighbors_{{buffer}};

CREATE TABLE {{gbmi_schema}}.buildings_neighbors_{{buffer}} AS (
                                   SELECT
                                       osm_id1 AS osm_id,
                                       COUNT(DISTINCT osm_id2) AS count_neighbor_{{buffer}},
                                       MIN(distance) AS min_distance_{{buffer}},
                                       percentile_cont(0.5) WITHIN GROUP (ORDER BY distance) AS median_distance_{{buffer}},
                                       MAX(distance) AS max_distance_{{buffer}},
                                       AVG(distance) AS mean_distance_{{buffer}},
                                       STDDEV_POP(distance) AS sd_distance_{{buffer}},
                                       CASE
                                           WHEN AVG(distance) > 0 THEN VAR_POP(distance) / AVG(distance)
                                       END AS d_distance_{{buffer}}
                                   FROM
                                       {{gbmi_schema}}.buildings_neighbors
                                   WHERE osm_id1 != osm_id2 AND distance <= {{buffer}}
                                   GROUP BY
                                       osm_id1
                                   );
