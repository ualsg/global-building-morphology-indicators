DROP TABLE IF EXISTS {{gbmi_schema}}.buildings_neighbours_{{buffer}};

CREATE TABLE {{gbmi_schema}}.buildings_neighbours_{{buffer}} AS (
                                   SELECT
                                       osm_id1 AS osm_id,
                                       COUNT(DISTINCT osm_id2) AS neighbour_{{buffer}}_count,
                                       MIN(distance) AS distance_{{buffer}}_min,
                                       percentile_cont(0.5) WITHIN GROUP (ORDER BY distance) AS distance_{{buffer}}_median,
                                       MAX(distance) AS distance_{{buffer}}_max,
                                       AVG(distance) AS distance_{{buffer}}_mean,
                                       STDDEV_POP(distance) AS distance_{{buffer}}_sd,
                                       CASE
                                           WHEN AVG(distance) > 0 THEN VAR_POP(distance) / AVG(distance)
                                       END AS distance_{{buffer}}_d
                                   FROM
                                       {{gbmi_schema}}.buildings_neighbours
                                   WHERE osm_id1 != osm_id2 AND distance <= {{buffer}}
                                   GROUP BY
                                       osm_id1
                                   );
