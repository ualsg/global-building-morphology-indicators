DROP TABLE IF EXISTS {{gbmi_schema}}.buildings_neighbors_{{buffer}};

CREATE TABLE {{gbmi_schema}}.buildings_neighbors_{{buffer}} AS (
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
                                       {{gbmi_schema}}.buildings_neighbors
                                   WHERE osm_id1 != osm_id2 AND distance <= {{buffer}}
                                   GROUP BY
                                       osm_id1
                                   );
