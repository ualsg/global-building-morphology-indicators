CREATE TABLE {{gbmi_schema}}.buildings_neighbors_indicators AS (
                                                    WITH bn_100 AS (
                                                                   SELECT *,
                                                                       CASE
                                                                           WHEN mean_distance_100 IS NOT NULL
                                                                               THEN percent_rank() OVER (ORDER BY mean_distance_100)
                                                                       END AS mean_distance_100_pct_rnk
                                                                   FROM
                                                                       {{gbmi_schema}}.buildings_neighbors_100
                                                                   ),
                                                        bn_200 AS (
                                                                  SELECT *,
                                                                      CASE
                                                                          WHEN mean_distance_200 IS NOT NULL
                                                                              THEN percent_rank() OVER (ORDER BY mean_distance_200)
                                                                      END AS mean_distance_200_pct_rnk
                                                                  FROM
                                                                      {{gbmi_schema}}.buildings_neighbors_200
                                                                  ),
                                                        bn_250 AS (
                                                                  SELECT *,
                                                                      CASE
                                                                          WHEN mean_distance_250 IS NOT NULL
                                                                              THEN percent_rank() OVER (ORDER BY mean_distance_250)
                                                                      END AS mean_distance_250_pct_rnk
                                                                  FROM
                                                                      {{gbmi_schema}}.buildings_neighbors_250
                                                                  )
                                                    SELECT
                                                        bn_250.osm_id AS osm_id3,
                                                        count_neighbor_250,
                                                        min_distance_250,
                                                        median_distance_250,
                                                        max_distance_250,
                                                        mean_distance_250,
                                                        sd_distance_250,
                                                        d_distance_250 AS mean_distance_250_pct_rnk,
                                                        bn_200.osm_id AS osm_id2,
                                                        count_neighbor_200,
                                                        min_distance_200,
                                                        median_distance_200,
                                                        max_distance_200,
                                                        mean_distance_200,
                                                        sd_distance_200,
                                                        d_distance_200,
                                                        mean_distance_200_pct_rnk,
                                                        bn_100.osm_id AS osm_id1,
                                                        count_neighbor_100,
                                                        min_distance_100,
                                                        median_distance_100,
                                                        max_distance_100,
                                                        mean_distance_100,
                                                        sd_distance_100,
                                                        d_distance_100,
                                                        mean_distance_100_pct_rnk
                                                    FROM
                                                        bn_250
                                                        LEFT JOIN bn_200 ON bn_250.osm_id = bn_200.osm_id
                                                        LEFT JOIN bn_100 ON bn_200.osm_id = bn_100.osm_id
                                                    );