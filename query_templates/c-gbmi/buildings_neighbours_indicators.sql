CREATE TABLE {{gbmi_schema}}.buildings_neighbours_indicators AS (
                                                    WITH bn_25 AS (
                                                                   SELECT *,
                                                                       CASE
                                                                           WHEN distance_25_mean IS NOT NULL
                                                                               THEN percent_rank() OVER (ORDER BY distance_25_mean)
                                                                       END AS distance_25_mean_pct_rnk
                                                                   FROM
                                                                       {{gbmi_schema}}.buildings_neighbours_25
                                                                   ),
                                                        bn_50 AS (
                                                                  SELECT *,
                                                                      CASE
                                                                          WHEN distance_50_mean IS NOT NULL
                                                                              THEN percent_rank() OVER (ORDER BY distance_50_mean)
                                                                      END AS distance_50_mean_pct_rnk
                                                                  FROM
                                                                      {{gbmi_schema}}.buildings_neighbours_50
                                                                  ),
                                                        bn_100 AS (
                                                                  SELECT *,
                                                                      CASE
                                                                          WHEN distance_100_mean IS NOT NULL
                                                                              THEN percent_rank() OVER (ORDER BY distance_100_mean)
                                                                      END AS distance_100_mean_pct_rnk
                                                                  FROM
                                                                      {{gbmi_schema}}.buildings_neighbours_100
                                                                  )
                                                    SELECT
                                                        bn_100.osm_id AS osm_id3,
                                                        neighbour_100_count,
                                                        distance_100_min,
                                                        distance_100_median,
                                                        distance_100_max,
                                                        distance_100_mean,
                                                        distance_100_sd,
                                                        distance_100_d,
                                                        distance_100_mean_pct_rnk,
                                                        bn_50.osm_id AS osm_id2,
                                                        neighbour_50_count,
                                                        distance_50_min,
                                                        distance_50_median,
                                                        distance_50_max,
                                                        distance_50_mean,
                                                        distance_50_sd,
                                                        distance_50_d,
                                                        distance_50_mean_pct_rnk,
                                                        bn_25.osm_id AS osm_id1,
                                                        neighbour_25_count,
                                                        distance_25_min,
                                                        distance_25_median,
                                                        distance_25_max,
                                                        distance_25_mean,
                                                        distance_25_sd,
                                                        distance_25_d,
                                                        distance_25_mean_pct_rnk
                                                    FROM
                                                        bn_100
                                                        LEFT JOIN bn_50 ON bn_100.osm_id = bn_50.osm_id
                                                        LEFT JOIN bn_25 ON bn_50.osm_id = bn_25.osm_id
                                                    );