DROP TABLE IF EXISTS {{gbmi_schema}}.buildings_neighbors CASCADE;

CREATE TABLE {{gbmi_schema}}.buildings_neighbors AS (
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
                                    ORDER BY osm_id1, osm_id2, st_distance(bldg1.way_centroid, bldg2.way_centroid)
                                    );