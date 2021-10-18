DROP TABLE IF EXISTS {{misc_schema}}.osm_polygon_attr_value_freqs CASCADE;


CREATE TABLE {{misc_schema}}.osm_polygon_attr_value_freqs AS (
                                            WITH tag_key_values AS (
                                                                   SELECT osm_id, (each(tags)).key AS "attr", (each(tags)).value AS "value"
                                                                   FROM {{public_schema}}.planet_osm_polygon pop)
                                            SELECT
                                                "attr",
                                                "value",
                                                count(osm_id) AS freq
                                            FROM tag_key_values
                                            GROUP BY attr, value
                                            ORDER BY attr, freq DESC, value
                                            );