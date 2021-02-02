DROP TABLE IF EXISTS {{db_schema}}.agg_gadm36_geom_areas_{{agg_level}} CASCADE;

CREATE TABLE {{db_schema}}.agg_gadm36_geom_areas_{{agg_level}} AS (
                                                        SELECT
                                                            {{agg_columns}},
                                                            st_multi(st_memunion({{agg_geom}}))::geometry(MultiPolygon, 4326) AS "agg_geom_{{agg_level}}",
                                                            sum(st_area({{agg_geom}}::geography)) AS "agg_area_{{agg_level}}"
                                                        FROM
                                                            public.{{source_table}}
                                                        WHERE ST_IsValid({{agg_geom}}) = TRUE
                                                        GROUP BY
                                                            {{order_columns}}
                                                        ORDER BY
                                                            {{order_columns}});
