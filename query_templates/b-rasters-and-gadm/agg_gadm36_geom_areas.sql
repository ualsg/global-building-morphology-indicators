CREATE TABLE {{db_schema}}.agg_gadm36_geom_areas AS (
                                            WITH agg_admin_div2 AS (
                                                                    SELECT
                                                                        name_0 AS country,
                                                                        name_1 AS admin_div1,
                                                                        name_2 AS admin_div2,
                                                                        st_union(geom) AS "agg_geom_admin_div2",
                                                                        sum(st_area(geom::geography)) AS "agg_area_admin_div2"
                                                                    FROM
                                                                        {{db_schema}}.gadm36
                                                                    GROUP BY name_0, name_1, name_2
                                                                    ORDER BY name_0, name_1, name_2
                                                                    ),
                                                agg_admin_div1 AS (
                                                                   SELECT
                                                                       name_0 AS country,
                                                                       name_1 AS admin_div1,
                                                                       st_union(geom) AS "agg_geom_admin_div1",
                                                                       sum(st_area(geom::geography))  AS "agg_area_admin_div1"
                                                                   FROM
                                                                       {{db_schema}}.gadm36
                                                                   GROUP BY name_0, name_1
                                                                   ORDER BY name_0, name_1
                                                                   ),
                                                agg_admin_div0 AS (
                                                                   SELECT
                                                                       name_0 AS country,
                                                                       st_union(geom) AS "agg_geom_country",
                                                                       sum(st_area(geom::geography))  AS "agg_area_country"
                                                                   FROM
                                                                       {{db_schema}}.gadm36
                                                                   GROUP BY name_0
                                                                   ORDER BY name_0
                                                                   )
                                            SELECT
                                                agl2.*,
                                                agl1.agg_geom_admin_div1,
                                                agl1.agg_area_admin_div1,
                                                agl0.agg_geom_country,
                                                agl0.agg_area_country
                                            FROM
                                                agg_admin_div2 aad2
                                                LEFT JOIN agg_admin_div1 aad1
                                                ON aad2.country = aad1.country AND aad2.admin_div1 = aad1.admin_div1
                                                LEFT JOIN agg_admin_div0 aad0 ON aad1.country = aad0.country
                                            );
