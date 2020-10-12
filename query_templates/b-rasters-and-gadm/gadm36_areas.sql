CREATE TABLE {{db_schema}}.gadm36_areas AS (
                                    SELECT *,
                                        st_area(geom) * 1000000000 AS "area_sqm"
                                    FROM
                                        {{db_schema}}.gadm36 g;
);
