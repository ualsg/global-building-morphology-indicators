DROP TABLE IF EXISTS {{db_schema}}.gadm36_areas CASCADE;


CREATE TABLE {{db_schema}}.gadm36_areas AS (
                                    SELECT *,
                                        st_area(geom::geography) AS "area_sqm"
                                    FROM
                                        {{db_schema}}.gadm36 g
);
