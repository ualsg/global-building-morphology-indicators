DROP TABLE IF EXISTS {{gbmi_schema}}.buildings_neighbours_by_{{raster_name}} CASCADE;

CREATE TABLE {{gbmi_schema}}.buildings_neighbours_by_{{raster_name}} AS (
                                    SELECT
                                        bldg1."osm_id" AS osm_id1,
                                        bldg1."way" AS way1,
                                        bldg1."way_centroid" AS way_centroid1,
                                        bldg1."cell_id",
                                        bldg1."cell_centroid",
                                        bldg1."cell_geom",
                                        bldg1."cell_area",
                                        bldg1."cell_country",
                                        bldg1."cell_admin_div1",
                                        bldg1."cell_admin_div2",
                                        bldg1."cell_admin_div3",
                                        bldg1."cell_admin_div4",
                                        bldg1."cell_admin_div5",{% if raster_population %}
                                        bldg1.{{raster_population}} AS "cell_population",{% endif %}
                                        bldg1."cell_country_official_name",
                                        bldg1."cell_country_code2",
                                        bldg1."cell_country_code3",
                                        bldg2."osm_id" AS osm_id2,
                                        bldg2."way" AS way2,
                                        bldg2."way_centroid" AS way_centroid2,
                                        st_distance(bldg1.way_centroid, bldg2.way_centroid) AS distance
                                    FROM
                                        {{gbmi_schema}}.buildings_by_{{raster_name}} AS bldg1
                                                         CROSS JOIN (
                                                                    SELECT osm_id, way, way_centroid
                                                                    FROM {{gbmi_schema}}.buildings_by_{{raster_name}}
                                                                    ) AS bldg2
                                    WHERE (NOT bldg1."way" = bldg2."way") AND ST_DWITHIN(bldg1.way_centroid, bldg2.way_centroid, 100)
                                    ORDER BY cell_country, cell_admin_div1, cell_admin_div2, cell_admin_div3, cell_admin_div4, cell_admin_div5, osm_id1, osm_id2, st_distance(bldg1.way_centroid, bldg2.way_centroid)
                                    );




CREATE INDEX buildings_neighbours_by_{{raster_name}}_osm_id ON {{gbmi_schema}}.buildings_neighbours_by_{{raster_name}}(osm_id1);

CREATE INDEX buildings_neighbours_by_{{raster_name}}_centroid_spgist ON {{gbmi_schema}}.buildings_neighbours_by_{{raster_name}} USING SPGIST (way_centroid1);

CREATE INDEX buildings_neighbours_by_{{raster_name}}_spgist ON {{gbmi_schema}}.buildings_neighbours_by_{{raster_name}} USING SPGIST (way1);

VACUUM ANALYZE {{gbmi_schema}}.buildings_neighbours_by_{{raster_name}};