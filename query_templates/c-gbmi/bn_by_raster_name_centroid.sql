-- MATERIALIZED VIEW FOR DEBUGGING
-- DROP MATERIALIZED VIEW IF EXISTS {{gbmi_schema}}.bn_by_{{raster_name}}_centroid_duplicates CASCADE;
-- DROP TABLE IF EXISTS {{gbmi_schema}}.bn_by_{{raster_name}}_centroid CASCADE;



CREATE TABLE {{gbmi_schema}}.bn_by_{{raster_name}}_centroid AS (
                                    SELECT DISTINCT
                                        bldg1."osm_id" AS osm_id1,
                                        bldg1."way" AS way1,
                                        bldg1."way_centroid" AS way_centroid1,
                                        bldg1."calc_way_area" AS way_area1,
                                        bldg1."height" AS height1,
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
                                        bldg2."calc_way_area" AS way_area2,
                                        bldg2."height" AS height2,
                                        st_distance(bldg1.way_centroid, bldg2.way_centroid) AS distance
                                    FROM
                                        {{gbmi_schema}}.buildings_by_{{raster_name}} AS bldg1
                                                         CROSS JOIN (
                                                                    SELECT DISTINCT osm_id, way, way_centroid, calc_way_area, height
                                                                    FROM {{gbmi_schema}}.buildings_by_{{raster_name}}
                                                                    ) AS bldg2
                                    WHERE ST_DWITHIN(bldg1.way_centroid, bldg2.way_centroid, {% if limit_buffer %}{{limit_buffer}}{% else %}100{% endif %}) AND NOT ST_Equals(bldg1."way"::geometry, bldg2."way"::geometry)
                                    );




CREATE INDEX bn_by_{{raster_name}}_centroid_osm_id ON {{gbmi_schema}}.bn_by_{{raster_name}}_centroid(osm_id1);

CREATE INDEX bn_by_{{raster_name}}_centroid_centroid_spgist ON {{gbmi_schema}}.bn_by_{{raster_name}}_centroid USING SPGIST (way_centroid1);

CREATE INDEX bn_by_{{raster_name}}_centroid_spgist ON {{gbmi_schema}}.bn_by_{{raster_name}}_centroid USING SPGIST (way1);

VACUUM ANALYZE {{gbmi_schema}}.bn_by_{{raster_name}}_centroid;



/*
-- This is to troubleshoot whether joints and building data are created correctly

CREATE MATERIALIZED VIEW {{gbmi_schema}}.bn_by_{{raster_name}}_centroid_duplicates AS
    SELECT
        ({{gbmi_schema}}.bn_by_{{raster_name}}_centroid.*)::text,
        count(*)
    FROM
        {{gbmi_schema}}.bn_by_{{raster_name}}_centroid
    GROUP BY
        {{gbmi_schema}}.bn_by_{{raster_name}}_centroid.*
    HAVING count(*) > 1;

 */