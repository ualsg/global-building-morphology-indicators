-- MATERIALIZED VIEW FOR DEBUGGING
-- DROP MATERIALIZED VIEW IF EXISTS {{gbmi_schema}}.buildings_by_{{raster_name}}_except_clipped_duplicates CASCADE;
-- DROP TABLE IF EXISTS {{gbmi_schema}}.buildings_by_{{raster_name}} CASCADE;


CREATE TABLE {{gbmi_schema}}.buildings_by_{{raster_name}} AS (
                                 WITH clipped AS (
                                                 SELECT
                                                    buildings."osm_id",
                                                    buildings."tags",
                                                    buildings."way",
                                                    buildings."way_centroid",
                                                    buildings."calc_way_area",
                                                    buildings."calc_perimeter",
                                                    buildings."calc_count_vertices",
                                                    buildings."convhull",
                                                    buildings."convhull_area",
                                                    buildings."mbr",
                                                    buildings."mbr_area",
                                                    buildings."oriented_mbr",
                                                    buildings."oriented_mbr_area",
                                                    buildings."is_residential",
                                                    buildings."addr:city",
                                                    buildings."addr:country",
                                                    buildings."o_height",
                                                    buildings."o_building:height",
                                                    buildings."height",
                                                    buildings."min_height",
                                                    buildings."building:min_height",
                                                    buildings."o_levels",
                                                    buildings."o_building:levels",
                                                    buildings."building:levels",
                                                    buildings."building:levels:underground",
                                                    buildings."min_level",
                                                    buildings."building:min_level",
                                                    buildings."building:condition",
                                                    buildings."building:material",
                                                    buildings."building:part",
                                                    buildings."building:use",
                                                    buildings."building:roof",
                                                    buildings."roof:shape",
                                                    buildings."building:roof:shape",
                                                    buildings."roof:levels",
                                                    buildings."building:roof:levels",
                                                    buildings."roof:material",
                                                    buildings."roof:orientation",
                                                    buildings."roof:height",
                                                    buildings."roof:angle",
                                                    buildings."roof:direction",
                                                    buildings."wall",
                                                    buildings."building:age",
                                                    buildings."start_date",
                                                    buildings."osm_uid",
                                                    buildings."osm_user",
                                                    buildings."osm_version",
                                                    cells.cell_id AS "cell_id",
                                                    cells.cell_centroid AS "cell_centroid",
                                                    cells.cell_geom AS "cell_geom",
                                                    cells.cell_area AS "cell_area",
                                                    cells.country AS "cell_country",
                                                    cells.admin_div1 AS "cell_admin_div1",
                                                    cells.admin_div2 AS "cell_admin_div2",
                                                    cells.admin_div3 AS "cell_admin_div3",
                                                    cells.admin_div4 AS "cell_admin_div4",
                                                    cells.admin_div5 AS "cell_admin_div5",{% if raster_population %}
                                                    cells.{{raster_population}} AS "cell_population",{% endif %}
                                                    cells.country_official_name AS "cell_country_official_name",
                                                    cells.country_code2 AS "cell_country_code2",
                                                    cells.country_code3 AS "cell_country_code3",
                                                    (ST_DUMP(ST_INTERSECTION(cells.cell_geom, buildings.way))).geom AS "clipped_way"
                                                 FROM
                                                    {{public_schema}}.cells_{{raster_name}} AS cells
                                                    INNER JOIN {{gbmi_schema}}.buildings ON ST_CONTAINS(cells.cell_geom::geometry, buildings.way_centroid::geometry)
                                                 )
                                 SELECT *,
                                     ST_AREA("clipped_way"::geography) AS "clipped_bldg_area",
                                     ST_PERIMETER("clipped_way"::geography) AS "clipped_bldg_perimeter"
                                 FROM
                                     clipped
                                 );


CREATE INDEX buildings_by_{{raster_name}}_osm_id ON {{gbmi_schema}}.buildings_by_{{raster_name}}(osm_id);

CREATE INDEX buildings_by_{{raster_name}}_centroid_spgist ON {{gbmi_schema}}.buildings_by_{{raster_name}} USING SPGIST (way_centroid);

CREATE INDEX buildings_by_{{raster_name}}_spgist ON {{gbmi_schema}}.buildings_by_{{raster_name}} USING SPGIST (way);

VACUUM ANALYZE {{gbmi_schema}}.buildings_by_{{raster_name}};

-- MATERIALIZED VIEW FOR DEBUGGING
DROP MATERIALIZED VIEW IF EXISTS {{gbmi_schema}}.buildings_by_{{raster_name}}_duplicates CASCADE;

CREATE MATERIALIZED VIEW {{gbmi_schema}}.buildings_by_{{raster_name}}_duplicates AS
    SELECT
        ({{gbmi_schema}}.buildings_by_{{raster_name}}.*)::text,
        count(*)
    FROM
        {{gbmi_schema}}.buildings_by_{{raster_name}}
    GROUP BY
        {{gbmi_schema}}.buildings_by_{{raster_name}}.*
    HAVING count(*) > 1;



/*
-- This is to troubleshoot whether joints and building data are created correctly

CREATE MATERIALIZED VIEW {{gbmi_schema}}.buildings_by_{{raster_name}}_except_clipped_duplicates AS
    WITH tbl AS (
            SELECT
                "osm_id", "tags", "way", "way_centroid", "calc_way_area", "calc_perimeter", "calc_count_vertices", "convhull", "convhull_area", "mbr", "mbr_area", "oriented_mbr", "oriented_mbr_area", "is_residential", "addr:city", "addr:country", "o_height", "o_building:height", "height", "min_height", "building:min_height", "o_levels", "o_building:levels", "building:levels", "building:levels:underground", "min_level", "building:min_level", "building:condition", "building:material", "building:part", "building:use", "building:roof", "roof:shape", "building:roof:shape", "roof:levels", "building:roof:levels", "roof:material", "roof:orientation", "roof:height", "roof:angle", "roof:direction", "wall", "building:age", "start_date", "osm_uid", "osm_user", "osm_version", "cell_id", "cell_centroid", "cell_geom", "cell_area", "cell_country", "cell_admin_div1", "cell_admin_div2", "cell_admin_div3", "cell_admin_div4", "cell_admin_div5", "cell_population", "cell_country_official_name", "cell_country_code2", "cell_country_code3"
            FROM
                {{gbmi_schema}}.buildings_by_{{raster_name}}
                )
    SELECT
        (tbl.*)::text,
        count(*)
    FROM
        tbl
    GROUP BY
        tbl.*
    HAVING count(*) > 1;

 */