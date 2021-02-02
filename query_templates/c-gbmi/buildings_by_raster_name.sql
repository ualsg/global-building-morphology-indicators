DROP TABLE IF EXISTS {{gbmi_schema}}.buildings_by_{{raster_name}} CASCADE;


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
                                                    buildings."year_of_construction",
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
                                                    {{db_schema}}.cells_{{raster_name}} AS cells
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

