DROP TABLE IF EXISTS {{gbmi_schema}}.buildings_by_{{raster_name}} CASCADE;


CREATE TABLE {{gbmi_schema}}.buildings_by_{{raster_name}} AS (
                                 WITH clipped AS (
                                                 SELECT
                                                    buildings."osm_id",
                                                    buildings."tags",
                                                    buildings."way",
                                                    buildings."way_centroid",
                                                    buildings."calc_way_area" AS "cmplt_bldg_area",
                                                    buildings."calc_perimeter" AS "cmplt_bldg_perimeter",
                                                    buildings."calc_count_vertices",
                                                    buildings."addr:city",
                                                    buildings."addr:country",
                                                    buildings."height",
                                                    buildings."building:height",
                                                    buildings."min_height",
                                                    buildings."building:min_height",
                                                    buildings."levels",
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
                                                    cells.cell_id AS cell_id,
                                                    cells.area AS cell_area,
                                                    cells.admin_div2 AS cell_admin_div2,
                                                    cells.admin_div1 AS cell_admin_div1,
                                                    cells.country AS cell_country,
                                                    cells.country_official_name AS cell_country_official_name,
                                                    cells.country_code2 AS cell_country_code2,
                                                    cells.country_code3 AS cell_country_code3,
                                                    cells.raster_population AS cell_population,
                                                    cells.centroid_geom AS cell_centroid,
                                                    cells.raster_geom AS cell_geom,
                                                    (ST_DUMP(ST_INTERSECTION(cells.raster_geom, buildings.way))).geom AS clipped_way
                                                 FROM
                                                    {{db_schema}}.cells_{{raster_name}} AS cells
                                                    INNER JOIN {{gbmi_schema}}.buildings ON ST_INTERSECTS(cells.raster_geom, buildings.way)
                                                 )
                                 SELECT *,
                                     ST_AREA(clipped_way::geography) AS clipped_bldg_area,
                                     ST_PERIMETER(clipped_way::geography) AS clipped_bldg_perimeter
                                 FROM
                                     clipped
                                 );


CREATE INDEX buildings_by_{{raster_name}}_osm_id ON {{gbmi_schema}}.buildings_by_{{raster_name}}(osm_id);


VACUUM ANALYZE {{gbmi_schema}}.buildings_by_{{raster_name}};

