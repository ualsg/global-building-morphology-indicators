DROP TABLE IF EXISTS {{gbmi_schema}}.buildings_geom_attributes_by_{{raster_name}} CASCADE;


CREATE TABLE {{gbmi_schema}}.buildings_geom_attributes_by_{{raster_name}} AS (
                                 WITH clipped AS (
                                                 SELECT
                                                    bga.*,
                                                    cells.cell_id AS cell_id,
                                                    cells.area AS cell_area,
                                                    cells.admin_div2 AS cell_admin_div2,
                                                    cells.admin_div1 AS cell_admin_div1,
                                                    cells.country AS cell_country,
                                                    cells.country_official_name AS cell_country_official_name,
                                                    cells.country_code2 AS cell_country_code2,
                                                    cells.country_code3 AS cell_country_code3,{% if raster_population  %}
                                                    cells.{{raster_population}} AS cell_population,{% endif %}
                                                    cells.centroid_geom AS cell_centroid,
                                                    cells.raster_geom AS cell_geom,
                                                    (ST_DUMP(ST_INTERSECTION(cells.raster_geom, bga.way))).geom AS clipped_way
                                                 FROM
                                                    {{db_schema}}.cells_{{raster_name}} AS cells
                                                    INNER JOIN {{gbmi_schema}}.buildings_geom_attributes bga ON ST_INTERSECTS(cells.raster_geom, bga.way)
                                                 )
                                 SELECT *,
                                     ST_AREA(clipped_way::geography) AS clipped_bldg_area,
                                     ST_PERIMETER(clipped_way::geography) AS clipped_bldg_perimeter
                                 FROM
                                     clipped
                                 );

CREATE INDEX buildings_geom_attributes_by_{{raster_name}}_osm_id ON {{gbmi_schema}}.buildings_geom_attributes_by_{{raster_name}}(osm_id);


VACUUM ANALYZE {{gbmi_schema}}.buildings_geom_attributes_by_{{raster_name}};

