CREATE TABLE {{gbmi_schema}}.buildings AS (
                          SELECT
                              "osm_id",
                              "tags",
                              "way",
                              st_centroid(way::geography) AS "way_centroid",
                              st_area(way::geography) AS "calc_way_area",
                              st_perimeter(way::geography) AS "calc_perimeter",
                              st_npoints(way) AS "calc_count_vertices",
                              tags -> 'addr:city' AS "addr:city",
                              tags -> 'addr:country' AS "addr:country",
                              tags -> 'height' AS "height",
                              tags -> 'building:height' AS "building:height",
                              tags -> 'min_height' AS "min_height",
                              tags -> 'building:min_height' AS "building:min_height",
                              tags -> 'levels' AS "levels",
                              tags -> 'building:levels' AS "building:levels",
                              tags -> 'building:levels:underground' AS "building:levels:underground",
                              tags -> 'min_level' AS "min_level",
                              tags -> 'building:min_level' AS "building:min_level",
                              tags -> 'building:condition' AS "building:condition",
                              tags -> 'building:material' AS "building:material",
                              tags -> 'building:part' AS "building:part",
                              tags -> 'building:use' AS "building:use",
                              tags -> 'building:roof' AS "building:roof",
                              tags -> 'roof:shape' AS "roof:shape",
                              tags -> 'building:roof:shape' AS "building:roof:shape",
                              tags -> 'roof:levels' AS "roof:levels",
                              tags -> 'building:roof:levels' AS "building:roof:levels",
                              tags -> 'roof:material' AS "roof:material",
                              tags -> 'roof:orientation' AS "roof:orientation",
                              tags -> 'roof:height' AS "roof:height",
                              tags -> 'roof:angle' AS "roof:angle",
                              tags -> 'roof:direction' AS "roof:direction",
                              tags -> 'wall' AS wall,
                              tags -> 'building:age' AS "building:age",
                              tags -> 'start_date' AS "start_date",
                              tags -> 'osm_uid' AS osm_uid,
                              tags -> 'osm_user' AS osm_user,
                              tags -> 'osm_version' AS osm_version
                          FROM
                              {{gbmi_schema}}.planet_osm_polygon pop
                          WHERE building IS NOT NULL AND lower(building) NOT IN ('no', 'not', 'non', 'none')
                          );


CREATE INDEX buildings_gidx ON {{gbmi_schema}}.buildings USING gist(way);

VACUUM ANALYZE {{gbmi_schema}}.buildings;