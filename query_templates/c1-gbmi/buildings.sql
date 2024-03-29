-- MATERIALIZED VIEW FOR DEBUGGING
-- DROP MATERIALIZED VIEW IF EXISTS {{gbmi_schema}}.buildings_duplicates CASCADE;
-- DROP TABLE IF EXISTS {{gbmi_schema}}.buildings CASCADE;


CREATE TABLE {{gbmi_schema}}.buildings AS (
                          SELECT
                              "osm_id",
                              "tags",
                              "way",
                              st_centroid(way::geography) AS "way_centroid",
                              st_area(way::geography) AS "calc_way_area",
                              st_perimeter(way::geography) AS "calc_perimeter",
                              st_npoints(way) AS "calc_count_vertices",
                              st_convexhull(way) AS convhull,
                              st_area(st_convexhull(way)::geography) AS convhull_area,
                              st_envelope(way) AS mbr,
                              st_area(st_envelope(way)::geography) AS mbr_area,
                              st_orientedenvelope(way) AS oriented_mbr,
                              st_area(st_orientedenvelope(way)::geography) AS oriented_mbr_area,
                              CASE
                                  WHEN lower(tags -> 'building') = 'residential' THEN TRUE
                                  ELSE FALSE
                              END AS "is_residential",
                              tags -> 'addr:city' AS "addr:city",
                              tags -> 'addr:country' AS "addr:country",
                              tags -> 'height' AS "o_height",
                              tags -> 'building:height' AS "o_building:height",
                              COALESCE(SUBSTRING(tags -> 'height' FROM '\d+\.?\d*'),
                                       SUBSTRING(tags -> 'building:height' FROM '\d+\.?\d*'))::numeric AS "height",
                              tags -> 'min_height' AS "min_height",
                              tags -> 'building:min_height' AS "building:min_height",
                              tags -> 'levels' AS "o_levels",
                              tags -> 'building:levels' AS "o_building:levels",
                              COALESCE(SUBSTRING(tags -> 'building:levels' FROM '\d+\.?\d*'),
                                       SUBSTRING(tags -> 'levels' FROM '\d+\.?\d*'))::numeric AS "building:levels",
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
                              SUBSTRING(tags -> 'start_date' FROM '\d{4}')::SMALLINT AS "start_date",
                              tags -> 'osm_uid' AS osm_uid,
                              tags -> 'osm_user' AS osm_user,
                              tags -> 'osm_version' AS osm_version
                          FROM
                              {{public_schema}}.planet_osm_polygon pop
                          WHERE building IS NOT NULL OR tags->'building:part' IS NOT NULL OR tags->'type'='building'
                          );


CREATE INDEX buildings_osm_id ON {{gbmi_schema}}.buildings(osm_id);

CREATE INDEX buildings_spgist ON {{gbmi_schema}}.buildings USING spgist(way);

CREATE INDEX buildings_centroid_spgist ON {{gbmi_schema}}.buildings USING spgist(way_centroid);


VACUUM ANALYZE {{gbmi_schema}}.buildings;



/*
-- This is to troubleshoot whether joints and building data are created correctly

CREATE MATERIALIZED VIEW {{gbmi_schema}}.buildings_duplicates AS
    SELECT
        ({{gbmi_schema}}.buildings.*)::text,
        count(*)
    FROM
        {{gbmi_schema}}.buildings
    GROUP BY
        {{gbmi_schema}}.buildings.*
    HAVING count(*) > 1;

 */