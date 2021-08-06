DROP TABLE IF EXISTS {{misc_schema}}.osm_polygon_attr_freqs;


CREATE TABLE {{misc_schema}}.osm_polygon_attr_freqs AS (
                                            (
                                            SELECT
                                                'abandoned:building' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'abandoned:building'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'access' AS "attr",
                                                "access" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "access"
                                            ORDER BY freq DESC, "access"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'admin_level' AS "attr",
                                                "admin_level" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "admin_level"
                                            ORDER BY freq DESC, "admin_level"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'aerialway' AS "attr",
                                                "aerialway" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "aerialway"
                                            ORDER BY freq DESC, "aerialway"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'aeroway' AS "attr",
                                                "aeroway" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "aeroway"
                                            ORDER BY freq DESC, "aeroway"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'amenity' AS "attr",
                                                "amenity" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "amenity"
                                            ORDER BY freq DESC, "amenity"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'architect' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'architect'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'area' AS "attr",
                                                "area",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "area"
                                            ORDER BY freq DESC, "area"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'barrier' AS "attr",
                                                "barrier" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "barrier"
                                            ORDER BY freq DESC, "barrier"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'bicycle' AS "attr",
                                                "bicycle" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "bicycle"
                                            ORDER BY freq DESC, "bicycle"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'brand' AS "attr",
                                                "brand" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "brand"
                                            ORDER BY freq DESC, "brand"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'bridge' AS "attr",
                                                "bridge" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "bridge"
                                            ORDER BY freq DESC, "bridge"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'boundary' AS "attr",
                                                "boundary" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "boundary"
                                            ORDER BY freq DESC, "boundary"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'building' AS "attr",
                                                "building" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "building"
                                            ORDER BY freq DESC, "building"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'building:age' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'building:age'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'building:colour' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'building:colour'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'building:condition' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'building:condition'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'building:entrances' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'building:entrances'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'building:flats' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'building:flats'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'building:height' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'building:height'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'building:levels' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'building:levels'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'building:levels:underground' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'building:levels:underground'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'building:material' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'building:material'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'building:min_level' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'building:min_level'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'building:part' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'building:part'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'building:roof' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'building:roof'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'building:roof:shape' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'building:roof:shape'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'building:structure' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'building:structure'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'building:units' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'building:units'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'building:use' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'building:use'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'construction' AS "attr",
                                                "construction" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "construction"
                                            ORDER BY freq DESC, "construction"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'covered' AS "attr",
                                                "covered" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "covered"
                                            ORDER BY freq DESC, "covered"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'culvert' AS "attr",
                                                "culvert" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "culvert"
                                            ORDER BY freq DESC, "culvert"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'cutting' AS "attr",
                                                "cutting" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "cutting"
                                            ORDER BY freq DESC, "cutting"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'denomination' AS "attr",
                                                "denomination" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "denomination"
                                            ORDER BY freq DESC, "denomination"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'disused' AS "attr",
                                                "disused" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "disused"
                                            ORDER BY freq DESC, "disused"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'embankment' AS "attr",
                                                "embankment" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "embankment"
                                            ORDER BY freq DESC, "embankment"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'foot' AS "attr",
                                                "foot",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "foot"
                                            ORDER BY freq DESC, "foot"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'generator:source' AS "attr",
                                                "generator:source" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "generator:source"
                                            ORDER BY freq DESC, "generator:source"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'harbour' AS "attr",
                                                "harbour" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "harbour"
                                            ORDER BY freq DESC, "harbour"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'height' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'height'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'highway' AS "attr",
                                                "highway" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "highway"
                                            ORDER BY freq DESC, "highway"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'historic' AS "attr",
                                                "historic" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "historic"
                                            ORDER BY freq DESC, "historic"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'horse' AS "attr",
                                                "horse" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "horse"
                                            ORDER BY freq DESC, "horse"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'intermittent' AS "attr",
                                                "intermittent" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "intermittent"
                                            ORDER BY freq DESC, "intermittent"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'junction' AS "attr",
                                                "junction" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "junction"
                                            ORDER BY freq DESC, "junction"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'landuse' AS "attr",
                                                "landuse" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "landuse"
                                            ORDER BY freq DESC, "landuse"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'layer' AS "attr",
                                                "layer" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "layer"
                                            ORDER BY freq DESC, "layer"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'leisure' AS "attr",
                                                "leisure" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "leisure"
                                            ORDER BY freq DESC, "leisure"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'levels' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'levels'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'lock' AS "attr",
                                                "lock",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "lock"
                                            ORDER BY freq DESC, "lock"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'man_made' AS "attr",
                                                "man_made" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "man_made"
                                            ORDER BY freq DESC, "man_made"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'military' AS "attr",
                                                "military" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "military"
                                            ORDER BY freq DESC, "military"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'motorcar' AS "attr",
                                                "motorcar" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "motorcar"
                                            ORDER BY freq DESC, "motorcar"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'name' AS "attr",
                                                "name",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "name"
                                            ORDER BY freq DESC, "name"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'natural' AS "attr",
                                                "natural" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "natural"
                                            ORDER BY freq DESC, "natural"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'office' AS "attr",
                                                "office" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "office"
                                            ORDER BY freq DESC, "office"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'oneway' AS "attr",
                                                "oneway" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "oneway"
                                            ORDER BY freq DESC, "oneway"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'operator' AS "attr",
                                                "operator" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "operator"
                                            ORDER BY freq DESC, "operator"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'place' AS "attr",
                                                "place" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "place"
                                            ORDER BY freq DESC, "place"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'population' AS "attr",
                                                "population" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "population"
                                            ORDER BY freq DESC, "population"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'power' AS "attr",
                                                "power" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "power"
                                            ORDER BY freq DESC, "power"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'power_source' AS "attr",
                                                "power_source" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "power_source"
                                            ORDER BY freq DESC, "power_source"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'public_transport' AS "attr",
                                                "public_transport" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "public_transport"
                                            ORDER BY freq DESC, "public_transport"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'railway' AS "attr",
                                                "railway" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "railway"
                                            ORDER BY freq DESC, "railway"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'ref' AS "attr",
                                                "ref",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "ref"
                                            ORDER BY freq DESC, "ref"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'religion' AS "attr",
                                                "religion" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "religion"
                                            ORDER BY freq DESC, "religion"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'roof:angle' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'roof:angle'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'roof:colour' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'roof:colour'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'roof:direction' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'roof:direction'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'roof:height' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'roof:height'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'roof:levels' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'roof:levels'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'roof:material' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'roof:material'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'roof:orientation' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'roof:orientation'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'roof:shape' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'roof:shape'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'route' AS "attr",
                                                "route" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "route"
                                            ORDER BY freq DESC, "route"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'service' AS "attr",
                                                "service" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "service"
                                            ORDER BY freq DESC, "service"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'shop' AS "attr",
                                                "shop",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "shop"
                                            ORDER BY freq DESC, "shop"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'source' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'source'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'source:date' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'source:date'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'sport' AS "attr",
                                                "sport" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "sport"
                                            ORDER BY freq DESC, "sport"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'start_date' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'start_date'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'surface' AS "attr",
                                                "surface" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "surface"
                                            ORDER BY freq DESC, "surface"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'toll' AS "attr",
                                                "toll",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "toll"
                                            ORDER BY freq DESC, "toll"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'tourism' AS "attr",
                                                "tourism" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "tourism"
                                            ORDER BY freq DESC, "tourism"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'tower:type' AS "attr",
                                                "tower:type" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "tower:type"
                                            ORDER BY freq DESC, "tower:type"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'type' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'type'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'tracktype' AS "attr",
                                                "tracktype",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "tracktype"
                                            ORDER BY freq DESC, "tracktype"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'tunnel' AS "attr",
                                                "tunnel" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "tunnel"
                                            ORDER BY freq DESC, "tunnel"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'wall' AS "attr",
                                                value AS "value",
                                                count(*) AS freq
                                            FROM
                                                (
                                                SELECT osm_id, (each(tags)).key, (each(tags)).value
                                                FROM {{public_schema}}.planet_osm_polygon pop
                                                ) AS stat
                                            WHERE key = 'wall'
                                            GROUP BY value
                                            ORDER BY freq DESC, value
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'water' AS "attr",
                                                "water" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "water"
                                            ORDER BY freq DESC, "water"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'waterway' AS "attr",
                                                "waterway" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "waterway"
                                            ORDER BY freq DESC, "waterway"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'wetland' AS "attr",
                                                "wetland" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "wetland"
                                            ORDER BY freq DESC, "wetland"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'width' AS "attr",
                                                "width" AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "width"
                                            ORDER BY freq DESC, "width"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'wood' AS "attr",
                                                "wood",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY "wood"
                                            ORDER BY freq DESC, "wood"
                                            )
                                            UNION ALL
                                            (
                                            SELECT
                                                'z_order' AS "attr",
                                                CAST("z_order" AS text) AS "value",
                                                count(*) AS freq
                                            FROM
                                                {{public_schema}}.planet_osm_polygon pop
                                            GROUP BY CAST("z_order" AS text)
                                            ORDER BY freq DESC, CAST("z_order" AS text)
                                            )
                                            );