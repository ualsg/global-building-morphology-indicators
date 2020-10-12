ALTER TABLE {{gbmi_schema}}.buildings_geom_attributes_by_{{raster_name}} RENAME TO bga_by_{{raster_name}};

ALTER TABLE {{gbmi_schema}}.buildings_geom_indicators_by_{{raster_name}} RENAME TO bgi_by_{{raster_name}};
CREATE INDEX bgi_by_{{raster_name}}_osm_id ON {{gbmi_schema}}.bgi_by_{{raster_name}}(osm_id);
CREATE INDEX bgi_by_{{raster_name}}_centroid_spgist ON {{gbmi_schema}}.bgi_by_{{raster_name}} USING SPGIST (way_centroid);
CREATE INDEX bgi_by_{{raster_name}}_spgist ON {{gbmi_schema}}.bgi_by_{{raster_name}} USING SPGIST (way);
VACUUM ANALYZE {{gbmi_schema}}.bgi_by_{{raster_name}};
CREATE MATERIALIZED VIEW {{gbmi_schema}}.bgi_by_{{raster_name}}_duplicates AS
    SELECT
        ({{gbmi_schema}}.bgi_by_{{raster_name}}.*)::text,
        count(*)
    FROM
        {{gbmi_schema}}.bgi_by_{{raster_name}}
    GROUP BY
        {{gbmi_schema}}.bgi_by_{{raster_name}}.*
    HAVING count(*) > 1;


ALTER TABLE {{gbmi_schema}}.buildings_geom_indicators_clipped_by_{{raster_name}} RENAME TO bgi_clipped_by_{{raster_name}};
CREATE INDEX bgi_clipped_by_{{raster_name}}_osm_id ON {{gbmi_schema}}.bgi_clipped_by_{{raster_name}}(osm_id);
CREATE INDEX bgi_clipped_by_{{raster_name}}_centroid_spgist ON {{gbmi_schema}}.bgi_clipped_by_{{raster_name}} USING SPGIST (way_centroid);
CREATE INDEX bgi_clipped_by_{{raster_name}}_spgist ON {{gbmi_schema}}.bgi_clipped_by_{{raster_name}} USING SPGIST (way);
VACUUM ANALYZE {{gbmi_schema}}.bgi_clipped_by_{{raster_name}};
CREATE MATERIALIZED VIEW {{gbmi_schema}}.bgi_clipped_by_{{raster_name}}_duplicates AS
    SELECT
        ({{gbmi_schema}}.bgi_clipped_by_{{raster_name}}.*)::text,
        count(*)
    FROM
        {{gbmi_schema}}.bgi_clipped_by_{{raster_name}}
    GROUP BY
        {{gbmi_schema}}.bgi_clipped_by_{{raster_name}}.*
    HAVING count(*) > 1;

ALTER TABLE {{gbmi_schema}}.buildings_neighbours_by_{{raster_name}} RENAME TO bn_by_{{raster_name}};
CREATE INDEX bn_by_{{raster_name}}_osm_id ON {{gbmi_schema}}.bn_by_{{raster_name}}(osm_id1);
CREATE INDEX bn_by_{{raster_name}}_centroid_spgist ON {{gbmi_schema}}.bn_by_{{raster_name}} USING SPGIST (way_centroid1);
CREATE INDEX bn_by_{{raster_name}}_spgist ON {{gbmi_schema}}.bn_by_{{raster_name}} USING SPGIST (way1);
VACUUM ANALYZE {{gbmi_schema}}.bn_by_{{raster_name}};
CREATE MATERIALIZED VIEW {{gbmi_schema}}.bn_by_{{raster_name}}_duplicates AS
    SELECT
        ({{gbmi_schema}}.bn_by_{{raster_name}}.*)::text,
        count(*)
    FROM
        {{gbmi_schema}}.bn_by_{{raster_name}}
    GROUP BY
        {{gbmi_schema}}.bn_by_{{raster_name}}.*
    HAVING count(*) > 1;


ALTER TABLE {{gbmi_schema}}.buildings_neighbours_100_by_{{raster_name}} RENAME TO bn_100_by_{{raster_name}};
CREATE INDEX bn_100_by_{{raster_name}}_osm_id ON {{gbmi_schema}}.bn_100_by_{{raster_name}}(osm_id);
CREATE INDEX bn_100_by_{{raster_name}}_centroid_spgist ON {{gbmi_schema}}.bn_100_by_{{raster_name}} USING spgist(way_centroid);
CREATE INDEX bn_100_by_{{raster_name}}_spgist ON {{gbmi_schema}}.bn_100_by_{{raster_name}} USING spgist(way);
VACUUM ANALYZE {{gbmi_schema}}.bn_100_by_{{raster_name}};
CREATE MATERIALIZED VIEW {{gbmi_schema}}.bn_100_by_{{raster_name}}_duplicates AS
    SELECT
        ({{gbmi_schema}}.bn_100_by_{{raster_name}}.*)::text,
        count(*)
    FROM
        {{gbmi_schema}}.bn_100_by_{{raster_name}}
    GROUP BY
        {{gbmi_schema}}.bn_100_by_{{raster_name}}.*
    HAVING count(*) > 1;

ALTER TABLE {{gbmi_schema}}.buildings_neighbours_50_by_{{raster_name}} RENAME TO bn_50_by_{{raster_name}};
CREATE INDEX bn_50_by_{{raster_name}}_osm_id ON {{gbmi_schema}}.bn_50_by_{{raster_name}}(osm_id);
CREATE INDEX bn_50_by_{{raster_name}}_centroid_spgist ON {{gbmi_schema}}.bn_50_by_{{raster_name}} USING spgist(way_centroid);
CREATE INDEX bn_50_by_{{raster_name}}_spgist ON {{gbmi_schema}}.bn_50_by_{{raster_name}} USING spgist(way);
VACUUM ANALYZE {{gbmi_schema}}.bn_50_by_{{raster_name}};
CREATE MATERIALIZED VIEW {{gbmi_schema}}.bn_50_by_{{raster_name}}_duplicates AS
    SELECT
        ({{gbmi_schema}}.bn_50_by_{{raster_name}}.*)::text,
        count(*)
    FROM
        {{gbmi_schema}}.bn_50_by_{{raster_name}}
    GROUP BY
        {{gbmi_schema}}.bn_50_by_{{raster_name}}.*
    HAVING count(*) > 1;


ALTER TABLE {{gbmi_schema}}.buildings_neighbours_25_by_{{raster_name}} RENAME TO bn_25_by_{{raster_name}};
CREATE INDEX bn_25_by_{{raster_name}}_osm_id ON {{gbmi_schema}}.bn_25_by_{{raster_name}}(osm_id);
CREATE INDEX bn_25_by_{{raster_name}}_centroid_spgist ON {{gbmi_schema}}.bn_25_by_{{raster_name}} USING spgist(way_centroid);
CREATE INDEX bn_25_by_{{raster_name}}_spgist ON {{gbmi_schema}}.bn_25_by_{{raster_name}} USING spgist(way);
VACUUM ANALYZE {{gbmi_schema}}.bn_25_by_{{raster_name}};
CREATE MATERIALIZED VIEW {{gbmi_schema}}.bn_25_by_{{raster_name}}_duplicates AS
    SELECT
        ({{gbmi_schema}}.bn_25_by_{{raster_name}}.*)::text,
        count(*)
    FROM
        {{gbmi_schema}}.bn_25_by_{{raster_name}}
    GROUP BY
        {{gbmi_schema}}.bn_25_by_{{raster_name}}.*
    HAVING count(*) > 1;

ALTER TABLE {{gbmi_schema}}.buildings_neighbours_indicators_by_{{raster_name}} RENAME TO bni_by_{{raster_name}};
CREATE INDEX bni_by_{{raster_name}}_osm_id ON {{gbmi_schema}}.bni_by_{{raster_name}}(osm_id);
CREATE INDEX bni_by_{{raster_name}}_centroid_spgist ON {{gbmi_schema}}.bni_by_{{raster_name}} USING SPGIST (way_centroid);
CREATE INDEX bni_by_{{raster_name}}_spgist ON {{gbmi_schema}}.bni_by_{{raster_name}} USING SPGIST (way);
VACUUM ANALYZE {{gbmi_schema}}.bni_by_{{raster_name}};
CREATE MATERIALIZED VIEW {{gbmi_schema}}.bni_by_{{raster_name}}_duplicates AS
    SELECT
        ({{gbmi_schema}}.bni_by_{{raster_name}}.*)::text,
        count(*)
    FROM
        {{gbmi_schema}}.bni_by_{{raster_name}}
    GROUP BY
        {{gbmi_schema}}.bni_by_{{raster_name}}.*
    HAVING count(*) > 1;

