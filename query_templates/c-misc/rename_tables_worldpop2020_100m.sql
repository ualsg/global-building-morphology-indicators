ALTER TABLE gbmi.buildings_geom_attributes_by_worldpop2020_100m RENAME TO bga_by_worldpop2020_100m;

ALTER TABLE gbmi.buildings_geom_indicators_by_worldpop2020_100m RENAME TO bgi_by_worldpop2020_100m;
CREATE INDEX bgi_by_worldpop2020_100m_osm_id ON gbmi.bgi_by_worldpop2020_100m(osm_id);
CREATE INDEX bgi_by_worldpop2020_100m_centroid_spgist ON gbmi.bgi_by_worldpop2020_100m USING SPGIST (way_centroid);
CREATE INDEX bgi_by_worldpop2020_100m_spgist ON gbmi.bgi_by_worldpop2020_100m USING SPGIST (way);
VACUUM ANALYZE gbmi.bgi_by_worldpop2020_100m;
CREATE MATERIALIZED VIEW gbmi.bgi_by_worldpop2020_100m_duplicates AS
    SELECT
        (gbmi.bgi_by_worldpop2020_100m.*)::text,
        count(*)
    FROM
        gbmi.bgi_by_worldpop2020_100m
    GROUP BY
        gbmi.bgi_by_worldpop2020_100m.*
    HAVING count(*) > 1;


ALTER TABLE gbmi.buildings_geom_indicators_clipped_by_worldpop2020_100m RENAME TO bgi_clipped_by_worldpop2020_100m;
CREATE INDEX bgi_clipped_by_worldpop2020_100m_osm_id ON gbmi.bgi_clipped_by_worldpop2020_100m(osm_id);
CREATE INDEX bgi_clipped_by_worldpop2020_100m_centroid_spgist ON gbmi.bgi_clipped_by_worldpop2020_100m USING SPGIST (way_centroid);
CREATE INDEX bgi_clipped_by_worldpop2020_100m_spgist ON gbmi.bgi_clipped_by_worldpop2020_100m USING SPGIST (way);
VACUUM ANALYZE gbmi.bgi_clipped_by_worldpop2020_100m;
CREATE MATERIALIZED VIEW gbmi.bgi_clipped_by_worldpop2020_100m_duplicates AS
    SELECT
        (gbmi.bgi_clipped_by_worldpop2020_100m.*)::text,
        count(*)
    FROM
        gbmi.bgi_clipped_by_worldpop2020_100m
    GROUP BY
        gbmi.bgi_clipped_by_worldpop2020_100m.*
    HAVING count(*) > 1;

ALTER TABLE gbmi.buildings_neighbours_by_worldpop2020_100m RENAME TO bn_by_worldpop2020_100m;
CREATE INDEX bn_by_worldpop2020_100m_osm_id ON gbmi.bn_by_worldpop2020_100m(osm_id1);
CREATE INDEX bn_by_worldpop2020_100m_centroid_spgist ON gbmi.bn_by_worldpop2020_100m USING SPGIST (way_centroid1);
CREATE INDEX bn_by_worldpop2020_100m_spgist ON gbmi.bn_by_worldpop2020_100m USING SPGIST (way1);
VACUUM ANALYZE gbmi.bn_by_worldpop2020_100m;
CREATE MATERIALIZED VIEW gbmi.bn_by_worldpop2020_100m_duplicates AS
    SELECT
        (gbmi.bn_by_worldpop2020_100m.*)::text,
        count(*)
    FROM
        gbmi.bn_by_worldpop2020_100m
    GROUP BY
        gbmi.bn_by_worldpop2020_100m.*
    HAVING count(*) > 1;


ALTER TABLE gbmi.buildings_neighbours_100_by_worldpop2020_100m RENAME TO bn_100_by_worldpop2020_100m;
CREATE INDEX bn_100_by_worldpop2020_100m_osm_id ON gbmi.bn_100_by_worldpop2020_100m(osm_id);
CREATE INDEX bn_100_by_worldpop2020_100m_centroid_spgist ON gbmi.bn_100_by_worldpop2020_100m USING spgist(way_centroid);
CREATE INDEX bn_100_by_worldpop2020_100m_spgist ON gbmi.bn_100_by_worldpop2020_100m USING spgist(way);
VACUUM ANALYZE gbmi.bn_100_by_worldpop2020_100m;
CREATE MATERIALIZED VIEW gbmi.bn_100_by_worldpop2020_100m_duplicates AS
    SELECT
        (gbmi.bn_100_by_worldpop2020_100m.*)::text,
        count(*)
    FROM
        gbmi.bn_100_by_worldpop2020_100m
    GROUP BY
        gbmi.bn_100_by_worldpop2020_100m.*
    HAVING count(*) > 1;

ALTER TABLE gbmi.buildings_neighbours_50_by_worldpop2020_100m RENAME TO bn_50_by_worldpop2020_100m;
CREATE INDEX bn_50_by_worldpop2020_100m_osm_id ON gbmi.bn_50_by_worldpop2020_100m(osm_id);
CREATE INDEX bn_50_by_worldpop2020_100m_centroid_spgist ON gbmi.bn_50_by_worldpop2020_100m USING spgist(way_centroid);
CREATE INDEX bn_50_by_worldpop2020_100m_spgist ON gbmi.bn_50_by_worldpop2020_100m USING spgist(way);
VACUUM ANALYZE gbmi.bn_50_by_worldpop2020_100m;
CREATE MATERIALIZED VIEW gbmi.bn_50_by_worldpop2020_100m_duplicates AS
    SELECT
        (gbmi.bn_50_by_worldpop2020_100m.*)::text,
        count(*)
    FROM
        gbmi.bn_50_by_worldpop2020_100m
    GROUP BY
        gbmi.bn_50_by_worldpop2020_100m.*
    HAVING count(*) > 1;


ALTER TABLE gbmi.buildings_neighbours_25_by_worldpop2020_100m RENAME TO bn_25_by_worldpop2020_100m;
CREATE INDEX bn_25_by_worldpop2020_100m_osm_id ON gbmi.bn_25_by_worldpop2020_100m(osm_id);
CREATE INDEX bn_25_by_worldpop2020_100m_centroid_spgist ON gbmi.bn_25_by_worldpop2020_100m USING spgist(way_centroid);
CREATE INDEX bn_25_by_worldpop2020_100m_spgist ON gbmi.bn_25_by_worldpop2020_100m USING spgist(way);
VACUUM ANALYZE gbmi.bn_25_by_worldpop2020_100m;
CREATE MATERIALIZED VIEW gbmi.bn_25_by_worldpop2020_100m_duplicates AS
    SELECT
        (gbmi.bn_25_by_worldpop2020_100m.*)::text,
        count(*)
    FROM
        gbmi.bn_25_by_worldpop2020_100m
    GROUP BY
        gbmi.bn_25_by_worldpop2020_100m.*
    HAVING count(*) > 1;

ALTER TABLE gbmi.buildings_neighbours_indicators_by_worldpop2020_100m RENAME TO bni_by_worldpop2020_100m;
CREATE INDEX bni_by_worldpop2020_100m_osm_id ON gbmi.bni_by_worldpop2020_100m(osm_id);
CREATE INDEX bni_by_worldpop2020_100m_centroid_spgist ON gbmi.bni_by_worldpop2020_100m USING SPGIST (way_centroid);
CREATE INDEX bni_by_worldpop2020_100m_spgist ON gbmi.bni_by_worldpop2020_100m USING SPGIST (way);
VACUUM ANALYZE gbmi.bni_by_worldpop2020_100m;
CREATE MATERIALIZED VIEW gbmi.bni_by_worldpop2020_100m_duplicates AS
    SELECT
        (gbmi.bni_by_worldpop2020_100m.*)::text,
        count(*)
    FROM
        gbmi.bni_by_worldpop2020_100m
    GROUP BY
        gbmi.bni_by_worldpop2020_100m.*
    HAVING count(*) > 1;

