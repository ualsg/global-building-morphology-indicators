DROP TABLE IF EXISTS {{misc_schema}}.agg_buildings_height_levels_qa_by_{{agg_level}}_{{raster_name}} CASCADE;


CREATE TABLE {{misc_schema}}.agg_buildings_height_levels_qa_by_{{agg_level}}_{{raster_name}} AS (
                                             WITH regexp_buildings AS (
                                                                      SELECT *,
                                                                          regexp_match("height"::text, '[+-]?((\d+\.?\d*)|(\.\d+))') AS height_regexp,
                                                                          regexp_match("o_building:height"::text, '[+-]?((\d+\.?\d*)|(\.\d+))') AS "building:height_regexp",
                                                                          regexp_match(
                                                                                  coalesce("height"::text, "o_building:height"::text),
                                                                                  '[+-]?((\d+\.?\d*)|(\.\d+))') AS coelesce_height_regexp,
                                                                          regexp_match("min_height"::text, '[+-]?((\d+\.?\d*)|(\.\d+))') AS min_height_regexp,
                                                                          regexp_match("building:min_height"::text, '[+-]?((\d+\.?\d*)|(\.\d+))') AS "building:min_height_regexp",
                                                                          regexp_match(
                                                                                  coalesce("min_height"::text, "building:min_height"::text),
                                                                                  '[+-]?((\d+\.?\d*)|(\.\d+))') AS coelesce_min_height_regexp,
                                                                          regexp_match("building:levels"::text, '[+-]?((\d+\.?\d*)|(\.\d+))') AS "building:levels_regexp",
                                                                          regexp_match("o_levels"::text, '[+-]?((\d+\.?\d*)|(\.\d+))') AS levels_regexp,
                                                                          regexp_match(
                                                                                  coalesce("building:levels"::text, "o_levels"::text),
                                                                                  '[+-]?((\d+\.?\d*)|(\.\d+))') AS "coelesce_building:levels_regexp",
                                                                          regexp_match("building:min_level"::text, '[+-]?((\d+\.?\d*)|(\.\d+))') AS "building:min_level_regexp",
                                                                          regexp_match("min_level"::text, '[+-]?((\d+\.?\d*)|(\.\d+))') AS "min_level_regexp",
                                                                          regexp_match(
                                                                                  coalesce("building:min_level"::text, "min_level"),
                                                                                  '[+-]?((\d+\.?\d*)|(\.\d+))') AS "coelesce_building:min_level_regexp"
                                                                      FROM
                                                                          {{gbmi_schema}}.buildings_by_{{raster_name}}
                                                                      ),
                                                 bldg_counts AS (
                                                                SELECT
                                                                    {{agg_columns}},
                                                                    {{agg_area}},
                                                                    count(*) AS building_count,
                                                                    count(*) FILTER (WHERE "height" IS NOT NULL) AS height_count,
                                                                    count(*) FILTER (WHERE "height" IS NOT NULL AND height_regexp IS NOT NULL) AS height_valid_count,                               -- somewhat valid (ignoring letters and characters)
                                                                    count(*) FILTER (WHERE "o_building:height" IS NOT NULL) AS "building:height_count",
                                                                            count(*)
                                                                            FILTER (WHERE "o_building:height" IS NOT NULL AND "building:height_regexp" IS NOT NULL) AS "building:height_valid_count", -- somewhat valid (ignoring letters and characters)
                                                                            count(*)
                                                                            FILTER (WHERE coalesce("height"::text, "o_building:height"::text) IS NOT NULL) AS "coelesce_height_count",
                                                                            count(*) FILTER (WHERE
                                                                            coalesce("height"::text, "o_building:height"::text) IS NOT NULL AND
                                                                            "coelesce_height_regexp" IS NOT NULL) AS "coelesce_height_valid_count",
                                                                    count(*) FILTER (WHERE min_height IS NOT NULL) AS min_height_count,
                                                                            count(*)
                                                                            FILTER (WHERE min_height IS NOT NULL AND min_height_regexp IS NOT NULL) AS min_height_valid_count,                      -- somewhat valid (ignoring letters and characters)
                                                                    count(*) FILTER (WHERE "building:min_height" IS NOT NULL) AS "building:min_height_count",
                                                                            count(*) FILTER (WHERE
                                                                            "building:min_height" IS NOT NULL AND
                                                                            "building:min_height_regexp" IS NOT NULL) AS "building:min_height_valid_count",                                         -- somewhat valid (ignoring letters and characters)
                                                                            count(*)
                                                                            FILTER (WHERE coalesce("min_height", "building:min_height") IS NOT NULL) AS "coelesce_min_height_count",
                                                                            count(*) FILTER (WHERE
                                                                            coalesce("min_height", "building:min_height") IS NOT NULL AND
                                                                            "coelesce_min_height_regexp" IS NOT NULL) AS "coelesce_min_height_valid_count",
                                                                    count(*) FILTER (WHERE "building:levels" IS NOT NULL) AS "building:levels_count",
                                                                            count(*)
                                                                            FILTER (WHERE "building:levels" IS NOT NULL AND "building:levels_regexp" IS NOT NULL) AS "building:levels_valid_count",
                                                                    count(*) FILTER (WHERE "o_levels" IS NOT NULL) AS "levels_count",
                                                                    count(*) FILTER (WHERE "o_levels" IS NOT NULL AND levels_regexp IS NOT NULL) AS "levels_valid_count",
                                                                            count(*)
                                                                            FILTER (WHERE coalesce("o_building:levels"::text, "o_levels"::text) IS NOT NULL) AS "coelesce_building:levels_count",
                                                                            count(*) FILTER (WHERE
                                                                            coalesce("o_building:levels"::text, "o_levels"::text) IS NOT NULL AND
                                                                            "coelesce_building:levels_regexp" IS NOT NULL) AS "coelesce_building:levels_valid_count",
                                                                    count(*) FILTER (WHERE "building:min_level" IS NOT NULL) AS "building:min_level_count",
                                                                            count(*) FILTER (WHERE
                                                                            "building:min_level" IS NOT NULL AND
                                                                            "building:min_level_regexp" IS NOT NULL) AS "building:min_level_valid_count",
                                                                    count(*) FILTER (WHERE "min_level" IS NOT NULL) AS "min_level_count",
                                                                            count(*)
                                                                            FILTER (WHERE "min_level" IS NOT NULL AND min_level_regexp IS NOT NULL) AS "min_level_valid_count",
                                                                            count(*)
                                                                            FILTER (WHERE coalesce("building:min_level", "min_level") IS NOT NULL) AS "coelesce_building:min_level_count",
                                                                            count(*) FILTER (WHERE
                                                                            coalesce("building:min_level", "min_level") IS NOT NULL AND
                                                                            "coelesce_building:min_level_regexp" IS NOT NULL) AS "coelesce_building:min_level_valid_count",
                                                                    count(*) FILTER (WHERE "addr:city" IS NOT NULL) AS city_count,
                                                                            count(*)
                                                                            FILTER (WHERE "addr:city" IS NOT NULL AND "addr:city" = cell_admin_div2) AS city_valid_count,                           -- somewhat valid (ignoring letters and characters)
                                                                    count(*) FILTER (WHERE "addr:country" IS NOT NULL) AS country_count,
                                                                            count(*)
                                                                            FILTER (WHERE "addr:country" IS NOT NULL AND UPPER("addr:country") = cell_country_code2) AS country_valid_count,        -- somewhat valid (ignoring letters and characters)
                                                                    sum(clipped_bldg_area) AS building_area_sum,
                                                                    sum(clipped_bldg_area) * 100 / {{agg_area}} AS building_area_sum_prct,
                                                                    count(DISTINCT osm_uid) AS mapper_count
                                                                FROM
                                                                    regexp_buildings rb {% if join_clause %}
                                                                    {{join_clause}}{% endif %}
                                                                GROUP BY
                                                                    {{agg_columns}},
                                                                    {{agg_area}}
                                                                )
                                             SELECT
                                                 {{agg_columns}},
                                                 {{agg_area}},
                                                 building_count,
                                                 height_count,
                                                 height_valid_count,
                                                 "building:height_count",
                                                 "building:height_valid_count",
                                                 coelesce_height_count,
                                                 coelesce_height_count * 100.0 / building_count AS "prct_height",
                                                 coelesce_height_valid_count,
                                                 coelesce_height_valid_count * 100.0 / building_count AS "prct_valid_height",
                                                 min_height_count,
                                                 min_height_valid_count,
                                                 "building:min_height_count",
                                                 "building:min_height_valid_count",
                                                 coelesce_min_height_count,
                                                 coelesce_min_height_count * 100.0 / building_count AS "prct_min_height",
                                                 coelesce_min_height_valid_count,
                                                 coelesce_min_height_valid_count * 100.0 / building_count AS "prct_valid_min_height",
                                                 "building:levels_count",
                                                 "building:levels_valid_count",
                                                 levels_count,
                                                 levels_valid_count,
                                                 "coelesce_building:levels_count",
                                                 "coelesce_building:levels_count" * 100.0 / building_count AS "prct_bldg_levels",
                                                 "coelesce_building:levels_valid_count",
                                                 "coelesce_building:levels_valid_count" * 100.0 / building_count AS "prct_valid_bldg_levels",
                                                 "building:min_level_count",
                                                 "building:min_level_valid_count",
                                                 min_level_count,
                                                 min_level_valid_count,
                                                 "coelesce_building:min_level_count",
                                                 "coelesce_building:min_level_count" * 100.0 / building_count AS "prct_bldg_min_levels",
                                                 "coelesce_building:min_level_valid_count",
                                                 "coelesce_building:min_level_valid_count" * 100.0 / building_count AS "prct_valid_bldg_min_levels",
                                                 city_count,
                                                 city_count * 100.0 / building_count AS "prct_city",
                                                 country_count,
                                                 country_count * 100.0 / building_count AS "prct_country",
                                                 country_valid_count,
                                                 country_valid_count * 100.0 / building_count AS "prct_valid_country",
                                                 building_area_sum,
                                                 building_area_sum_prct AS "prct_building_area_sum",
                                                 mapper_count,
                                                 mapper_count * 100.0 / building_count AS "ratio_mapper_to_building",
                                                 building_count * 100.0 / mapper_count AS "ratio_building_to_mapper"
                                             FROM
                                                 bldg_counts
                                             ORDER BY {{order_columns}}
                                    );


