-- MATERIALIZED VIEW FOR DEBUGGING
DROP MATERIALIZED VIEW IF EXISTS {{gbmi_schema}}.buildings_indicators_by_{{raster_name}}_centroid_duplicates CASCADE;
DROP TABLE IF EXISTS {{gbmi_schema}}.buildings_indicators_by_{{raster_name}}_centroid CASCADE;


CREATE TABLE {{gbmi_schema}}.buildings_indicators_by_{{raster_name}}_centroid AS (
                                                          WITH bldg_indicators AS (
                                                                                      SELECT
                                                                                          bgi."osm_id",
                                                                                          bgi."tags",
                                                                                          bgi."way",
                                                                                          bgi."way_centroid",
                                                                                          bgi."vertices_count",
                                                                                          bgi."vertices_count_pct_rnk",
                                                                                          bgi."footprint_area",
                                                                                          bgi."footprint_area_pct_rnk",
                                                                                          bgi."perimeter",
                                                                                          bgi."perimeter_pct_rnk",
                                                                                          bgi."mbr",
                                                                                          bgi."mbr_area",
                                                                                          bgi."mbr_area_pct_rnk",
                                                                                          bgi."mbr_length",
                                                                                          bgi."mbr_length_pct_rnk",
                                                                                          bgi."mbr_width",
                                                                                          bgi."mbr_width_pct_rnk",
                                                                                          bgi."azimuth",
                                                                                          bgi."is_residential",
                                                                                          bgi."height",
                                                                                          bgi."height_pct_rnk",
                                                                                          bgi."building:levels",
                                                                                          bgi."building:levels_pct_rnk",
                                                                                          bgi.ratio_height_to_footprint_area,
                                                                                          bgi.ratio_height_to_footprint_area_pct_rnk,
                                                                                          bgi."floor_area",
                                                                                          bgi."floor_area_pct_rnk",
                                                                                          bgi."wall_area",
                                                                                          bgi."wall_area_pct_rnk",
                                                                                          bgi."envelope_area",
                                                                                          bgi."envelope_area_pct_rnk",
                                                                                          bgi."volume",
                                                                                          bgi."volume_pct_rnk",
                                                                                          bgi."compactness",
                                                                                          bgi."compactness_pct_rnk",
                                                                                          bgi."complexity",
                                                                                          bgi."complexity_pct_rnk",
                                                                                          bgi."equivalent_rectangular_index",
                                                                                          bgi."equivalent_rectangular_index_pct_rnk",
                                                                                          bgi."start_date",
                                                                                          bgi."start_date_pct_rnk"{% if not limit_buffer %},
                                                                                          bni."buffer_area_100",
                                                                                          bni."neighbour_100_count",
                                                                                          bni."distance_100_min",
                                                                                          bni."distance_100_median",
                                                                                          bni."distance_100_max",
                                                                                          bni."distance_100_mean",
                                                                                          bni."distance_100_sd",
                                                                                          bni."distance_100_d",
                                                                                          bni."distance_100_cv",
                                                                                          bni."distance_100_mean_pct_rnk",
                                                                                          bni."neighbour_footprint_area_100_sum",
                                                                                          bni."neighbour_footprint_area_100_min",
                                                                                          bni."neighbour_footprint_area_100_max",
                                                                                          bni."neighbour_footprint_area_100_median",
                                                                                          bni."neighbour_footprint_area_100_mean",
                                                                                          bni."neighbour_footprint_area_100_sd",
                                                                                          bni."neighbour_footprint_area_100_d",
                                                                                          bni."neighbour_footprint_area_100_cv",
                                                                                          bni."neighbour_footprint_area_100_mean_pct_rnk",
                                                                                          bni."ratio_neighbour_footprint_sum_to_buffer_100",
                                                                                          bni."ratio_neighbour_height_to_distance_100_min",
                                                                                          bni."ratio_neighbour_height_to_distance_100_max",
                                                                                          bni."ratio_neighbour_height_to_distance_100_median",
                                                                                          bni."ratio_neighbour_height_to_distance_100_mean",
                                                                                          bni."ratio_neighbour_height_to_distance_100_sd",
                                                                                          bni."ratio_neighbour_height_to_distance_100_d",
                                                                                          bni."ratio_neighbour_height_to_distance_100_cv",
                                                                                          bni."ratio_neighbour_height_to_distance_100_mean_pct_rnk"{% endif %},
                                                                                          bni."buffer_area_50",
                                                                                          bni."neighbour_50_count",
                                                                                          bni."distance_50_min",
                                                                                          bni."distance_50_median",
                                                                                          bni."distance_50_max",
                                                                                          bni."distance_50_mean",
                                                                                          bni."distance_50_sd",
                                                                                          bni."distance_50_d",
                                                                                          bni."distance_50_cv",
                                                                                          bni."distance_50_mean_pct_rnk",
                                                                                          bni."neighbour_footprint_area_50_sum",
                                                                                          bni."neighbour_footprint_area_50_min",
                                                                                          bni."neighbour_footprint_area_50_max",
                                                                                          bni."neighbour_footprint_area_50_median",
                                                                                          bni."neighbour_footprint_area_50_mean",
                                                                                          bni."neighbour_footprint_area_50_sd",
                                                                                          bni."neighbour_footprint_area_50_d",
                                                                                          bni."neighbour_footprint_area_50_cv",
                                                                                          bni."neighbour_footprint_area_50_mean_pct_rnk",
                                                                                          bni."ratio_neighbour_footprint_sum_to_buffer_50",
                                                                                          bni."ratio_neighbour_height_to_distance_50_min",
                                                                                          bni."ratio_neighbour_height_to_distance_50_max",
                                                                                          bni."ratio_neighbour_height_to_distance_50_median",
                                                                                          bni."ratio_neighbour_height_to_distance_50_mean",
                                                                                          bni."ratio_neighbour_height_to_distance_50_sd",
                                                                                          bni."ratio_neighbour_height_to_distance_50_d",
                                                                                          bni."ratio_neighbour_height_to_distance_50_cv",
                                                                                          bni."ratio_neighbour_height_to_distance_50_mean_pct_rnk",
                                                                                          bni."buffer_area_25",
                                                                                          bni."neighbour_25_count",
                                                                                          bni."distance_25_min",
                                                                                          bni."distance_25_median",
                                                                                          bni."distance_25_max",
                                                                                          bni."distance_25_mean",
                                                                                          bni."distance_25_sd",
                                                                                          bni."distance_25_d",
                                                                                          bni."distance_25_cv",
                                                                                          bni."distance_25_mean_pct_rnk",
                                                                                          bni."neighbour_footprint_area_25_sum",
                                                                                          bni."neighbour_footprint_area_25_min",
                                                                                          bni."neighbour_footprint_area_25_max",
                                                                                          bni."neighbour_footprint_area_25_median",
                                                                                          bni."neighbour_footprint_area_25_mean",
                                                                                          bni."neighbour_footprint_area_25_sd",
                                                                                          bni."neighbour_footprint_area_25_d",
                                                                                          bni."neighbour_footprint_area_25_cv",
                                                                                          bni."neighbour_footprint_area_25_mean_pct_rnk",
                                                                                          bni."ratio_neighbour_footprint_sum_to_buffer_25",
                                                                                          bni."ratio_neighbour_height_to_distance_25_min",
                                                                                          bni."ratio_neighbour_height_to_distance_25_max",
                                                                                          bni."ratio_neighbour_height_to_distance_25_median",
                                                                                          bni."ratio_neighbour_height_to_distance_25_mean",
                                                                                          bni."ratio_neighbour_height_to_distance_25_sd",
                                                                                          bni."ratio_neighbour_height_to_distance_25_d",
                                                                                          bni."ratio_neighbour_height_to_distance_25_cv",
                                                                                          bni."ratio_neighbour_height_to_distance_25_mean_pct_rnk",
                                                                                          bgi."cell_id",
                                                                                          bgi."cell_centroid",
                                                                                          bgi."cell_geom",
                                                                                          bgi."cell_area",
                                                                                          bgi."cell_country",
                                                                                          bgi."cell_admin_div1",
                                                                                          bgi."cell_admin_div2",
                                                                                          bgi."cell_admin_div3",
                                                                                          bgi."cell_admin_div4",
                                                                                          bgi."cell_admin_div5",{% if raster_population %}
                                                                                          bgi."cell_population",{% endif %}
                                                                                          bgi."cell_country_official_name",
                                                                                          bgi."cell_country_code2",
                                                                                          bgi."cell_country_code3"
                                                                                      FROM
                                                                                          {{gbmi_schema}}.bgi_by_{{raster_name}} bgi
                                                                                          LEFT JOIN {{gbmi_schema}}.bni_by_{{raster_name}}_centroid bni
                                                                                              ON bgi.osm_id = bni.osm_id AND ST_Equals(bgi.way::geometry, bni.way::geometry)
                                                                                      )
                                                          SELECT DISTINCT * FROM bldg_indicators WHERE cell_country IS NOT NULL
                                                          );



CREATE INDEX buildings_indicators_by_{{raster_name}}_centroid_osm_id ON {{gbmi_schema}}.buildings_indicators_by_{{raster_name}}_centroid(osm_id);

CREATE INDEX buildings_indicators_by_{{raster_name}}_centroid_centroid_spgist ON {{gbmi_schema}}.buildings_indicators_by_{{raster_name}}_centroid USING SPGIST (way_centroid);

CREATE INDEX buildings_indicators_by_{{raster_name}}_centroid_spgist ON {{gbmi_schema}}.buildings_indicators_by_{{raster_name}}_centroid USING SPGIST (way);

VACUUM ANALYZE {{gbmi_schema}}.buildings_indicators_by_{{raster_name}}_centroid;



/*
-- This is to troubleshoot whether joints and building data are created correctly

CREATE MATERIALIZED VIEW {{gbmi_schema}}.buildings_indicators_by_{{raster_name}}_centroid_duplicates AS
    SELECT
        ({{gbmi_schema}}.buildings_indicators_by_{{raster_name}}_centroid.*)::text,
        count(*)
    FROM
        {{gbmi_schema}}.buildings_indicators_by_{{raster_name}}_centroid
    GROUP BY
        {{gbmi_schema}}.buildings_indicators_by_{{raster_name}}_centroid.*
    HAVING count(*) > 1;

 */