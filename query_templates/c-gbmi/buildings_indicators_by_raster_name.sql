DROP TABLE IF EXISTS {{gbmi_schema}}.buildings_indicators_by_{{raster_name}} CASCADE;

CREATE TABLE {{gbmi_schema}}.buildings_indicators_by_{{raster_name}} AS (
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
                                                              bgi."oriented_mbr",
                                                              bgi."oriented_mbr_area",
                                                              bgi."oriented_mbr_area_pct_rnk",
                                                              bgi."oriented_mbr_length",
                                                              bgi."oriented_mbr_length_pct_rnk",
                                                              bgi."oriented_mbr_width",
                                                              bgi."oriented_mbr_width_pct_rnk",
                                                              bgi."azimuth",
                                                              bgi."is_residential",
                                                              bgi."height",
                                                              bgi."height_pct_rnk",
                                                              bgi."building:levels",
                                                              bgi."building:levels_pct_rnk",
                                                              bgi."est_floor_area",
                                                              bgi."est_floor_area_pct_rnk",
                                                              bgi."est_wall_area",
                                                              bgi."est_wall_area_pct_rnk",
                                                              bgi."est_envelope_area",
                                                              bgi."est_envelope_area_pct_rnk",
                                                              bgi."est_volume",
                                                              bgi."est_volume_pct_rnk",
                                                              bgi."compactness",
                                                              bgi."compactness_pct_rnk",
                                                              bgi."complexity",
                                                              bgi."complexity_pct_rnk",
                                                              bgi."equivalent_rectangular_index",
                                                              bgi."cequivalent_rectangular_index_pct_rnk",
                                                              bgi."year_of_construction",
                                                              bgi."year_of_construction_pct_rnk",
                                                              bgi."start_date",
                                                              bgi."start_date_pct_rnk",
                                                              bni."neighbour_100_count",
                                                              bni."distance_100_min",
                                                              bni."distance_100_median",
                                                              bni."distance_100_max",
                                                              bni."distance_100_mean",
                                                              bni."distance_100_sd",
                                                              bni."distance_100_d",
                                                              bni."distance_100_mean_pct_rnk",
                                                              bni."neighbour_50_count",
                                                              bni."distance_50_min",
                                                              bni."distance_50_median",
                                                              bni."distance_50_max",
                                                              bni."distance_50_mean",
                                                              bni."distance_50_sd",
                                                              bni."distance_50_d",
                                                              bni."distance_50_mean_pct_rnk",
                                                              bni."neighbour_25_count",
                                                              bni."distance_25_min",
                                                              bni."distance_25_median",
                                                              bni."distance_25_max",
                                                              bni."distance_25_mean",
                                                              bni."distance_25_sd",
                                                              bni."distance_25_d",
                                                              bni."distance_25_mean_pct_rnk",
                                                              bgi."cell_id",
                                                              bgi."cell_centroid",
                                                              bgi."cell_geom",
                                                              bgi."cell_area",
                                                              bgi."cell_country",
                                                              bgi."cell_admin_div1",
                                                              bgi."cell_admin_div2",
                                                              bgi."cell_admin_div3",
                                                              bgi."cell_admin_div4",
                                                              bgi."cell_admin_div5",
                                                              bgi."cell_population",
                                                              bgi."cell_country_official_name",
                                                              bgi."cell_country_code2",
                                                              bgi."cell_country_code3",
                                                              bgi."clipped_way",
                                                              bgi."clipped_bldg_area",
                                                              bgi."clipped_bldg_perimeter"
                                                          FROM
                                                              gbmi.buildings_geom_indicators_by_{{raster_name}}_by_centroid bgi
                                                              LEFT JOIN gbmi.buildings_neighbours_indicators_by_{{raster_name}} bni
                                                                  ON bgi.osm_id = bni.osm_id AND bgi.way = bni.way
                                                          );



CREATE INDEX buildings_indicators_by_{{raster_name}}_osm_id ON {{gbmi_schema}}.buildings_indicators_by_{{raster_name}}(osm_id);

CREATE INDEX buildings_indicators_by_{{raster_name}}_centroid_spgist ON {{gbmi_schema}}.buildings_indicators_by_{{raster_name}} USING SPGIST (way_centroid);

CREATE INDEX buildings_indicators_by_{{raster_name}}_spgist ON {{gbmi_schema}}.buildings_indicators_by_{{raster_name}} USING SPGIST (way);

VACUUM ANALYZE {{gbmi_schema}}.buildings_indicators_by_{{raster_name}};
