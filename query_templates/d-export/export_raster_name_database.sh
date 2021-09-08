#!/bin/bash

export_base_dir="{{ export_base_dir }}"
export_query_dir="{{ export_script_dir }}"
database="{{ database }}"
host="{{ host_address }}"
raster_name="{{ raster_name }}"

declare -a export_levels=('bldg')
# get agg_levels from config.json
# declare -a agg_levels=('bldg' 'cell' 'country' 'admin_div1' 'admin_div2' 'admin_div3' 'admin_div4' 'admin_div5')
declare -a agg_levels=({{ agg_levels }})
# add 'bldg' as a level to loop through and export
export_levels+=(${agg_levels[@]})
echo "${export_levels[@]}"

# define indicator types - geometric and neighbour
declare -a indicators_types=('bgi' 'bni')

# define export formats
declare -a formats=('shp' 'gpkg' 'csv')

# define field names and respective abbreviation tuples
declare -a bgi_fields=(
  "buildings_count int bc 16BUI"
  "buildings_count_normalised float bc_nm 32BF"
  "footprint_area_min float fpa_min 32BF"
  "footprint_area_median float fpa_med 32BF"
  "footprint_area_mean float fpa_mean 32BF"
  "footprint_area_max float fpa_max 32BF"
  "footprint_area_sum float fpas 32BF"
  "footprint_area_sum_normalised float fpas_nm 32BF"
  "footprint_area_sd float fpa_sd 32BF"
  "footprint_area_d float fpa_d 32BF"
  "footprint_area_cv float fpa_cv 32BF"
  "footprint_area_mean_pct_rnk float fpa_mpr 32BF"
  "perimeter_min float p_min 32BF"
  "perimeter_median float p_med 32BF"
  "perimeter_mean float p_mean 32BF"
  "perimeter_max float p_max 32BF"
  "perimeter_sum float p_sm 32BF"
  "perimeter_sum_normalised float p_s_nm 32BF"
  "perimeter_sd float p_sd 32BF"
  "perimeter_d float p_d 32BF"
  "perimeter_cv float p_cv 32BF"
  "perimeter_mean_pct_rnk float p_mpr 32BF"
  "height_min float h_min 32BF"
  "height_median float h_med 32BF"
  "height_mean float h_mean 32BF"
  "height_max float h_max 32BF"
  "height_sd float h_sd 32BF"
  "height_d float h_d 32BF"
  "height_cv float h_cv 32BF"
  "height_mean_pct_rnk float h_mpr 32BF"
  "height_weighted_mean float h_wmean 32BF"
  "ratio_height_to_footprint_area_min float rhfpa_min 32BF"
  "ratio_height_to_footprint_area_median float rhfpa_med 32BF"
  "ratio_height_to_footprint_area_mean float rhfpa_mean 32BF"
  "ratio_height_to_footprint_area_max float rhfpa_max 32BF"
  "ratio_height_to_footprint_area_sd float rhfpa_sd 32BF"
  "ratio_height_to_footprint_area_d float rhfpa_d 32BF"
  "ratio_height_to_footprint_area_cv float rhfpa_cv 32BF"
  "ratio_height_to_footprint_area_mean_pct_rnk float rhfpa_mpr 32BF"
  "volume_min float vol_min 32BF"
  "volume_median float vol_med 32BF"
  "volume_mean float vol_mean 32BF"
  "volume_max float vol_max 32BF"
  "volume_sum float vol_sm 32BF"
  "volume_sum_normalised float vol_s_nm 32BF"
  "volume_sd float vol_sd 32BF"
  "volume_d float vol_d 32BF"
  "volume_cv float vol_cv 32BF"
  "volume_mean_pct_rnk float vol_mpr 32BF"
  "wall_area_min float wa_min 32BF"
  "wall_area_median float wa_med 32BF"
  "wall_area_mean float wa_mean 32BF"
  "wall_area_max float wa_max 32BF"
  "wall_area_sum float wa_sm 32BF"
  "wall_area_sum_normalised float wa_s_nm 32BF"
  "wall_area_sd float wa_sd 32BF"
  "wall_area_d float wa_d 32BF"
  "wall_area_cv float wa_cv 32BF"
  "wall_area_mean_pct_rnk float wa_mpr 32BF"
  "envelope_area_min float ea_min 32BF"
  "envelope_area_median float ea_med 32BF"
  "envelope_area_mean float ea_mean 32BF"
  "envelope_area_max float ea_max 32BF"
  "envelope_area_sum float ea_sm 32BF"
  "envelope_area_sum_normalised float ea_s_nm 32BF"
  "envelope_area_sd float ea_sd 32BF"
  "envelope_area_d float ea_d 32BF"
  "envelope_area_cv float ea_cv 32BF"
  "envelope_area_mean_pct_rnk float ea_mpr 32BF"
  "vertices_count_min int vc_min 16BUI"
  "vertices_count_median float vc_med 32BF"
  "vertices_count_mean float vc_mean 32BF"
  "vertices_count_max int vc_max 16BUI"
  "vertices_count_sum int vc_sm 16BUI"
  "vertices_count_sum_normalised float vc_s_nm 32BF"
  "vertices_count_sd float vc_sd 32BF"
  "vertices_count_d float vc_d 32BF"
  "vertices_count_cv float vc_cv 32BF"
  "vertices_count_mean_pct_rnk float vc_mpr 32BF"
  "complexity_min float cplx_min 32BF"
  "complexity_median float cplx_med 32BF"
  "complexity_mean float cplx_mean 32BF"
  "complexity_max float cplx_max 32BF"
  "complexity_sd float cplx_sd 32BF"
  "complexity_d float cplx_d 32BF"
  "complexity_cv float cplx_cv 32BF"
  "complexity_mean_pct_rnk float cplx_mpr 32BF"
  "compactness_min float cpct_min 32BF"
  "compactness_median float cpct_med 32BF"
  "compactness_mean float cpct_mean 32BF"
  "compactness_max float cpct_max 32BF"
  "compactness_sd float cpct_sd 32BF"
  "compactness_d float cpct_d 32BF"
  "compactness_cv float cpct_cv 32BF"
  "compactness_mean_pct_rnk float cpct_mpr 32BF"
  "equivalent_rectangular_index_min float eri_min 32BF"
  "equivalent_rectangular_index_median float eri_med 32BF"
  "equivalent_rectangular_index_mean float eri_mean 32BF"
  "equivalent_rectangular_index_max float eri_max 32BF"
  "equivalent_rectangular_index_sd float eri_sd 32BF"
  "equivalent_rectangular_index_d float eri_d 32BF"
  "equivalent_rectangular_index_cv float eri_cv 32BF"
  "equivalent_rectangular_index_mean_pct_rnk float eri_mpr 32BF"
  "azimuth_min float az_min 32BF"
  "azimuth_median float az_med 32BF"
  "azimuth_mean float az_mean 32BF"
  "azimuth_max float az_max 32BF"
  "azimuth_sd float az_sd 32BF"
  "azimuth_d float az_d 32BF"
  "azimuth_cv float az_cv 32BF"
  "azimuth_mean_pct_rnk float az_mpr 32BF"
  "mbr_length_min float l_min 32BF"
  "mbr_length_median float l_med 32BF"
  "mbr_length_mean float l_mean 32BF"
  "mbr_length_max float l_max 32BF"
  "mbr_length_sd float l_sd 32BF"
  "mbr_length_d float l_d 32BF"
  "mbr_length_cv float l_cv 32BF"
  "mbr_length_mean_pct_rnk float l_mpr 32BF"
  "mbr_width_min float w_min 32BF"
  "mbr_width_median float w_med 32BF"
  "mbr_width_mean float w_mean 32BF"
  "mbr_width_max float w_max 32BF"
  "mbr_width_sd float w_sd 32BF"
  "mbr_width_d float w_d 32BF"
  "mbr_width_cv float w_cv 32BF"
  "mbr_width_mean_pct_rnk float w_mpr 32BF"
  "mbr_area_min float mbra_min 32BF"
  "mbr_area_median float mbra_med 32BF"
  "mbr_area_mean float mbra_mean 32BF"
  "mbr_area_max float mbra_max 32BF"
  "mbr_area_sum float mbra_sm 32BF"
  "mbr_area_sum_normalised float mbra_sm 32BF"
  "mbr_area_sd float mbra_sd 32BF"
  "mbr_area_d float mbra_d 32BF"
  "mbr_area_cv float mbra_cv 32BF"
  "mbr_area_mean_pct_rnk float mbra_mpr 32BF"
  "building:levels_min float blvl_min 32BF"
  "building:levels_median float blvl_med 32BF"
  "building:levels_mean float blvl_mean 32BF"
  "building:levels_max float blvl_max 32BF"
  "building:levels_sd float blvl_sd 32BF"
  "building:levels_d float blvl_d 32BF"
  "building:levels_cv float blvl_cv 32BF"
  "building:levels_mean_pct_rnk float blvl_mpr 32BF"
  "floor_area_min float fla_min 32BF"
  "floor_area_median float fla_med 32BF"
  "floor_area_mean float fla_mea 32BF"
  "floor_area_max float fla_max 32BF"
  "floor_area_sum float fla_sm 32BF"
  "floor_area_sum_normalised float fla_s_nm 32BF"
  "floor_area_sd float fla_sd 32BF"
  "floor_area_d float fla_d 32BF"
  "floor_area_cv float fla_cv 32BF"
  "floor_area_mean_pct_rnk float fla_mpr 32BF"
  "residential_count float resc 32BF"
  "residential_count_normalised float resc_nm 32BF"
  "residential_floor_area_min float rfla_min 32BF"
  "residential_floor_area_median float rfla_med 32BF"
  "residential_floor_area_mean float rfla_mean 32BF"
  "residential_floor_area_max float rfla_max 32BF"
  "residential_floor_area_sum float rfla_sm 32BF"
  "residential_floor_area_sum_normalised float rfla_s_nm 32BF"
  "residential_floor_area_sd float rfla_sd 32BF"
  "residential_floor_area_d float rfla_d 32BF"
  "residential_floor_area_cv float rfla_cv 32BF"
  "residential_floor_area_mean_pct_rnk float rfla_mpr 32BF"
)

# define field names and respective abbreviation tuples
declare -a bni_fields=(
  "buffer_area_25_mean float ba25_mean 32BF"
  "neighbour_25_count_min int n25c_min 16BUI"
  "neighbour_25_count_median float n25c_med 32BF"
  "neighbour_25_count_mean float n25c_mean 32BF"
  "neighbour_25_count_max int n25c_max 16BUI"
  "neighbour_25_count_sd float n25c_sd 32BF"
  "neighbour_25_count_d float n25c_d 32BF"
  "neighbour_25_count_cv float n25c_cv 32BF"
  "neighbour_25_count_mean_pct_rnk float n25c_mpr 32BF"
  "distance_25_min float d25_min 32BF"
  "distance_25_median float d25_med 32BF"
  "distance_25_mean float d25_mean 32BF"
  "distance_25_max float d25_max 32BF"
  "distance_25_sd float d25_sd 32BF"
  "distance_25_d float d25_d 32BF"
  "distance_25_cv float d25_cv 32BF"
  "distance_25_mean_pct_rnk float d25_mpr 32BF"
  "neighbour_footprint_area_25_sum_min float fpa25smin 32BF"
  "neighbour_footprint_area_25_sum_median float fpa25smed 32BF"
  "neighbour_footprint_area_25_sum_mean float fpa25smean 32BF"
  "neighbour_footprint_area_25_sum_max float fpa25smax 32BF"
  "neighbour_footprint_area_25_sum_sd float fpa25ssd 32BF"
  "neighbour_footprint_area_25_sum_d float fpa25sd 32BF"
  "neighbour_footprint_area_25_sum_cv float fpa25scv 32BF"
  "neighbour_footprint_area_25_sum_mean_pct_rnk float fpa25smpr 32BF"
  "neighbour_footprint_area_25_min float fpa25_min 32BF"
  "neighbour_footprint_area_25_median float fpa25_med 32BF"
  "neighbour_footprint_area_25_mean float fpa25_mea 32BF"
  "neighbour_footprint_area_25_max float fpa25_max 32BF"
  "neighbour_footprint_area_25_sd float fpa25_sd 32BF"
  "neighbour_footprint_area_25_d float fpa25_d 32BF"
  "neighbour_footprint_area_25_cv float fpa25_cv 32BF"
  "neighbour_footprint_area_25_mean_pct_rnk float fpa25_mpr 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_25_min float rfb25_min 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_25_median float rfb25_med 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_25_mean float rfb25_mea 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_25_max float rfb25_max 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_25_sd float rfb25_sd 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_25_d float rfb25_d 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_25_cv float rfb25_cv 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_25_mean_pct_rnk float rfb25_mpr 32BF"
  "ratio_neighbour_height_to_distance_25_min float rhd25_min 32BF"
  "ratio_neighbour_height_to_distance_25_median float rhd25_med 32BF"
  "ratio_neighbour_height_to_distance_25_mean float rhd25_mea 32BF"
  "ratio_neighbour_height_to_distance_25_max float rhd25_max 32BF"
  "ratio_neighbour_height_to_distance_25_sd float rhd25_sd 32BF"
  "ratio_neighbour_height_to_distance_25_d float rhd25_d 32BF"
  "ratio_neighbour_height_to_distance_25_cv float rhd25_cv 32BF"
  "ratio_neighbour_height_to_distance_25_mean_pct_rnk float rhd25_mpr 32BF"
  "buffer_area_50_mean float ba50_mean 32BF"
  "neighbour_50_count_min int n50c_min 16BUI"
  "neighbour_50_count_median float n50c_med 32BF"
  "neighbour_50_count_mean float n50c_mean 32BF"
  "neighbour_50_count_max int n50c_max 16BUI"
  "neighbour_50_count_sd float n50c_sd 32BF"
  "neighbour_50_count_d float n50c_d 32BF"
  "neighbour_50_count_cv float n50c_cv 32BF"
  "neighbour_50_count_mean_pct_rnk float n50c_mpr 32BF"
  "distance_50_min float d50_min 32BF"
  "distance_50_median float d50_med 32BF"
  "distance_50_mean float d50_mean 32BF"
  "distance_50_max float d50_max 32BF"
  "distance_50_sd float d50_sd 32BF"
  "distance_50_d float d50_d 32BF"
  "distance_50_cv float d50_cv 32BF"
  "distance_50_mean_pct_rnk float d50_mpr 32BF"
  "neighbour_footprint_area_50_sum_min float fpa50smin 32BF"
  "neighbour_footprint_area_50_sum_median float fpa50smed 32BF"
  "neighbour_footprint_area_50_sum_mean float fpa50smean 32BF"
  "neighbour_footprint_area_50_sum_max float fpa50smax 32BF"
  "neighbour_footprint_area_50_sum_sd float fpa50ssd 32BF"
  "neighbour_footprint_area_50_sum_d float fpa50sd 32BF"
  "neighbour_footprint_area_50_sum_cv float fpa50scv 32BF"
  "neighbour_footprint_area_50_sum_mean_pct_rnk float fpa50smpr 32BF"
  "neighbour_footprint_area_50_min float fpa50_min 32BF"
  "neighbour_footprint_area_50_median float fpa50_med 32BF"
  "neighbour_footprint_area_50_mean float fpa50_mea 32BF"
  "neighbour_footprint_area_50_max float fpa50_max 32BF"
  "neighbour_footprint_area_50_sd float fpa50_sd 32BF"
  "neighbour_footprint_area_50_d float fpa50_d 32BF"
  "neighbour_footprint_area_50_cv float fpa50_cv 32BF"
  "neighbour_footprint_area_50_mean_pct_rnk float fpa50_mpr 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_50_min float rfb50_min 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_50_median float rfb50_med 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_50_mean float rfb50_mea 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_50_max float rfb50_max 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_50_sd float rfb50_sd 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_50_d float rfb50_d 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_50_cv float rfb50_cv 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_50_mean_pct_rnk float rfb50_mpr 32BF"
  "ratio_neighbour_height_to_distance_50_min float rhd50_min 32BF"
  "ratio_neighbour_height_to_distance_50_median float rhd50_med 32BF"
  "ratio_neighbour_height_to_distance_50_mean float rhd50_mea 32BF"
  "ratio_neighbour_height_to_distance_50_max float rhd50_max 32BF"
  "ratio_neighbour_height_to_distance_50_sd float rhd50_sd 32BF"
  "ratio_neighbour_height_to_distance_50_d float rhd50_d 32BF"
  "ratio_neighbour_height_to_distance_50_cv float rhd50_cv 32BF"
  "ratio_neighbour_height_to_distance_50_mean_pct_rnk float rhd50_mpr 32BF"{% if not limit_buffer %}
  "buffer_area_100_mean float ba100_mean 32BF"
  "neighbour_100_count_min int n100c_min 16BUI"
  "neighbour_100_count_median float n100c_med 32BF"
  "neighbour_100_count_mean float n100c_mean 32BF"
  "neighbour_100_count_max int n100c_max 16BUI"
  "neighbour_100_count_sd float n100c_sd 32BF"
  "neighbour_100_count_d float n100c_d 32BF"
  "neighbour_100_count_cv float n100c_cv 32BF"
  "neighbour_100_count_mean_pct_rnk float n100c_mpr 32BF"
  "distance_100_min float d100_min 32BF"
  "distance_100_median float d100_med 32BF"
  "distance_100_mean float d100_mean 32BF"
  "distance_100_max float d100_max 32BF"
  "distance_100_sd float d100_sd 32BF"
  "distance_100_d float d100_d 32BF"
  "distance_100_cv float d100_cv 32BF"
  "distance_100_mean_pct_rnk float d100_mpr 32BF"
  "neighbour_footprint_area_100_sum_min float fpa100smean 32BF"
  "neighbour_footprint_area_100_sum_median float fpa100smean 32BF"
  "neighbour_footprint_area_100_sum_mean float fpa100smean 32BF"
  "neighbour_footprint_area_100_sum_max float fpa100smax 32BF"
  "neighbour_footprint_area_100_sum_sd float fpa100smean 32BF"
  "neighbour_footprint_area_100_sum_d float fpa100sd 32BF"
  "neighbour_footprint_area_100_sum_cv float fpa100scv 32BF"
  "neighbour_footprint_area_100_sum_mean_pct_rnk float fpa100smean 32BF"
  "neighbour_footprint_area_100_min float fpa100_min 32BF"
  "neighbour_footprint_area_100_median float fpa100_med 32BF"
  "neighbour_footprint_area_100_mean float fpa100_mea 32BF"
  "neighbour_footprint_area_100_max float fpa100_max 32BF"
  "neighbour_footprint_area_100_sd float fpa100_sd 32BF"
  "neighbour_footprint_area_100_d float fpa100_d 32BF"
  "neighbour_footprint_area_100_cv float fpa100_cv 32BF"
  "neighbour_footprint_area_100_mean_pct_rnk float fpa100_mpr 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_100_min float rfb100_min 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_100_median float rfb100_med 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_100_mean float rfb100_mea 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_100_max float rfb100_max 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_100_sd float rfb100_sd 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_100_d float rfb100_d 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_100_cv float rfb100_cv 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_100_mean_pct_rnk float rfb100_mpr 32BF"
  "ratio_neighbour_height_to_distance_100_min float rhd100_min 32BF"
  "ratio_neighbour_height_to_distance_100_median float rhd100_med 32BF"
  "ratio_neighbour_height_to_distance_100_mean float rhd100_mea 32BF"
  "ratio_neighbour_height_to_distance_100_max float rhd100_max 32BF"
  "ratio_neighbour_height_to_distance_100_sd float rhd100_sd 32BF"
  "ratio_neighbour_height_to_distance_100_d float rhd100_d 32BF"
  "ratio_neighbour_height_to_distance_100_cv float rhd100_cv 32BF"
  "ratio_neighbour_height_to_distance_100_mean_pct_rnk float rhd100_mpr 32BF"{% endif %}
)


# loop through levels
for level in "${export_levels[@]}"; do

  # add geotiff as export format if level is cell
  if [[ "$level" == "cell" ]]; then
    formats+=('geotiff')
  else
    del='geotiff'
    formats=("${formats[@]/$del/}")
  fi


  # loop through each format and export
  for fmt in "${formats[@]}"; do
    echo
    echo
    echo "$database" "$level" "$fmt"

    # Create export directory structures
    if [[ "$level" == "bldg" ]]; then
      export_dir="${export_base_dir}/${database}/${level}"
      cmd_mkdir="[[ ! -d \"${export_dir}\" ]] && mkdir -p \"${export_dir}\""
    else
      export_dir="${export_base_dir}/${database}/agg_${level}"
      cmd_mkdir="[[ ! -d \"${export_dir}\" ]] && mkdir -p \"${export_dir}\""
    fi
    eval "$cmd_mkdir"

    case "$fmt" in
    "shp") # Export Shapefiles
      echo "======================== SHP ========================"
      if [[ "$level" == "bldg" ]]; then # export building_indicators

        ## bldg queries
        q_bldg=$(<"${export_query_dir}/buildings_indicators_by_${raster_name}.sql")             # neighbour by polygon
        qc_bldg="$(<"${export_query_dir}/buildings_indicators_by_${raster_name}_centroid.sql")" # neighbour by centroid
        #          echo "$q_bldg"
        #          echo "$qc_bldg"
        ## bldg with neighbour by polygon export command
        cmd_export_bldg="time pgsql2shp -h ${host} -p 5432 -f ${export_dir}/buildings_indicators_by_${raster_name} ${database} \"${q_bldg}\""
        echo "\$cmd_export_bldg:: $cmd_export_bldg"
        eval "$cmd_export_bldg"
        ## bldg with neighbour by centroid export command
        cmd_export_bldg_c="time pgsql2shp -h ${host} -p 5432 -f ${export_dir}/buildings_indicators_by_${raster_name}_centroid ${database} \"${qc_bldg}\""
        echo "\$cmd_export_bldg_c:: $cmd_export_bldg_c"
        eval "$cmd_export_bldg_c"

      else # export aggregated indicators by type

        for indicator_type in "${indicators_types[@]}"; do
          ## agg query
          q_agg=$(<"${export_query_dir}/agg_${indicator_type}_by_${level}_${raster_name}.sql")
          #            echo "$q_agg"

          ## agg export command
          cmd_export_agg="time pgsql2shp -h ${host} -p 5432 -f ${export_dir}/agg_${indicator_type}_by_${level}_${raster_name} ${database} \"${q_agg}\""
          echo "\$cmd_export_agg:: $cmd_export_agg"
          eval "$cmd_export_agg"

          if [[ "$indicator_type" == "bni" ]]; then
            qc_agg=$(<"${export_query_dir}/agg_${indicator_type}_by_${level}_${raster_name}_centroid.sql")
            #              echo "$qc_agg"
            cmd_export_agg_c="time pgsql2shp -h ${host} -p 5432  -f ${export_dir}/agg_${indicator_type}_by_${level}_${raster_name}_centroid ${database} \"${qc_agg}\""
            echo "\$cmd_export_agg_c:: $cmd_export_agg_c"
            eval "$cmd_export_agg_c"
          fi
        done
      fi
      ;;

    \
      "gpkg")
      echo "======================== GPKG ========================"
      if [[ "$level" == "bldg" ]]; then
        # convert building_indicators
        cmd_convert_bldg="time ogr2ogr -nlt PROMOTE_TO_MULTI -f GPKG ${export_dir}/buildings_indicators_by_${raster_name}.gpkg ${export_dir}/buildings_indicators_by_${raster_name}.shp"
        echo "\$cmd_convert_bldg:: $cmd_convert_bldg"
        eval "$cmd_convert_bldg"
      else
        # convert aggregated indicators by type
        for indicator_type in "${indicators_types[@]}"; do
          cmd_convert_agg="time ogr2ogr -nlt PROMOTE_TO_MULTI -f GPKG ${export_dir}/agg_${indicator_type}_by_${level}_${raster_name}.gpkg ${export_dir}/agg_${indicator_type}_by_${level}_${raster_name}.shp"
          echo "\$cmd_convert_agg:: $cmd_convert_agg"
          eval "$cmd_convert_agg"

          if [[ "$indicator_type" == "bni" ]]; then
            cmd_convert_agg_c="time ogr2ogr -nlt PROMOTE_TO_MULTI -f GPKG ${export_dir}/agg_${indicator_type}_by_${level}_${raster_name}_centroid.gpkg ${export_dir}/agg_${indicator_type}_by_${level}_${raster_name}_centroid.shp"
            echo "\$cmd_convert_agg_c:: $cmd_convert_agg_c"
            eval "$cmd_convert_agg_c"
          fi
        done
      fi
      ;;

    \
      "csv")
      echo "======================== CSV ========================"
      conn_str="\"dbname=${database} host=${host} port=5432 sslmode=require\""

      if [[ "$level" == "bldg" ]]; then
        # export building_indicators
        ## bldg query (WKT)
        q_bldg_wkt=$(<"${export_query_dir}/buildings_indicators_by_${raster_name}_wkt.sql")
        qc_bldg_wkt=$(<"${export_query_dir}/buildings_indicators_by_${raster_name}_centroid_wkt.sql")
        #          echo "$q_bldg_wkt"
        #          echo "$qc_bldg_wkt"

        ## bldg export command (WKT)
        cmd_export_bldg_csv_wkt="time psql ${conn_str} -c \"COPY (${q_bldg_wkt}) To STDOUT With CSV HEADER DELIMITER ',';\" > ${export_dir}/buildings_indicators_by_${raster_name}_wkt.csv"
        echo "\$cmd_export_bldg_csv_wkt:: $cmd_export_bldg_csv_wkt"
        eval "$cmd_export_bldg_csv_wkt"
        cmd_export_bldg_c_csv_wkt="time psql ${conn_str} -c \"COPY (${qc_bldg_wkt}) To STDOUT With CSV HEADER DELIMITER ',';\" > ${export_dir}/buildings_indicators_by_${raster_name}_centroid_wkt.csv"
        echo "\$cmd_export_bldg_c_csv_wkt:: $cmd_export_bldg_c_csv_wkt"
        eval "$cmd_export_bldg_c_csv_wkt"

      else
        # convert aggregated indicators by type
        for indicator_type in "${indicators_types[@]}"; do
          ## agg query (WKT)
          q_agg_wkt=$(<"${export_query_dir}/agg_${indicator_type}_by_${level}_${raster_name}_wkt.sql")
          #            echo "$q_agg_wkt"
          ## agg export command (WKT)
          cmd_export_agg_csv_wkt="time psql ${conn_str} -c \"COPY (${q_agg_wkt}) To STDOUT With CSV HEADER DELIMITER ',';\" > ${export_dir}/agg_${indicator_type}_by_${level}_${raster_name}_wkt.csv"
          echo "\$cmd_export_agg_csv_wkt:: cmd_export_agg_csv_wkt"
          eval "$cmd_export_agg_csv_wkt"

          if [[ "$indicator_type" == "bni" ]]; then
            qc_agg_wkt=$(<"${export_query_dir}/agg_${indicator_type}_by_${level}_${raster_name}_centroid_wkt.sql")
            #              echo "$qc_agg_wkt"
            cmd_export_agg_c_csv_wkt="time psql ${conn_str} -c \"COPY (${qc_agg_wkt}) To STDOUT With CSV HEADER DELIMITER ',';\" > ${export_dir}/agg_${indicator_type}_by_${level}_${raster_name}_centroid_wkt.csv"
            echo "\$cmd_export_agg_c_csv_wkt:: $cmd_export_agg_c_csv_wkt"
            eval "$cmd_export_agg_c_csv_wkt"
          fi
        done
      fi
      ;;

    "geotiff") # export geotiff only for agg_by_cell
      echo "======================== GeoTiFF ========================"
      conn_str="\"dbname=${database} host=${host} port=5432 sslmode=require\""

      # convert aggregated indicators by type
      for indicator_type in "${indicators_types[@]}"; do
        if [[ "$indicator_type" == "bgi" ]]; then

          ## agg bgi query
          q_agg=$(<"${export_query_dir}/rast_agg_${indicator_type}_by_${level}_${raster_name}.sql")
#          echo "$q_agg"

          for field in "${bgi_fields[@]}"; do
            read -a strarr <<<"$field" # uses default whitespace IFS
            attr=${strarr[0]}
            psql_dtype=${strarr[1]}
            attr_abbr=${strarr[2]}
            band_pix_type=${strarr[3]}
            echo ${attr} ${psql_dtype} ${attr_abbr} ${band_pix_type} ${export_dir}
            # sed -e 's|attr_abbr|'${attr_abbr}'|g' -e 's|band_pix_type|'${band_pix_type}'|g' -e 's|attr|'${attr}'|g' -e 's|psql_dtype|'${psql_dtype}'|g'  -e 's|export_dir|'${export_dir}'|g' < "../../query_output/d-export/rast_agg_bgi_by_cell_worldpop2020_1km.sql" > tempt.sql
            q_agg_sed=$(sed -e 's|attr_abbr|'${attr_abbr}'|g' -e 's|band_pix_type|'${band_pix_type}'|g' -e 's|attr|'${attr}'|g' -e 's|psql_dtype|'${psql_dtype}'|g' -e 's|export_dir|'${export_dir}'|g' <<< "$q_agg" )
#            echo $q_agg_sed

            ## agg bgi export command
            cmd_export_agg_tiff="time psql ${conn_str} -c \"${q_agg_sed}\""
#            echo "\$cmd_export_agg_tiff:: $cmd_export_agg_tiff"
            eval "$cmd_export_agg_tiff"
          done

        elif [[ "$indicator_type" == "bni" ]]; then

          ## agg bni query
          q_agg=$(<"${export_query_dir}/rast_agg_${indicator_type}_by_${level}_${raster_name}.sql")
#          echo "$q_agg"
          qc_agg=$(<"${export_query_dir}/rast_agg_${indicator_type}_by_${level}_${raster_name}_centroid.sql")
#          echo "$qc_agg"

          for field in "${bni_fields[@]}"; do
            read -a strarr <<<"$field" # uses default whitespace IFS
            attr=${strarr[0]}
            psql_dtype=${strarr[1]}
            attr_abbr=${strarr[2]}
            band_pix_type=${strarr[3]}
            echo ${attr} ${psql_dtype} ${attr_abbr} ${band_pix_type} ${export_dir}
            # sed -e 's|attr_abbr|'${attr_abbr}'|g' -e 's|band_pix_type|'${band_pix_type}'|g' -e 's|attr|'${attr}'|g' -e 's|psql_dtype|'${psql_dtype}'|g'  -e 's|export_dir|'${export_dir}'|g' < "../../query_output/d-export/rast_agg_bgi_by_cell_worldpop2020_1km.sql" > tempt.sql
            q_agg_sed=$(sed -e 's|attr_abbr|'${attr_abbr}'|g' -e 's|band_pix_type|'${band_pix_type}'|g' -e 's|attr|'${attr}'|g' -e 's|psql_dtype|'${psql_dtype}'|g' -e 's|export_dir|'${export_dir}'|g' <<< "$q_agg" )
            qc_agg_sed=$(sed -e 's|attr_abbr|'${attr_abbr}'|g' -e 's|band_pix_type|'${band_pix_type}'|g' -e 's|attr|'${attr}'|g' -e 's|psql_dtype|'${psql_dtype}'|g' -e 's|export_dir|'${export_dir}'|g' <<< "$qc_agg" )
#            echo $q_agg_sed
#            echo $qc_agg_sed

            ## agg export command
            cmd_export_agg_tiff="time psql ${conn_str} -c \"${q_agg_sed}\""
#            echo "\$cmd_export_agg_tiff:: $cmd_export_agg_tiff"
            eval "$cmd_export_agg_tiff"
            cmd_export_agg_c_tiff="time psql ${conn_str} -c \"${qc_agg_sed}\""
#            echo "\$cmd_export_agg_c_tiff:: $cmd_export_agg_c_tiff"
            eval "$cmd_export_agg_c_tiff"
          done

        fi
      done
      ;;
    esac
  done
done
