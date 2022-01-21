#!/bin/bash

export_base_dir="{{ export_base_dir }}"
export_query_dir="{{ export_script_dir }}"
database="{{ database }}"
host="{{ host_address }}"
raster_name="{{ raster_name }}"

# define indicator types - geometric and neighbour
declare -a indicators_types=('bgi' 'bni')

# define field names and respective abbreviation tuples
declare -a bgi_fields=(
  "buildings_count int bc 16BUI"
  "buildings_count_normalised float bc_nm 32BF"
  "footprint_area_mean float fpa_mean 32BF"
  "footprint_area_sum float fpas 32BF"
  "footprint_area_sum_normalised float fpas_nm 32BF"
  "perimeter_mean float p_mean 32BF"
  "perimeter_sum float p_sm 32BF"
  "perimeter_sum_normalised float p_s_nm 32BF"
  "height_mean float h_mean 32BF"
  "height_weighted_mean float h_wmean 32BF"
  "ratio_height_to_footprint_area_mean float rhfpa_mean 32BF"
  "volume_mean float vol_mean 32BF"
  "volume_sum float vol_sm 32BF"
  "volume_sum_normalised float vol_s_nm 32BF"
  "wall_area_mean float wa_mean 32BF"
  "wall_area_sum float wa_sm 32BF"
  "wall_area_sum_normalised float wa_s_nm 32BF"
  "envelope_area_mean float ea_mean 32BF"
  "envelope_area_sum float ea_sm 32BF"
  "envelope_area_sum_normalised float ea_s_nm 32BF"
  "vertices_count_mean float vc_mean 32BF"
  "vertices_count_sum int vc_sm 16BUI"
  "vertices_count_sum_normalised float vc_s_nm 32BF"
  "complexity_mean float cplx_mean 32BF"
  "compactness_mean float cpct_mean 32BF"
  "equivalent_rectangular_index_mean float eri_mean 32BF"
  "azimuth_mean float az_mean 32BF"
  "mbr_length_mean float l_mean 32BF"
  "mbr_width_mean float w_mean 32BF"
  "mbr_area_mean float mbra_mean 32BF"
  "mbr_area_sum float mbra_sm 32BF"
  "mbr_area_sum_normalised float mbra_s_nm 32BF"
  "building:levels_mean float blvl_mean 32BF"
  "floor_area_mean float fla_mea 32BF"
  "floor_area_sum float fla_sm 32BF"
  "floor_area_sum_normalised float fla_s_nm 32BF"
  "residential_count float resc 32BF"
  "residential_count_normalised float resc_nm 32BF"
  "residential_floor_area_mean float rfla_mean 32BF"
  "residential_floor_area_sum float rfla_sm 32BF"
  "residential_floor_area_sum_normalised float rfla_s_nm 32BF"
)

# define field names and respective abbreviation tuples
declare -a bni_fields=(
  "buffer_area_25_mean float ba25_mean 32BF"
  "neighbour_25_count_mean float n25c_mean 32BF"
  "distance_25_mean float d25_mean 32BF"
  "neighbour_footprint_area_25_sum_mean float fpa25smean 32BF"
  "neighbour_footprint_area_25_mean float fpa25_mean 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_25_mean float rfb25_mean 32BF"
  "ratio_neighbour_height_to_distance_25_mean float rhd25_mean 32BF"
  "buffer_area_50_mean float ba50_mean 32BF"
  "neighbour_50_count_mean float n50c_mean 32BF"
  "distance_50_mean float d50_mean 32BF"
  "neighbour_footprint_area_50_sum_mean float fpa50smean 32BF"
  "neighbour_footprint_area_50_mean float fpa50_mean 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_50_mean float rfb50_mean 32BF"
  "ratio_neighbour_height_to_distance_50_mean float rhd50_mean 32BF"{% if not limit_buffer %}
  "buffer_area_100_mean float ba100_mean 32BF"
  "neighbour_100_count_mean float n100c_mean 32BF"
  "distance_100_mean float d100_mean 32BF"
  "neighbour_footprint_area_100_sum_mean float fpa100smen 32BF"
  "neighbour_footprint_area_100_mean float fpa100_mea 32BF"
  "ratio_neighbour_footprint_sum_to_buffer_100_mean float rfb100_mea 32BF"
  "ratio_neighbour_height_to_distance_100_mean float rhd100_mea 32BF"{% endif %})


export_dir="${export_base_dir}/${database}/agg_cell"
cmd_mkdir="[[ ! -d \"${export_dir}\" ]] && mkdir -p \"${export_dir}\""
eval "$cmd_mkdir"

echo "======================== GeoTiFF ========================"
conn_str="\"dbname=${database} host=${host} port=5432 sslmode=require\""

# convert aggregated indicators by type
for indicator_type in "${indicators_types[@]}"; do
  if [[ "$indicator_type" == "bgi" ]]; then

    ## agg bgi query
    q_agg=$(<"${export_query_dir}/rast_agg_${indicator_type}_by_cell_${raster_name}.sql")
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
#      echo $q_agg_sed

      ## agg bgi export command
      cmd_export_agg_tiff="time psql ${conn_str} -c \"${q_agg_sed}\""
#      echo "\$cmd_export_agg_tiff:: $cmd_export_agg_tiff"
      eval "$cmd_export_agg_tiff"
    done

  elif [[ "$indicator_type" == "bni" ]]; then

    ## agg bni query
    q_agg=$(<"${export_query_dir}/rast_agg_${indicator_type}_by_cell_${raster_name}.sql")
#          echo "$q_agg"
    qc_agg=$(<"${export_query_dir}/rast_agg_${indicator_type}_by_cell_${raster_name}_centroid.sql")
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
#      echo $q_agg_sed
#      echo $qc_agg_sed

      ## agg export command
      cmd_export_agg_tiff="time psql ${conn_str} -c \"${q_agg_sed}\""
#      echo "\$cmd_export_agg_tiff:: $cmd_export_agg_tiff"
      eval "$cmd_export_agg_tiff"
      cmd_export_agg_c_tiff="time psql ${conn_str} -c \"${qc_agg_sed}\""
#      echo "\$cmd_export_agg_c_tiff:: $cmd_export_agg_c_tiff"
      eval "$cmd_export_agg_c_tiff"
    done
  fi
done

