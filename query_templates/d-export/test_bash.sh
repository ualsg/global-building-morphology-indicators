declare -a bgi_fields=(
    "buildings_count int bc 8BUI"
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
    "vertices_count_min int vc_min 8BUI"
    "vertices_count_median float vc_med 32BF"
    "vertices_count_mean float vc_mean 32BF"
    "vertices_count_max int vc_max 8BUI"
    "vertices_count_sum int vc_sm 8BUI"
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
)

q_agg=$(<"../../query_output/d-export/rast_agg_bgi_by_cell_worldpop2020_1km.sql")
echo $q_agg

#export_dir="${export_base_dir}/${database}/agg_${level}"
export_dir='\/data\/gbmi_export\/amsterdam\/agg_cell'
for field in "${bgi_fields[@]}"; do
  read -a strarr <<< "$field"  # uses default whitespace IFS
  attr=${strarr[0]}
  psql_dtype=${strarr[1]}
  attr_abbr=${strarr[2]}
  band_pix_type=${strarr[3]}
  echo ${attr} ${psql_dtype} ${attr_abbr} ${band_pix_type}
  sed -e 's/attr_abbr/'${attr_abbr}'/g' -e 's/band_pix_type/'${band_pix_type}'/g' -e 's/attr/'${attr}'/g' -e 's/psql_dtype/'${psql_dtype}'/g'  -e 's/export_dir/'${export_dir}'/g' < "../../query_output/d-export/rast_agg_bgi_by_cell_worldpop2020_1km.sql" > tempt.sql
  q_agg_sed=$(sed -e 's/attr_abbr/'${attr_abbr}'/g' -e 's/band_pix_type/'${band_pix_type}'/g' -e 's/attr/'${attr}'/g' -e 's/psql_dtype/'${psql_dtype}'/g' <<< "$q_agg")
  echo $q_agg_sed


done