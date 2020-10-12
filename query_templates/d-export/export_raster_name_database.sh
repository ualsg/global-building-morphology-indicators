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


# loop through levels
for level in "${export_levels[@]}"; do
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
    esac
  done
done
