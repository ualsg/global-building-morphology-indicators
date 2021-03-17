#!/bin/bash

declare -a agg_levels=('bldg' 'cell' 'country' 'admin_div1' 'admin_div2' 'admin_div3' 'admin_div4' 'admin_div5')
declare -a indicators_types=('bgi' 'bni')
declare -a formats=('shp' 'gpkg' 'csv')


field_mapping_file="{{ field_mapping_path }}"
export_base_dir="{{ export_base_dir }}"
database="{{ database }}"
host="{{ host_address }}"
user="{{ username }}"
password="{{ password }}"
res="100m"


for level in "${agg_levels[@]}"; do

  if [[ "$level" == "cell" ]]; then
    formats+=('geotiff')
  else
    del='geotiff'
    formats=("${formats[@]/$del}")
  fi

  for fmt in "${formats[@]}"; do
    echo
    echo
    echo "$database" "$level" "$fmt"

    if [[ "$level" == "bldg" ]]; then
      export_dir="${export_base_dir}/${database}/${level}"
      cmd_mkdir="[[ ! -d \"${export_dir}\" ]] && mkdir -p \"${export_dir}\""
    else
      export_dir="${export_base_dir}/${database}/agg_${level}"
      cmd_mkdir="[[ ! -d \"${export_dir}\" ]] && mkdir -p \"${export_dir}\""
    fi
    eval "$cmd_mkdir"

    case "$fmt" in
      "shp")
        echo "======================== SHP ========================"
        if [[ "$level" == "bldg" ]]; then
          # export building_indicators

          ## bldg query
          q_bldg=$(<buildings_indicators_by_worldpop2020_${res}.sql)
          qc_bldg="${q_bldg}_centroid"
#          echo "$q_bldg"
#          echo "$qc_bldg"
          ## bldg export command
          cmd_export_bldg="time pgsql2shp -h ${host} -p 5432 -u ${user} -P ${password} -m ${field_mapping_file} -f ${export_dir}/buildings_indicators_by_worldpop2020_${res} ${database} \"${q_bldg}\""
#          echo "\$cmd_export_bldg:: $cmd_export_bldg"
          eval "$cmd_export_bldg"
          cmd_export_bldg_c="time pgsql2shp -h ${host} -p 5432 -u ${user} -P ${password} -m ${field_mapping_file} -f ${export_dir}/buildings_indicators_by_worldpop2020_${res}_centroid ${database} \"${qc_bldg}\""
#          echo "\$cmd_export_bldg_c:: $cmd_export_bldg_c"
          eval "$cmd_export_bldg_c"

        else
          # export aggregated indicators by type
          for indicator_type in "${indicators_types[@]}"; do
            ## agg query
            q_agg=$(<agg_${indicator_type}_by_${level}_worldpop2020_${res}.sql)
#            echo "$q_agg"

            ## agg export command
            cmd_export_agg="time pgsql2shp -h ${host} -p 5432 -u ${user} -P ${password} -m ${field_mapping_file} -f ${export_dir}/agg_${indicator_type}_by_${level}_worldpop2020_${res} ${database} \"${q_agg}\""
#            echo "\$cmd_export_agg:: $cmd_export_agg"
            eval "$cmd_export_agg"

            if [[ "$indicator_type" == "bni" ]]; then
              qc_agg="${q_agg}_centroid"
#              echo "$qc_agg"
              cmd_export_agg_c="time pgsql2shp -h ${host} -p 5432 -u ${user} -P ${password} -m ${field_mapping_file} -f ${export_dir}/agg_${indicator_type}_by_${level}_worldpop2020_${res}_centroid ${database} \"${qc_agg}\""
#              echo "\$cmd_export_agg_c:: $cmd_export_agg_c"
              eval "$cmd_export_agg_c"
            fi
          done
        fi
        ;;


      "gpkg")
        echo "======================== GPKG ========================"
        if [[ "$level" == "bldg" ]]; then
          # convert building_indicators
          cmd_convert_bldg="time ogr2ogr -nlt PROMOTE_TO_MULTI -f GPKG ${export_dir}/buildings_indicators_by_worldpop2020_${res}.gpkg ${export_dir}/buildings_indicators_by_worldpop2020_${res}.shp"
#          echo "\$cmd_convert_bldg:: $cmd_convert_bldg"
          eval "$cmd_convert_bldg"
        else
          # convert aggregated indicators by type
          for indicator_type in "${indicators_types[@]}"; do
            cmd_convert_agg="time ogr2ogr -nlt PROMOTE_TO_MULTI -f GPKG ${export_dir}/agg_${indicator_type}_by_${level}_worldpop2020_${res}.gpkg ${export_dir}/agg_${indicator_type}_by_${level}_worldpop2020_${res}.shp"
#            echo "\$cmd_convert_agg:: $cmd_convert_agg"
            eval "$cmd_convert_agg"

            if [[ "$indicator_type" == "bni" ]]; then
              cmd_convert_agg_c="time ogr2ogr -nlt PROMOTE_TO_MULTI -f GPKG ${export_dir}/agg_${indicator_type}_by_${level}_worldpop2020_${res}_centroid.gpkg ${export_dir}/agg_${indicator_type}_by_${level}_worldpop2020_${res}_centroid.shp"
#              echo "\$cmd_convert_agg_c:: $cmd_convert_agg_c"
              eval "$cmd_convert_agg_c"
            fi
          done
        fi
        ;;


      "csv")
        echo "======================== CSV ========================"
        conn_str="\"dbname=${database} host=${host} user=${user} password=${password} port=5432 sslmode=require\""

        if [[ "$level" == "bldg" ]]; then
          # export building_indicators
          ## bldg query (WKT)
          q_bldg_wkt=$(<buildings_indicators_by_worldpop2020_${res}_wkt.sql)
          qc_bldg_wkt="${q_bldg_wkt}_centroid"
#          echo "$q_bldg_wkt"
#          echo "$qc_bldg_wkt"

          ## bldg export command (WKT)
          cmd_export_bldg_csv_wkt="time psql ${conn_str} -c \"COPY (${q_bldg_wkt}) To STDOUT With CSV HEADER DELIMITER ',';\" > ${export_dir}/buildings_indicators_by_worldpop2020_${res}_wkt.csv"
#          echo "\$cmd_export_bldg_csv_wkt:: $cmd_export_bldg_csv_wkt"
          eval "$cmd_export_bldg_csv_wkt"
          cmd_export_bldg_c_csv_wkt="time psql ${conn_str} -c \"COPY (${qc_bldg_wkt}) To STDOUT With CSV HEADER DELIMITER ',';\" > ${export_dir}/buildings_indicators_by_worldpop2020_${res}_centroid_wkt.csv"
#          echo "\$cmd_export_bldg_c_csv_wkt:: $cmd_export_bldg_c_csv_wkt"
          eval "$cmd_export_bldg_c_csv_wkt"

        else
          # convert aggregated indicators by type
          for indicator_type in "${indicators_types[@]}"; do
            ## agg query (WKT)
            q_agg_wkt=$(<agg_${indicator_type}_by_${level}_worldpop2020_${res}_wkt.sql)
#            echo "$q_agg_wkt"
            ## agg export command (WKT)
            cmd_export_agg_csv_wkt="time psql ${conn_str} -c \"COPY (${q_agg_wkt}) To STDOUT With CSV HEADER DELIMITER ',';\" > ${export_dir}/agg_${indicator_type}_by_${level}_worldpop2020_${res}_wkt.csv"
#            echo "\$cmd_export_agg_csv_wkt:: cmd_export_agg_csv_wkt"
            eval "$cmd_export_agg_csv_wkt"

            if [[ "$indicator_type" == "bni" ]]; then
              qc_agg_wkt="${q_agg}_centroid"
#              echo "$qc_agg_wkt"
              cmd_export_agg_c_csv_wkt="time psql ${conn_str} -c \"COPY (${qc_agg_wkt}) To STDOUT With CSV HEADER DELIMITER ',';\" > ${export_dir}/agg_${indicator_type}_by_${level}_worldpop2020_${res}_centroid_wkt.csv"
#              echo "\$cmd_export_agg_c_csv_wkt:: $cmd_export_agg_c_csv_wkt"
              eval "$cmd_export_agg_c_csv_wkt"
            fi
          done
        fi
        ;;


      "geotiff")
        cmd_export_to_tiff="time export geotiff command here"
        echo "\$cmd_export_to_tiff:: $cmd_export_to_tiff"
        ;;
    esac
  done
done

