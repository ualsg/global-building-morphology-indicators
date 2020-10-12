#!/bin/bash


host="{{ host_address }}"
database="{{ database }}"
query_dir="{{ db_script_dir }}"


# the following uses postgres user, please make sure password is defined in pg_hba.conf
create_db_cmd="psql -h ${host} -p 5432 -U postgres -w -f ${query_dir}/create_db_${database}.sql"
echo "$create_db_cmd"
eval "$create_db_cmd"

# the -w option prevents prompts for password, uses .pgpass
extension_cmd="psql -h ${host} -p 5432 -d ${database} -w -f ${query_dir}/install_extensions.sql"
echo "$extension_cmd"
eval "$extension_cmd"

# the -w option prevents prompts for password, uses .pgpass
schema_cmd="psql -h ${host} -p 5432 -d ${database} -w -f ${query_dir}/schema_setup.sql"
echo "$schema_cmd"
eval "$schema_cmd"

# the -w option prevents prompts for password, uses .pgpass
tbl_history_cmd="psql -h ${host} -p 5432 -d ${database} -w -f ${query_dir}/tbl_create_history.sql"
echo "$tbl_history_cmd"
#eval "$tbl_history_cmd"



declare -a superusers=({{ superusers }})

# the -w option prevents prompts for password, uses .pgpass
for susr in "${superusers[@]}"; do
  script="db_superuser_${database}_${susr}"
  cmd="time psql -h ${host} -p 5432 -d ${database} -w -f ${query_dir}/${script}.sql"
  echo "$cmd"
  eval "$cmd"
done



declare -a users=({{ users }})

# the -w option prevents prompts for password, uses .pgpass
for usr in "${users[@]}"; do
  script="db_user_${database}_${usr}"
  cmd="time psql -h ${host} -p 5432 -d ${database} -w -f ${query_dir}/${script}.sql"
  echo "$cmd"
  eval "$cmd"
done