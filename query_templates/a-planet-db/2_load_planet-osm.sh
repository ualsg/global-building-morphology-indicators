#!/bin/bash
wget https://planet.osm.org/pbf/planet-latest.osm.pbf -O /data/planet-latest-dl.osm.pbf
time osm2pgsql --create --slim --cache 50000 --keep-coastlines --flat-nodes /data/tmp-osm --hstore --extra-attributes -l --host {{host_address}} --port 5432 --username {{superuser}} --password --database {{db_name}} /data/planet-latest-dl.osm.pbf
# exit

