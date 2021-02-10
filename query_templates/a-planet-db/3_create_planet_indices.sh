#!/bin/bash
psql "host={{host_address}} port=5432 user={{user}} dbname={{db_name}} password='xxxxxxxx'" -f planet_indices.sql

