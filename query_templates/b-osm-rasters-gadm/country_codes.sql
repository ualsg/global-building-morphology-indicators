DROP TABLE IF EXISTS {{public_schema}}.country_codes CASCADE;

CREATE TABLE {{public_schema}}.country_codes (
	country_name_english varchar(1024) NULL,
	country_name_french varchar(1024) NULL,
	gadm_country varchar(1024) NULL,
	alpha2_code varchar(1024) NULL,
	alpha3_code varchar(1024) NULL,
	numeric_code int4 NULL
);

\COPY {{public_schema}}.country_codes FROM '{{ country_codes_dir }}/{{country_codes_file}}' WITH DELIMITER AS ',' CSV HEADER;