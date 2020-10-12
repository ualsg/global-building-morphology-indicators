CREATE INDEX idx_planet_osm_point_tags ON {{db_schema}}.planet_osm_point USING gist(tags);
CREATE INDEX idx_planet_osm_polygon_tags ON {{db_schema}}.planet_osm_polygon USING gist(tags);
CREATE INDEX idx_planet_osm_line_tags ON {{db_schema}}.planet_osm_line USING gist(tags);
CREATE INDEX idx_planet_osm_polygon_admin_level ON {{db_schema}}.planet_osm_polygon(admin_level);
CREATE INDEX idx_planet_osm_polygon_boundary ON {{db_schema}}.planet_osm_polygon(boundary);
CREATE INDEX idx_planet_osm_polygon_name ON {{db_schema}}.planet_osm_polygon(name);
CREATE INDEX idx_planet_osm_polygon_building ON {{db_schema}}.planet_osm_polygon(building);
VACUUM analyse;