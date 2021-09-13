import json
from pathlib import Path

import jsonschema


class ConfigKeys:
    template_dirname = "template_dirname"
    output_dirname = "output_dirname"
    parameters = "parameters"
    common = "common"
    host_address = "host_address"
    db_script_dir = "db_script_dir"
    public_schema = "public_schema"
    public_script_dir = "public_script_dir"
    gbmi_schema = "gbmi_schema"
    gbmi_script_dir = "gbmi_script_dir"
    misc_schema = "misc_schema"
    misc_script_dir = "misc_script_dir"
    qa_schema = "qa_schema"
    qa_script_dir = "qa_script_dir"
    base_source_dir = "base_source_dir"
    site_source_dir = "site_source_dir"
    country_codes_dir = "country_codes_dir"
    country_codes_file = "country_codes_file"
    gadm_source_dir = "gadm_source_dir"
    gadm_source_file = "gadm_source_file"
    gadm_target_table = "gadm_target_table"
    export_base_dir = "export_base_dir"
    export_script_dir = "export_script_dir"
    a_db_setup = "a-db-setup"
    databases = "databases"
    database = "database"
    superusers = "superusers"
    new_superuser = "new_superuser"
    users = "users"
    new_user = "new_user"
    password = "password"
    b_osm_rasters_gadm = "b-osm-rasters-gadm"
    osm_source_files = "osm_source_files"
    osm_source_file = "osm_source_file"
    raster_names = "raster_names"
    raster_name = "raster_name"
    raster_file_suffix = "raster_file_suffix"
    agg_levels = "agg_levels"
    source_table = "source_table"
    agg_level = "agg_level"
    agg_geom = "agg_geom"
    agg_columns = "agg_columns"
    order_columns = "order_columns"
    c0_misc = "c0-misc"
    agg_area = "agg_area"
    join_clause = "join_clause"
    c1_gbmi = "c1-gbmi"
    raster_population = "raster_population"
    limit_buffer = "limit_buffer"
    buffers = "buffers"
    buffer = "buffer"
    d_export = "d-export"
    agg_geom_wkt = "agg_geom_wkt"
    overwrite = "overwrite"


class QueryGeneratorConfiguration:
    """
        Class for reading and validate the configurations
    """
    config_schema = {
        "type": "object",
        "properties": {
            ConfigKeys.template_dirname: {
                "type": "string"
            },
            ConfigKeys.output_dirname: {
                "type": "string"
            },
            ConfigKeys.parameters: {
                "type": "object",
                "properties": {
                    ConfigKeys.common: {
                        "type": "object",
                        "properties": {
                            ConfigKeys.host_address: {
                                "type": "string"
                            },
                            ConfigKeys.db_script_dir: {
                                "type": "string"
                            },
                            ConfigKeys.public_schema: {
                                "type": "string"
                            },
                            ConfigKeys.public_script_dir: {
                                "type": "string"
                            },
                            ConfigKeys.misc_schema: {
                                "type": "string"
                            },
                            ConfigKeys.misc_script_dir: {
                                "type": "string"
                            },
                            ConfigKeys.gbmi_schema: {
                                "type": "string"
                            },
                            ConfigKeys.gbmi_script_dir: {
                                "type": "string"
                            },
                            ConfigKeys.qa_schema: {
                                "type": "string"
                            },
                            ConfigKeys.qa_script_dir: {
                                "type": "string"
                            },
                            ConfigKeys.base_source_dir: {
                                "type": "string"
                            },
                            ConfigKeys.site_source_dir: {
                                "type": "string"
                            },
                            ConfigKeys.country_codes_dir: {
                                "type": "string"
                            },
                            ConfigKeys.country_codes_file: {
                                "type": "string"
                            },
                            ConfigKeys.gadm_source_dir: {
                                "type": "string"
                            },
                            ConfigKeys.gadm_source_file: {
                                "type": "string"
                            },
                            ConfigKeys.gadm_target_table: {
                                "type": "string"
                            },
                            ConfigKeys.export_script_dir: {
                                "type": "string"
                            },
                            ConfigKeys.export_base_dir: {
                                "type": "string"
                            }
                        },
                        "required": [ConfigKeys.host_address, ConfigKeys.db_script_dir, ConfigKeys.public_schema,
                                     ConfigKeys.public_script_dir, ConfigKeys.misc_schema, ConfigKeys.misc_script_dir,
                                     ConfigKeys.gbmi_schema, ConfigKeys.gbmi_script_dir, ConfigKeys.qa_schema,
                                     ConfigKeys.qa_script_dir, ConfigKeys.base_source_dir,
                                     ConfigKeys.country_codes_dir, ConfigKeys.country_codes_file,
                                     ConfigKeys.site_source_dir, ConfigKeys.gadm_source_file,
                                     ConfigKeys.gadm_target_table, ConfigKeys.export_script_dir,
                                     ConfigKeys.export_base_dir]
                    },
                    ConfigKeys.a_db_setup: {
                        "type": "object",
                        "properties": {
                            ConfigKeys.databases: {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        ConfigKeys.database: {"type": "string"}
                                    },
                                    "required": [ConfigKeys.database]
                                }
                            },
                            ConfigKeys.users: {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        ConfigKeys.new_user: {"type": "string"},
                                        ConfigKeys.password: {"type": "string"}
                                    },
                                    "required": [ConfigKeys.new_user, ConfigKeys.password]
                                }
                            },
                            ConfigKeys.superusers: {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        ConfigKeys.new_superuser: {"type": "string"},
                                        ConfigKeys.password: {"type": "string"}
                                    },
                                    "required": [ConfigKeys.new_superuser, ConfigKeys.password]
                                }
                            }
                        },
                        "required": [
                            ConfigKeys.databases]
                    },
                    ConfigKeys.b_osm_rasters_gadm: {
                        "type": "object",
                        "properties": {
                            ConfigKeys.osm_source_files: {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        ConfigKeys.database: {"type": "string"},
                                        ConfigKeys.osm_source_file: {"type": "string"}
                                    },
                                    "required": [ConfigKeys.database, ConfigKeys.osm_source_file]
                                }
                            },
                            ConfigKeys.raster_names: {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        ConfigKeys.raster_name: {"type": "string"},
                                        ConfigKeys.raster_file_suffix: {"type": "string"}
                                    },
                                    "required": [ConfigKeys.raster_name, ConfigKeys.raster_file_suffix]
                                }
                            },
                            ConfigKeys.agg_levels: {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        ConfigKeys.source_table: {"type": "string"},
                                        ConfigKeys.agg_level: {"type": "string"},
                                        ConfigKeys.agg_geom: {"type": "string"},
                                        ConfigKeys.agg_columns: {"type": "string"},
                                        ConfigKeys.order_columns: {"type": "string"}
                                    },
                                    "required": [ConfigKeys.source_table, ConfigKeys.agg_level, ConfigKeys.agg_geom, ConfigKeys.agg_columns, ConfigKeys.order_columns]
                                }
                            }
                        },
                        "required": [ConfigKeys.osm_source_files, ConfigKeys.raster_names, ConfigKeys.agg_levels]
                    },
                    ConfigKeys.c0_misc: {
                        "type": "object",
                        "properties": {
                            ConfigKeys.databases: {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        ConfigKeys.database: {"type": "string"}
                                    },
                                    "required": [ConfigKeys.database]
                                },
                            },
                            ConfigKeys.raster_names: {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        ConfigKeys.raster_name: {"type": "string"}
                                    },
                                    "required": [ConfigKeys.raster_name]
                                },
                            },
                            ConfigKeys.agg_levels: {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        ConfigKeys.agg_level: {"type": "string"},
                                        ConfigKeys.agg_columns: {"type": "string"},
                                        ConfigKeys.agg_geom: {"type": "string"},
                                        ConfigKeys.agg_area: {"type": "string"},
                                        ConfigKeys.join_clause: {"type": "string"},
                                        ConfigKeys.order_columns: {"type": "string"}
                                    },
                                    "required": [ConfigKeys.agg_level, ConfigKeys.agg_columns, ConfigKeys.agg_geom, ConfigKeys.agg_area, ConfigKeys.join_clause, ConfigKeys.order_columns]
                                }
                            }
                        },
                        "required": [ConfigKeys.databases, ConfigKeys.raster_names, ConfigKeys.agg_levels]
                    },
                    ConfigKeys.c1_gbmi: {
                        "type": "object",
                        "properties": {
                            ConfigKeys.databases: {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        ConfigKeys.database: {"type": "string"}
                                    },
                                    "required": [ConfigKeys.database]
                                },
                            },
                            ConfigKeys.raster_names: {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        ConfigKeys.raster_name: {"type": "string"},
                                        ConfigKeys.raster_population: {"type": "string"},
                                        ConfigKeys.limit_buffer: {"type": "integer"}
                                    },
                                    "required": [ConfigKeys.raster_name]
                                },
                            },
                            ConfigKeys.buffers: {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        ConfigKeys.buffer: {"type": "integer"}
                                    },
                                    "required": [ConfigKeys.buffer]
                                }
                            },
                            ConfigKeys.agg_levels: {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        ConfigKeys.agg_level: {"type": "string"},
                                        ConfigKeys.agg_columns: {"type": "string"},
                                        ConfigKeys.agg_geom: {"type": "string"},
                                        ConfigKeys.agg_area: {"type": "string"},
                                        ConfigKeys.join_clause: {"type": "string"},
                                        ConfigKeys.order_columns: {"type": "string"}
                                    },
                                    "required": [ConfigKeys.agg_level, ConfigKeys.agg_columns, ConfigKeys.agg_geom, ConfigKeys.agg_area, ConfigKeys.join_clause, ConfigKeys.order_columns]
                                }
                            }
                        },
                        "required": [ConfigKeys.databases, ConfigKeys.raster_names, ConfigKeys.buffers, ConfigKeys.agg_levels]
                    },
                    ConfigKeys.d_export: {
                        "type": "object",
                        "properties": {
                            ConfigKeys.databases: {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        ConfigKeys.database: {"type": "string"}
                                    },
                                    "required": [ConfigKeys.database]
                                },
                            },
                            ConfigKeys.raster_names: {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        ConfigKeys.raster_name: {"type": "string"}
                                    },
                                    "required": [ConfigKeys.raster_name]
                                },
                            },
                            ConfigKeys.agg_levels: {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        ConfigKeys.agg_level: {"type": "string"},
                                        ConfigKeys.agg_columns: {"type": "string"},
                                        ConfigKeys.agg_geom: {"type": "string"},
                                        ConfigKeys.agg_geom_wkt: {"type": "string"}
                                    },
                                    "required": [ConfigKeys.agg_level, ConfigKeys.agg_columns, ConfigKeys.agg_geom, ConfigKeys.agg_geom_wkt]
                                }
                            }
                        },
                        "required": [ConfigKeys.databases, ConfigKeys.raster_names, ConfigKeys.agg_levels]
                    }
                },
                "required": [
                    ConfigKeys.a_db_setup,
                    ConfigKeys.b_osm_rasters_gadm,
                    ConfigKeys.c0_misc,
                    ConfigKeys.c1_gbmi,
                    ConfigKeys.d_export,
                    ]
                },
            ConfigKeys.overwrite: {
                "type": "boolean"
            },
        },
        "required": [ConfigKeys.template_dirname, ConfigKeys.output_dirname, ConfigKeys.parameters, ConfigKeys.overwrite]
    }

    def __init__(self, config_file_path: str):
        self.config_file_path = config_file_path
        self.config = self._get_config_from_file()
        if self.config:
            self._validate_config()

    def get_parameter(self, key:str):
        return self.config[key]

    def _get_config_from_file(self) -> dict:
        if not Path(self.config_file_path).exists() or not Path(self.config_file_path).is_file():
            raise ConfigurationFileDoesNotExistException(
                f"Configuration file '{self.config_file_path}' doesn't exist"
            )
        try:
            with open(self.config_file_path) as config_file:
                return json.load(config_file)
        except Exception as e:
            raise ConfigurationLoadingException(str(e))

    def _validate_config(self):
        try:
            jsonschema.validate(self.config, self.config_schema)
        except jsonschema.ValidationError as ve:
            raise ConfigurationValidationUnexpectedException(
                f"Configuration is not valid.\n{ve.message}"
            )
        except Exception as e:
            raise ConfigurationValidationUnexpectedException(str(e))


class QueryGeneratorConfigurationException(Exception):
    """
    General Exception class for QueryGeneratorConfiguration class.
    """
    pass


class ConfigurationFileDoesNotExistException(QueryGeneratorConfigurationException):
    """
    The exception is raised when path to configuration file does not exist.
    """
    pass


class ConfigurationLoadingException(QueryGeneratorConfigurationException):
    """
    The exception is raised when is not possible read the content of
        configuration file (rights to read ...).
    """
    pass


class ConfigurationValidationUnexpectedException(QueryGeneratorConfigurationException):
    """
    The exception is raised when when during the validation of configuration
        file content occurred other exception then validation exception
        (jsonschema.ValidationError).
    """
    pass
