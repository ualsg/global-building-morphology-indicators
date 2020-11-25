import json
from pathlib import Path

import jsonschema


class ConfigKeys:
    template_dirname = "template_dirname"
    output_dirname = "output_dirname"
    parameters = "parameters"
    a_planet_db = "a-planet-db"
    host_address = "host_address"
    superuser = "superuser"
    db_name = "db_name"
    db_schema = "db_schema"
    gbmi_schema = "gbmi_schema"
    new_users = "new_users"
    new_user = "new_user"
    password = "password"
    b_rasters_and_gadm = "b-rasters-and-gadm"
    user = "user"
    gadm_source_file = "gadm_source_file"
    gadm_target_table = "gadm_target_table"
    rasters = "rasters"
    raster_name = "raster_name"
    raster_source_file = "raster_source_file"
    raster_target_table = "raster_target_table"
    c_gbmi = "c-gbmi"
    raster_names = "raster_names"
    buffers = "buffers"
    buffer = "buffer"
    overwrite = "overwrite"
    agg_levels = "agg_levels"
    source_table = "source_table"
    agg_level = "agg_level"
    agg_columns = "agg_columns"
    agg_geom = "agg_geom"
    agg_area = "agg_area"
    join_clause = "join_clause"
    order_columns = "order_columns"


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
                    ConfigKeys.a_planet_db: {
                        "type": "object",
                        "properties": {
                            ConfigKeys.host_address: {
                                "type": "string"
                            },
                            ConfigKeys.superuser: {
                                "type": "string"
                            },
                            ConfigKeys.db_name: {
                                "type": "string"
                            },
                            ConfigKeys.db_schema: {
                                "type": "string"
                            },
                            ConfigKeys.gbmi_schema: {
                                "type": "string"
                            },
                            ConfigKeys.new_users: {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        ConfigKeys.new_user: {"type": "string"},
                                        ConfigKeys.password: {"type": "string"}
                                    },
                                    "required": [ConfigKeys.new_user, ConfigKeys.password]
                                }
                            }
                        },
                        "required": [
                            ConfigKeys.host_address,
                            ConfigKeys.superuser,
                            ConfigKeys.db_name,
                            ConfigKeys.db_schema,
                            ConfigKeys.gbmi_schema]
                    },
                    ConfigKeys.b_rasters_and_gadm: {
                        "type": "object",
                        "properties": {
                            ConfigKeys.host_address: {
                                "type": "string"
                            },
                            ConfigKeys.db_name: {
                                "type": "string"
                            },
                            ConfigKeys.db_schema: {
                                "type": "string"
                            },
                            ConfigKeys.user: {
                                "type": "string"
                            },
                            ConfigKeys.gadm_source_file: {
                                "type": "string"
                            },
                            ConfigKeys.gadm_target_table: {
                                "type": "string"
                            },
                            ConfigKeys.rasters: {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        ConfigKeys.raster_name: {"type": "string"},
                                        ConfigKeys.raster_source_file: {"type": "string"},
                                        ConfigKeys.raster_target_table: {"type": "string"}
                                    },
                                    "required": [ConfigKeys.raster_name, ConfigKeys.raster_source_file, ConfigKeys.raster_target_table]
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
                        "required": [ConfigKeys.host_address, ConfigKeys.db_name, ConfigKeys.db_schema, ConfigKeys.user, ConfigKeys.gadm_source_file, ConfigKeys.gadm_target_table, ConfigKeys.rasters]
                    },
                    ConfigKeys.c_gbmi: {
                        "type": "object",
                        "properties": {
                            ConfigKeys.db_schema: {
                                "type": "string"
                            },
                            ConfigKeys.gbmi_schema: {
                                "type": "string"
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
                        "required": [ConfigKeys.db_schema, ConfigKeys.gbmi_schema, ConfigKeys.raster_names, ConfigKeys.buffers, ConfigKeys.agg_levels]
                    }
                },
                "required": [
                    ConfigKeys.a_planet_db,
                    ConfigKeys.b_rasters_and_gadm,
                    ConfigKeys.c_gbmi
                    ]
                },
            ConfigKeys.overwrite: {
                "type": "boolean"
            },
        },
        "required": [ConfigKeys.template_dirname, ConfigKeys.output_dirname, ConfigKeys.parameters,
                                     ConfigKeys.overwrite]
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
