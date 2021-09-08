import glob
from collections import OrderedDict
from pathlib import Path
from typing import List

from jinjasql import JinjaSql


class QueryParamsExpander:
    def __init__(self, key: str, params: dict):
        self.key = key
        self.params = params

    def _expand_a_db_setup_params(self) -> List[dict]:
        params_list = list()
        superusers = ""
        users = ""
        if "databases" in self.params.keys() and len(self.params["databases"]) == 0:
            raise QueryParamsExpanderException(
                "`databases` cannot be an empty list. Please define at least one database name.")

        if "superusers" in self.params.keys() and len(self.params["superusers"]) > 0:
            for susr_dict in self.params["superusers"]:
                superusers += "\'" + susr_dict["new_superuser"] + "\' "
        superusers.strip()

        if "users" in self.params.keys() and len(self.params["users"]) > 0:
            for usr_dict in self.params["users"]:
                users += "\'" + usr_dict["new_user"] + "\' "
        users.strip()

        try:
            for db_dict in self.params["databases"]:
                if len(superusers.strip()) > 0:
                    db_dict["superusers"] = superusers.strip()
                if len(users.strip()) > 0:
                    db_dict["users"] = users.strip()
                if "superusers" in self.params.keys() and len(self.params["superusers"]) > 0:
                    for susr_dict in self.params["superusers"]:
                        a = dict(db_dict, **susr_dict)
                        if "users" in self.params.keys() and len(self.params["users"]) > 0:
                            for usr_dict in self.params["users"]:
                                a = dict(a, **usr_dict)
                                params_list.append(a)
                        else:
                            params_list.append(a)
                else:
                    if "users" in self.params.keys() and len(self.params["users"]) > 0:
                        for usr_dict in self.params["users"]:
                            a = dict(db_dict, **usr_dict)
                            params_list.append(a)
                    else:
                        params_list.append(db_dict)

            return params_list
        except Exception as e:
            raise QueryParamsExpanderException(e)

    def _expand_b_osm_rasters_gadm_params(self) -> List[dict]:
        params_list = list()
        if "osm_source_files" in self.params.keys() and len(self.params["osm_source_files"]) == 0:
            raise QueryParamsExpanderException(
                "`osm_source_files` cannot be an empty list. Please define at least one database and its corresponding OSM file.")

        if "raster_names" in self.params.keys() and len(self.params["raster_names"]) == 0:
            raise QueryParamsExpanderException(
                "`raster_names` cannot be an empty list. Please define at least one raster and its corresponding raster file suffix.")

        if "agg_levels" in self.params.keys() and len(self.params["agg_levels"]) == 0:
            raise QueryParamsExpanderException(
                "`agg_levels` cannot be an empty list. Please define at least one set of parameters for at least one agg_level.")

        try:
            for db_dict in self.params["osm_source_files"]:
                for raster_dict in self.params["raster_names"]:
                    a = dict(db_dict, **raster_dict)
                    for agg_dict in self.params["agg_levels"]:
                        a = dict(a, **agg_dict)
                        params_list.append(a)

            return params_list
        except Exception as e:
            raise QueryParamsExpanderException(e)

    def _expand_c0_misc_params(self) -> List[dict]:
        params_list = list()
        if "databases" in self.params.keys() and len(self.params["databases"]) == 0:
            raise QueryParamsExpanderException(
                "`databases` cannot be an empty list. Please define at least one database.")

        if "raster_names" in self.params.keys() and len(self.params["raster_names"]) == 0:
            raise QueryParamsExpanderException(
                "`raster_names` cannot be an empty list. Please define at least one raster.")

        if "agg_levels" in self.params.keys() and len(self.params["agg_levels"]) == 0:
            raise QueryParamsExpanderException(
                "`agg_levels` cannot be an empty list. Please define at least one set of parameters for at least one agg_level.")

        try:
            for db_dict in self.params["databases"]:
                for raster_dict in self.params["raster_names"]:
                    a = dict(db_dict, **raster_dict)
                    for agg_dict in self.params["agg_levels"]:
                        a = dict(a, **agg_dict)
                        params_list.append(a)

            return params_list
        except Exception as e:
            raise QueryParamsExpanderException(e)

    def _expand_c1_gbmi_params(self) -> List[dict]:
        params_list = list()
        agg_levels = ""
        if "databases" in self.params.keys() and len(self.params["databases"]) == 0:
            raise QueryParamsExpanderException("`databases` cannot be an empty list. Please define at least one database.")

        if "raster_names" in self.params.keys() and len(self.params["raster_names"]) == 0:
            raise QueryParamsExpanderException(
                "`raster_names` cannot be an empty list. Please define at least one raster.")

        if "buffers" in self.params.keys() and len(self.params["buffers"]) == 0:
            raise QueryParamsExpanderException("`buffers` cannot be an empty list. Please define at least one buffer.")

        if "agg_levels" in self.params.keys() and len(self.params["agg_levels"]) == 0:
            raise QueryParamsExpanderException(
                "`agg_levels` cannot be an empty list. Please define at least one set of parameters for at least one agg_level.")
        elif "agg_levels" in self.params.keys() and len(self.params["agg_levels"]) > 0:
            for agg_dict in self.params["agg_levels"]:
                agg_levels += "\'" + agg_dict["agg_level"] + "\' "
            agg_levels.strip()
        else:
            pass

        try:
            for db_dict in self.params["databases"]:
                if len(agg_levels.strip()) > 0:
                    db_dict["agg_levels"] = agg_levels.strip()
                for raster_dict in self.params["raster_names"]:
                    a = dict(db_dict, **raster_dict)
                    for buff_dict in self.params["buffers"]:
                        a = dict(a, **buff_dict)
                        for agg_dict in self.params["agg_levels"]:
                            a = dict(a, **agg_dict)
                            params_list.append(a)

            return params_list
        except Exception as e:
            raise QueryParamsExpanderException(e)

    def _expand_d_export_params(self) -> List[dict]:
        params_list = list()
        agg_levels = ""

        if "databases" in self.params.keys() and len(self.params["databases"]) == 0:
            raise QueryParamsExpanderException("`databases` cannot be an empty list. Please define at least one database.")

        if "raster_names" in self.params.keys() and len(self.params["raster_names"]) == 0:
            raise QueryParamsExpanderException(
                "`raster_names` cannot be an empty list. Please define at least one raster.")

        if "agg_levels" in self.params.keys() and len(self.params["agg_levels"]) == 0:
            raise QueryParamsExpanderException(
                "`agg_levels` cannot be an empty list. Please define at least one set of parameters for at least one agg_level.")
        elif "agg_levels" in self.params.keys() and len(self.params["agg_levels"]) > 0:
            for agg_dict in self.params["agg_levels"]:
                agg_levels += "\'" + agg_dict["agg_level"] + "\' "
            agg_levels.strip()
        else:
            pass

        try:
            for db_dict in self.params["databases"]:
                if len(agg_levels.strip()) > 0:
                    db_dict["agg_levels"] = agg_levels.strip()
                for raster_dict in self.params["raster_names"]:
                    a = dict(db_dict, **raster_dict)
                    for agg_dict in self.params["agg_levels"]:
                        a = dict(a, **agg_dict)
                        params_list.append(a)

            return params_list
        except Exception as e:
            raise QueryParamsExpanderException(e)

    def run(self):
        default_err = "No matching expansion function."
        func_name = '_expand_' + str(self.key).replace('-', '_') + '_params'
        try:
            return getattr(self, func_name, lambda: QueryParamsExpanderException(default_err))()
        except Exception as e:
            raise QueryParamsExpanderException(default_err)


class QueryGenerator:
    def __init__(self, template_dirname: str, output_dirname: str, common_params: dict, params: dict, sections: list = None,
                 overwrite: bool = False, debug_mode: bool = True):
        self.template_dir = self._validate_dir(template_dirname, mkdir=False, exist_ok=True)
        self.output_dir = self._validate_dir(output_dirname, mkdir=True, exist_ok=True)
        self.common_params = common_params
        self.params = params
        self.sections = sections
        self.overwrite = overwrite
        self.debug_mode = debug_mode

    @staticmethod
    def _validate_dir(dirname: str, mkdir: bool = False, exist_ok: bool = True) -> Path:
        if Path(dirname).exists() and Path(dirname).is_dir():
            return Path(dirname)
        else:
            if mkdir:
                Path(dirname).mkdir(parents=mkdir, exist_ok=exist_ok)
                return Path(dirname)
            else:
                return None

    def _output_sql_file(self, output_filepath: Path, query: str, bind_params: OrderedDict):
        with open(output_filepath, 'w') as f:
            if Path(output_filepath).exists() and Path(output_filepath).is_file() and not self.overwrite:
                raise QueryGeneratorException("File already exists and overwrite mode set to false.")
            f.write(query % bind_params)

    def _get_output_filepath(self, sect_key: str, orig_filename: str, params: dict) -> Path:
        new_filename = orig_filename
        for k, v in params.items():
            new_filename = new_filename.replace(k, str(v))
        return Path(self.output_dir).joinpath(sect_key).joinpath(new_filename)

    def run(self):
        # if no sections specified, run all sections (adding all keys to sections)
        if len(self.sections) == 0:
            self.sections = list()
            for sect_key, sect_params in self.params.items():
                self.sections.append(sect_key)

        for sect_key, sect_params in self.params.items():
            if sect_key in self.sections:
                bind_param_list = QueryParamsExpander(sect_key, sect_params).run()
                for params in bind_param_list:
                    for k, v in self.common_params.items():
                        params[k] = v
                self._validate_dir(self.output_dir.name + '/' + sect_key, mkdir=True, exist_ok=True)
                j = JinjaSql(param_style="pyformat")
                for template_file in glob.glob(f'{self.template_dir.name}/{sect_key}/*'):
                    with open(template_file, 'r') as f:
                        template = f.read()
                        for params in bind_param_list:
                            output_filepath = self._get_output_filepath(sect_key,
                                                                        Path(template_file).name,
                                                                        params)
                            query, params = j.prepare_query(template, params)
                            self._output_sql_file(output_filepath, query, params)


class QueryParamsExpanderException(Exception):
    """
    General Exception class for QueryParamsExpander class.
    """
    pass


class QueryGeneratorException(Exception):
    """
    General Exception class for QueryGenerator class.
    """
    pass
