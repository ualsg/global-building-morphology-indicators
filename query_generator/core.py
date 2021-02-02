import glob
from collections import OrderedDict
from pathlib import Path
from typing import List

from jinjasql import JinjaSql


class QueryParamsExpander:
    def __init__(self, key: str, params: dict):
        self.key = key
        self.params = params

    def _expand_a_planet_db_params(self) -> List[dict]:
        params_list = list()
        try:
            if "new_users" in self.params.keys() and len(self.params["new_users"]) > 0:
                for li in self.params["new_users"]:
                    for k, v in self.params.items():
                        if type(v) is str:
                            li[k] = v
                    params_list.append(li)
            else:
                popped = self.params.pop("new_users", None)
                params_list.append(self.params)
            return params_list
        except Exception as e:
            raise QueryParamsExpanderException(e)

    def _expand_b_rasters_and_gadm_params(self) -> List[dict]:
        params_list = list()
        if "rasters" in self.params.keys() and len(self.params["rasters"]) == 0:
            raise QueryParamsExpanderException(
                "`rasters` cannot be an empty list. Please define at least one raster.")

        if "agg_levels" in self.params.keys() and len(self.params["agg_levels"]) == 0:
            raise QueryParamsExpanderException(
                "`agg_levels` cannot be an empty list. Please define at least one set of parameters for at least one agg_level.")

        try:
            for agg_dict in self.params["agg_levels"]:
                for raster_dict in self.params["rasters"]:
                    a = dict(agg_dict, **raster_dict)
                    for k, v in self.params.items():
                        if type(v) is str:
                            a[k] = v
                    params_list.append(a)

            return params_list
        except Exception as e:
            raise QueryParamsExpanderException(e)

    def _expand_c_gbmi_params(self) -> List[dict]:
        params_list = list()
        if "raster_names" in self.params.keys() and len(self.params["raster_names"]) == 0:
            raise QueryParamsExpanderException(
                "`raster_names` cannot be an empty list. Please define at least one raster.")

        if "buffers" in self.params.keys() and len(self.params["buffers"]) == 0:
            raise QueryParamsExpanderException("`buffers` cannot be an empty list. Please define at least one buffer.")

        if "agg_levels" in self.params.keys() and len(self.params["agg_levels"]) == 0:
            raise QueryParamsExpanderException("`agg_levels` cannot be an empty list. Please define at least one set of parameters for at least one agg_level.")

        try:
            for dict_a in self.params["raster_names"]:
                for dict_b in self.params["buffers"]:
                    a = dict(dict_a, **dict_b)
                    for dict_c in self.params["agg_levels"]:
                        a = dict(a, **dict_c)
                        for k, v in self.params.items():
                            if type(v) is str:
                                a[k] = v
                        params_list.append(a)

            print(len(params_list))
            return params_list
        except Exception as e:
            raise QueryParamsExpanderException(e)

    def run(self):
        default_err = "No matching expansion function."
        func_name = '_expand_' + str(self.key).replace('-', '_') + '_params'
        try:
            return getattr(self, func_name, lambda: default_err)()
        except Exception as e:
            raise QueryParamsExpanderException(default_err)


class QueryGenerator:
    def __init__(self, template_dirname: str, output_dirname: str, params: dict, overwrite: bool = False,
                 debug_mode: bool = True):
        self.template_dir = self._validate_dir(template_dirname, mkdir=False, exist_ok=True)
        self.output_dir = self._validate_dir(output_dirname, mkdir=True, exist_ok=True)
        self.params = params
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

    def _output_sql_file(self, output_filepath: Path, query: str,
                         bind_params: OrderedDict):
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
        for sect_key, sect_params in self.params.items():
            bind_params_list = QueryParamsExpander(sect_key, sect_params).run()
            self._validate_dir(self.output_dir.name + '/' + sect_key, mkdir=True, exist_ok=True)
            j = JinjaSql(param_style="pyformat")
            for template_file in glob.glob(f'{self.template_dir.name}/{sect_key}/*'):
                with open(template_file, 'r') as f:
                    template = f.read()
                    for params in bind_params_list:
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
