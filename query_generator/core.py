import glob
from collections import OrderedDict
from pathlib import Path
from typing import List, Tuple

from jinjasql import JinjaSql


class QueryParamsExpander:
    def __init__(self, key: str, params: dict):
        self.key = key
        self.params = params

    def _ensure_non_empty_list(self, param_key: str):
        if param_key in self.params.keys() and len(self.params[param_key]) == 0:
            raise QueryParamsExpanderException(f"`{param_key}` cannot be an empty list. Please define at least one {param_key[:-1]}.")

    def _concat_dict_values_separated_with_space(self, dict_key: str, value_key: str) -> str:
        concat_str = ""
        for dict_in_list in self.params[dict_key]:
            concat_str += "\'" + dict_in_list[value_key] + "\' "
        return concat_str.strip()

    def _join_dicts(self, dict_keys: List[str]) -> List[dict]:
        base_dicts = list()
        updated_param_dicts = list()
        for dict_idx, dict_key in enumerate(dict_keys):
            param_dicts = self.params[dict_key]
            if dict_idx == 0:
                base_dicts = param_dicts
            else:
                if len(updated_param_dicts) > 0:
                    base_dicts = updated_param_dicts
                temp_list = list()
                for base_dict_idx, base_dict in enumerate(base_dicts):
                    for param_dict_idx, param_dict in enumerate(param_dicts):
                        a = dict(base_dict, **param_dict)
                        temp_list.append(a)
                updated_param_dicts = temp_list
        return updated_param_dicts

    def _expand_a_db_setup_params(self) -> List[dict]:
        try:
            dict_keys = ["databases"]
            users, superusers = str(), str()
            if "superusers" in self.params.keys() and len(self.params["superusers"]) > 0:
                superusers = self._concat_dict_values_separated_with_space("superusers", "new_superuser")
                superusers = superusers.strip()
                dict_keys.append("superusers")

            if "users" in self.params.keys() and len(self.params["users"]) > 0:
                users = self._concat_dict_values_separated_with_space("users", "new_user")
                users = users.strip()
                dict_keys.append("users")

            for dict_key in dict_keys:
                self._ensure_non_empty_list(dict_key)

            params_list = self._join_dicts(dict_keys)
            for param_dict in params_list:
                if len(superusers.strip()) > 0:
                    param_dict["superusers"] = superusers
                if len(users.strip()) > 0:
                    param_dict["users"] = users
            return params_list
        except Exception as e:
            raise QueryParamsExpanderException(e)

    def _expand_b_osm_rasters_gadm_params(self) -> List[dict]:
        try:
            dict_keys = ["osm_source_files", "raster_names", "agg_levels"]
            for dict_key in dict_keys:
                self._ensure_non_empty_list(dict_key)

            params_list = self._join_dicts(dict_keys)
            return params_list
        except Exception as e:
            raise QueryParamsExpanderException(e)

    def _expand_c0_misc_params(self) -> List[dict]:
        try:
            dict_keys = ["databases", "raster_names", "agg_levels"]
            for dict_key in dict_keys:
                self._ensure_non_empty_list(dict_key)

            params_list = self._join_dicts(dict_keys)
            return params_list
        except Exception as e:
            raise QueryParamsExpanderException(e)

    def _expand_c1_gbmi_params(self) -> List[dict]:
        try:
            agg_levels = str()
            if "agg_levels" in self.params.keys() and len(self.params["agg_levels"]) > 0:
                agg_levels = self._concat_dict_values_separated_with_space("agg_levels", "agg_level")
                agg_levels = agg_levels.strip()

            dict_keys = ["databases", "raster_names", "agg_levels", "buffers"]
            for dict_key in dict_keys:
                self._ensure_non_empty_list(dict_key)

            params_list = self._join_dicts(dict_keys)
            for param_dict in params_list:
                if len(agg_levels.strip()) > 0:
                    param_dict["agg_levels"] = agg_levels

            return params_list
        except Exception as e:
            raise QueryParamsExpanderException(e)

    def _expand_d_export_params(self) -> List[dict]:
        try:
            agg_levels = str()
            if "agg_levels" in self.params.keys() and len(self.params["agg_levels"]) > 0:
                agg_levels = self._concat_dict_values_separated_with_space("agg_levels", "agg_level")
                agg_levels = agg_levels.strip()

            dict_keys = ["databases", "raster_names", "agg_levels"]
            for dict_key in dict_keys:
                self._ensure_non_empty_list(dict_key)

            params_list = self._join_dicts(dict_keys)
            for param_dict in params_list:
                if len(agg_levels.strip()) > 0:
                    param_dict["agg_levels"] = agg_levels

            return params_list
        except Exception as e:
            raise QueryParamsExpanderException(e)

    def run(self):
        default_err = "No matching expansion function."
        func_name = '_expand_' + str(self.key).replace('-', '_') + '_params'
        print(func_name)
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
                return

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
