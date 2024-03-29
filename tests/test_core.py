import filecmp
import shutil
from pathlib import Path
from typing import List
from unittest import TestCase, mock
from unittest.mock import Mock
from jinjasql import JinjaSql

from query_generator.configuration import ConfigurationLoadingException, QueryGeneratorConfiguration, \
    ConfigurationFileDoesNotExistException, ConfigurationValidationUnexpectedException
from query_generator.core import QueryParamsExpander, QueryParamsExpanderException, QueryGenerator
from query_generator.logging import Logger


class BaseTestQueryGenerator(TestCase):
    logger = Logger(debug=True)
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        cls.test_dir = Path("tests")
        cls.output_dir = Path("tests").joinpath("output")
        cls.fixture_dir = Path("tests").joinpath("fixtures")
        cls.templates_dir = Path("tests").joinpath("fixtures").joinpath("templates")

    def setUp(self):
        self._make_dir(self.output_dir)
        self._make_dir(self.fixture_dir)

    def tearDown(self):
        if self.output_dir.exists():
            print("removing test", self.output_dir)
            shutil.rmtree(self.output_dir)

    @staticmethod
    def _make_dir(dirpath: Path):
        dirpath.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _save_content(content: str, file_path: Path):
        with open(file_path, "w") as f:
            f.write(content)
            f.close()

    @staticmethod
    def _remove_content(file_path: Path):
        file_path.unlink()


class TestQueryGeneratorConfiguration(BaseTestQueryGenerator):
    def test__get_config_from_file(self):
        malformed_config_path = Path("tests/fixtures/malformed_config.json")
        self.assertRaises(
            ConfigurationLoadingException,
            QueryGeneratorConfiguration,
            malformed_config_path
        )
        non_existing_config_path = Path("tests/fixtures/non_exixting.json")
        self.assertRaises(
            ConfigurationFileDoesNotExistException,
            QueryGeneratorConfiguration,
            non_existing_config_path
        )

    def test__validate_config(self):
        invalid_config_path = Path("tests/fixtures/invalid_config.json")
        self.assertRaises(
            ConfigurationValidationUnexpectedException,
            QueryGeneratorConfiguration,
            invalid_config_path
        )

    def test_success(self):
        valid_config_path = Path("config.json")
        QueryGeneratorConfiguration(str(valid_config_path))


class TestQueryParamsExpander(BaseTestQueryGenerator):
    def setUp(self):
        self.common_key = "common"
        self.common_params = {
            "host_address": "localhost",
            "public_schema": "public",
            "public_script_dir": "/data/sql_scripts/osm-public",
            "misc_schema": "misc",
            "misc_script_dir": "/data/sql_scripts/osm-misc",
            "gbmi_schema": "gbmi",
            "gbmi_script_dir": "/data/sql_scripts/osm-gbmi",
            "qa_schema": "osm_qa",
            "qa_script_dir": "/data/sql_scripts/osm-qa",
            "base_source_dir": "/data/base-sources",
            "site_source_dir": "/data/gbmi-sources",
            "country_codes_dir": "/data/base-sources/country_codes",
            "country_codes_file": "country_codes.csv",
            "gadm_source_dir": "/data/base-sources/gadm36",
            "gadm_source_file": "gadm36.shp",
            "gadm_target_table": "gadm36",
            "export_user": "yoongshin",
            "export_base_dir": "/data/gbmi_export"
        }
        self.a_key = "a-db-setup"
        self.b_key = "b-osm-rasters-gadm"
        self.c0_key = "c0-misc"
        self.c1_key = "c1-gbmi"
        self.d_key = "d-export"
        self.invalid_key = "a-setup-db"
        self.invalid_params = {
            "host_address": "host",
            "superuser": "spuser",
            "db_name": "db",
            "user": "usr",
            "new_users": []
        }

    @staticmethod
    def _sorted_dicts(dicts: List[dict]):
        sorted_dicts = list()
        for a in dicts:
            sorted_a = dict()
            for i in sorted(a):
                sorted_a[i] = a[i]
            sorted_dicts.append(sorted_a)
        # sortedlist = sorted(sorted_dicts, key=lambda elem: "%02d %s" % (elem['age'], elem['name']))
        return sorted_dicts

    def test__ensure_non_empty_list(self):
        empty_list_params = {
            "cannot_be_empty": [],
            "not_empty": [{"key1": "val1"}, {"key2": "val2"}]}
        query_params_expander = QueryParamsExpander("some_key", empty_list_params)
        self.assertRaises(
            QueryParamsExpanderException,
            query_params_expander._ensure_non_empty_list,
            "cannot_be_empty")
        self.assertRaises(
            QueryParamsExpanderException,
            query_params_expander._ensure_non_empty_list,
            "cannot_be_empty")

    def test__concat_dict_values_separated_with_space(self):
        test_dict = {
            "folders": [
                {"folder": "a", "file": "1"}, {"folder": "b", "file": "2"}, {"folder": "c", "file": "3"}, {"folder": "d", "file": "4"}],
            "rasters": [
                {"raster": "r_a"}, {"raster": "r_b"}]
        }
        query_params_expander = QueryParamsExpander("some_key", test_dict)
        actual0 = query_params_expander._concat_dict_values_separated_with_space("folders", "folder")
        expected0 = "'a' 'b' 'c' 'd'"
        self.assertEqual(actual0, expected0)
        actual1 = query_params_expander._concat_dict_values_separated_with_space("folders", "file")
        expected1 = "'1' '2' '3' '4'"
        self.assertEqual(actual1, expected1)
        actual2 = query_params_expander._concat_dict_values_separated_with_space("rasters", "raster")
        expected2 = "'r_a' 'r_b'"
        self.assertEqual(actual2, expected2)

    def test__join_dicts(self):
        dicts = {"agg_levels": [{"agg_level": "cell"}, {"agg_level": "country"}, {"agg_level": "admin_div1"}],
                     "databases": [{"database": "earth"}, {"database": "moon"}],
                     "resolutions": [{"res": "100m"}, {"res": "1km"}],
                     "rasters": [{"raster": "worldpop"}]}
        dict_keys = list(dicts.keys())
        query_params_expander = QueryParamsExpander("some_key", dicts)
        actual = query_params_expander._join_dicts(dict_keys)
        expected = [{'agg_level': 'cell', 'database': 'earth', 'res': '100m', 'raster': 'worldpop'}, {'agg_level': 'cell', 'database': 'earth', 'res': '1km', 'raster': 'worldpop'}, {'agg_level': 'cell', 'database': 'moon', 'res': '100m', 'raster': 'worldpop'}, {'agg_level': 'cell', 'database': 'moon', 'res': '1km', 'raster': 'worldpop'}, {'agg_level': 'country', 'database': 'earth', 'res': '100m', 'raster': 'worldpop'}, {'agg_level': 'country', 'database': 'earth', 'res': '1km', 'raster': 'worldpop'}, {'agg_level': 'country', 'database': 'moon', 'res': '100m', 'raster': 'worldpop'}, {'agg_level': 'country', 'database': 'moon', 'res': '1km', 'raster': 'worldpop'}, {'agg_level': 'admin_div1', 'database': 'earth', 'res': '100m', 'raster': 'worldpop'}, {'agg_level': 'admin_div1', 'database': 'earth', 'res': '1km', 'raster': 'worldpop'}, {'agg_level': 'admin_div1', 'database': 'moon', 'res': '100m', 'raster': 'worldpop'}, {'agg_level': 'admin_div1', 'database': 'moon', 'res': '1km', 'raster': 'worldpop'}]
        self.assertEqual(actual, expected)

    def test_a(self):
        a_params = {
            "databases": [{"database": "planet"}, {"database": "melbourne"}, {"database": "switzerland"}],
            "users": [{"new_user": "filip", "password": "xsxsxsxs"}, {"new_user": "jupyter", "password": "xyxyxyxy"}],
            "superusers": [{"new_superuser": "yoongshin", "password": "yyyyyyyyy"}]}
        actual = QueryParamsExpander(self.a_key, a_params).run()
        expected = [{'database': 'planet', 'superusers': "'yoongshin'", 'users': "'filip' 'jupyter'",
                     'new_superuser': 'yoongshin', 'password': 'xsxsxsxs', 'new_user': 'filip'},
                    {'database': 'planet', 'superusers': "'yoongshin'", 'users': "'filip' 'jupyter'",
                     'new_superuser': 'yoongshin', 'password': 'xyxyxyxy', 'new_user': 'jupyter'},
                    {'database': 'melbourne', 'superusers': "'yoongshin'", 'users': "'filip' 'jupyter'",
                     'new_superuser': 'yoongshin', 'password': 'xsxsxsxs', 'new_user': 'filip'},
                    {'database': 'melbourne', 'superusers': "'yoongshin'", 'users': "'filip' 'jupyter'",
                     'new_superuser': 'yoongshin', 'password': 'xyxyxyxy', 'new_user': 'jupyter'},
                    {'database': 'switzerland', 'superusers': "'yoongshin'", 'users': "'filip' 'jupyter'",
                     'new_superuser': 'yoongshin', 'password': 'xsxsxsxs', 'new_user': 'filip'},
                    {'database': 'switzerland', 'superusers': "'yoongshin'", 'users': "'filip' 'jupyter'",
                     'new_superuser': 'yoongshin', 'password': 'xyxyxyxy', 'new_user': 'jupyter'}]
        self.assertEqual(actual, expected)

    def test_a_params_no_superusers(self):
        a_params_no_superusers = {
            "databases": [{"database": "planet"}, {"database": "melbourne"}, {"database": "switzerland"}],
            "users": [{"new_user": "filip", "password": "xsxsxsxs"}, {"new_user": "jupyter", "password": "xyxyxyxy"}]}
        actual = QueryParamsExpander(self.a_key, a_params_no_superusers).run()
        expected = [{'database': 'planet', 'users': "'filip' 'jupyter'", 'new_user': 'filip', 'password': 'xsxsxsxs'},
                    {'database': 'planet', 'users': "'filip' 'jupyter'", 'new_user': 'jupyter', 'password': 'xyxyxyxy'},
                    {'database': 'melbourne', 'users': "'filip' 'jupyter'", 'new_user': 'filip',
                     'password': 'xsxsxsxs'},
                    {'database': 'melbourne', 'users': "'filip' 'jupyter'", 'new_user': 'jupyter',
                     'password': 'xyxyxyxy'},
                    {'database': 'switzerland', 'users': "'filip' 'jupyter'", 'new_user': 'filip',
                     'password': 'xsxsxsxs'},
                    {'database': 'switzerland', 'users': "'filip' 'jupyter'", 'new_user': 'jupyter',
                     'password': 'xyxyxyxy'}]
        self.assertEqual(actual, expected)

    def test_a_params_no_users(self):
        a_params_no_users = {
            "databases": [{"database": "planet"}, {"database": "melbourne"}, {"database": "switzerland"}],
            "superusers": [{"new_superuser": "yoongshin", "password": "yyyyyyyyy"}]}
        actual = QueryParamsExpander(self.a_key, a_params_no_users).run()
        expected = [
            {'database': 'planet', 'superusers': "'yoongshin'", 'new_superuser': 'yoongshin', 'password': 'yyyyyyyyy'},
            {'database': 'melbourne', 'superusers': "'yoongshin'", 'new_superuser': 'yoongshin',
             'password': 'yyyyyyyyy'},
            {'database': 'switzerland', 'superusers': "'yoongshin'", 'new_superuser': 'yoongshin',
             'password': 'yyyyyyyyy'}]
        self.assertEqual(actual, expected)

    def test_b(self):
        b_params = {
            "osm_source_files": [
                {"database": "planet", "osm_source_file": "planet-20210216.osm.pbf"},
                {"database": "melbourne", "osm_source_file": "melbourne-20210308.osm.pbf"},
                {"database": "switzerland", "osm_source_file": "switzerland-20210803.osm.pbf"}],
            "raster_names": [
                {"raster_name": "worldpop2020_1km", "raster_file_suffix": "_ppp_2020_1km_Aggregated"},
                {"raster_name": "worldpop2020_100m", "raster_file_suffix": "_ppp_2020_100m"}],
            "agg_levels": [
                {"source_table": "xx", "agg_level": "a", "agg_geom": "agga.abc", "agg_columns": "b, c, d",
                 "order_columns": "d, c, a"},
                {"source_table": "aa", "agg_level": "x", "agg_geom": "agga.xyz", "agg_columns": "x, y, z",
                 "order_columns": "d, c, b"}]
        }
        actual = QueryParamsExpander(self.b_key, b_params).run()
        expected = [
            {'database': 'planet', 'osm_source_file': 'planet-20210216.osm.pbf', 'raster_name': 'worldpop2020_1km',
             'raster_file_suffix': '_ppp_2020_1km_Aggregated', 'source_table': 'xx', 'agg_level': 'a',
             'agg_geom': 'agga.abc', 'agg_columns': 'b, c, d', 'order_columns': 'd, c, a'},
            {'database': 'planet', 'osm_source_file': 'planet-20210216.osm.pbf', 'raster_name': 'worldpop2020_1km',
             'raster_file_suffix': '_ppp_2020_1km_Aggregated', 'source_table': 'aa', 'agg_level': 'x',
             'agg_geom': 'agga.xyz', 'agg_columns': 'x, y, z', 'order_columns': 'd, c, b'},
            {'database': 'planet', 'osm_source_file': 'planet-20210216.osm.pbf', 'raster_name': 'worldpop2020_100m',
             'raster_file_suffix': '_ppp_2020_100m', 'source_table': 'xx', 'agg_level': 'a', 'agg_geom': 'agga.abc',
             'agg_columns': 'b, c, d', 'order_columns': 'd, c, a'},
            {'database': 'planet', 'osm_source_file': 'planet-20210216.osm.pbf', 'raster_name': 'worldpop2020_100m',
             'raster_file_suffix': '_ppp_2020_100m', 'source_table': 'aa', 'agg_level': 'x', 'agg_geom': 'agga.xyz',
             'agg_columns': 'x, y, z', 'order_columns': 'd, c, b'},
            {'database': 'melbourne', 'osm_source_file': 'melbourne-20210308.osm.pbf',
             'raster_name': 'worldpop2020_1km', 'raster_file_suffix': '_ppp_2020_1km_Aggregated', 'source_table': 'xx',
             'agg_level': 'a', 'agg_geom': 'agga.abc', 'agg_columns': 'b, c, d', 'order_columns': 'd, c, a'},
            {'database': 'melbourne', 'osm_source_file': 'melbourne-20210308.osm.pbf',
             'raster_name': 'worldpop2020_1km', 'raster_file_suffix': '_ppp_2020_1km_Aggregated', 'source_table': 'aa',
             'agg_level': 'x', 'agg_geom': 'agga.xyz', 'agg_columns': 'x, y, z', 'order_columns': 'd, c, b'},
            {'database': 'melbourne', 'osm_source_file': 'melbourne-20210308.osm.pbf',
             'raster_name': 'worldpop2020_100m', 'raster_file_suffix': '_ppp_2020_100m', 'source_table': 'xx',
             'agg_level': 'a', 'agg_geom': 'agga.abc', 'agg_columns': 'b, c, d', 'order_columns': 'd, c, a'},
            {'database': 'melbourne', 'osm_source_file': 'melbourne-20210308.osm.pbf',
             'raster_name': 'worldpop2020_100m', 'raster_file_suffix': '_ppp_2020_100m', 'source_table': 'aa',
             'agg_level': 'x', 'agg_geom': 'agga.xyz', 'agg_columns': 'x, y, z', 'order_columns': 'd, c, b'},
            {'database': 'switzerland', 'osm_source_file': 'switzerland-20210803.osm.pbf',
             'raster_name': 'worldpop2020_1km', 'raster_file_suffix': '_ppp_2020_1km_Aggregated', 'source_table': 'xx',
             'agg_level': 'a', 'agg_geom': 'agga.abc', 'agg_columns': 'b, c, d', 'order_columns': 'd, c, a'},
            {'database': 'switzerland', 'osm_source_file': 'switzerland-20210803.osm.pbf',
             'raster_name': 'worldpop2020_1km', 'raster_file_suffix': '_ppp_2020_1km_Aggregated', 'source_table': 'aa',
             'agg_level': 'x', 'agg_geom': 'agga.xyz', 'agg_columns': 'x, y, z', 'order_columns': 'd, c, b'},
            {'database': 'switzerland', 'osm_source_file': 'switzerland-20210803.osm.pbf',
             'raster_name': 'worldpop2020_100m', 'raster_file_suffix': '_ppp_2020_100m', 'source_table': 'xx',
             'agg_level': 'a', 'agg_geom': 'agga.abc', 'agg_columns': 'b, c, d', 'order_columns': 'd, c, a'},
            {'database': 'switzerland', 'osm_source_file': 'switzerland-20210803.osm.pbf',
             'raster_name': 'worldpop2020_100m', 'raster_file_suffix': '_ppp_2020_100m', 'source_table': 'aa',
             'agg_level': 'x', 'agg_geom': 'agga.xyz', 'agg_columns': 'x, y, z', 'order_columns': 'd, c, b'}]
        self.assertEqual(actual, expected)

    def test_b_params_empty_osm_source_files(self):
        b_params_empty_osm_source_files = {
            "osm_source_files": [],
            "raster_names": [
                {"raster_name": "worldpop2020_1km", "raster_file_suffix": "_ppp_2020_1km_Aggregated"},
                {"raster_name": "worldpop2020_100m", "raster_file_suffix": "_ppp_2020_100m"}],
            "agg_levels": [
                {"source_table": "xx", "agg_level": "a", "agg_geom": "agga.abc", "agg_columns": "b, c, d",
                 "order_columns": "d, c, a"},
                {"source_table": "aa", "agg_level": "x", "agg_geom": "agga.xyz", "agg_columns": "x, y, z",
                 "order_columns": "d, c, b"}]
        }
        param_expander = QueryParamsExpander(self.b_key, b_params_empty_osm_source_files)
        self.assertRaises(
            QueryParamsExpanderException,
            param_expander.run
        )

    def test_b_params_empty_raster_names(self):
        b_params_empty_raster_names = {
            "osm_source_files": [
                {"database": "planet", "osm_source_file": "planet-20210216.osm.pbf"},
                {"database": "melbourne", "osm_source_file": "melbourne-20210308.osm.pbf"},
                {"database": "switzerland", "osm_source_file": "switzerland-20210803.osm.pbf"}],
            "raster_names": [],
            "agg_levels": [
                {"source_table": "xx", "agg_level": "a", "agg_geom": "agga.abc", "agg_columns": "b, c, d",
                 "order_columns": "d, c, a"},
                {"source_table": "aa", "agg_level": "x", "agg_geom": "agga.xyz", "agg_columns": "x, y, z",
                 "order_columns": "d, c, b"}]
        }
        param_expander = QueryParamsExpander(self.b_key, b_params_empty_raster_names)
        self.assertRaises(
            QueryParamsExpanderException,
            param_expander.run
        )

    def test_b_params_empty_agg_levels(self):
        b_params_empty_agg_levels = {
            "osm_source_files": [
                {"database": "planet", "osm_source_file": "planet-20210216.osm.pbf"},
                {"database": "melbourne", "osm_source_file": "melbourne-20210308.osm.pbf"},
                {"database": "switzerland", "osm_source_file": "switzerland-20210803.osm.pbf"}],
            "raster_names": [
                {"raster_name": "worldpop2020_1km", "raster_file_suffix": "_ppp_2020_1km_Aggregated"},
                {"raster_name": "worldpop2020_100m", "raster_file_suffix": "_ppp_2020_100m"}],
            "agg_levels": []
        }
        param_expander = QueryParamsExpander(self.b_key, b_params_empty_agg_levels)
        self.assertRaises(
            QueryParamsExpanderException,
            param_expander.run
        )

    def test_c0(self):
        c0_params = {
            "databases": [{"database": "planet"}, {"database": "melbourne"}, {"database": "switzerland"}],
            "raster_names": [{"raster_name": "worldpop2020_1km"}, {"raster_name": "worldpop2020_100m"}],
            "agg_levels": [
                {"agg_level": "a", "agg_columns": "b, c, d", "agg_geom": "agga.abc", "agg_area": "agga.xyz",
                 "join_clause": "", "order_columns": "d, c, a"},
                {"agg_level": "x", "agg_columns": "x, y, z", "agg_geom": "agga.xyz", "agg_area": "agga.abc",
                 "join_clause": "test.test on tt.a = b.a", "order_columns": "d, c, b"}]
        }
        actual = QueryParamsExpander(self.c0_key, c0_params).run()
        expected = [
            {'database': 'planet', 'raster_name': 'worldpop2020_1km', 'agg_level': 'a', 'agg_columns': 'b, c, d',
             'agg_geom': 'agga.abc', 'agg_area': 'agga.xyz', 'join_clause': '', 'order_columns': 'd, c, a'},
            {'database': 'planet', 'raster_name': 'worldpop2020_1km', 'agg_level': 'x', 'agg_columns': 'x, y, z',
             'agg_geom': 'agga.xyz', 'agg_area': 'agga.abc', 'join_clause': 'test.test on tt.a = b.a',
             'order_columns': 'd, c, b'},
            {'database': 'planet', 'raster_name': 'worldpop2020_100m', 'agg_level': 'a', 'agg_columns': 'b, c, d',
             'agg_geom': 'agga.abc', 'agg_area': 'agga.xyz', 'join_clause': '', 'order_columns': 'd, c, a'},
            {'database': 'planet', 'raster_name': 'worldpop2020_100m', 'agg_level': 'x', 'agg_columns': 'x, y, z',
             'agg_geom': 'agga.xyz', 'agg_area': 'agga.abc', 'join_clause': 'test.test on tt.a = b.a',
             'order_columns': 'd, c, b'},
            {'database': 'melbourne', 'raster_name': 'worldpop2020_1km', 'agg_level': 'a', 'agg_columns': 'b, c, d',
             'agg_geom': 'agga.abc', 'agg_area': 'agga.xyz', 'join_clause': '', 'order_columns': 'd, c, a'},
            {'database': 'melbourne', 'raster_name': 'worldpop2020_1km', 'agg_level': 'x', 'agg_columns': 'x, y, z',
             'agg_geom': 'agga.xyz', 'agg_area': 'agga.abc', 'join_clause': 'test.test on tt.a = b.a',
             'order_columns': 'd, c, b'},
            {'database': 'melbourne', 'raster_name': 'worldpop2020_100m', 'agg_level': 'a', 'agg_columns': 'b, c, d',
             'agg_geom': 'agga.abc', 'agg_area': 'agga.xyz', 'join_clause': '', 'order_columns': 'd, c, a'},
            {'database': 'melbourne', 'raster_name': 'worldpop2020_100m', 'agg_level': 'x', 'agg_columns': 'x, y, z',
             'agg_geom': 'agga.xyz', 'agg_area': 'agga.abc', 'join_clause': 'test.test on tt.a = b.a',
             'order_columns': 'd, c, b'},
            {'database': 'switzerland', 'raster_name': 'worldpop2020_1km', 'agg_level': 'a', 'agg_columns': 'b, c, d',
             'agg_geom': 'agga.abc', 'agg_area': 'agga.xyz', 'join_clause': '', 'order_columns': 'd, c, a'},
            {'database': 'switzerland', 'raster_name': 'worldpop2020_1km', 'agg_level': 'x', 'agg_columns': 'x, y, z',
             'agg_geom': 'agga.xyz', 'agg_area': 'agga.abc', 'join_clause': 'test.test on tt.a = b.a',
             'order_columns': 'd, c, b'},
            {'database': 'switzerland', 'raster_name': 'worldpop2020_100m', 'agg_level': 'a', 'agg_columns': 'b, c, d',
             'agg_geom': 'agga.abc', 'agg_area': 'agga.xyz', 'join_clause': '', 'order_columns': 'd, c, a'},
            {'database': 'switzerland', 'raster_name': 'worldpop2020_100m', 'agg_level': 'x', 'agg_columns': 'x, y, z',
             'agg_geom': 'agga.xyz', 'agg_area': 'agga.abc', 'join_clause': 'test.test on tt.a = b.a',
             'order_columns': 'd, c, b'}]
        self.assertEqual(actual, expected)

    def test_c0_params_empty_databases(self):
        c0_params_empty_databases = {
            "databases": [],
            "raster_names": [{"raster_name": "worldpop2020_1km"}, {"raster_name": "worldpop2020_100m"}],
            "agg_levels": [
                {"agg_level": "a", "agg_columns": "b, c, d", "agg_geom": "agga.abc", "agg_area": "agga.xyz",
                 "join_clause": "", "order_columns": "d, c, a"},
                {"agg_level": "x", "agg_columns": "x, y, z", "agg_geom": "agga.xyz", "agg_area": "agga.abc",
                 "join_clause": "test.test on tt.a = b.a", "order_columns": "d, c, b"}]
        }
        param_expander = QueryParamsExpander(self.c0_key, c0_params_empty_databases)
        self.assertRaises(
            QueryParamsExpanderException,
            param_expander.run
        )

    def test_c0_params_empty_raster_names(self):
        c0_params_empty_raster_names = {
            "databases": [{"database": "planet"}, {"database": "melbourne"}, {"database": "switzerland"}],
            "raster_names": [],
            "agg_levels": [
                {"agg_level": "a", "agg_columns": "b, c, d", "agg_geom": "agga.abc", "agg_area": "agga.xyz",
                 "join_clause": "", "order_columns": "d, c, a"},
                {"agg_level": "x", "agg_columns": "x, y, z", "agg_geom": "agga.xyz", "agg_area": "agga.abc",
                 "join_clause": "test.test on tt.a = b.a", "order_columns": "d, c, b"}]
        }
        param_expander = QueryParamsExpander(self.c0_key, c0_params_empty_raster_names)
        self.assertRaises(
            QueryParamsExpanderException,
            param_expander.run
        )

    def test_c0_params_empty_agg_levels(self):
        c0_params_empty_agg_levels = {
            "databases": [{"database": "planet"}, {"database": "melbourne"}, {"database": "switzerland"}],
            "raster_names": [{"raster_name": "worldpop2020_1km"}, {"raster_name": "worldpop2020_100m"}],
            "agg_levels": []
        }
        param_expander = QueryParamsExpander(self.c0_key, c0_params_empty_agg_levels)
        self.assertRaises(
            QueryParamsExpanderException,
            param_expander.run
        )

    def test_c1(self):
        c1_params = {
            "databases": [{"database": "planet"}, {"database": "melbourne"}, {"database": "switzerland"}],
            "raster_names": [
                {"raster_name": "worldpop2020_1km", "raster_population": "cell_population", "limit_buffer": 50},
                {"raster_name": "worldpop2020_100m", "raster_population": "cell_population"}],
            "buffers": [{"buffer": 25}, {"buffer": 50}, {"buffer": 100}],
            "agg_levels": [
                {"agg_level": "a", "agg_columns": "b, c, d", "agg_geom": "agga.abc", "agg_area": "agga.xyz",
                 "join_clause": "", "order_columns": "d, c, a"},
                {"agg_level": "x", "agg_columns": "x, y, z", "agg_geom": "agga.xyz", "agg_area": "agga.abc",
                 "join_clause": "test.test on tt.a = b.a", "order_columns": "d, c, b"}]
        }
        actual = QueryParamsExpander(self.c1_key, c1_params).run()
        expected = [{'agg_area': 'agga.xyz', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_level': 'a',
                     'agg_levels': "'a' 'x'", 'buffer': 25, 'database': 'planet', 'join_clause': '', 'limit_buffer': 50,
                     'order_columns': 'd, c, a', 'raster_name': 'worldpop2020_1km',
                     'raster_population': 'cell_population'},
                    {'agg_area': 'agga.xyz', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_level': 'a',
                     'agg_levels': "'a' 'x'", 'buffer': 50, 'database': 'planet', 'join_clause': '', 'limit_buffer': 50,
                     'order_columns': 'd, c, a', 'raster_name': 'worldpop2020_1km',
                     'raster_population': 'cell_population'},
                    {'agg_area': 'agga.xyz', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_level': 'a',
                     'agg_levels': "'a' 'x'", 'buffer': 100, 'database': 'planet', 'join_clause': '',
                     'limit_buffer': 50, 'order_columns': 'd, c, a', 'raster_name': 'worldpop2020_1km',
                     'raster_population': 'cell_population'},
                    {'agg_area': 'agga.abc', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_level': 'x',
                     'agg_levels': "'a' 'x'", 'buffer': 25, 'database': 'planet',
                     'join_clause': 'test.test on tt.a = b.a', 'limit_buffer': 50, 'order_columns': 'd, c, b',
                     'raster_name': 'worldpop2020_1km', 'raster_population': 'cell_population'},
                    {'agg_area': 'agga.abc', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_level': 'x',
                     'agg_levels': "'a' 'x'", 'buffer': 50, 'database': 'planet',
                     'join_clause': 'test.test on tt.a = b.a', 'limit_buffer': 50, 'order_columns': 'd, c, b',
                     'raster_name': 'worldpop2020_1km', 'raster_population': 'cell_population'},
                    {'agg_area': 'agga.abc', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_level': 'x',
                     'agg_levels': "'a' 'x'", 'buffer': 100, 'database': 'planet',
                     'join_clause': 'test.test on tt.a = b.a', 'limit_buffer': 50, 'order_columns': 'd, c, b',
                     'raster_name': 'worldpop2020_1km', 'raster_population': 'cell_population'},
                    {'agg_area': 'agga.xyz', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_level': 'a',
                     'agg_levels': "'a' 'x'", 'buffer': 25, 'database': 'planet', 'join_clause': '',
                     'order_columns': 'd, c, a', 'raster_name': 'worldpop2020_100m',
                     'raster_population': 'cell_population'},
                    {'agg_area': 'agga.xyz', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_level': 'a',
                     'agg_levels': "'a' 'x'", 'buffer': 50, 'database': 'planet', 'join_clause': '',
                     'order_columns': 'd, c, a', 'raster_name': 'worldpop2020_100m',
                     'raster_population': 'cell_population'},
                    {'agg_area': 'agga.xyz', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_level': 'a',
                     'agg_levels': "'a' 'x'", 'buffer': 100, 'database': 'planet', 'join_clause': '',
                     'order_columns': 'd, c, a', 'raster_name': 'worldpop2020_100m',
                     'raster_population': 'cell_population'},
                    {'agg_area': 'agga.abc', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_level': 'x',
                     'agg_levels': "'a' 'x'", 'buffer': 25, 'database': 'planet',
                     'join_clause': 'test.test on tt.a = b.a', 'order_columns': 'd, c, b',
                     'raster_name': 'worldpop2020_100m', 'raster_population': 'cell_population'},
                    {'agg_area': 'agga.abc', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_level': 'x',
                     'agg_levels': "'a' 'x'", 'buffer': 50, 'database': 'planet',
                     'join_clause': 'test.test on tt.a = b.a', 'order_columns': 'd, c, b',
                     'raster_name': 'worldpop2020_100m', 'raster_population': 'cell_population'},
                    {'agg_area': 'agga.abc', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_level': 'x',
                     'agg_levels': "'a' 'x'", 'buffer': 100, 'database': 'planet',
                     'join_clause': 'test.test on tt.a = b.a', 'order_columns': 'd, c, b',
                     'raster_name': 'worldpop2020_100m', 'raster_population': 'cell_population'},
                    {'agg_area': 'agga.xyz', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_level': 'a',
                     'agg_levels': "'a' 'x'", 'buffer': 25, 'database': 'melbourne', 'join_clause': '',
                     'limit_buffer': 50, 'order_columns': 'd, c, a', 'raster_name': 'worldpop2020_1km',
                     'raster_population': 'cell_population'},
                    {'agg_area': 'agga.xyz', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_level': 'a',
                     'agg_levels': "'a' 'x'", 'buffer': 50, 'database': 'melbourne', 'join_clause': '',
                     'limit_buffer': 50, 'order_columns': 'd, c, a', 'raster_name': 'worldpop2020_1km',
                     'raster_population': 'cell_population'},
                    {'agg_area': 'agga.xyz', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_level': 'a',
                     'agg_levels': "'a' 'x'", 'buffer': 100, 'database': 'melbourne', 'join_clause': '',
                     'limit_buffer': 50, 'order_columns': 'd, c, a', 'raster_name': 'worldpop2020_1km',
                     'raster_population': 'cell_population'},
                    {'agg_area': 'agga.abc', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_level': 'x',
                     'agg_levels': "'a' 'x'", 'buffer': 25, 'database': 'melbourne',
                     'join_clause': 'test.test on tt.a = b.a', 'limit_buffer': 50, 'order_columns': 'd, c, b',
                     'raster_name': 'worldpop2020_1km', 'raster_population': 'cell_population'},
                    {'agg_area': 'agga.abc', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_level': 'x',
                     'agg_levels': "'a' 'x'", 'buffer': 50, 'database': 'melbourne',
                     'join_clause': 'test.test on tt.a = b.a', 'limit_buffer': 50, 'order_columns': 'd, c, b',
                     'raster_name': 'worldpop2020_1km', 'raster_population': 'cell_population'},
                    {'agg_area': 'agga.abc', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_level': 'x',
                     'agg_levels': "'a' 'x'", 'buffer': 100, 'database': 'melbourne',
                     'join_clause': 'test.test on tt.a = b.a', 'limit_buffer': 50, 'order_columns': 'd, c, b',
                     'raster_name': 'worldpop2020_1km', 'raster_population': 'cell_population'},
                    {'agg_area': 'agga.xyz', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_level': 'a',
                     'agg_levels': "'a' 'x'", 'buffer': 25, 'database': 'melbourne', 'join_clause': '',
                     'order_columns': 'd, c, a', 'raster_name': 'worldpop2020_100m',
                     'raster_population': 'cell_population'},
                    {'agg_area': 'agga.xyz', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_level': 'a',
                     'agg_levels': "'a' 'x'", 'buffer': 50, 'database': 'melbourne', 'join_clause': '',
                     'order_columns': 'd, c, a', 'raster_name': 'worldpop2020_100m',
                     'raster_population': 'cell_population'},
                    {'agg_area': 'agga.xyz', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_level': 'a',
                     'agg_levels': "'a' 'x'", 'buffer': 100, 'database': 'melbourne', 'join_clause': '',
                     'order_columns': 'd, c, a', 'raster_name': 'worldpop2020_100m',
                     'raster_population': 'cell_population'},
                    {'agg_area': 'agga.abc', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_level': 'x',
                     'agg_levels': "'a' 'x'", 'buffer': 25, 'database': 'melbourne',
                     'join_clause': 'test.test on tt.a = b.a', 'order_columns': 'd, c, b',
                     'raster_name': 'worldpop2020_100m', 'raster_population': 'cell_population'},
                    {'agg_area': 'agga.abc', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_level': 'x',
                     'agg_levels': "'a' 'x'", 'buffer': 50, 'database': 'melbourne',
                     'join_clause': 'test.test on tt.a = b.a', 'order_columns': 'd, c, b',
                     'raster_name': 'worldpop2020_100m', 'raster_population': 'cell_population'},
                    {'agg_area': 'agga.abc', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_level': 'x',
                     'agg_levels': "'a' 'x'", 'buffer': 100, 'database': 'melbourne',
                     'join_clause': 'test.test on tt.a = b.a', 'order_columns': 'd, c, b',
                     'raster_name': 'worldpop2020_100m', 'raster_population': 'cell_population'},
                    {'agg_area': 'agga.xyz', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_level': 'a',
                     'agg_levels': "'a' 'x'", 'buffer': 25, 'database': 'switzerland', 'join_clause': '',
                     'limit_buffer': 50, 'order_columns': 'd, c, a', 'raster_name': 'worldpop2020_1km',
                     'raster_population': 'cell_population'},
                    {'agg_area': 'agga.xyz', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_level': 'a',
                     'agg_levels': "'a' 'x'", 'buffer': 50, 'database': 'switzerland', 'join_clause': '',
                     'limit_buffer': 50, 'order_columns': 'd, c, a', 'raster_name': 'worldpop2020_1km',
                     'raster_population': 'cell_population'},
                    {'agg_area': 'agga.xyz', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_level': 'a',
                     'agg_levels': "'a' 'x'", 'buffer': 100, 'database': 'switzerland', 'join_clause': '',
                     'limit_buffer': 50, 'order_columns': 'd, c, a', 'raster_name': 'worldpop2020_1km',
                     'raster_population': 'cell_population'},
                    {'agg_area': 'agga.abc', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_level': 'x',
                     'agg_levels': "'a' 'x'", 'buffer': 25, 'database': 'switzerland',
                     'join_clause': 'test.test on tt.a = b.a', 'limit_buffer': 50, 'order_columns': 'd, c, b',
                     'raster_name': 'worldpop2020_1km', 'raster_population': 'cell_population'},
                    {'agg_area': 'agga.abc', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_level': 'x',
                     'agg_levels': "'a' 'x'", 'buffer': 50, 'database': 'switzerland',
                     'join_clause': 'test.test on tt.a = b.a', 'limit_buffer': 50, 'order_columns': 'd, c, b',
                     'raster_name': 'worldpop2020_1km', 'raster_population': 'cell_population'},
                    {'agg_area': 'agga.abc', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_level': 'x',
                     'agg_levels': "'a' 'x'", 'buffer': 100, 'database': 'switzerland',
                     'join_clause': 'test.test on tt.a = b.a', 'limit_buffer': 50, 'order_columns': 'd, c, b',
                     'raster_name': 'worldpop2020_1km', 'raster_population': 'cell_population'},
                    {'agg_area': 'agga.xyz', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_level': 'a',
                     'agg_levels': "'a' 'x'", 'buffer': 25, 'database': 'switzerland', 'join_clause': '',
                     'order_columns': 'd, c, a', 'raster_name': 'worldpop2020_100m',
                     'raster_population': 'cell_population'},
                    {'agg_area': 'agga.xyz', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_level': 'a',
                     'agg_levels': "'a' 'x'", 'buffer': 50, 'database': 'switzerland', 'join_clause': '',
                     'order_columns': 'd, c, a', 'raster_name': 'worldpop2020_100m',
                     'raster_population': 'cell_population'},
                    {'agg_area': 'agga.xyz', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_level': 'a',
                     'agg_levels': "'a' 'x'", 'buffer': 100, 'database': 'switzerland', 'join_clause': '',
                     'order_columns': 'd, c, a', 'raster_name': 'worldpop2020_100m',
                     'raster_population': 'cell_population'},
                    {'agg_area': 'agga.abc', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_level': 'x',
                     'agg_levels': "'a' 'x'", 'buffer': 25, 'database': 'switzerland',
                     'join_clause': 'test.test on tt.a = b.a', 'order_columns': 'd, c, b',
                     'raster_name': 'worldpop2020_100m', 'raster_population': 'cell_population'},
                    {'agg_area': 'agga.abc', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_level': 'x',
                     'agg_levels': "'a' 'x'", 'buffer': 50, 'database': 'switzerland',
                     'join_clause': 'test.test on tt.a = b.a', 'order_columns': 'd, c, b',
                     'raster_name': 'worldpop2020_100m', 'raster_population': 'cell_population'},
                    {'agg_area': 'agga.abc', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_level': 'x',
                     'agg_levels': "'a' 'x'", 'buffer': 100, 'database': 'switzerland',
                     'join_clause': 'test.test on tt.a = b.a', 'order_columns': 'd, c, b',
                     'raster_name': 'worldpop2020_100m', 'raster_population': 'cell_population'}]
        self.assertEqual(actual, expected)

    def test_c1_params_empty_databases(self):
        c1_params_empty_databases = {
            "databases": [],
            "raster_names": [
                {"raster_name": "worldpop2020_1km", "raster_population": "cell_population", "limit_buffer": 50},
                {"raster_name": "worldpop2020_100m", "raster_population": "cell_population"}],
            "buffers": [{"buffer": 25}, {"buffer": 50}, {"buffer": 100}],
            "agg_levels": [
                {"agg_level": "a", "agg_columns": "b, c, d", "agg_geom": "agga.abc", "agg_area": "agga.xyz",
                 "join_clause": "", "order_columns": "d, c, a"},
                {"agg_level": "x", "agg_columns": "x, y, z", "agg_geom": "agga.xyz", "agg_area": "agga.abc",
                 "join_clause": "test.test on tt.a = b.a", "order_columns": "d, c, b"}]
        }
        param_expander = QueryParamsExpander(self.c1_key, c1_params_empty_databases)
        self.assertRaises(
            QueryParamsExpanderException,
            param_expander.run
        )

    def test_c1_params_empty_raster_names(self):
        c1_params_empty_raster_names = {
            "databases": [{"database": "planet"}, {"database": "melbourne"}, {"database": "switzerland"}],
            "buffers": [{"buffer": 25}, {"buffer": 50}, {"buffer": 100}],
            "raster_names": [],
            "agg_levels": [
                {"agg_level": "a", "agg_columns": "b, c, d", "agg_geom": "agga.abc", "agg_area": "agga.xyz",
                 "join_clause": "", "order_columns": "d, c, a"},
                {"agg_level": "x", "agg_columns": "x, y, z", "agg_geom": "agga.xyz", "agg_area": "agga.abc",
                 "join_clause": "test.test on tt.a = b.a", "order_columns": "d, c, b"}]
        }
        param_expander = QueryParamsExpander(self.c1_key, c1_params_empty_raster_names)
        self.assertRaises(
            QueryParamsExpanderException,
            param_expander.run
        )

    def test_c1_params_empty_buffers(self):
        c1_params_empty_buffers = {
            "databases": [{"database": "planet"}, {"database": "melbourne"}, {"database": "switzerland"}],
            "raster_names": [
                {"raster_name": "worldpop2020_1km", "raster_population": "cell_population", "limit_buffer": 50},
                {"raster_name": "worldpop2020_100m", "raster_population": "cell_population"}],
            "buffers": [],
            "agg_levels": [
                {"agg_level": "a", "agg_columns": "b, c, d", "agg_geom": "agga.abc", "agg_area": "agga.xyz",
                 "join_clause": "", "order_columns": "d, c, a"},
                {"agg_level": "x", "agg_columns": "x, y, z", "agg_geom": "agga.xyz", "agg_area": "agga.abc",
                 "join_clause": "test.test on tt.a = b.a", "order_columns": "d, c, b"}]
        }
        param_expander = QueryParamsExpander(self.c1_key, c1_params_empty_buffers)
        self.assertRaises(
            QueryParamsExpanderException,
            param_expander.run
        )

    def test_c1_params_empty_agg_levels(self):
        c1_params_empty_agg_levels = {
            "databases": [{"database": "planet"}, {"database": "melbourne"}, {"database": "switzerland"}],
            "raster_names": [
                {"raster_name": "worldpop2020_1km", "raster_population": "cell_population", "limit_buffer": 50},
                {"raster_name": "worldpop2020_100m", "raster_population": "cell_population"}],
            "buffers": [{"buffer": 25}, {"buffer": 50}, {"buffer": 100}],
            "agg_levels": []
        }
        param_expander = QueryParamsExpander(self.c1_key, c1_params_empty_agg_levels)
        self.assertRaises(
            QueryParamsExpanderException,
            param_expander.run
        )

    def test_d(self):
        d_params = {
            "databases": [{"database": "planet"}, {"database": "melbourne"}, {"database": "switzerland"}],
            "raster_names": [{"raster_name": "worldpop2020_1km"}, {"raster_name": "worldpop2020_100m"}],
            "agg_levels": [
                {"agg_level": "a", "agg_columns": "b, c, d", "agg_geom": "agga.abc", "agg_geom_wkt": "agga.xyz"},
                {"agg_level": "x", "agg_columns": "x, y, z", "agg_geom": "agga.xyz", "agg_geom_wkt": "agga.abc"}]
        }
        actual = QueryParamsExpander(self.d_key, d_params).run()
        expected = [{'database': 'planet', 'agg_levels': "'a' 'x'", 'raster_name': 'worldpop2020_1km', 'agg_level': 'a',
                     'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_geom_wkt': 'agga.xyz'},
                    {'database': 'planet', 'agg_levels': "'a' 'x'", 'raster_name': 'worldpop2020_1km', 'agg_level': 'x',
                     'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_geom_wkt': 'agga.abc'},
                    {'database': 'planet', 'agg_levels': "'a' 'x'", 'raster_name': 'worldpop2020_100m',
                     'agg_level': 'a', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_geom_wkt': 'agga.xyz'},
                    {'database': 'planet', 'agg_levels': "'a' 'x'", 'raster_name': 'worldpop2020_100m',
                     'agg_level': 'x', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_geom_wkt': 'agga.abc'},
                    {'database': 'melbourne', 'agg_levels': "'a' 'x'", 'raster_name': 'worldpop2020_1km',
                     'agg_level': 'a', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_geom_wkt': 'agga.xyz'},
                    {'database': 'melbourne', 'agg_levels': "'a' 'x'", 'raster_name': 'worldpop2020_1km',
                     'agg_level': 'x', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_geom_wkt': 'agga.abc'},
                    {'database': 'melbourne', 'agg_levels': "'a' 'x'", 'raster_name': 'worldpop2020_100m',
                     'agg_level': 'a', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_geom_wkt': 'agga.xyz'},
                    {'database': 'melbourne', 'agg_levels': "'a' 'x'", 'raster_name': 'worldpop2020_100m',
                     'agg_level': 'x', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_geom_wkt': 'agga.abc'},
                    {'database': 'switzerland', 'agg_levels': "'a' 'x'", 'raster_name': 'worldpop2020_1km',
                     'agg_level': 'a', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_geom_wkt': 'agga.xyz'},
                    {'database': 'switzerland', 'agg_levels': "'a' 'x'", 'raster_name': 'worldpop2020_1km',
                     'agg_level': 'x', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_geom_wkt': 'agga.abc'},
                    {'database': 'switzerland', 'agg_levels': "'a' 'x'", 'raster_name': 'worldpop2020_100m',
                     'agg_level': 'a', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_geom_wkt': 'agga.xyz'},
                    {'database': 'switzerland', 'agg_levels': "'a' 'x'", 'raster_name': 'worldpop2020_100m',
                     'agg_level': 'x', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_geom_wkt': 'agga.abc'}]
        self.assertEqual(actual, expected)

    def test_d_params_empty_databases(self):
        d_params_empty_databases = {
            "databases": [],
            "raster_names": [{"raster_name": "worldpop2020_1km"}, {"raster_name": "worldpop2020_100m"}],
            "agg_levels": [
                {"agg_level": "a", "agg_columns": "b, c, d", "agg_geom": "agga.abc", "agg_geom_wkt": "agga.xyz"},
                {"agg_level": "x", "agg_columns": "x, y, z", "agg_geom": "agga.xyz", "agg_geom_wkt": "agga.abc"}]
        }
        param_expander = QueryParamsExpander(self.d_key, d_params_empty_databases)
        self.assertRaises(
            QueryParamsExpanderException,
            param_expander.run
        )

    def test_d_params_empty_raster_names(self):
        d_params_empty_raster_names = {
            "databases": [{"database": "planet"}, {"database": "melbourne"}, {"database": "switzerland"}],
            "raster_names": [],
            "agg_levels": [
                {"agg_level": "a", "agg_columns": "b, c, d", "agg_geom": "agga.abc", "agg_geom_wkt": "agga.xyz"},
                {"agg_level": "x", "agg_columns": "x, y, z", "agg_geom": "agga.xyz", "agg_geom_wkt": "agga.abc"}]
        }
        param_expander = QueryParamsExpander(self.d_key, d_params_empty_raster_names)
        self.assertRaises(
            QueryParamsExpanderException,
            param_expander.run
        )

    def test_d_params_empty_agg_levels(self):
        d_params_empty_agg_levels = {
            "databases": [{"database": "planet"}, {"database": "melbourne"}, {"database": "switzerland"}],
            "raster_names": [{"raster_name": "worldpop2020_1km"}, {"raster_name": "worldpop2020_100m"}],
            "agg_levels": []
        }
        param_expander = QueryParamsExpander(self.d_key, d_params_empty_agg_levels)
        self.assertRaises(
            QueryParamsExpanderException,
            param_expander.run
        )

    def test_failure(self):
        param_expander_output = QueryParamsExpander(self.invalid_key, self.invalid_params).run()
        self.assertIsInstance(
            param_expander_output,
            QueryParamsExpanderException
        )


class TestQueryGenerator(BaseTestQueryGenerator):
    def setUp(self):
        self.mock_params = {"gbmi_schema": "gbmi", "raster_name": "worldpop", "schema": "public",
                            "table_name": "table1"}
        self.mock_common_params = {"host_address": "localhost", "public_schema": "public", "misc_schema": "misc",
                                   "gbmi_schema": "gbmi", "qa_schema": "osm_qa",
                                   "base_source_dir": "/data/base-sources",
                                   "gbmi_source_dir": "/data/gbmi-sources", "export_base_dir": "/data/gbmi_export"}
        self.mock_overwrite = True
        self.mock_debug = True
        self.q_generator = QueryGenerator(
            template_dirname=self.templates_dir,
            output_dirname=self.output_dir,
            common_params=self.mock_common_params,
            params=self.mock_params,
            overwrite=self.mock_overwrite,
            debug_mode=self.mock_debug)

    def test__validate_dir(self):
        val_dirname = "tests/output/val_dir"
        mkdir = True
        exist_ok = True
        val_dir = self.q_generator._validate_dir(val_dirname, mkdir, exist_ok)
        self.assertTrue(val_dir.exists())
        self.assertTrue(val_dir.is_dir())

    def test__get_output_filepath(self):
        orig_filename = "gbmi_schema_raster_name.sql"
        actual = self.q_generator._get_output_filepath("z-test", orig_filename, self.mock_params)
        expected = Path("tests/output/z-test/gbmi_worldpop.sql")
        self.assertEqual(actual, expected)

    def test__output_sql_file(self):
        template_file = "tests/fixtures/templates/gbmi_schema_raster_name.sql"
        actual = Path("tests/output/gbmi_worldpop.sql")
        j = JinjaSql(param_style="pyformat")
        with open(template_file, 'r') as f:
            template = f.read()
            query, params = j.prepare_query(template, self.mock_params)
            self.q_generator._output_sql_file(actual, query, params)
        expected = Path("tests/fixtures/gbmi_worldpop.sql")
        self.assertTrue(filecmp.cmp(actual, expected, shallow=False))
