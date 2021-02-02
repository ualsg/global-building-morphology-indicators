import json
import json
import shutil
from pathlib import Path
from unittest import TestCase
import filecmp

from jinjasql import JinjaSql

from query_generator.core import QueryParamsExpander, QueryParamsExpanderException, QueryGenerator
from query_generator.configuration import ConfigurationLoadingException, QueryGeneratorConfiguration, \
    ConfigurationFileDoesNotExistException, ConfigurationValidationUnexpectedException
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
        QueryGeneratorConfiguration(valid_config_path)


class TestQueryParamsExpander(BaseTestQueryGenerator):
    def setUp(self):
        self.a_key = "a-planet-db"
        self.a_params = {
            "host_address": "host",
            "superuser": "spuser",
            "db_name": "db",
            "db_schema": "db_schema",
            "gbmi_schema": "gbmi_schema",
            "new_users": []
        }
        self.a_params_single = {
            "host_address": "host",
            "superuser": "spuser",
            "db_name": "db",
            "db_schema": "db_schema",
            "gbmi_schema": "gbmi_schema",
            "new_users": [{"new_user": "a", "password": "abc"}]
        }
        self.a_params_multiple = {
            "host_address": "host",
            "superuser": "spuser",
            "db_name": "db",
            "db_schema": "db_schema",
            "gbmi_schema": "gbmi_schema",
            "new_users": [{"new_user": "a", "password": "abc"}, {"new_user": "x", "password": "xyz"}]
        }
        self.b_key = "b-rasters-and-gadm"
        self.b_params = {
            "host_address": "host",
            "db_name": "db",
            "db_schema": "db_schema",
            "user": "usr",
            "gadm_source_file": "gadm_source_file",
            "gadm_target_table": "gadm_target_table",
            "rasters": []
        }
        self.b_params_single = {
            "host_address": "host",
            "db_name": "db",
            "db_schema": "db_schema",
            "user": "usr",
            "gadm_source_file": "gadm_source_file",
            "gadm_target_table": "gadm_target_table",
            "rasters": [{"raster_name": "raster_name1", "raster_source_file": "raster_src1", "raster_target_table": "raster_tbl1"}]
        }
        self.b_params_multiple = {
            "host_address": "host",
            "db_name": "db",
            "db_schema": "db_schema",
            "user": "usr",
            "gadm_source_file": "gadm_source_file",
            "gadm_target_table": "gadm_target_table",
            "rasters": [{"raster_name": "raster_name1", "raster_source_file": "raster_src1", "raster_target_table": "raster_tbl1"}, {"raster_name": "raster_name2", "raster_source_file": "raster_src2", "raster_target_table": "raster_tbl2"}]
        }
        self.c_key = "c-gbmi"
        self.c_params = {
            "db_schema": "db_schema",
            "gbmi_schema": "gbmi_schema",
            "raster_names": [{"raster_name": "raster1"}, {"raster_name": "raster2"}],
            "buffers": [{"buffer": 10}, {"buffer": 20}],
            "agg_levels": [{"agg_level": "a", "agg_columns": "b, c, d", "agg_geom": "agga.abc", "agg_area": "agga.xyz", "join_clause": "", "order_columns": "d, c, a"},
                           {"agg_level": "x", "agg_columns": "x, y, z", "agg_geom": "agga.xyz", "agg_area": "agga.abc", "join_clause": "test.test on tt.a = b.a", "order_columns": "d, c, b"}]
        }
        self.c_params_empty_buffers = {
            "db_schema": "db_schema",
            "gbmi_schema": "gbmi_schema",
            "raster_names": [{"raster_name": "raster1"}, {"raster_name": "raster2"}],
            "buffers": [],
            "agg_levels": [{"agg_level": "a", "agg_columns": "b, c, d", "agg_geom": "agga.abc", "agg_area": "agga.xyz",
                            "join_clause": "", "order_columns": "d, c, a"},
                           {"agg_level": "x", "agg_columns": "x, y, z", "agg_geom": "agga.xyz", "agg_area": "agga.abc",
                            "join_clause": "test.test on tt.a = b.a", "order_columns": "d, c, b"}]

        }
        self.c_params_empty_rasters = {
            "db_schema": "db_schema",
            "gbmi_schema": "gbmi_schema",
            "raster_names": [],
            "buffers": [{"buffer": 10}, {"buffer": 20}],
            "agg_levels": [{"agg_level": "a", "agg_columns": "b, c, d", "agg_geom": "agga.abc", "agg_area": "agga.xyz",
                            "join_clause": "", "order_columns": "d, c, a"},
                           {"agg_level": "x", "agg_columns": "x, y, z", "agg_geom": "agga.xyz", "agg_area": "agga.abc",
                            "join_clause": "test.test on tt.a = b.a", "order_columns": "d, c, b"}]

        }
        self.c_params_empty_agg_labels = {
            "db_schema": "db_schema",
            "gbmi_schema": "gbmi_schema",
            "raster_names": [{"raster_name": "raster1"}, {"raster_name": "raster2"}],
            "buffers": [{"buffer": 10}, {"buffer": 20}],
            "agg_levels": []

        }
        self.c_params_empty_both = {
            "db_schema": "db_schema",
            "gbmi_schema": "gbmi_schema",
            "raster_names": [],
            "buffers": []
        }
        self.invalid_key = "a-planet-db"
        self.invalid_params = {
            "host_address": "host",
            "superuser": "spuser",
            "db_name": "db",
            "user": "usr",
            "new_users": []
        }

    def test_a(self):
        actual = QueryParamsExpander(self.a_key, self.a_params).run()
        expected = [{'host_address': 'host', 'superuser': 'spuser', 'db_name': 'db', 'db_schema': 'db_schema', 'gbmi_schema': 'gbmi_schema'}]
        self.assertEqual(actual, expected)

    def test_a_single(self):
        actual = QueryParamsExpander(self.a_key, self.a_params_single).run()
        expected = [{'new_user': 'a', 'password': 'abc', 'host_address': 'host', 'superuser': 'spuser', 'db_name': 'db', 'db_schema': 'db_schema', 'gbmi_schema': 'gbmi_schema'}]
        self.assertEqual(actual, expected)

    def test_a_multiple(self):
        actual = QueryParamsExpander(self.a_key, self.a_params_multiple).run()
        expected = [{'new_user': 'a', 'password': 'abc', 'host_address': 'host', 'superuser': 'spuser', 'db_name': 'db', 'db_schema': 'db_schema', 'gbmi_schema': 'gbmi_schema'}, {'new_user': 'x', 'password': 'xyz', 'host_address': 'host', 'superuser': 'spuser', 'db_name': 'db', 'db_schema': 'db_schema', 'gbmi_schema': 'gbmi_schema'}]
        self.assertEqual(actual, expected)

    def test_b(self):
        actual = QueryParamsExpander(self.b_key, self.b_params).run()
        expected = [{'host_address': 'host', 'db_name': 'db', 'db_schema': 'db_schema', 'user': 'usr', 'gadm_source_file': 'gadm_source_file', 'gadm_target_table': 'gadm_target_table'}]
        self.assertEqual(actual, expected)

    def test_b_single(self):
        actual = QueryParamsExpander(self.b_key, self.b_params_single).run()
        expected = [{'raster_name': 'raster_name1', 'raster_source_file': 'raster_src1', 'raster_target_table': 'raster_tbl1', 'host_address': 'host', 'db_name': 'db', 'db_schema': 'db_schema', 'user': 'usr', 'gadm_source_file': 'gadm_source_file', 'gadm_target_table': 'gadm_target_table'}]
        self.assertEqual(actual, expected)

    def test_b_multiple(self):
        actual = QueryParamsExpander(self.b_key, self.b_params_multiple).run()
        expected = [{'raster_name': 'raster_name1', 'raster_source_file': 'raster_src1', 'raster_target_table': 'raster_tbl1', 'host_address': 'host', 'db_name': 'db', 'db_schema': 'db_schema', 'user': 'usr', 'gadm_source_file': 'gadm_source_file', 'gadm_target_table': 'gadm_target_table'}, {'raster_name': 'raster_name2', 'raster_source_file': 'raster_src2', 'raster_target_table': 'raster_tbl2', 'host_address': 'host', 'db_name': 'db', 'db_schema': 'db_schema', 'user': 'usr', 'gadm_source_file': 'gadm_source_file', 'gadm_target_table': 'gadm_target_table'}]
        self.assertEqual(actual, expected)

    def test_c(self):
        actual = QueryParamsExpander(self.c_key, self.c_params).run()
        expected = [{'raster_name': 'raster1', 'buffer': 10, 'agg_level': 'a', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_area': 'agga.xyz', 'join_clause': '', 'order_columns': 'd, c, a', 'db_schema': 'db_schema', 'gbmi_schema': 'gbmi_schema'}, {'raster_name': 'raster1', 'buffer': 10, 'agg_level': 'x', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_area': 'agga.abc', 'join_clause': 'test.test on tt.a = b.a', 'order_columns': 'd, c, b', 'db_schema': 'db_schema', 'gbmi_schema': 'gbmi_schema'}, {'raster_name': 'raster1', 'buffer': 20, 'agg_level': 'a', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_area': 'agga.xyz', 'join_clause': '', 'order_columns': 'd, c, a', 'db_schema': 'db_schema', 'gbmi_schema': 'gbmi_schema'}, {'raster_name': 'raster1', 'buffer': 20, 'agg_level': 'x', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_area': 'agga.abc', 'join_clause': 'test.test on tt.a = b.a', 'order_columns': 'd, c, b', 'db_schema': 'db_schema', 'gbmi_schema': 'gbmi_schema'}, {'raster_name': 'raster2', 'buffer': 10, 'agg_level': 'a', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_area': 'agga.xyz', 'join_clause': '', 'order_columns': 'd, c, a', 'db_schema': 'db_schema', 'gbmi_schema': 'gbmi_schema'}, {'raster_name': 'raster2', 'buffer': 10, 'agg_level': 'x', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_area': 'agga.abc', 'join_clause': 'test.test on tt.a = b.a', 'order_columns': 'd, c, b', 'db_schema': 'db_schema', 'gbmi_schema': 'gbmi_schema'}, {'raster_name': 'raster2', 'buffer': 20, 'agg_level': 'a', 'agg_columns': 'b, c, d', 'agg_geom': 'agga.abc', 'agg_area': 'agga.xyz', 'join_clause': '', 'order_columns': 'd, c, a', 'db_schema': 'db_schema', 'gbmi_schema': 'gbmi_schema'}, {'raster_name': 'raster2', 'buffer': 20, 'agg_level': 'x', 'agg_columns': 'x, y, z', 'agg_geom': 'agga.xyz', 'agg_area': 'agga.abc', 'join_clause': 'test.test on tt.a = b.a', 'order_columns': 'd, c, b', 'db_schema': 'db_schema', 'gbmi_schema': 'gbmi_schema'}]
        self.assertEqual(actual, expected)

    def test_c_params_empty_buffers(self):
        param_expander = QueryParamsExpander(self.c_key, self.c_params_empty_buffers)
        self.assertRaises(
            QueryParamsExpanderException,
            param_expander.run
        )

    def test_c_params_empty_rasters(self):
        param_expander = QueryParamsExpander(self.c_key, self.c_params_empty_rasters)
        self.assertRaises(
            QueryParamsExpanderException,
            param_expander.run
        )

    def test_c_params_empty_agg_labels(self):
        param_expander = QueryParamsExpander(self.c_key, self.c_params_empty_agg_labels)
        self.assertRaises(
            QueryParamsExpanderException,
            param_expander.run
        )

    def test_c_params_empty_both(self):
        param_expander = QueryParamsExpander(self.c_key, self.c_params_empty_both)
        self.assertRaises(
            QueryParamsExpanderException,
            param_expander.run
        )

    def test_failure(self):
        actual = QueryParamsExpander(self.invalid_key, self.invalid_params).run()
        expected = [{'host_address': 'host', 'superuser': 'spuser', 'db_name': 'db', 'user': 'usr'}]
        self.assertEqual(actual, expected)


class TestQueryGenerator(BaseTestQueryGenerator):
    def setUp(self):
        self.mock_params = {"gbmi_schema": "gbmi", "raster_name": "worldpop", "schema": "public", "table_name": "table1"}
        self.mock_overwrite = True
        self.mock_debug = True
        self.q_generator = QueryGenerator(
            template_dirname=self.templates_dir,
            output_dirname=self.output_dir,
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



