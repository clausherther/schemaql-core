from mock_connector import MockConnector
from schemaql.jinja import JinjaConfig
from schemaql.helpers.logger import logger
from helpers import strings_match_ignore_whitespace

test_schema = "test_schema"
test_entity = "test_entity"
test_column = "test_column"
test_kwargs = {"schema": test_schema, "entity": test_entity, "column": test_column}


class TestJinja(object):

    def _get_jinja_config(self, template_type):
        conn_info = {"type": "postgresql", "user": "user", "password": "password", "url": "postgresql://localhost"}
        conn = MockConnector(conn_info)
        cfg = JinjaConfig(template_type, connector=conn)
        return cfg

    def _get_template(self, template_type, template_name):
        cfg = self._get_jinja_config(template_type)
        template = cfg.get_template(template_name)
        return template

    def test_get_yaml_template(self):
        template_name = "schema.yml"
        template = self._get_template("yaml", template_name)
        assert template is not None

    def test_get_test_template(self):
        template_name = "not_null.sql"
        template = self._get_template("tests", template_name)
        assert template is not None

    def test_get_metrics_template(self):
        template_name = "sum.sql"
        template = self._get_template("metrics", template_name)
        assert template is not None

    def _render_template(self, template_type, template_name, kwargs):
        cfg = self._get_jinja_config(template_type)
        return cfg.get_rendered(template_name, kwargs=kwargs)

    def test_render_yaml_template(self):
        template_name = "schema.yml"
        expected_contents = f"""
        version: 2

            models:
              - name: {test_entity}
                columns:
                - name: {test_column}_1
                  description:
                  tests:
                    - not_null
                - name: {test_column}_2
                  description:
                  tests:
                    - not_null
        """.strip()

        _kwargs = {
            "schema": test_schema,
            "entity": test_entity,
            "columns": [{"name": f"{test_column}_1"}, {"name": f"{test_column}_2"}],
        }

        template_contents = self._render_template("yaml", template_name, _kwargs)

        assert template_contents is not None
        assert strings_match_ignore_whitespace(template_contents, expected_contents)

    def test_render_test_template(self):
        template_name = "not_null.sql"
        expected_contents = f"""
        with validation_errors as (
            select
                *
            from
                {test_schema}.{test_entity}
            where
                {test_column} is null
        )
        select count(*) as test_result
        from
            validation_errors
        """.strip()

        template_contents = self._render_template("tests", template_name, test_kwargs)

        assert template_contents is not None
        assert strings_match_ignore_whitespace(template_contents, expected_contents)

    def test_render_test_template_with_macro(self):
        template_name = "unique_rows.sql"
        expected_contents = f"""
        with hashed_rows as (
            select
                md5(concat(
            coalesce(cast({test_column}_1 as varchar), ''),'|',coalesce(cast({test_column}_2 as varchar), ''))) as row_hash
            from {test_schema}.{test_entity}
        )
        select (count(*) - count(distinct row_hash)) as test_result
        from hashed_rows
        """.strip()

        _kwargs = {
            "schema": test_schema,
            "entity": test_entity,
            "kwargs": {"columns": [f"{test_column}_1", f"{test_column}_2"]},
        }

        template_contents = self._render_template("tests", template_name, _kwargs)
        logger.info(template_contents)
        assert template_contents is not None
        assert strings_match_ignore_whitespace(template_contents, expected_contents)

    def test_render_metrics_template(self):
        template_name = "sum.sql"
        expected_contents = f"""
        select sum({test_column})
        from {test_schema}.{test_entity}
        """.strip()

        template_contents = self._render_template("metrics", template_name, test_kwargs)

        assert template_contents is not None
        assert strings_match_ignore_whitespace(template_contents, expected_contents)
