import re

from schemaql.jinja import JinjaConfig


def strings_match_ignore_whitespace(a, b):
    """
    Compare two base strings, disregarding whitespace
    """
    return re.sub("\\s*", "", a) == re.sub("\\s*", "", b)


class TestJinja(object):

    def _get_template(self, template_type, template_name):
        cfg = JinjaConfig(template_type, connector=None)
        template = cfg.get_template(template_name)
        return template

    def test_get_test_template(self):
        template_name = "not_null.sql"
        template = self._get_template("tests", template_name)
        assert template is not None

    def test_get_yaml_template(self):
        template_name = "schema.yml"
        template = self._get_template("yaml", template_name)
        assert template is not None

    def _render_template(self, template_type, template_name, kwargs):
        cfg = JinjaConfig(template_type, connector=None)
        return cfg.get_rendered(template_name, kwargs=kwargs)

    def test_render_yaml_template(self):
        template_name = "schema.yml"
        expected_contents = """
        version: 2

            models:
              - name: test_entity
                columns:
                - name: test_column_1
                  description:
                  tests:
                    - not_null
                - name: test_column_2
                  description:
                  tests:
                    - not_null
        """.strip()

        kwargs = {
            "schema": "test_schema",
            "entity": "test_entity",
            "columns": [{"name": "test_column_1"}, {"name": "test_column_2"}]
        }

        template_contents = self._render_template("yaml", template_name, kwargs)

        assert template_contents is not None
        assert strings_match_ignore_whitespace(template_contents, expected_contents)

    def test_render_test_template(self):
        template_name = "not_null.sql"
        expected_contents = """
        with validation_errors as (
            select
                *
            from
                test_schema.test_entity
            where
                test_column is null
        )
        select count(*) as test_result
        from
            validation_errors
        """.strip()

        kwargs = {
            "schema": "test_schema",
            "entity": "test_entity",
            "column": "test_column"
        }

        template_contents = self._render_template("tests", template_name, kwargs)

        assert template_contents is not None
        assert strings_match_ignore_whitespace(template_contents, expected_contents)

    def test_render_metrics_template(self):
        template_name = "sum.sql"
        expected_contents = """
        select sum(test_column)
        from test_schema.test_entity
        """.strip()

        kwargs = {
            "schema": "test_schema",
            "entity": "test_entity",
            "column": "test_column"
        }

        template_contents = self._render_template("metrics", template_name, kwargs)

        assert template_contents is not None
        assert strings_match_ignore_whitespace(template_contents, expected_contents)
