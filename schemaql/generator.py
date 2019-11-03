from pathlib import Path
from jinja2 import Template, FileSystemLoader, Environment

from schemaql.helper import check_directory_exists, read_yaml, schemaql_path
from schemaql.logger import logger, Fore, Back, Style


class TableSchemaGenerator(object):
    """
    Schema Generator
    """

    def __init__(self, project_name, connector, database, schema, table):

        self._project_name = project_name
        self._connector = connector
        self._database = database
        self._schema = schema
        self._table = table
        self._columns = None
        
        self._template_path = schemaql_path.joinpath("templates", "yaml").resolve()
        self._loader = FileSystemLoader(str(self._template_path))
        self._env = Environment(loader=self._loader)

    def _make_schema_yaml(self,):
        """Renders schema yaml template from metadata
        
        Arguments:
            schema {string} -- name of schema
            table {string} -- name of table
            columns {list} -- list of columns
        
        Returns:
            string -- rendered yaml
        """

        template = self._env.get_template("schema.yml")
        yml = template.render(schema=self._schema, table=self._table, columns=self._columns)

        return yml

    def _write_table_schema_yaml(self, yaml):

        schema_directory = Path("output").joinpath(self._project_name, self._database, self._schema)
        check_directory_exists(schema_directory)
        yml_file_path = schema_directory.joinpath(f"{self._table}.yml")
        yml_file_path.write_text(yaml)

        return yml_file_path

    def generate_table_schema(self):

        self._columns = self._connector.get_columns(self._table, self._schema)
        logger.info(
            f"Generating schema for {self._database}.{self._schema}.{self._table} ({len(self._columns)} columns)"
        )
        yml = self._make_schema_yaml()

        self._write_table_schema_yaml(yml)

        return yml
