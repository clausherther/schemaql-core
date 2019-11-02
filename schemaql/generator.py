from pathlib import Path
from jinja2 import Template, FileSystemLoader, Environment

from schemaql.helper import check_directory_exists, read_yaml, schemaql_path
from schemaql.logger import logger, Fore, Back, Style


class SchemaGenerator(object):
    """
    Schema Generator
    """

    def __init__(self, project_name, connector, databases):

        self._project_name = project_name
        self._connector = connector
        self._databases = databases

        self._template_path = schemaql_path.joinpath("templates", "yaml").resolve()
        self._loader = FileSystemLoader(str(self._template_path))
        self._env = Environment(loader=self._loader)

    def _make_schema_yaml(self, schema, table, columns):
        """Renders schema yaml template from metadata
        
        Arguments:
            schema {string} -- name of schema
            table {string} -- name of table
            columns {list} -- list of columns
        
        Returns:
            string -- rendered yaml
        """

        template = self._env.get_template("schema.yml")
        yml = template.render(schema=schema, table=table, columns=columns)

        return yml

    def _generate_table_schema_file(self, database, schema, table):

        columns = self._connector.get_columns(table, schema)
        logger.info(
            f"Generating schema for {database}.{schema}.{table} ({len(columns)} columns)"
        )
        yml = self._make_schema_yaml(schema, table, columns)

        return yml

    def _write_table_schema_yaml(self, yaml, database, schema, table):

        schema_directory = Path("output").joinpath(self._project_name, database, schema)
        check_directory_exists(schema_directory)
        yml_file_path = schema_directory.joinpath(f"{table}.yml")
        yml_file_path.write_text(yaml)

        return yml_file_path

    def generate_database_schema(self):
        """Generates yaml output file for connection and databases"""
        for database in self._databases:

            logger.info(f"database: {database}")
            self._connector.database = database

            schemas = self._databases[database]
            logger.info(f"schemas: {schemas}")

            if schemas is None:
                logger.info(
                    "No schemas specified, getting all schemas from database..."
                )
                schemas = self._connector.get_schema_names(database)

            for schema in schemas:
                logger.info(f"schema: {schema}")

                tables = self._connector.get_table_names(schema)
                # remove schema prefixes if in table name
                # (this can happen on BigQuery)
                tables = [table.replace(f"{schema}.", "") for table in tables]

                for table in tables:
                    yml = self._generate_table_schema_file(database, schema, table)
                    self._write_table_schema_yaml(yml, database, schema, table)
