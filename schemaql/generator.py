from pathlib import Path

from schemaql.helpers.fileio import check_directory_exists
from schemaql.helpers.logger import logger
from schemaql.jinja import JinjaConfig


class EntitySchemaGenerator(object):
    """
    Table Schema Generator class
    """

    def __init__(self, project_name, connector, database, schema, entity):

        self._project_name = project_name
        self._connector = connector
        self._database = database
        self._schema = schema
        self._entity = entity
        self._columns = None

        self._jinja = JinjaConfig("yaml", self._connector)

    def _make_schema_yaml(self,):

        template_name = "schema.yml"
        yml = self._jinja.get_rendered(template_name,
                                       kwargs={"schema": self._schema,
                                               "entity": self._entity,
                                               "columns": self._columns}
                                       )

        return yml

    def _write_entity_schema_yaml(self, yaml):

        schema_directory = Path("output").joinpath(self._project_name, self._database, self._schema)
        check_directory_exists(schema_directory)
        yml_file_path = schema_directory.joinpath(f"{self._entity}.yml")
        yml_file_path.write_text(yaml)

        return yml_file_path

    def generate_entity_schema(self):

        self._columns = self._connector.get_columns(self._entity, self._schema)
        logger.info(
            f"Generating schema for {self._database}.{self._schema}.{self._entity} ({len(self._columns)} columns)"
        )
        yml = self._make_schema_yaml()

        self._write_entity_schema_yaml(yml)

        return yml
