from schemaql.helpers.logger import logger, Fore, Back, Style
from schemaql.generator import TableSchemaGenerator
import schemaql.tester as tester

class Project(object):
    """
    Project class
    """

    def __init__(self, project_name, connector, databases):

        self._project_name = project_name
        self._connector = connector
        self._databases = databases


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
                    generator = TableSchemaGenerator(self._project_name, self._connector, database, schema, table)
                    generator.generate_table_schema()
    
    def test_database_schema(self):

        test_results = tester.test_schema(self._connector, self._databases, self._project_name)

        return test_results