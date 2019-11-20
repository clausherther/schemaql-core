from pathlib import Path

from schemaql.helpers.fileio import read_yaml
from schemaql.helpers.logger import logger
from schemaql.generator import TableSchemaGenerator
from schemaql.tester import TestAggregator
from schemaql.metrics import MetricsAggregator


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

    def _process_database_schema(self, aggregation_type):

        supported_aggregators = {
            "tests": TestAggregator,
            "metrics": MetricsAggregator
        }

        aggregation_results = []

        for database_name in self._databases:
            self._connector.database = database_name

            logger.info(f"Inspecting database {database_name}...")

            schemas = self._databases[database_name]

            if schemas is None:
                logger.info("No schemas specified, getting all schemas from database...")
                schemas = self._connector.get_schema_names()

            for schema_name in schemas:

                p = Path("output")
                schema_path = Path(self._project_name).joinpath(
                    database_name, schema_name, "*.yml"
                )
                schema_files = sorted(list(p.glob(str(schema_path))))

                for p in schema_files:

                    entity_schema = read_yaml(p.resolve())

                    for entity in entity_schema["models"]:

                        entity_name = entity["name"]

                        entity_aggregator = supported_aggregators[aggregation_type](
                            self._connector, self._project_name, database_name, schema_name, entity_name
                        )

                        entity_aggregations = entity[aggregation_type] if aggregation_type in entity else None
                        if entity_aggregations:
                            entity_aggregation_results = entity_aggregator.run_entity_aggregations(entity_aggregations)
                            aggregation_results += entity_aggregation_results

                        columns = entity["columns"]
                        column_aggregation_results = entity_aggregator.run_column_aggregations(columns)
                        aggregation_results += column_aggregation_results

        return aggregation_results

    def test_database_schema(self):

        test_results = self._process_database_schema("tests")

        return test_results

    def aggregate_database_schema(self):

        metric_results = self._process_database_schema("metrics")

        return metric_results
