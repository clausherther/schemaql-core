import json
from pathlib import Path

from sqlalchemy.inspection import inspect

from schemaql.helpers.fileio import (check_directory_exists, read_yaml,
                                     schemaql_path)
from schemaql.helpers.logger import Back, Fore, Style, logger
from schemaql.jinja import JinjaConfig


class EntityAggregator(object):
    """
    EntityAggregator class
    """

    def __init__(self, connector, project_name, database_name, schema_name, entity_name):

        self._connector = connector
        self._project_name = project_name
        self._database_name = database_name
        self._schema_name = schema_name
        self._entity_name = entity_name

        cfg = JinjaConfig("metrics", self._connector)
        self._env = cfg.environment
        
    def _get_sql(self, metric_name, column_name, kwargs=None):

        template = self._env.get_template(f"{metric_name}.sql")

        sql = template.render(
            schema=self._schema_name,
            entity=self._entity_name,
            column=column_name,
            kwargs=kwargs,
        ).strip()

        return sql

    def _log_metric(self, column_name, metric_name, metric_result):

        LINE_WIDTH = 88
        RESULT_WIDTH = 30
        MSG_WIDTH = LINE_WIDTH - RESULT_WIDTH  # =58

        result_msg = f"{self._entity_name}.{column_name}__{metric_name}"[:MSG_WIDTH]
        result_msg = result_msg.ljust(MSG_WIDTH, ".")

        colored_pass = Fore.GREEN + f"{metric_result:,}" + Style.RESET_ALL
        logger.info(result_msg + f"[{colored_pass}]".rjust(RESULT_WIDTH, "."))


    def _get_metric_results(self, metric_name, column_name, kwargs=None):

        sql = self._get_sql(metric_name, column_name, kwargs)
        result = self._connector.execute_return_one(sql)
        metric_result = result["metric_result"]

        return metric_result

    def _make_metric_result_row(self, column_name, metric_name, metric_result):

        return {
                "project_name": self._project_name,
                "database_name": self._database_name,
                "schema_name": self._schema_name,
                "entity_name": self._entity_name, 
                "column_name": column_name, 
                "metric_name": metric_name,
                "metric_result": metric_result
                }

    def run_entity_aggregations(self, metrics):

        metric_results = []
        for metric in metrics:

            if type(metric) is dict:
                metric_name = list(metric)[0]
                kwargs = metric[metric_name]
            else:
                metric_name = metric
                kwargs = None

            column_name = "__entity_METRIC__"
            metric_result = self._get_metric_results(metric_name, column_name, kwargs,)
            metric_results.append(self._make_metric_result_row(column_name, metric_name, metric_result))

            self._log_metric(
                column_name, metric_name, metric_result,
            )
        return metric_results

    def run_column_aggregations(self, column_schema):

        metric_results = []

        for column in column_schema:

            column_name = column["name"]
            kwargs = None

            column_metrics = column["metrics"] if "metrics" in column else []

            for metric in column_metrics:

                if type(metric) is dict:
                    metric_name = list(metric)[0]
                    kwargs = metric[metric_name]
                else:
                    metric_name = metric

                metric_result = self._get_metric_results(metric_name, column_name, kwargs,)

                metric_results.append(self._make_metric_result_row(column_name, metric_name, metric_result))

                self._log_metric(column_name, metric_name, metric_result)
 
        return metric_results


def aggregate_schema(connector, databases, project_name):

    metric_results = []

    for database_name in databases:
        connector.database = database_name

        logger.info(f"Inspecting database {database_name}...")

        schemas = databases[database_name]

        if schemas is None:
            logger.info("No schemas specified, getting all schemas from database...")
            schemas = connector.get_schema_names()

        for schema_name in schemas:

            p = Path("output")
            schema_path = Path(project_name).joinpath(
                database_name, schema_name, "*.yml"
            )
            schema_files = sorted(list(p.glob(str(schema_path))))

            for p in schema_files:

                entity_schema = read_yaml(p.resolve())

                for entity in entity_schema["models"]:

                    entity_name = entity["name"]
                    entity_aggregator = EntityAggregator(
                        connector, project_name, database_name, schema_name, entity_name
                    )

                    entity_metrics = entity["metrics"] if "metrics" in entity else None
                    if entity_metrics:
                        entity_metric_results = entity_aggregator.run_entity_aggregations(entity_metrics)
                        metric_results += entity_metric_results

                    columns = entity["columns"]
                    column_metric_results = entity_aggregator.run_column_aggregations(columns)
                    metric_results += column_metric_results

    return metric_results
