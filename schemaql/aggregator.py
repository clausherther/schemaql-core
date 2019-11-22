from schemaql.helpers.logger import logger, LINE_WIDTH
from schemaql.jinja import JinjaConfig


class Aggregator(object):
    """
    Aggregator class
    """

    def __init__(self, connector, project_name, database_name, schema_name, entity_name, aggregation_type):

        self.RESULT_WIDTH = 30
        self.MSG_WIDTH = LINE_WIDTH - self.RESULT_WIDTH  # =58

        self._connector = connector
        self._project_name = project_name
        self._database_name = database_name
        self._schema_name = schema_name
        self._entity_name = entity_name
        self._aggregation_type = aggregation_type

        self._jinja = JinjaConfig(self._aggregation_type, self._connector)

        self._passed_func = lambda c: None

    def _get_sql(self, aggregation_name, column_name, kwargs=None):

        sql = self._jinja.get_rendered(
            f"{aggregation_name}.sql",
            kwargs={
                "schema": self._schema_name,
                "entity": self._entity_name,
                "column": column_name,
                "kwargs": kwargs
            }
        )
        return sql

    def _get_aggregation_results(self, aggregation_name, column_name, kwargs=None):

        sql = self._get_sql(aggregation_name, column_name, kwargs)
        result = self._connector.execute_return_one(sql)
        aggregation_result = result[0] if result and result[0] is not None else 0

        return aggregation_result

    def _make_aggregation_result_row(
        self,
        aggregation_name_fqn,
        column_name,
        aggregation_name,
        aggregation_description,
        aggregation_passed,
        aggregation_result,
    ):

        return {
            "project_name": self._project_name,
            "database_name": self._database_name,
            "schema_name": self._schema_name,
            "entity_name": self._entity_name,
            "column_name": column_name,
            "aggregation_type": self._aggregation_type,
            "aggregation_function": aggregation_name,
            "aggregation_name": aggregation_name_fqn,
            "aggregation_description": aggregation_description,
            "aggregation_passed": aggregation_passed,
            "aggregation_result": aggregation_result,
        }

    def _aggregation_name_fqn(self, column_name, aggregation_name):
        agg_name = f"{self._entity_name}.{column_name}__{aggregation_name}"
        return agg_name

    def _format_left_align(self, msg):
        return msg[:self.MSG_WIDTH].ljust(self.MSG_WIDTH, ".")

    def _format_right_align(self, msg):
        return f"[{msg}]".rjust(self.RESULT_WIDTH, ".")

    def _log_result(self, bullet, aggregation_name, result_msg):
        logger.info(self._format_left_align(f"{bullet} {aggregation_name}") +
                    self._format_right_align(result_msg)
                    )

    def _run_aggregations(self, aggregations, column_name):

        aggregation_results = []

        for aggregation in aggregations:

            kwargs = None

            if type(aggregation) is dict:
                aggregation_name = list(aggregation)[0]
                kwargs = aggregation[aggregation_name]
            else:
                aggregation_name = aggregation

            aggregation_description = kwargs.get("description", "") if kwargs else ""

            aggregation_result = self._get_aggregation_results(aggregation_name, column_name, kwargs)

            if aggregation_result is not None:
                aggregation_passed = self._passed_func(aggregation_result)
            else:
                aggregation_passed = False

            aggregation_name_fqn = self._aggregation_name_fqn(column_name, aggregation_name)

            aggregation_results.append(
                self._make_aggregation_result_row(
                    aggregation_name_fqn,
                    column_name,
                    aggregation_name,
                    aggregation_description,
                    aggregation_passed,
                    aggregation_result,
                )
            )

            self.log(aggregation_name_fqn, column_name, aggregation_name, aggregation_passed, aggregation_result)

        return aggregation_results

    def log(self):
        raise NotImplementedError("log not implemented!")

    def run_entity_aggregations(self, aggregations):

        column_name = "__entity__"

        aggregation_results = self._run_aggregations(aggregations, column_name)
        return aggregation_results

    def run_column_aggregations(self, column_schema):

        aggregation_results = []

        for column in column_schema:

            column_name = column["name"]

            column_aggregations = column[self._aggregation_type] if self._aggregation_type in column else []

            aggregation_results += self._run_aggregations(column_aggregations, column_name)

        return aggregation_results
