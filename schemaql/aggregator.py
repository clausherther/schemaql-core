from colorama import Fore, Style

from schemaql.helpers.logger import logger
from schemaql.jinja import JinjaConfig


class Aggregator(object):
    """
    Aggregator class
    """

    def __init__(
        self,
        connector,
        project_name,
        database_name,
        schema_name,
        entity_name,
        aggregation_type,
    ):

        self.LINE_WIDTH = 88
        self.RESULT_WIDTH = 30
        self.MSG_WIDTH = self.LINE_WIDTH - self.RESULT_WIDTH  # =58

        self._connector = connector
        self._project_name = project_name
        self._database_name = database_name
        self._schema_name = schema_name
        self._entity_name = entity_name
        self._aggregation_type = aggregation_type

        cfg = JinjaConfig(self._aggregation_type, self._connector)
        self._env = cfg.environment

        self._passed_func = lambda c: None

    def _get_sql(self, aggregation_name, column_name, kwargs=None):

        template = self._env.get_template(f"{aggregation_name}.sql")

        sql = template.render(
            schema=self._schema_name,
            entity=self._entity_name,
            column=column_name,
            kwargs=kwargs,
        ).strip()

        return sql

    def _get_aggregation_results(self, aggregation_name, column_name, kwargs=None):

        sql = self._get_sql(aggregation_name, column_name, kwargs)
        result = self._connector.execute_return_one(sql)
        aggregation_result = result[0] if result and result[0] is not None else 0

        return aggregation_result

    def _make_aggregation_result_row(
        self,
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
            "aggregation_name": aggregation_name,
            "aggregation_description": aggregation_description,
            "aggregation_passed": aggregation_passed,
            "aggregation_result": aggregation_result,
        }

    def _aggregation_name_fqn(self, column_name, aggregation_name):
        agg_name = f"{self._entity_name}.{column_name}__{aggregation_name}"[:self.MSG_WIDTH]
        agg_name = agg_name.ljust(self.MSG_WIDTH, ".")
        return agg_name

    def _color_me(self, msg, color):

        colors = {
            "red": Fore.RED,
            "green": Fore.GREEN,
            "white": Fore.WHITE,
            "black": Fore.BLACK,
            "blue": Fore.BLUE,
            "yellow": Fore.YELLOW
        }
        assert color in colors, f"'{color}' is not supported"
        return colors[color] + msg + Style.RESET_ALL

    def _log_result(self, aggregation_name, result_msg):
        logger.info(aggregation_name + f"[{result_msg}]".rjust(self.RESULT_WIDTH, "."))

    def _run_aggregations(self, aggregations, column_name, aggregation_results):

        for aggregation in aggregations:

            kwargs = None
            if type(aggregation) is dict:
                aggregation_name = list(aggregation)[0]
                kwargs = aggregation[aggregation_name]
            else:
                aggregation_name = aggregation
            aggregation_description = kwargs.get("description", "") if kwargs else ""

            aggregation_result = self._get_aggregation_results(
                aggregation_name, column_name, kwargs,
            )
            aggregation_passed = (
                self._passed_func(aggregation_result)
                if aggregation_result is not None
                else False
            )
            aggregation_results.append(
                self._make_aggregation_result_row(
                    column_name,
                    aggregation_name,
                    aggregation_description,
                    aggregation_passed,
                    aggregation_result,
                )
            )

            self.log(
                column_name, aggregation_name, aggregation_passed, aggregation_result,
            )

    def log(self):
        raise NotImplementedError("log not implemented!")

    def run_entity_aggregations(self, aggregations):

        aggregation_results = []
        column_name = "__entity__"

        self._run_aggregations(aggregations, column_name, aggregation_results)
        return aggregation_results

    def run_column_aggregations(self, column_schema):

        aggregation_results = []

        for column in column_schema:

            column_name = column["name"]

            column_aggregations = (
                column[self._aggregation_type]
                if self._aggregation_type in column
                else []
            )

            self._run_aggregations(
                column_aggregations, column_name, aggregation_results
            )

        return aggregation_results
