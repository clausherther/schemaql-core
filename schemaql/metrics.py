from schemaql.aggregator import Aggregator


class MetricsAggregator(Aggregator):
    """
    MetricsAggregator class
    """

    def __init__(self, connector, project_name, database_name, schema_name, entity_name):

        super().__init__(connector, project_name, database_name, schema_name, entity_name, "metrics")

        self._passed_func = lambda c: c >= 0

    def log(self, column_name, aggregation_name, aggregation_passed, aggregation_result):

        aggregation_name_fqn = self._aggregation_name_fqn(column_name, aggregation_name)

        if aggregation_passed:
            colored_msg = self._color_me(f"{aggregation_result}", "green")
        else:
            aggregation_result = -1 if not aggregation_result else aggregation_result
            colored_msg = self._color_me(f"FAIL {aggregation_result:,}", "red")

        self._log_result(aggregation_name_fqn, colored_msg)
