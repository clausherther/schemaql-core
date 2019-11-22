from schemaql.aggregator import Aggregator
from schemaql.helpers.logger import color_me, CHECK_MARK, X


class TestAggregator(Aggregator):
    """
    TestAggregator class
    """

    def __init__(self, connector, project_name, database_name, schema_name, entity_name):

        super().__init__(connector, project_name, database_name, schema_name, entity_name, "tests")

        self._passed_func = lambda c: c == 0

    def log(self, aggregation_name_fqn, column_name, aggregation_name, aggregation_passed, aggregation_result):

        if aggregation_passed:
            colored_msg = color_me("PASS", "green")
            bullet = color_me(CHECK_MARK, "green")
        else:
            aggregation_result = -1 if not aggregation_result else aggregation_result
            colored_msg = color_me(f"FAIL {aggregation_result:,}", "red")
            bullet = color_me(X, "red")

        self._log_result(bullet, aggregation_name_fqn, colored_msg)
