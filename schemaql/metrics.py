import json
from pathlib import Path

from sqlalchemy.inspection import inspect

from schemaql.helpers.fileio import (check_directory_exists, read_yaml,
                                     schemaql_path)
from schemaql.helpers.logger import Back, Fore, Style, logger
from schemaql.jinja import JinjaConfig

from schemaql.aggregator import Aggregator

class MetricsAggregator(Aggregator):
    """
    MetricsAggregator class
    """

    def __init__(self, connector, project_name, database_name, schema_name, entity_name):

        super().__init__(connector,project_name, database_name, schema_name, entity_name, "metrics")

        self._passed_func = lambda c: c >= 0

    def log(self, column_name, aggregation_name, aggregation_passed, aggregation_result):

        LINE_WIDTH = 88
        RESULT_WIDTH = 30
        MSG_WIDTH = LINE_WIDTH - RESULT_WIDTH  # =58

        result_msg = f"{self._entity_name}.{column_name}__{aggregation_name}"[:MSG_WIDTH]
        result_msg = result_msg.ljust(MSG_WIDTH, ".")

        if aggregation_passed:
            colored_pass = Fore.GREEN + f"{aggregation_result}" + Style.RESET_ALL
            logger.info(result_msg + f"[{colored_pass}]".rjust(RESULT_WIDTH, "."))

        else:
            aggregation_result = -1 if not aggregation_result else aggregation_result
            colored_fail = Fore.RED + f"FAIL {aggregation_result:,}" + Style.RESET_ALL
            logger.error(result_msg + f"[{colored_fail}]".rjust(RESULT_WIDTH, "."))


