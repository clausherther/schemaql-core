import json
from pathlib import Path

from sqlalchemy.inspection import inspect

from schemaql.helpers.fileio import (check_directory_exists, read_yaml,
                                     schemaql_path)
from schemaql.helpers.logger import Back, Fore, Style, logger
from schemaql.jinja import JinjaConfig


class EntityTester(object):
    """
    EntityTester class
    """

    def __init__(self, connector, database_name, schema_name, entity_name):

        self._connector = connector
        self._database_name = database_name
        self._schema_name = schema_name
        self._entity_name = entity_name

        cfg = JinjaConfig("tests", self._connector)
        self._env = cfg.environment
        
    def _get_test_sql(self, test_name, column_name, kwargs=None):

        template = self._env.get_template(f"{test_name}.sql")

        sql = template.render(
            schema=self._schema_name,
            entity=self._entity_name,
            column=column_name,
            kwargs=kwargs,
        ).strip()

        return sql

    def _log_test_result(self, column_name, test_name, test_passed, test_result):

        LINE_WIDTH = 88
        RESULT_WIDTH = 30
        MSG_WIDTH = LINE_WIDTH - RESULT_WIDTH  # =58

        result_msg = f"{self._entity_name}.{column_name}: {test_name}"[:MSG_WIDTH]
        result_msg = result_msg.ljust(MSG_WIDTH, ".")

        if test_passed:
            colored_pass = Fore.GREEN + "PASS" + Style.RESET_ALL
            logger.info(result_msg + f"[{colored_pass}]".rjust(RESULT_WIDTH, "."))

        else:
            colored_fail = Fore.RED + f"FAIL {test_result:,}" + Style.RESET_ALL
            logger.error(result_msg + f"[{colored_fail}]".rjust(RESULT_WIDTH, "."))

    def _get_test_results(self, test_name, column_name, kwargs=None):

        sql = self._get_test_sql(test_name, column_name, kwargs)
        result = self._connector.execute_return_one(sql)
        test_result = result["test_result"]

        return test_result

    def run_entity_tests(self, tests):

        test_results = []
        for test in tests:

            if type(test) is dict:
                test_name = list(test)[0]
                kwargs = test[test_name]
            else:
                test_name = test
                kwargs = None

            column_name = "__entity_TEST__"
            test_result = self._get_test_results(test_name, column_name, kwargs,)
            test_passed = test_result == 0
            test_results.append({
                "entity_name": self._entity_name, 
                "column_name": column_name, 
                "test_name": test_name,
                "test_passed": test_passed,
                "test_result": test_result
                })

            self._log_test_result(
                column_name, test_name, test_passed, test_result,
            )
        return test_results

    def run_column_tests(self, column_schema):

        test_results = []

        for column in column_schema:

            column_name = column["name"]
            kwargs = None

            for test in column["tests"]:

                if type(test) is dict:
                    test_name = list(test)[0]
                    kwargs = test[test_name]
                else:
                    test_name = test

                test_result = self._get_test_results(test_name, column_name, kwargs,)

                test_passed = test_result == 0

                test_results.append({
                    "entity_name": self._entity_name, 
                    "column_name": column_name, 
                    "test_name": test_name,
                    "test_passed": test_passed,
                    "test_result": test_result
                    })

                self._log_test_result(column_name, test_name, test_passed, test_result)
 
        return test_results


def test_schema(connector, databases, project_name):

    test_results = {}
    test_results[project_name] = []

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
                    entity_tester = EntityTester(
                        connector, database_name, schema_name, entity_name
                    )

                    entity_tests = entity["tests"] if "tests" in entity else None
                    if entity_tests:
                        entity_test_results = entity_tester.run_entity_tests(entity_tests)
                        test_results[project_name] += entity_test_results

                    columns = entity["columns"]
                    column_test_results = entity_tester.run_column_tests(columns)
                    test_results[project_name] += column_test_results

    return test_results
