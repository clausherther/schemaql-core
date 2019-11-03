from pathlib import Path
import json
from jinja2 import Template, FileSystemLoader, Environment
from sqlalchemy.inspection import inspect

from schemaql.helper import check_directory_exists, read_yaml, schemaql_path
from schemaql.logger import logger, Fore, Back, Style



class TableTester(object):
    """
    Schema Generator
    """

    def __init__(self, project_name, connector, database_name, schema_name, table_name):

        self._project_name = project_name
        self._connector = connector
        self._database_name = database_name
        self._schema_name = schema_name
        self._table_name = table_name
        
        self._template_path = schemaql_path.joinpath("templates", "tests").resolve()
        self._loader = FileSystemLoader(str(self._template_path))
        self._env = Environment(loader=self._loader)

    def get_test_sql(self, test_name, column_name, kwargs=None):
        template = self._env.get_template(f"{test_name}.sql")
        sql = template.render(
            # database=self._database_name, 
            schema=self._schema_name, 
            table=self._table_name, 
            column=column_name, 
            kwargs=kwargs
        )
        return sql


    def log_test_result(self, column_name, test_name, test_result):

        LINE_WIDTH = 88
        RESULT_WIDTH = 30
        MSG_WIDTH = LINE_WIDTH - RESULT_WIDTH  # =58

        result_msg = f"{self._table_name}.{column_name}: {test_name}"[:MSG_WIDTH]
        result_msg = result_msg.ljust(MSG_WIDTH, ".")

        if test_result == 0:
            colored_pass = Fore.GREEN + "PASS" + Style.RESET_ALL
            logger.info(result_msg + f"[{colored_pass}]".rjust(RESULT_WIDTH, "."))

        else:
            colored_fail = Fore.RED + f"FAIL {test_result:,}" + Style.RESET_ALL
            logger.error(result_msg + f"[{colored_fail}]".rjust(RESULT_WIDTH, "."))


    def update_test_results(
        self,
        column_name,
        test_name,
        test_results,
        test_result,
    ):

        if test_result > 0:
            if self._schema_name not in test_results[self._project_name]:
                test_results[self._project_name][self._schema_name] = {}

            if self._table_name not in test_results[self._project_name][self._schema_name]:
                test_results[self._project_name][self._schema_name][self._table_name] = {}

            if column_name not in test_results[self._project_name][self._schema_name][self._table_name]:
                test_results[self._project_name][self._schema_name][self._table_name][column_name] = {}

            test_results[self._project_name][self._schema_name][self._table_name][column_name] = {
                test_name: test_result
            }

        return test_results



def test_schema(connector, databases, project_name):

    test_results = {}
    test_results[project_name] = {}

    for database_name in databases:
        connector.database = database_name

        logger.info(f"Inspecting database {database_name}...")

        schemas = databases[database_name]

        if schemas is None:
            logger.info("No schemas specified, getting all schemas from database...")
            schemas = connector.get_schema_names()

        for schema_name in schemas:

            with connector.connect() as cur:

                p = Path("output")
                schema_path = Path(project_name).joinpath(database_name, schema_name, "*.yml")
                schema_files = sorted(list(p.glob(str(schema_path))))

                for p in schema_files:

                    table_schema = read_yaml(p.resolve())

                    for table in table_schema["models"]:

                        table_name = table["name"]
                        table_tester = TableTester(project_name, connector, database_name, schema_name, table_name)
                        for column in table["columns"]:

                            column_name = column["name"]
                            kwargs = None
                            for test in column["tests"]:

                                if type(test) is dict:
                                    test_name = list(test)[0]
                                    kwargs = test[test_name]
                                else:
                                    test_name = test

                                sql = table_tester.get_test_sql(
                                    test_name,
                                    column_name,
                                    kwargs,
                                )

                                rs = cur.execute(sql)
                                result = rs.fetchone()
                                test_result = result["test_result"]

                                table_tester.log_test_result(
                                    column_name,
                                    test_name,
                                    test_result,
                                )

                                test_results = table_tester.update_test_results(
                                    column_name,
                                    test_name,
                                    test_results,
                                    test_result,
                                )

    return test_results
