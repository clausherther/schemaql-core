from pathlib import Path
import json
from jinja2 import Template, FileSystemLoader, Environment
from sqlalchemy.inspection import inspect

from schemaql.helper import check_directory_exists, read_yaml, schemaql_path
from schemaql.connection import Connection
from schemaql.logger import logger, Fore, Back, Style


def get_test_sql(test_name, database, schema, table, column, kwargs=None):
    template_path = schemaql_path.joinpath("templates", "tests").resolve()
    loader = FileSystemLoader(str(template_path))
    env = Environment(loader=loader)
    template = env.get_template(f"{test_name}.sql")
    sql = template.render(
        database=database, schema=schema, table=table, column=column, kwargs=kwargs
    )
    return sql


def log_test_result(schema_name, table_name, column_name, test_name, test_result):

    LINE_WIDTH = 88
    RESULT_WIDTH = 30
    MSG_WIDTH = LINE_WIDTH - RESULT_WIDTH  # =58

    result_msg = f"{table_name}.{column_name}: {test_name}"[:MSG_WIDTH]
    result_msg = result_msg.ljust(MSG_WIDTH, ".")

    if test_result == 0:
        colored_pass = Fore.GREEN + "PASS" + Style.RESET_ALL
        logger.info(result_msg + f"[{colored_pass}]".rjust(RESULT_WIDTH, "."))

    else:
        colored_fail = Fore.RED + f"FAIL {test_result:,}" + Style.RESET_ALL
        logger.error(result_msg + f"[{colored_fail}]".rjust(RESULT_WIDTH, "."))


def update_test_results(
    project_name,
    schema_name,
    table_name,
    column_name,
    test_name,
    test_results,
    test_result,
):

    if test_result > 0:
        if schema_name not in test_results[project_name]:
            test_results[project_name][schema_name] = {}

        if table_name not in test_results[project_name][schema_name]:
            test_results[project_name][schema_name][table_name] = {}

        if column_name not in test_results[project_name][schema_name][table_name]:
            test_results[project_name][schema_name][table_name][column_name] = {}

        test_results[project_name][schema_name][table_name][column_name] = {
            test_name: test_result
        }

    return test_results


def test_schema(conn, databases, project_name):

    test_results = {}
    test_results[project_name] = {}

    for database in databases:

        conn.database = database
        conn.create_engine()

        inspector = inspect(conn.engine)

        schemas = databases[database]

        if schemas is None:
            logger.info("No schemas specified, getting all schemas from database...")
            schemas = sorted(inspector.get_schema_names())

        for schema_name in schemas:

            with conn.connect() as cur:

                p = Path("output")
                schema_path = Path(project_name).joinpath(database, schema_name, "*.yml")
                schema_files = sorted(list(p.glob(str(schema_path))))

                for p in schema_files:

                    table_schema = read_yaml(p.resolve())

                    for table in table_schema["models"]:

                        table_name = table["name"]

                        for column in table["columns"]:

                            column_name = column["name"]
                            kwargs = None
                            for test in column["tests"]:

                                if type(test) is dict:
                                    test_keys = list(test)
                                    test_name = test_keys[0]
                                    kwargs = {k: test[k] for k in test_keys[1:]}
                                else:
                                    test_name = test

                                sql = get_test_sql(
                                    test_name,
                                    database,
                                    schema_name,
                                    table_name,
                                    column_name,
                                    kwargs,
                                )
                                # logger.info(sql)
                                rs = cur.execute(sql)
                                result = rs.fetchone()
                                test_result = result["test_result"]

                                log_test_result(
                                    schema_name,
                                    table_name,
                                    column_name,
                                    test_name,
                                    test_result,
                                )

                                test_results = update_test_results(
                                    project_name,
                                    schema_name,
                                    table_name,
                                    column_name,
                                    test_name,
                                    test_results,
                                    test_result,
                                )

    return test_results

def save_test_results(project_name, test_results):
    test_results_json = json.dumps(test_results, indent=4, sort_keys=True)

    output_directory = Path("output").joinpath(project_name)
    check_directory_exists(output_directory)
    json_output_file = output_directory.joinpath("test_results.json")
    json_output_file.write_text(test_results_json)
