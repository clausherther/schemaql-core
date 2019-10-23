import os
from pathlib import Path
import json
import plac
from jinja2 import Template, FileSystemLoader, Environment
from sqlalchemy.inspection import inspect

import helper
from connection import Connection
from logger import logger, Fore, Back, Style


def check_directory_exists(directory):
    try:
        os.makedirs(directory)
    except FileExistsError:
        # directory already exists
        pass

def make_schema_yaml(schema, table, columns):
    loader = FileSystemLoader("templates/yaml")
    env = Environment(loader=loader)
    template = env.get_template("schema.yml")
    yml = template.render(schema=schema, table=table, columns=columns)
    return yml


def get_test_sql(test_name, database, schema, table, column, kwargs=None):
    loader = FileSystemLoader("templates/tests")
    env = Environment(loader=loader)
    template = env.get_template(f"{test_name}.sql")
    sql = template.render(database=database, schema=schema, table=table, column=column, kwargs=kwargs)
    return sql


def generate_table_schema(conn, databases, project_name):

    for database in databases:

        conn.database = database
        conn.create_engine()

        inspector = inspect(conn.engine)
        schemas = databases[database]

        if schemas is None:
            logger.info("No schemas specified, getting all schemas from database...")
            schemas = sorted(inspector.get_schema_names())

        for schema in schemas:

            # conn.database = database
            # conn.schema = schema
            # conn.create_engine()

            inspector = inspect(conn.engine)
            tables = sorted(inspector.get_table_names(f"{schema}"))

            for table in tables:
                columns = inspector.get_columns(table, schema)
                logger.info(f"Generating schema for {database}.{schema}.{table} ({len(columns)})")
                yml = make_schema_yaml(schema, table, columns)

                schema_directory = f"output/{project_name}/{database}/{schema}"
                check_directory_exists(schema_directory)

                with open(f"{schema_directory}/{table}.yml", "w") as f:
                    f.write(yml)

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

def update_test_results(project_name, schema_name, table_name, column_name, test_name, test_results, test_result):

    if test_result > 0:
        if schema_name not in test_results[project_name]:
            test_results[project_name][schema_name] = {}

        if (table_name not in test_results[project_name][schema_name]):
            test_results[project_name][schema_name][table_name] = {}

        if (column_name not in test_results[project_name][schema_name][table_name]):
            test_results[project_name][schema_name][table_name][column_name] = {}

        test_results[project_name][schema_name][table_name][column_name] = {test_name: test_result}

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
                schema_path = f"{project_name}/{database}/{schema_name}/*.yml"
                schema_files = sorted(list(p.glob(schema_path)))
                # logger.info(f"{database} {schema_name}")

                for p in schema_files:

                    table_schema = helper.read_yaml(p.resolve())

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
                                    kwargs
                                )
                                # logger.info(sql)
                                rs = cur.execute(sql)
                                result = rs.fetchone()
                                test_result = result["test_result"]

                                log_test_result(schema_name, table_name, column_name, test_name, test_result)

                                test_results = update_test_results(project_name,
                                                                   schema_name,
                                                                   table_name,
                                                                   column_name,
                                                                   test_name,
                                                                   test_results,
                                                                   test_result)

    return test_results


def main(
    action_prm: ("Action ('test', or 'generate')", "option", "a") = "test",
    project_prm: ("Project", "option", "p") = "",
    project_file_prm: ("Project file", "option", "f") = "projects.yml",
    connections_file_prm: ("Connections file", "option", "c") = "connections.yml",
):

    projects = helper.read_yaml(project_file_prm)["projects"]
    connections = helper.read_yaml(connections_file_prm)

    for project_name in projects:
        if len(project_prm) > 0 and project_prm != project_name:
            continue

        logger.info(project_name)
        project = projects[project_name]
        connection_name = project["connection"]
        logger.info(f"{project_name} {connection_name}")

        connection_info = connections[connection_name]
        connection_type = connection_info["type"]

        databases = project["schema"]
        logger.info(databases)
        conn = Connection(connection_info)

        if action_prm == "generate":
            generate_table_schema(conn, databases, project_name)
        elif action_prm == "test":
            test_results = test_schema(conn, databases, project_name)
            test_results_json = json.dumps(test_results, indent=4, sort_keys=True)

            output_directory = f"output/{project_name}"
            check_directory_exists(output_directory)
            with open(f"{output_directory}/test_results.json", "w") as f:
                f.write(test_results_json)


if __name__ == "__main__":

    plac.call(main)
