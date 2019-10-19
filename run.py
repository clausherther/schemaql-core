import os
from pathlib import Path
import json
import plac
from jinja2 import Template, FileSystemLoader, Environment
from sqlalchemy.inspection import inspect

import helper
from connection import SnowflakeConnection
from logger import logger


def make_schema_yaml(schema, table, columns):
    loader = FileSystemLoader("templates/yaml")
    env = Environment(loader=loader)
    template = env.get_template("schema.yml")
    yml = template.render(schema=schema, table=table, columns=columns)
    return yml


def get_test_sql(test_name, schema, table, column):
    loader = FileSystemLoader("templates/tests")
    env = Environment(loader=loader)
    template = env.get_template(f"{test_name}.sql")
    sql = template.render(schema=schema, table=table, column=column)
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

            tables = sorted(inspector.get_table_names(f"{database}.{schema}"))

            for table in tables:
                columns = inspector.get_columns(table, schema)
                yml = make_schema_yaml(schema, table, columns)

                schema_directory = f"output/{project_name}/{database}/{schema}"
                try:
                    os.makedirs(schema_directory)
                except FileExistsError:
                    # directory already exists
                    pass

                with open(f"{schema_directory}/{table}.yml", "w") as f:
                    f.write(yml)


def test_schema(conn, databases, project_name):

    test_results = {}
    test_results[project_name] = {}

    with conn.connect() as cur:

        for database in databases:
            inspector = inspect(conn.engine)
            schemas = databases[database]

            for schema_name in schemas:

                p = Path("output")
                schema_path = f"{project_name}/{schema_name}/*.yml"
                schema_files = sorted(list(p.glob(schema_path)))
                logger.info(f"{schema_name} {schema_path} {schema_files}")

                for p in schema_files:

                    table_schema = helper.read_yaml(p.resolve())

                    for table in table_schema["models"]:

                        table_name = table["name"]

                        for column in table["columns"]:

                            column_name = column["name"]

                            for test_name in column["tests"]:

                                sql = get_test_sql(
                                    test_name, schema_name, table_name, column_name
                                )
                                rs = cur.execute(sql)
                                result = rs.fetchone()
                                test_result = result["test_result"]
                                if test_result > 0:
                                    logger.info(
                                        f"{schema_name}_{table_name}_{column_name}_{test_name}: {test_result}"
                                    )
                                    if schema_name not in test_results[project_name]:
                                        test_results[project_name][schema_name] = {}
                                    if (
                                        table_name
                                        not in test_results[project_name][schema_name]
                                    ):
                                        test_results[project_name][schema_name][
                                            table_name
                                        ] = {}
                                    if (
                                        column_name
                                        not in test_results[project_name][schema_name][
                                            table_name
                                        ]
                                    ):
                                        test_results[project_name][schema_name][
                                            table_name
                                        ][column_name] = {}

                                    test_results[project_name][schema_name][table_name][
                                        column_name
                                    ] = {test_name: test_result}

    return test_results


def main(
    action_prm: ("Action ('test', or 'generate')", "option", "a") = "dry",
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
        connection_types = {"snowflake": SnowflakeConnection}
        connection_type = connection_info["type"]
        if connection_type not in connection_types:
            raise NotImplementedError(
                f"Connection '{connection_type}' is not yet implemented"
            )

        databases = project["schema"]
        logger.info(databases)
        conn = connection_types[connection_type](connection_info)

        if action_prm == "generate":
            generate_table_schema(conn, databases, project_name)
        elif action_prm == "test":
            test_results = test_schema(conn, databases, project_name)
            test_results_json = json.dumps(test_results, indent=4, sort_keys=True)
            with open(f"output/{project_name}/test_results.json", "w") as f:
                f.write(test_results_json)


if __name__ == "__main__":

    plac.call(main)
