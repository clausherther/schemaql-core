from pathlib import Path
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


def generate_table_schema(conn, schemas):

    inspector = inspect(conn.engine)

    for schema in schemas:
        tables = inspector.get_table_names(schema)
        for table in tables:
            columns = inspector.get_columns(table, schema)
            yml = make_schema_yaml(schema, table, columns)

            with open(f"output/{table}.yml", 'w') as f:
                f.write(yml)

def test_schema(conn, schemas):

    with conn.connect() as cur:
        p = Path("output")
        for schema_name in schemas:
            schema_file_paths = list(p.glob(f"{schema_name}/*.yml"))

            for p in schema_file_paths:

                table_schema = helper.read_yaml(p.resolve())

                for table in table_schema["models"]:
                    table_name = table["name"]
                    for column in table["columns"]:
                        column_name = column["name"]
                        for test_name in column["tests"]:
                            sql = get_test_sql(test_name, schema_name, table_name, column_name)
                            rs = cur.execute(sql)
                            result = rs.fetchone()
                            if result["test_result"] > 0:
                                logger.info(f'{schema_name}_{table_name}_{column_name}_{test_name}: {result["test_result"]}')

def main(action: ("Action ('test', or 'generate')", "option", "a") = "test",
         project_file: ("Project file", "option", "p") = "projects.yml",
         connections_file: ("Connections file", "option", "c") = "connections.yml"):

    projects = helper.read_yaml(project_file)["projects"]

    connections = helper.read_yaml(connections_file)

    for project in projects:

        connection_name = project["connection"]
        logger.info(connection_name)

        connection_info = connections[connection_name]
        connection_types = {"snowflake": SnowflakeConnection}
        connection_type = connection_info["type"]

        schemas = project["schema"]

        conn = connection_types[connection_type](connection_info)

        if action == "generate":
            generate_table_schema(conn, schemas)
        elif action == "test":
            test_schema(conn, schemas)

if __name__ == '__main__':

    plac.call(main)
