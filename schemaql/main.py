from pathlib import Path
import json
import plac
from jinja2 import Template, FileSystemLoader, Environment
from sqlalchemy.inspection import inspect

import schemaql.generator as generator
import schemaql.tester as tester
from schemaql.helper import check_directory_exists, read_yaml, schemaql_path
from schemaql.connections.base_connection import Connection
from schemaql.connections.bigquery import BigQueryConnection
from schemaql.logger import logger, Fore, Back, Style


def main(
    action_prm: ("Action ('test', or 'generate')", "option", "a") = "generate",
    project_prm: ("Project", "option", "p") = "",
    project_file_prm: ("Project file", "option", "f") = "projects.yml",
    connections_file_prm: ("Connections file", "option", "c") = "connections.yml",
):

    projects = read_yaml(project_file_prm)["projects"]
    connections = read_yaml(connections_file_prm)

    for project_name in projects:
        if len(project_prm) > 0 and project_prm != project_name:
            continue

        project = projects[project_name]
        connection_name = project["connection"]

        connection_info = connections[connection_name]

        databases = project["schema"]

        assert "type" in connection_info, "'type' needs to be specified in connetions.yml"
        connection_type = connection_info["type"]
        supported_connections = {"snowflake": Connection, "bigquery": BigQueryConnection}
        
        assert connection_type in supported_connections, f"'{connection_type}' is currently not supported"
        conn = supported_connections[connection_type](connection_info)

        if action_prm == "generate":
            generator.generate_table_schema(conn, databases, project_name)
        elif action_prm == "test":
            test_results = tester.test_schema(conn, databases, project_name)
            tester.save_test_results(project_name, test_results)

if __name__ == "__main__":

    plac.call(main)
