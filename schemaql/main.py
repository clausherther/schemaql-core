from pathlib import Path
import json
import plac
from jinja2 import Template, FileSystemLoader, Environment
from sqlalchemy.inspection import inspect

import schemaql.generator as generator
import schemaql.tester as tester
from schemaql.helper import check_directory_exists, read_yaml, schemaql_path
from schemaql.connection import Connection
from schemaql.logger import logger, Fore, Back, Style


def main(
    action_prm: ("Action ('test', or 'generate')", "option", "a") = "test",
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
        connection_type = connection_info["type"]

        databases = project["schema"]
        conn = Connection(connection_info)

        if action_prm == "generate":
            generator.generate_table_schema(conn, databases, project_name)
        elif action_prm == "test":
            test_results = tester.test_schema(conn, databases, project_name)
            tester.save_test_results(project_name, test_results)
