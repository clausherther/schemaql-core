from pathlib import Path
import json
import plac
from jinja2 import Template, FileSystemLoader, Environment
from sqlalchemy.inspection import inspect

from schemaql.project import Project
from schemaql.collector import JsonCollector

from schemaql.helper import check_directory_exists, read_yaml, schemaql_path
from schemaql.connections.base_connection import Connection
from schemaql.connections.bigquery import BigQueryConnection
from schemaql.connections.snowflake import SnowflakeConnection
from schemaql.logger import logger, Fore, Back, Style


@plac.annotations(
    action=("Action ('test', or 'generate')", "option", "a"),
    project=("Project", "option", "p"),
    config_file=("Config file", "option", "c"),
    connections_file=("Connections file", "option", "x"),
)
def main(
    action="test",
    project=None,
    config_file="config.yml",
    connections_file="connections.yml",
):

    supported_collectors = {"json": JsonCollector}
    supported_connections = {
        "snowflake": SnowflakeConnection,
        "bigquery": BigQueryConnection,
    }

    config = read_yaml(config_file)

    collector_config = config["collector"]
    assert (
        "type" in collector_config
    ), "collector 'type' needs to be specified in config.yml"
    collector_type = collector_config["type"]
    collector = supported_collectors[collector_type](collector_config)

    projects = config["projects"]
    if project is not None:
        projects = {project: projects[project]}

    connections = read_yaml(connections_file)

    for project_name in projects:

        project = projects[project_name]
        connection_name = project["connection"]

        connection_info = connections[connection_name]

        databases = project["schema"]

        assert (
            "type" in connection_info
        ), "'type' needs to be specified in connections.yml"
        connection_type = connection_info["type"]

        assert (
            connection_type in supported_connections
        ), f"'{connection_type}' is currently not supported"
        connector = supported_connections[connection_type](connection_info)

        project = Project(project_name, connector, databases)

        if action == "generate":

            project.generate_database_schema() 

        elif action == "test":

            test_results = project.test_database_schema()
            collector.save_test_results(project_name, test_results)


if __name__ == "__main__":

    plac.call(main)
