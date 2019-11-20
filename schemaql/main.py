import plac

from schemaql.collectors import CsvCollector, DbCollector, JsonCollector
from schemaql.connectors.bigquery import BigQueryConnector
from schemaql.connectors.snowflake import SnowflakeConnector
from schemaql.helpers.fileio import read_yaml, schemaql_path
from schemaql.helpers.logger import logger
from schemaql.project import Project


def _get_collector(collector_config, connections):

    supported_collectors = {
        "json": JsonCollector,
        "csv": CsvCollector,
        "database": DbCollector,
    }

    assert (
        "type" in collector_config
    ), "collector 'type' needs to be specified in config.yml"
    collector_type = collector_config["type"]

    if "connection" in collector_config:
        connector = _get_connector(connections[collector_config["connection"]])
        collector = supported_collectors[collector_type](collector_config,
                                                         connector)
    else:
        collector = supported_collectors[collector_type](collector_config)

    return collector


def _get_connector(connection_info):

    supported_connectors = {
        "snowflake": SnowflakeConnector,
        "bigquery": BigQueryConnector,
    }

    assert (
        "type" in connection_info
    ), "'type' needs to be specified in connections.yml"
    connection_type = connection_info["type"]

    assert (
        connection_type in supported_connectors
    ), f"'{connection_type}' is currently not supported"
    connector = supported_connectors[connection_type](connection_info)

    return connector


def _get_project_config(projects_config, project_name, connections):

    project_config = projects_config[project_name]
    connection_info = connections[project_config["connection"]]

    connector = _get_connector(connection_info)

    databases = project_config["schema"]
    return connector, databases


@plac.annotations(
    action=("Action ('test', 'agg' or 'generate')"),
    project=("Project", "option", "p"),
    config_file=("Config file", "option", "c"),
    connections_file=("Connections file", "option", "x"),
)
def main(
    action,
    project=None,
    config_file="config.yml",
    connections_file="connections.yml",
):

    logger.info(f"schemaql_path: {schemaql_path}")

    config = read_yaml(config_file)
    connections = read_yaml(connections_file)

    assert (
        "collector" in config
    ), "'collector' needs to be specified in config.yml"

    collector_config = config["collector"]
    collector = _get_collector(collector_config, connections)

    projects_config = config["projects"]
    if project is not None:
        projects_config = {project: projects_config[project]}

    for project_name in projects_config:

        connector, databases = _get_project_config(
            projects_config, project_name, connections
        )

        project = Project(project_name, connector, databases)

        if action == "generate":

            project.generate_database_schema()

        elif action == "test":

            test_results = project.test_database_schema()
            collector.save_test_results(project_name, test_results)

        elif action == "agg":
            metric_results = project.aggregate_database_schema()
            collector.save_test_results(project_name, metric_results)


if __name__ == "__main__":

    plac.call(main)
