import plac
import helper
from logger import logger
from connection import SnowflakeConnection
from sqlalchemy.inspection import inspect

def main(project_file: ("Project file", "option", "p") = "projects.yml",
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

        list_tables(conn, schemas)

def list_tables(conn, schemas):

    inspector = inspect(conn.engine)

    for schema in schemas:
        tables = inspector.get_table_names(schema)
        for table in tables:
            logger.info(table)
            # columns = inspector.get_columns(table, schema)
            # logger.info(columns)
                    
if __name__ == '__main__':

    plac.call(main)
