from pathlib import Path
from jinja2 import Template, FileSystemLoader, Environment
from sqlalchemy.inspection import inspect

from schemaql.helper import check_directory_exists, read_yaml, schemaql_path
from schemaql.connection import Connection
from schemaql.logger import logger, Fore, Back, Style


def make_schema_yaml(schema, table, columns):
    template_path = schemaql_path.joinpath("templates", "yaml").resolve()
    loader = FileSystemLoader(str(template_path))
    env = Environment(loader=loader)
    template = env.get_template("schema.yml")
    yml = template.render(schema=schema, table=table, columns=columns)
    return yml


def generate_table_schema(conn, databases, project_name):

    for database in databases:
        logger.info(f"database=={database}")
        conn.database = database
        conn.create_engine()
        logger.info(conn.engine)

        inspector = inspect(conn.engine)
        schemas = databases[database]
        logger.info(f"schemas=={schemas}")

        if schemas is None:
            logger.info("No schemas specified, getting all schemas from database...")
            schemas = sorted(inspector.get_schema_names())

        for schema in schemas:
            logger.info(f"schema=={schema}")
            # inspector = inspect(conn.engine)
            tables = sorted(inspector.get_table_names(f"{schema}"))
            tables = [table.replace(f"{schema}.", "") for table in tables if schema in table]
            logger.info(tables)
            for table in tables:
                columns = inspector.get_columns(table, schema)
                logger.info(
                    f"Generating schema for {database}.{schema}.{table} ({len(columns)})"
                )
                yml = make_schema_yaml(schema, table, columns)

                schema_directory = Path("output").joinpath(project_name, database, schema)
                check_directory_exists(schema_directory)
                yml_file_path = schema_directory.joinpath(f"{table}.yml")
                yml_file_path.write_text(yml)
