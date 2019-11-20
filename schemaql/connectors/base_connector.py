import sqlalchemy
from sqlalchemy import create_engine, exc
from sqlalchemy.inspection import inspect

from schemaql.helpers.logger import logger


class Connector(object):
    """
    Database Connector
    """

    def __init__(self, connection_info):

        self._connector_type = connection_info["type"]

        self._user = connection_info.get("user", None)
        self._password = connection_info.get("password", None)
        self._database = connection_info.get("database", None)
        self._schema = connection_info.get("schema", None)
        self._url = connection_info.get("password", None)
        self._supports_multi_insert = connection_info.get("supports_multi_insert", True)

        self._connect_url = None
        self._engine = None
        self._inspector = None


    @property
    def connector_type(self):
        return self._connector_type

    @property
    def engine(self):
        if self._engine is None:
            self._engine = self._make_engine()
            logger.info(self._engine)
        return self._engine

    @property
    def inspector(self):
        if self._inspector is None:
            self._inspector = inspect(self.engine)
        return self._inspector

    @property
    def user(self):
        return self._user

    @user.setter
    def user(self, val):
        self._user = val

    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, val):
        self._password = val

    @property
    def database(self):
        return self._database

    @database.setter
    def database(self, val):
        self._database = val

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, val):
        self._schema = val

    @property
    def connect_url(self):
        self._connect_url = self._make_url()
        return self._connect_url


    @property
    def supports_multi_insert(self):
        return self._supports_multi_insert
        
    
    def _make_url(self):

        url = f"{self._url}"
        if self.database:
            url += f"/{self.database}"
        if self.schema:
            url += f"/{self.schema}"

        return url

    def _make_engine(self):

        return create_engine(self.connect_url)

    def connect(self):

        return self.engine.connect()

    def get_schema_names(self, database):

        schema_names = sorted(self.inspector.get_schema_names())

        return schema_names

    def get_table_names(self, schema):

        table_names = sorted(self.inspector.get_table_names(schema))

        return table_names

    def get_columns(self, table, schema):

        columns = self.inspector.get_columns(table, schema)

        return columns

    def get_column_names(self, table, schema):

        columns = self.inspector.get_columns(table, schema)

        return [c["name"] for c in columns]

    def execute(self, sql):

        with self.engine.connect() as cur:

            try:
                rs = cur.execute(sql)
                return rs
            except exc.DBAPIError as ex:
                # an exception is raised, Connection is invalidated.
                logger.error(f"Connection Error {ex}")
                raise ex
    
    def execute_return_one(self, sql):

        rs = self.execute(sql)
        result = rs.fetchone() if rs else None
        return result
