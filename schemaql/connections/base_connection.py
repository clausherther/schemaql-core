import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.inspection import inspect

from schemaql.logger import logger


class Connection(object):
    """
    Database Connection
    """

    def __init__(self, connection_info):

        self._connection_type = connection_info["type"]
        self._user = connection_info["user"] if "user" in connection_info else None
        self._password = (
            connection_info["password"] if "password" in connection_info else None
        )
        self._database = (
            connection_info["database"] if "database" in connection_info else None
        )
        self._schema = (
            connection_info["schema"] if "schema" in connection_info else None
        )
        self._url = connection_info["url"] if "url" in connection_info else None

        self._connect_url = None
        self._engine = None
        self._inspector = None

    @property
    def engine(self):
        if self._engine is None:
            self._make_engine(connect=False)
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

    # @schema.setter
    # def connect_url(self, val):
    #     self._connect_url = val

    def _make_url(self):

        url = f"{self._url}"
        if self.database:
            url += f"/{self.database}"
        if self.schema:
            url += f"/{self.schema}"

        return url

    def _make_engine(self, connect=False):

        self._engine = create_engine(self.connect_url)
        logger.info(self.engine)

        if connect:
            return self._engine.connect()
        else:
            return self._engine

    def connect(self):

        return self._engine.connect()

    def get_schema_names(self, database):

        schema_names = sorted(self.inspector.get_schema_names())

        return schema_names

    def get_table_names(self, schema):

        table_names = sorted(self.inspector.get_table_names(schema))

        return table_names

    def get_columns(self, table, schema):

        columns = self.inspector.get_columns(table, schema)

        return columns
