import sqlalchemy
from sqlalchemy import create_engine

# from snowflake.sqlalchemy import URL
# import snowflake.connector

class Connection(object):
    """
    Database Connection
    """

    def __init__(self, connection_info):

        self._connection_type = connection_info["type"]
        self._url = connection_info["url"]
        self._database = connection_info["database"]
        self._schema = connection_info["schema"]
        # self._user_name = connection_info["user"]
        # self._password = connection_info["password"]

        self._connect_url = self._make_url()
        self._engine = None

    @property
    def engine(self):
        if self._engine is None:
            self.create_engine(connect=False)
        return self._engine

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
        return f"{self._url}/{self.database}/{self.schema}"

    def create_engine(self, connect=False):

        self._engine = create_engine(self.connect_url)

        if connect:
            return self._engine.connect()
        else:
            return self._engine

    def connect(self):

        return self._engine.connect()
