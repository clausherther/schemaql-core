import sqlalchemy
from sqlalchemy import create_engine

# from snowflake.sqlalchemy import URL
# import snowflake.connector

from schemaql.connections.base_connection import Connection


class SnowflakeConnection(Connection):
    """
     Snowflake Connection
    """

    def __init__(self, connection_info):

        super().__init__(connection_info)
        self._account = connection_info["account"]

    @property
    def account(self):
        return self._account

    @account.setter
    def account(self, val):
        self._account = val

    @property
    def connect_url(self):
        self._connect_url = self._make_url()
        return self._connect_url

    def _make_url(self):

        url = f"snowflake://{self._user}:{self._password}@{self._account}"
        if self.database:
            url += f"/{self.database}"
        if self.schema:
            url += f"/{self.schema}"

        return url
