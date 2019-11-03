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

        self._account = connection_info["account"]
        super().__init__(connection_info)

    @property
    def account(self):
        return self._account

    @account.setter
    def account(self, val):
        self._account = val

    def _make_url(self):

        url = f"snowflake://{self._user}:{self._password}@{self._account}"
        if self.database:
            url += f"/{self.database}"
        if self.schema:
            url += f"/{self.schema}"

        return url
