import sqlalchemy
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

from schemaql.connectors.base_connector import Connector


class SnowflakeConnector(Connector):
    """
     Snowflake Connection
    """

    def __init__(self, connection_info):

        self._account = connection_info["account"]
        self._conn_params = {}

        _prms = ["warehouse", "role"]

        for p in _prms:
            if p in connection_info:
                self._conn_params[p] = connection_info[p]

        super().__init__(connection_info)

    @property
    def account(self):
        return self._account

    @account.setter
    def account(self, val):
        self._account = val

    @property
    def warehouse(self):
        return self._warehouse

    @warehouse.setter
    def warehouse(self, val):
        self._warehouse = val

    @property
    def role(self):
        return self._role

    @role.setter
    def role(self, val):
        self._role = val


    def _make_url(self):

        url = f"snowflake://{self._user}:{self._password}@{self._account}"
        if self.database:
            url += f"/{self.database}"
        if self.schema:
            url += f"/{self.schema}"

        if self._conn_params:
            url += f"?"
            for k,v in self._conn_params.items():
                url += f"{k}={v}&"

        return url
