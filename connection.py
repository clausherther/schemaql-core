import sqlalchemy
from sqlalchemy import create_engine

from snowflake.sqlalchemy import URL
# import snowflake.connector


class Connection(object):
    """
    Database Connection
    """

    def __init__(self, connection_info):

        self._connection_type = connection_info["type"]
        self._database = connection_info["database"]
        self._schema = connection_info["schema"]
        self._user_name = connection_info["user"]
        self._password = connection_info["password"]

        self._engine = None

    @property
    def connection_type(self):
        return self._connection_type

    @connection_type.setter
    def connection_type(self, val):
        self._connection_type = val

    @property
    def engine(self):
        return self._engine

    @engine.setter
    def engine(self, val):
        self._engine = val

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
    def user_name(self):
        return self._user_name

    @user_name.setter
    def user_name(self, val):
        self._user_name = val

    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, val):
        self._password = val

    def _get_connection(self, connect=False):

        driver = "mysql+pymysql"
        connect_url = f"{self._driver}://{self._user_name}:{self._password}@{self._host}"

        engine = create_engine(connect_url)

        if connect:
            return engine.connect()
        else:
            return engine

    def connect(self):

        self._engine = self._get_connection()
        return self._engine.connect()

class SnowflakeConnection(Connection):
    """
    Snowflake Database Connection
    """

    def __init__(self, connection_info):

        super().__init__(connection_info)

        self._account = connection_info["account"]
        self._role = connection_info["role"]
        self._warehouse = connection_info["warehouse"]

    @property
    def account(self):
        return self._account

    @account.setter
    def account(self, val):
        self._account = val

    @property
    def engine(self):
        if self._engine is None:
            self._engine = self._get_connection(connect=False)
        return self._engine

    @property
    def role(self):
        return self._role

    @role.setter
    def role(self, val):
        self._role = val

    @property
    def warehouse(self):
        return self._warehouse

    @warehouse.setter
    def warehouse(self, val):
        self._warehouse = val

    def _get_connection(self, connect=False):

        engine = create_engine(
            URL(
                account=self._account,
                user=self._user_name,
                password=self._password,
                role=self._role,
                warehouse=self._warehouse,
                database=self._database,
                schema=self._schema,
            )
        )

        if connect:
            return engine.connect()
        else:
            return engine

    def connect(self):

        return self.engine.connect()
