import sqlalchemy
from snowflake.sqlalchemy import URL
import snowflake.connector
from sqlalchemy import create_engine


class Connection(object):
    """
    Database Connection
    """

    def __init__(self, connection_info):
        self._connection_type = connection_info['type']
        
        self._database = connection_info['database']
        self._schema = connection_info['schema']
        self._user_name = connection_info['user']
        self._password = connection_info['password']

    @property
    def connection_type(self):
        return self._connection_type

    @connection_type.setter
    def connection_type(self, val):
        self._connection_type = val

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

class SnowflakeConnection(Connection):
    """
    Snowflake Database Connection
    """

    def __init__(self, connection_info):
        
        super().__init__(connection_info)
        
        self._account = connection_info['account']
        self._role = connection_info['role']
        self._warehouse = connection_info['warehouse']
        
        self._engine = self._get_connection()
    
    @property
    def engine(self):
        return self._engine

    @engine.setter
    def engine(self, val):
        self._engine = val
        
    @property
    def account(self):
        return self._account

    @account.setter
    def account(self, val):
        self._account = val
        
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
    
        engine = create_engine(URL(
            account=self._account,
            user=self._user_name,
            password=self._password,
            role=self._role,
            warehouse=self._warehouse,
            database=self._database,
            schema=self._schema
        ))

        if connect:
            return engine.connect()
        else:
            return engine

    def connect():
        
        self._engine.connect()
                        

