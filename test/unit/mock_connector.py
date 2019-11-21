from schemaql.helpers.logger import logger


class MockConnector(object):
    """
    Mock Database Connector
    """

    def __init__(self, connection_info):

        self._connector_type = connection_info["type"]

        self._user = connection_info.get("user", None)
        self._password = connection_info.get("password", None)
        self._database = connection_info.get("database", None)
        self._schema = connection_info.get("schema", None)
        self._url = connection_info.get("url", None)
        self._supports_multi_insert = connection_info.get("supports_multi_insert", True)

        self._connect_url = None
        self._engine = None

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

        return None

    def connect(self):

        raise NotImplementedError("connect not implemented!")

    def get_schema_names(self, database):

        return []

    def get_table_names(self, schema):

        return []

    def get_columns(self, table, schema):

        return []

    def get_column_names(self, table, schema):

        return []

    def execute(self, sql):

        raise NotImplementedError("execute not implemented!")

    def execute_return_one(self, sql):

        raise NotImplementedError("execute_return_one not implemented!")
