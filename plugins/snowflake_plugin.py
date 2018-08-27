import logging
import snowflake.connector
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator, TaskInstance
from airflow.utils.decorators import apply_defaults
from airflow.hooks.dbapi_hook import DbApiHook

class SnowflakeHook(DbApiHook):
    """
    Interact with Snowflake.
    get_sqlalchemy_engine() depends on snowflake-sqlalchemy
    """

    conn_name_attr = 'snowflake_conn_id'
    default_conn_name = 'snowflake_default'
    supports_autocommit = True

    def __init__(self, *args, **kwargs):
        super(SnowflakeHook, self).__init__(*args, **kwargs)
        # self.account = kwargs.pop("account", None)
        #self.warehouse = kwargs.pop("warehouse", None)
        #self.database = kwargs.pop("database", None)

    def _get_conn_params(self):
        """
        one method to fetch connection params as a dict
        used in get_uri() and get_connection()
        """
        conn = self.get_connection(self.snowflake_conn_id)
        # account = conn.extra_dejson.get('account', None)
        warehouse = conn.extra_dejson.get('warehouse', None)
        database = conn.extra_dejson.get('database', None)

        conn_config = {
            "user": conn.login,
            "password": conn.password or '',
            "schema": conn.schema or '',
            "database": database or self.database or '',
            "account": conn.host,
            "warehouse": warehouse or self.warehouse or ''
        }
        print(conn_config)
        return conn_config

    def get_uri(self):
        """
        override DbApiHook get_uri method for get_sqlalchemy_engine()
        """
        conn_config = self._get_conn_params()
        uri = 'snowflake://{user}:{password}@{account}/{database}/'
        uri += '{schema}?warehouse={warehouse}'
        print(uri.format(**conn_config))
        return uri.format(
            **conn_config)
   
    def get_conn(self):
        """
        Returns a snowflake.connection object
        """
        conn_config = self._get_conn_params()
        #conn = self._snowflake_connection(conn_config['account'], conn_config['user'], conn_config['password'],conn_config['database'],conn_config['schema'],conn_config['warehouse'])
        conn = snowflake.connector.connect(**conn_config)
        return conn

    def _get_aws_credentials(self):
        """
        returns aws_access_key_id, aws_secret_access_key
        from extra
        intended to be used by external import and export statements
        """
        if self.snowflake_conn_id:
            connection_object = self.get_connection(self.snowflake_conn_id)
            if 'aws_secret_access_key' in connection_object.extra_dejson:
                aws_access_key_id = connection_object.extra_dejson.get(
                    'aws_access_key_id')
                aws_secret_access_key = connection_object.extra_dejson.get(
                    'aws_secret_access_key')
        return aws_access_key_id, aws_secret_access_key

    def set_autocommit(self, conn, autocommit):
        conn.autocommit(autocommit)

class SnowflakeOperator(BaseOperator):
    """
    Executes sql code in a Snowflake database
    :param snowflake_conn_id: reference to specific snowflake connection id
    :type snowflake_conn_id: string
    :param sql: the sql code to be executed. (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param warehouse: name of warehouse which overwrite defined
        one in connection
    :type warehouse: string
    :param database: name of database which overwrite defined one in connection
    :type database: string
    """

    template_fields = ('sql',)
    template_ext = ('.sql', '.abcedg')
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self, sql, snowflake_conn_id='snowflake_default', parameters=None,
            autocommit=True, warehouse=None, database=None, *args, **kwargs):
        super(SnowflakeOperator, self).__init__(*args, **kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self.sql = sql
        self.autocommit = autocommit
        self.parameters = parameters
        self.warehouse = warehouse
        self.database = database

    def get_hook(self):
        return SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id,
                             warehouse=self.warehouse, database=self.database)

    def execute(self, context):
        logging.info('Executing: %s', self.sql)
        hook = self.get_hook()
        try:
           response =  hook.get_conn().execute_string(self.sql)
           logging.info('Successful')
           logging.ifo(response)
           #return response
        except Exception as e: 
            logging.info('Unable to execute')
            print(e)


class SnowflakePlugin(AirflowPlugin):
    name = "snowflake_plugin"
    operators = [SnowflakeOperator]
    # A list of class(es) derived from BaseHook
    hooks = [SnowflakeHook]
    # A list of class(es) derived from BaseExecutor
    executors = []
    # A list of references to inject into the macros namespace
    macros = []
    # A list of objects created from a class derived
    # from flask_admin.BaseView
    admin_views = []
    # A list of Blueprint object created from flask.Blueprint
    flask_blueprints = []
    # A list of menu links (flask_admin.base.MenuLink)
    menu_links = []
