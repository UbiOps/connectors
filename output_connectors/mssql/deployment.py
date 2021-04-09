import logging
import sqlalchemy
import sqlalchemy.exc


from ubiops_connector import OutputConnector, ConnectorError, RecoverableConnectorError, get_variable, retry


logger = logging.getLogger('MSSQL Connector')


class Deployment(OutputConnector):
    """
    MsSQL connector
    """

    def __init__(self, base_directory, context):
        """
        :param str base_directory: absolute path to the directory where the deployment file is located
        :param dict context: a dictionary containing details of the deployment that might be useful in your code
        """

        OutputConnector.__init__(self, base_directory, context)

        # Initialize a connection object. It will be set by the connect method.
        self.connection = None

    def connect(self):
        """
        Connect to MSSQL database

        :return: connection: a new connection object to the database
        """

        self.connection = None

        username = get_variable('USERNAME')
        password = get_variable('PASSWORD')
        host = get_variable('HOST')
        port = get_variable('PORT', '1433')
        database = get_variable('DATABASE')
        timeout = get_variable('TIMEOUT', '60')

        try:
            connection_string = f"mssql+pyodbc://{username}:{password}@{host}:{port}/{database}?" \
                "driver=ODBC Driver 17 for SQL Server"
            # fast_executemany speeds up the insertion up to a 100-fold 
            self.connection = sqlalchemy.create_engine(
                url=connection_string,
                fast_executemany=True,
                connect_args={"timeout": int(timeout)}).connect()
        except (sqlalchemy.exc.OperationalError, sqlalchemy.exc.InterfaceError) as e:
            raise RecoverableConnectorError(f"Failed to connect to database: {e}")

    @retry(attempts=3)
    def insert(self, data):
        """
        Insert given data to MsSQL

        :param dict data: a dictionary containing the data to be inserted
        """

        # Including the connect in the insert method such that it can benefit from retrying
        if not self.connection:
            self.connect()

        # Construct the insert query
        columns = ", ".join([f"{col}" for col in data.keys()])
        values = ", ".join([f":{col}" for col in data.keys()])
        query = f"INSERT INTO {get_variable('SCHEMA')}.{get_variable('TABLE')} ({columns}) " \
                f"VALUES ({values})"

        try:
            self.connection.execute(sqlalchemy.text(query), **data)

        except (sqlalchemy.exc.ProgrammingError, sqlalchemy.exc.InvalidRequestError, sqlalchemy.exc.StatementError) \
                as e:
            self.connection = None
            raise RecoverableConnectorError(f"Failed to insert data: {e}")

        except (sqlalchemy.exc.DatabaseError, Exception) as e:
            self.connection = None
            raise ConnectorError(f"Failed to insert data: {e}")

        logger.info("Data inserted successfully")

    def stop(self):
        """
        Close connection to the database if the connection has been initialised
        """

        try:
            self.connection.close()
        except (AttributeError, sqlalchemy.exc.DatabaseError, Exception):
            pass

        self.connection = None
