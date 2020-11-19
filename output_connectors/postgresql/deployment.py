import logging
import psycopg2

from ubiops_connector import OutputConnector, ConnectorError, RecoverableConnectorError, get_variable, retry


logger = logging.getLogger('PostgreSQL Connector')


class Deployment(OutputConnector):
    """
    PostgreSQL connector
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
        Connect to PostgreSQL database

        :return: connection: a new connection object to the database
        """

        self.connection = None

        try:
            self.connection = psycopg2.connect(
                host=get_variable('HOST'),
                port=int(get_variable('PORT', '5432')),
                user=get_variable('USERNAME'),
                password=get_variable('PASSWORD'),
                database=get_variable('DATABASE'),
                connect_timeout=int(get_variable('TIMEOUT', '10'))
            )
        except psycopg2.Error as e:
            raise RecoverableConnectorError(f"Failed to connect to database: {e}")

    @retry(attempts=3)
    def insert(self, data):
        """
        Insert given data to PostgreSQL. If the insert fails due to lost database connection, retry inserting.

        :param dict data: a dictionary containing the data to be inserted
        """

        # Including the connect in the insert method such that it can benefit from retrying
        if not self.connection:
            self.connect()

        # Construct the insert query
        columns = ", ".join([f"{col}" for col in data.keys()])
        values = ", ".join(["%s" for _ in data])
        query = f"INSERT INTO {get_variable('SCHEMA', 'public')}.{get_variable('TABLE')} ({columns}) " \
                f"VALUES ({values})"

        params = tuple(data.values())

        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            self.connection.commit()
            cursor.close()

        except psycopg2.OperationalError as e:
            self.connection = None
            raise RecoverableConnectorError(f"Failed to insert data: {e}")

        except psycopg2.Error as e:
            logger.error(f"Failed to insert data. Error Message: {e}")
            raise ConnectorError(f"Failed to insert data: {e}")

        logger.info("Data inserted successfully")

    def stop(self):
        """
        Close connection to the database if the connection has been initialised
        """

        try:
            self.connection.close()
        except (AttributeError, psycopg2.Error):
            pass

        self.connection = None
