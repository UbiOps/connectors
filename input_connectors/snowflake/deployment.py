import logging

import snowflake
import snowflake.connector.errors
from ubiops_connector import InputConnector, ConnectorError, RecoverableConnectorError, get_variable, retry


logger = logging.getLogger('Snowflake Connector')


class Deployment(InputConnector):
    """
    Snowflake connector
    """

    def __init__(self, base_directory, context):
        """
        :param str base_directory: absolute path to the directory where the deployment file is located
        :param dict context: a dictionary containing details of the deployment that might be useful in your code
        """

        InputConnector.__init__(self, base_directory, context)

        self.connection = None
        self.database = None
        self.schema = None

    def connect(self):
        """
        Connect to a Snowflake database based on the environment variables specified.
        """

        self.connection = None
        self.database = get_variable('DATABASE')
        self.schema = get_variable('SCHEMA', 'public')

        try:
            self.connection = snowflake.connector.connect(
                user=get_variable('USERNAME'),
                password=get_variable('PASSWORD'),
                account=get_variable('ACCOUNT'),
                database=self.database
            )
        except snowflake.connector.errors.DatabaseError as e:
            raise RecoverableConnectorError(f"Failed to connect to database: {e}")
        except Exception as e:
            raise RecoverableConnectorError(f'Unknown error occurred when connecting to database {e}')

    @retry(attempts=3)
    def retrieve(self):
        """
        Retrieve data from Snowflake by executing the string query.

        :return dict|list: dictionary with the values expected as output of the deployment, or a list of those
            dictionaries
        """

        # Including the connect in the retrieve method such that it can benefit from retrying
        if not self.connection:
            self.connect()

        try:
            logger.info("Retrieving data")
            cursor = self.connection.cursor(snowflake.connector.DictCursor)
            
            # Use the schema that is specified by the user
            cursor.execute(f"USE SCHEMA {self.database}.{self.schema}")
            cursor.execute(self.get_query())
            result = cursor.fetchall()

            logger.info(f"Retrieved {len(result)} rows")

            # Convert all database columns to lower case
            result = [{k.lower(): v for k, v in x.items()} for x in result]

            # Return the result, assuming that the column names returned from the query are matching the deployment
            # output fields
            return result

        except snowflake.connector.errors.ProgrammingError as e:
            raise ConnectorError(f"Invalid query or schema error: {e}")
        except snowflake.connector.errors.DatabaseError as e:
            raise ConnectorError(f"Error while fetching data: {e}")

    def stop(self):
        """
        Close connection to the database if the connection has been initialised
        """

        try:
            self.connection.close()
        except Exception:
            pass

        self.connection = None

    @staticmethod
    def get_query():
        """
        Gets query string, either from the environment variable or, if that is not provided, the hardcoded query

        :return str: the query string that will run when a request is made
        """

        # You can hard code a query here, that will be used if the QUERY environment variable is not set.
        default_query = "SELECT * FROM product WHERE price < 2.5;"
        env_query = get_variable('QUERY', default_query)

        if env_query and len(env_query) > 0:
            return env_query
        return default_query
