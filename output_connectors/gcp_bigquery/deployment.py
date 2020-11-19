import json
import logging
import tempfile

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError, NotFound

from ubiops_connector import OutputConnector, ConnectorError, RecoverableConnectorError, get_variable, retry

logger = logging.getLogger('BigQuery Connector')


class Deployment(OutputConnector):

    def __init__(self, base_directory, context):
        """
        :param str base_directory: absolute path to the directory where the deployment file is located
        :param dict context: a dictionary containing details of the deployment that might be useful in your code
        """

        OutputConnector.__init__(self, base_directory, context)

        # Write credentials string to a temporary file
        self.key_file = tempfile.NamedTemporaryFile()

        # Setup the client
        self.client = self.setup()

    def setup(self):
        """
        Setup the BigQuery client

        :return: bigquery.Client client: the client which is created with the retrieved JSON credentials
        """

        try:
            self.key_file.write(bytearray(get_variable('JSON_KEY_FILE'), 'utf-8'))
            self.key_file.flush()
            return bigquery.Client.from_service_account_json(json_credentials_path=self.key_file.name)
        except (json.decoder.JSONDecodeError, TypeError) as e:
            raise ConnectorError(f"Failed to initialise BigQuery client: {e}")

    @retry(attempts=3)
    def insert(self, data):
        """
        Insert given data to BigQuery

        :param dict data: a dictionary containing the data to be inserted
        """

        # Initialize table
        table_name = f"{get_variable('DATASET')}.{get_variable('TABLE')}"

        try:
            table = self.client.get_table(table_name)
            error = self.client.insert_rows(table=table, rows=[data])
        except NotFound:
            raise ConnectorError(f"Table {table_name} was not found")
        except (ValueError, TypeError, GoogleAPIError) as e:
            raise ConnectorError(f"Failed to insert data: {e}")

        if error:
            message = error[0]['errors'][0]['message']
            raise RecoverableConnectorError(f"Failed to insert data: {message}")

        logger.info("Data inserted successfully")

    def stop(self):
        """
        Close connection to BigQuery by closing the credentials file descriptor
        """

        self.key_file.close()
