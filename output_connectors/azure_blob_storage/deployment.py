import os
import logging

import azure.storage.blob
import azure.core.exceptions

from ubiops_connector import OutputConnector, ConnectorError, RecoverableConnectorError, get_variable, retry

logger = logging.getLogger('Azure Blob Storage Connector')


class Deployment(OutputConnector):

    def __init__(self, base_directory, context):
        """
        :param str base_directory: absolute path to the directory where the deployment file is located
        :param dict context: a dictionary containing details of the deployment that might be useful in your code
        """

        OutputConnector.__init__(self, base_directory, context)

        # Setup the Azure blob client
        self.blob_service_client = self.setup()

    def setup(self):
        """
        Setup the Azure Blob Storage client

        :return: azure.storage.blob.BlobServiceClient client: a blob service client
        """

        try:
            return azure.storage.blob.BlobServiceClient.from_connection_string(
                conn_str=get_variable('CONNECTION_STRING', '')
            )
        except azure.core.exceptions.AzureError as e:
            raise ConnectorError(f"Failed to initialise Azure Storage client: {e}")

    @retry(attempts=3)
    def insert(self, data):
        """
        Insert given data to Azure Blob Storage

        :param dict data: a dictionary containing the data to be inserted
        """

        # The field 'blob' must be present in the data. It contains the blob file path to be uploaded.
        if 'blob' not in data:
            raise ConnectorError("Field 'blob' is not given in the input")

        file_path = data['blob']
        filename = os.path.basename(file_path)

        path_prefix = get_variable('PATH_PREFIX', '')

        # Generate the object path by concatenating (optional) path prefix and filename
        object_path = os.path.join(path_prefix, filename)

        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=get_variable('CONTAINER'), blob=object_path
            )

            with open(file_path, "rb") as data:
                blob_client.upload_blob(data=data,  timeout=int(get_variable('TIMEOUT', '10')))

        except azure.core.exceptions.AzureError as e:
            raise RecoverableConnectorError(f"Failed to insert blob: {e}")

        logger.info("Blob inserted successfully")
