import json
import logging
import os
import tempfile

from google.cloud import storage, exceptions

from ubiops_connector import OutputConnector, ConnectorError, RecoverableConnectorError, get_variable, retry

logger = logging.getLogger('Google Cloud Storage Connector')

DEFAULT_MULTIPART_CHUNK_SIZE = 10 * 1024 * 1024  # 10 MB


class Deployment(OutputConnector):

    def __init__(self, base_directory, context):
        """
        :param str base_directory: absolute path to the directory where the deployment file is located
        :param dict context: a dictionary containing details of the deployment that might be useful in your code
        """

        OutputConnector.__init__(self, base_directory, context)

        # Set the chunk size for uploads
        self.chunk_size = DEFAULT_MULTIPART_CHUNK_SIZE

        # Write credentials string to a temporary file
        self.key_file = tempfile.NamedTemporaryFile()

        # Setup the client
        self.client = self.setup()

    def setup(self):
        """
        Setup the GCS client

        :return: storage.Client client: the client which is created with the retrieved JSON credentials
        """

        try:
            self.key_file.write(bytearray(get_variable('JSON_KEY_FILE'), 'utf-8'))
            self.key_file.flush()
            return storage.Client.from_service_account_json(json_credentials_path=self.key_file.name)
        except (json.decoder.JSONDecodeError, TypeError, ValueError) as e:
            raise ConnectorError(f"Failed to initialise GCS client: {e}")

    @retry(attempts=3)
    def insert(self, data):
        """
        Insert given data to GCS

        :param dict data: a dictionary containing the data to be inserted
        """

        # The field 'blob' must be present in the data. It contains the blob file path to be uploaded.
        if 'blob' not in data:
            raise ConnectorError("Field 'blob' is not given in the input")

        file_path = data['blob']
        filename = os.path.basename(file_path)

        bucket = get_variable('BUCKET')
        path_prefix = get_variable('PATH_PREFIX', '')

        # Generate the destination blob name by concatenating (optional) path prefix and filename
        object_path = os.path.join(path_prefix, filename)

        try:
            bucket = self.client.bucket(bucket_name=bucket)
            blob = bucket.blob(object_path, chunk_size=self.chunk_size)
            blob.upload_from_filename(filename=file_path)

        except exceptions.GoogleCloudError as e:
            raise RecoverableConnectorError(f"Failed to insert blob: {e}")

        logger.info("Blob inserted successfully")

    def stop(self):
        """
        Close connection to GCS by closing the credentials file descriptor
        """

        self.key_file.close()
