import boto3
import boto3.exceptions
import botocore.exceptions
import logging
import os

from ubiops_connector import OutputConnector, ConnectorError, RecoverableConnectorError, get_variable, retry


logger = logging.getLogger('Amazon S3 Connector')


class Deployment(OutputConnector):

    def __init__(self, base_directory, context):
        """
        :param str base_directory: absolute path to the directory where the deployment file is located
        :param dict context: a dictionary containing details of the deployment that might be useful in your code
        """

        OutputConnector.__init__(self, base_directory, context)

        # Setup the client
        self.s3_client = self.setup()

    def setup(self):
        """
        Setup the S3 client

        :return: boto3.client client: the client representing the S3 storage
        """

        try:
            return boto3.client(
                service_name='s3',
                region_name=get_variable('REGION'),
                aws_access_key_id=get_variable('ACCESS_KEY'),
                aws_secret_access_key=get_variable('SECRET_KEY')
            )
        except (botocore.exceptions.ClientError, botocore.exceptions.BotoCoreError) as e:
            raise ConnectorError(f"Failed to initialise S3 client {e}")

    @retry(attempts=3)
    def insert(self, data):
        """
        Insert given data to S3 storage

        :param dict data: a dictionary containing the data to be inserted
        """

        # The field 'blob' must be present in the data. It contains the blob file path to be uploaded.
        if 'blob' not in data:
            raise ConnectorError("Field 'blob' is not given in the input")

        file_path = data['blob']
        filename = os.path.basename(file_path)

        bucket = get_variable('BUCKET')
        path_prefix = get_variable('PATH_PREFIX', '')

        # Generate the object path by concatenating (optional) path prefix and filename
        object_path = os.path.join(path_prefix, filename)

        try:
            # The upload_file method accepts a file name, a bucket name, and an object name. The method handles large
            # files by splitting them into smaller chunks and uploading each chunk in parallel.
            self.s3_client.upload_file(file_path, bucket, object_path)

        except (botocore.exceptions.ClientError, botocore.exceptions.BotoCoreError) as e:
            raise RecoverableConnectorError(f"Failed to insert blob: {e}")
        except boto3.exceptions.S3UploadFailedError as e:
            raise ConnectorError(f"Failed to insert blob: {e}")

        logger.info("Blob inserted successfully")
