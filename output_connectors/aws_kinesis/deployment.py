import boto3
import botocore.exceptions
import json
import logging
import random
import string

from ubiops_connector import OutputConnector, ConnectorError, RecoverableConnectorError, get_variable, retry

logger = logging.getLogger('Amazon Kinesis Stream Connector')


class Deployment(OutputConnector):

    def __init__(self, base_directory, context):
        """
        :param str base_directory: absolute path to the directory where the deployment file is located
        :param dict context: a dictionary containing details of the deployment that might be useful in your code
        """

        OutputConnector.__init__(self, base_directory, context)

        # Setup Kinesis client
        self.kinesis_client = self.setup()

    def setup(self):
        """
        Connect to Amazon Kinesis Stream

        :return: boto3.client client: the client representing the Kinesis stream
        """

        try:
            return boto3.client(
                service_name='kinesis',
                region_name=get_variable('REGION'),
                aws_access_key_id=get_variable('ACCESS_KEY'),
                aws_secret_access_key=get_variable('SECRET_KEY')
            )
        except (botocore.exceptions.ClientError, botocore.exceptions.BotoCoreError) as e:
            raise ConnectorError(f"Failed to initialise Kinesis client {e}")

    @retry(attempts=3)
    def insert(self, data):
        """
        Insert given data to Kinesis stream

        :param dict data: a dictionary containing the data to be inserted
        """

        # Convert the input data dictionary to a single Kinesis record. Generate the partition key randomly.
        record = {
            'Data': json.dumps(data),
            'PartitionKey': ''.join(random.sample(
                string.ascii_uppercase + string.ascii_lowercase + string.digits, 16
            ))
        }

        try:
            self.kinesis_client.put_record(
                StreamName=get_variable('STREAM'),
                Data=record['Data'],
                PartitionKey=record['PartitionKey']
            )

        except (botocore.exceptions.ClientError, botocore.exceptions.BotoCoreError) as e:
            raise RecoverableConnectorError(f"Failed to insert record: {e.response['Error']['Message']}")

        logger.info("Data inserted successfully")
