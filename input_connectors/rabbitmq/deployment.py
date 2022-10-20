import logging
import pika
import pika.exceptions
import socket

from ubiops_connector import InputConnector, ConnectorError, RecoverableConnectorError, get_variable, retry


logger = logging.getLogger('RabbitMQ Connector')


class Deployment(InputConnector):
    """
    RabbitMQ connector
    """

    def __init__(self, base_directory, context):
        """
        :param str base_directory: absolute path to the directory where the deployment file is located
        :param dict context: a dictionary containing details of the deployment that might be useful in your code
        """

        InputConnector.__init__(self, base_directory, context)

        # Initialize a connection object. It will be set by the connect method.
        self.connection = None

    def connect(self):
        """
        Connect to RabbitMQ

        :return: a new connection object to RabbitMQ
        """

        parameters = pika.ConnectionParameters(
            host=get_variable('HOST'),
            port=int(get_variable('PORT')),
            credentials=pika.PlainCredentials(
                username=get_variable('USERNAME'),
                password=get_variable('PASSWORD')
            ),
            virtual_host=get_variable('VIRTUAL_HOST', '/'),
            socket_timeout=int(get_variable('TIMEOUT', '10')),
            blocked_connection_timeout=int(get_variable('TIMEOUT', '10')),
            stack_timeout=int(get_variable('TIMEOUT', '10')),
            heartbeat=int(get_variable('TIMEOUT', '10'))
        )

        try:
            self.connection = pika.BlockingConnection(parameters)
        except pika.exceptions.ProbableAuthenticationError as e:
            raise ConnectorError(f"Failed to authenticate at RabbitMQ server: {e}")
        except (socket.gaierror, pika.exceptions.AMQPError) as e:
            raise RecoverableConnectorError(f"Failed to connect to RabbitMQ: {e}")

    @retry(attempts=3)
    def retrieve(self):
        """
        Retrieve data from RabbitMQ, maximum MAX_OUTPUT messages at a time. Retry when failing due to a lost
        connection.

        :return dict|list: dictionary with the values expected as output of the deployment, or a list of those
            dictionaries
        """

        # Include connect() in the retrieve method such that it can benefit from retrying
        if not self.connection:
            self.connect()

        data = []

        # Collect MAX_OUTPUT messages in one call at most
        for _ in range(0, int(get_variable('MAX_OUTPUT', '50'))):
            try:
                channel = self.connection.channel()
                method_frame, header_frame, body = channel.basic_get('input_connector')
            except pika.exceptions.AMQPError as e:
                # If we already collected some messages that were acknowledged, we need to return those now.
                # Otherwise, these will be lost
                if len(data) > 0:
                    logger.warning(f"Failed to acknowledge message: {e}. Returning already collecting messages")
                    break

                # If nothing was collected yet, we raise an exception to retry
                self.connection = None
                raise RecoverableConnectorError(f"Failed to retrieve message from RabbitMQ: {e}")

            if method_frame:
                try:
                    channel.basic_ack(method_frame.delivery_tag)
                except pika.exceptions.AMQPError as e:
                    # If we already collected some messages that were acknowledged, we need to return those now.
                    # Otherwise, these will be lost
                    if len(data) > 0:
                        logger.warning(f"Failed to acknowledge message: {e}. Returning already collecting messages")
                        break

                    # If nothing was collected yet, we raise an exception to retry
                    self.connection = None
                    raise RecoverableConnectorError(f"Failed to acknowledge message: {e}")

                data.append({'message': body.decode('utf-8')})

            else:
                # No more messages available, end the loop
                break

        logger.info(f"Retrieved {len(data)} rows")

        return data

    def stop(self):
        """
        Close connection to RabbitMQ
        """

        # Close the connection. Ignore errors as we do not care about the connection anymore.
        try:
            self.connection.close()
        except (AttributeError, pika.exceptions.AMQPError):
            pass

        self.connection = None
