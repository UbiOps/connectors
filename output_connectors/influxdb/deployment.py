import dateutil
import logging
import influxdb_client
import influxdb_client.rest
import json
import urllib3

from influxdb_client import WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from ubiops_connector import OutputConnector, ConnectorError, RecoverableConnectorError, get_variable, retry


logger = logging.getLogger('InfluxDB Connector')


class Deployment(OutputConnector):
    """
    InfluxDB connector
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
        Connect to InfluxDB server database

        :return: connection: a new connection object to the database
        """

        self.connection = None

        try:
            self.connection = influxdb_client.InfluxDBClient(
                url=get_variable('URL'),
                token=get_variable('TOKEN'),
                org=get_variable('ORGANIZATION'),
                debug=False
            ).write_api(write_options=SYNCHRONOUS)

        except urllib3.exceptions.HTTPError as e:
            raise RecoverableConnectorError(f"Failed to connect to database: {e}")

    @staticmethod
    def _extract_elements(elements_csv, elements_type='tags'):
        """
        Transforms a string containing key-value pairs of either tags or fields, each separated by commas,
        into a dictionary containing the same key-value pairs.

        :param string elements_csv: string containing key-value pairs of either tags or fields
        :param string elements_type: type of element that is being extracted, for logging purposes
        :return: dictionary containing the key-value pairs of tags or fields
        """

        elements = {}
        try:
            for element in elements_csv.split(','):
                key, value = element.split('=')
                try:
                    elements[key.strip()] = json.loads(value.strip())
                except json.decoder.JSONDecodeError:
                    elements[key.strip()] = value

        except ValueError:
            raise ConnectorError(f"Structure of the '{elements_type}' request input field is wrongly formatted")

        return elements

    @staticmethod
    def _match_write_precision(data):
        """
        Match the `write_precision` input field string to a corresponding `WritePrecision` object.
        If no corresponding match is found or the `write_precision` input fields does not exist in the request data,
        default to `WritePrecision.NS`.

        :param dict data: a dictionary containing the data to be inserted
        :return: `WritePrecision` object matching the `write_precision` input field value
        """

        if 'write_precision' in data:
            write_precision = data['write_precision']
            if write_precision == 'ms':
                return WritePrecision.MS
            if write_precision == 's':
                return WritePrecision.S
            if write_precision == 'us':
                return WritePrecision.US

        # Default Write Precision to NS
        return WritePrecision.NS

    @retry(attempts=3)
    def insert(self, data):
        """
        Insert given data to InfluxDB bucket. If the insert fails due to lost database connection, retry inserting.

        :param dict data: a dictionary containing the data to be inserted
        """

        # Including the connect in the insert method such that it can benefit from retrying
        if not self.connection:
            self.connect()

        point_values = {}

        if 'time' in data:
            point_values['time'] = data['time']

        if 'tags' in data and data['tags']:
            point_values['tags'] = self._extract_elements(elements_csv=data['tags'])

        try:
            point_values['measurement'] = data['measurement']

            fields = self._extract_elements(elements_csv=data['fields'], elements_type='fields')
            if fields:
                point_values['fields'] = fields
            else:
                raise ConnectorError("Request input field named 'fields' must contain at least one element")

            point = influxdb_client.Point.from_dict(
                dictionary=point_values,
                write_precision=self._match_write_precision(data)
            )
            self.connection.write(get_variable('BUCKET'), record=point)

        except dateutil.parser._parser.ParserError:
            self.connection = None
            raise ConnectorError(f"Failed to insert data: request input field 'time' is wrongly formatted")

        except KeyError as e:
            self.connection = None
            raise ConnectorError(f"Failed to insert data: request data missing required input field named {e}")

        except influxdb_client.rest.ApiException as e:
            self.connection = None
            raise ConnectorError(f"Failed to insert data: {e}")

        logger.info("Data inserted successfully")

    def stop(self):
        """
        Close connection to the database if the connection has been initialised
        """

        self.connection.close()
        self.connection = None
