# UbiOps Output Connector - Amazon Web Services Kinesis 

The UbiOps Kinesis output connector is based on the AWS boto3 library. Data is published in JSON format with a randomly
generated partition key with each inserted record.


## Configuration

The Kinesis output connector works with Python 3.6 or higher, and should be set up as a deployment with structured
input. The data is written as JSON dictionaries to Kinesis, where the field names are used as the keys. The output type
of the deployment does not matter, as no output is returned from the connector. We recommend to allocate at least 512 MB
of memory to the deployment.
  
The connector can be configured using the following environment variables:

| Variable   | Default | Description                              |
|------------|---------|------------------------------------------|
| REGION     | None    | AWS region of the Kinesis stream         |
| ACCESS_KEY | None    | AWS access key ID                        |
| SECRET_KEY | None    | AWS access key secret                    |
| STREAM     | None    | Name of the Kinesis stream to publish to |

The connector by default uses the names of the input fields as keys in the Kinesis JSON structure. As with all
connectors, you can optionally pass a `MAPPING` variable containing a JSON dictionary to map input fields to different
key names. 
