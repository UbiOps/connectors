# UbiOps Output Connector - InfluxDB

The UbiOps InfluxDB output connector is based on the `influxdb-client` package.


## Configuration

The InfluxDB output connector works with Python 3. It should be set up as a deployment with structured input. 

The connector makes use of the following request input fields that should be configured when creating the deployment:

- **measurement**: string value containing the name of the measurement for which to insert data
- **fields**: a csv string containing key-value pairs of fields
- **tags** (optional): a csv string containing key-value pairs of tags
- **time** (optional): Unix timestamp. In case no time is given, the system time of the InfluxDB server host is used.
- **write_precision** (optional): string value representing the write precision to use when inserting the data (can be 
one of "ms", "s", "us", "ns"). Defaults to "ns".

The output type of the connector does not matter, as no output is returned from the connector. 

Furthermore, we recommend to allocate at least 512 MB of memory to the deployment.
  
The connector can be configured using the following environment variables:

| Variable     | Default  | Description                                                                    |
|--------------|----------|--------------------------------------------------------------------------------|
| URL          | None     | Hostname or IP address of the server + port to use (default 8086)              |
| TOKEN        | None     | Token to authorize requests to InfluxDB server                                 |
| ORGANIZATION | None     | Name of organization to use                                                    |
| BUCKET       | None     | Name of bucket to use                                                          |
| TIMEOUT      | `10`     | Timeout of the connection in seconds                                           |

## Examples of request data inputs

```
{
    "measurement": "example-measurement",
    "fields": "value1=1, value2=1.5, value3=example-field",
    "tags": "tag1=1, tag2=example-tag"
}
```

```
{
    "measurement": "example-measurement",
    "fields": "value1=1, value2=1.5, value3=example-field",
    "time": "1577836800000000000",
    "write_precision": "ns"
}
```

```
{
    "measurement": "example-measurement",
    "fields": "value1=1, value2=1.5, value3=example-field",
}
```
