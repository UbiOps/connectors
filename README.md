# UbiOps Connectors

This repository contains a number of input and output connectors that can be used as UbiOps deployments. General
information about connectors and their functionality can be found in the 
[UbiOps documentation](https://ubiops.com/docs/data-connections/connectors/).
 
Many of the connectors are ready to be used immediately without further modification. For advanced use cases we
encourage you to make your own copy of a connector and use it as a template to adapt it to your needs. This can for
example be useful if you want to write custom queries to update or insert data or do some data processing. 

Each of the connectors have their own `README.md` file that describes their requirements and functionality in more
detail.


## Output connectors

The following output connectors are currently available:

- Amazon Web Services Kinesis
- Amazon Web Services S3
- Azure Blob Storage
- Google Cloud BigQuery
- Google Cloud Storage
- InfluxDB
- MSSQL
- MySQL
- PostgreSQL


## Input connectors

The following input connectors are currently available:

- RabbitMQ
- Snowflake
