# UbiOps Input Connector - Snowflake 

The UbiOps Snowflake input connector is based on the `snowflake-connector-python` package. It runs a query against
your Snowflake database and returns the results of that query as structured output.


## Configuration

The Snowflake input connector should be set up as a deployment with structured output, with the output fields matching
the column names in Snowflake. The input type does not matter, as no input is required for the connector. We recommend
to allocate at least 512 MB of memory to the deployment.

The deployment can be configured using a number of environment variables:

| Variable      | Default   | Description                                          |
|---------------|---------- |------------------------------------------------------|
| USERNAME      | None      | Username of the Snowflake account                    |
| PASSWORD      | None      | Password of the Snowflake account                    |
| ACCOUNT       | None      | Your account name at Snowflake (1)                   |
| DATABASE      | None      | The name of the database from which to retrieve data |
| SCHEMA        | PUBLIC    | The name of the schema to use                        |
| QUERY         | hardcoded | The query that will run when a request is received   |

(1): Follow this format for the account: `<PROJECT_ID>.<REGION_OF_SNOWFLAKE>.<CLOUD_PROVIDER>`. For example: 
`xxxxxx.europe-west4.gcp`

In the `QUERY` variable you can define the query that will be run against your database every time the connector runs. 
Please make sure that the query result is limited such that the size of the returned result remains under the 
[output limit of a deployment](https://ubiops.com/docs/miscellaneous/platform-limits/).

You can also choose to include your query in the code of the deployment, which will allow you to create dynamic
queries with variables. If no `QUERY` environment variable is set, the query in the code will be used. 
