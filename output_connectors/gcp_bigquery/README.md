# UbiOps Output Connector - Google Cloud Platform BigQuery 

The UbiOps BigQuery output connector is based on the Google Cloud library.


## Configuration

The BigQuery output connector works with Python 3.6 or higher, and should be set up as a deployment with structured
input. Each of the input fields corresponds to a field in the database used for inserting. The output type does not
matter, as no output is returned from the connector. We recommend to allocate at least 512 MB of memory to the
deployment.
  
The connector can be configured using the following environment variables:

| Variable      | Default  | Description                                   |
|---------------|----------|-----------------------------------------------|
| JSON_KEY_FILE | None     | JSON service account credentials file in text |
| DATASET       | None     | BigQuery dataset                              |
| TABLE         | None     | Table in the dataset to insert to             |

The connector by default uses the names of the input fields as column names of the table. As with all connectors, you
can optionally pass a `MAPPING` variable containing a JSON dictionary to map input fields to different column names. 
