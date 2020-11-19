# UbiOps Output Connector - Azure Blob Storage 

The UbiOps Azure blob storage output connector is based on on the Azure Python library. It inserts blobs to blob 
storage, in this template 1 blob per request.


## Configuration

The blob storage output connector works with Python 3.6 or higher, and should be set up as a deployment with structured
input. The connector expects an input field with the name `blob`, which should be of type blob and contain the file that
needs to be uploaded. The output type does not matter, as no output is returned from the connector. We recommend to 
allocate at least 512 MB of memory to the deployment.
  
The connector can be configured using the following environment variables:

| Variable          | Default | Description                                                     |
|-------------------|---------|-----------------------------------------------------------------|
| CONNECTION_STRING | None    | Azure connection string containing credentials for blob storage |
| CONTAINER         | None    | Name of the blob storage container to upload to                 |
| PATH_PREFIX       | Empty   | String to add as a prefix to the S3 path when uploading         |
| TIMEOUT           | `10`    | Timeout of upload in seconds                                    |

For more information about the Azure connection string, see the 
[Azure Documentation](https://docs.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string).