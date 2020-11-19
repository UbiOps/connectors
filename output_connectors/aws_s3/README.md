# UbiOps Output Connector - Amazon Web Services S3 

The UbiOps S3 output connector is based on on the AWS boto3 library. It inserts blobs to S3 storage, in this template 1 
blob per request.


## Configuration

The S3 output connector works with Python 3.6 or higher, and should be set up as a deployment with structured input. 
The connector expects an input field with the name `blob`, which should be of type blob and contain the file that needs
to be uploaded. The output type does not matter, as no output is returned from the connector. We recommend to allocate 
at least 512 MB of memory to the deployment.
  
The connector can be configured using the following environment variables:

| Variable    | Default | Description                                             |
|-------------|---------|---------------------------------------------------------|
| REGION      | None    | AWS region of the S3 bucket                             |
| ACCESS_KEY  | None    | AWS access key ID                                       |
| SECRET_KEY  | None    | AWS access key secret                                   |
| BUCKET      | None    | Name of the S3 bucket to upload to                      |
| PATH_PREFIX | Empty   | String to add as a prefix to the S3 path when uploading |
