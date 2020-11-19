# UbiOps Output Connector - Google Cloud Platform Cloud Storage 

The UbiOps Cloud Storage output connector is based on on the Google Cloud library. It inserts blobs to Cloud Storage, in
this template 1 blob per request.


## Configuration

The Cloud Storage output connector works with Python 3.6 or higher, and should be set up as a deployment with structured
input. The connector expects an input field with the name `blob`, which should be of type blob and contain the file
that needs to be uploaded. The output type does not matter, as no output is returned from the connector. We recommend to
allocate at least 512 MB of memory to the deployment.
  
The connector can be configured using the following environment variables:

| Variable          | Default | Description                                                        |
|-------------------|---------|--------------------------------------------------------------------|
| JSON_KEY_FILE     | None    | JSON service account credentials file in text                      |
| BUCKET            | None    | Name of the Cloud Storage bucket to upload to                      |
| PATH_PREFIX       | Empty   | String to add as a prefix to the Cloud Storage path when uploading |
