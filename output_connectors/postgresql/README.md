# UbiOps Output Connector - PostgreSQL 

The UbiOps PostgreSQL output connector is based on the populair `psycopg2` package and therefore supports inserting
data to most major PostgreSQL versions.


## Configuration

The PostgreSQL output connector works with Python 3.6 or higher, and should be set up as a deployment with structured
input. Each of the input fields corresponds to a field in the database used for inserting. The output type does not
matter, as no output is returned from the connector. We recommend to allocate at least 512 MB of memory to the
deployment.
  
The connector can be configured using the following environment variables:

| Variable | Default  | Description                                |
|----------|----------|--------------------------------------------|
| HOST     | None     | Hostname or IP address of the server       |
| PORT     | `5432`   | Port of the server                         |
| USERNAME | None     | Username to authenticate on the server     |
| PASSWORD | None     | Password to authenticate on the server     |
| DATABASE | None     | Database containing your destination table |
| SCHEMA   | `public` | Schema containing your destination table   |
| TABLE    | None     | Table to write the data to                 |
| TIMEOUT  | `10`     | Timeout of the connection in seconds       |

The connector by default uses the names of the input fields as column names of the database table. As with all
connectors, you can optionally pass a `MAPPING` variable containing a JSON dictionary to map input fields to different
column names. 
