# UbiOps Input Connector - RabbitMQ 

The UbiOps RabbitMQ input connector is implemented using the `pika` library. It collects a number of messages from a
RabbitMQ server and returns these as the output of the deployment. Each message is acknowledged on the server to
indicate that it was processed and to make sure it does not get collected again.


## Configuration

The RabbitMQ input connector should be set up as a deployment with structured output, with a single output field called
`message` of type `string`. The input type does not matter, as no input is required for the connector. We recommend to
allocate at least 512 MB of memory to the deployment.

The connector can be configured using the following environment variables:

| Variable     | Default  | Description                                        |
|--------------|----------|----------------------------------------------------|
| HOST         | None     | Hostname or IP address of the server               |
| PORT         | `5432`   | Port of the server                                 |
| USERNAME     | None     | Username to authenticate on the server             |
| PASSWORD     | None     | Password to authenticate on the server             |
| VIRTUAL_HOST | `/`      | RabbitMQ virtual host where the queue is located   |
| QUEUE        | None     | Queue to consume messages from                     |
| TIMEOUT      | `10`     | Timeout of the connection and heartbeat in seconds |
| MAX_OUTPUT   | `50`     | Maximum number of items to consume in one request  |
