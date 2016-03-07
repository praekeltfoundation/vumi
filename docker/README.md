# Vumi Docker image
Documentation for Vumi is available online at http://vumi.readthedocs.org/ and in the `docs` directory of the repository.

## Usage
The Docker image provides an entrypoint for Vumi that allows the configuration of Vumi workers via environment variables.

### Environment variables
#### Basic options:
* `TWISTD_COMMAND`: the command to pass to `twistd` (default: `vumi_worker`)
* `WORKER_CLASS`: the Vumi worker class to use
* `CONFIG_FILE`: the path to the YAML configuration file to use
* `SENTRY_DSN`: the Sentry DSN to use for reporting errors

#### AMQP options:
AMQP/RabbitMQ options can be set via environment variables. At a minimum, the `AMQP_HOST` variable must be set or else none of the other AMQP variables will take effect.
* `AMQP_HOST`: the address for the RabbitMQ server
* `AMQP_PORT`: the port for the RabbitMQ server (default: `5672`)
* `AMQP_VHOST`: the name of the RabbitMQ vhost to use (default: `/`)
* `AMQP_USERNAME`: the username to authenticate with RabbitMQ (default: `guest`)
* `AMQP_PASSWORD`: the password to authenticate with RabbitMQ (default: `guest`)

#### `VUMI_OPT_` options
Additional options can be passed to Vumi via variables that start with `VUMI_OPT_`. These variables will be converted to `--set-option` CLI options. For example, the environment variable `VUMI_OPT_BUCKET=1` will result in the CLI option `--set-option=bucket:1`.

### `/app` directory
The `/app` directory is created and set as the current working directory. This is the directory where the files (e.g. YAML config) for your application should be put.

### Examples
Running a built-in worker class without a config file:
```shell
docker run --rm -it \
  -e WORKER_CLASS=vumi.blinkenlights.MetricTimeBucket \
  -e AMQP_HOST=rabbitmq.service.consul \
  -e VUMI_OPT_BUCKETS=3 \
  -e VUMI_OPT_BUCKET_SIZE=10 \
  praekeltfoundation/vumi
```

Dockerfile for an image with an external worker class and a config file:
```dockerfile
FROM praekeltfoundation/vumi
RUN pip install vumi-http-api
COPY ./my-config.yaml /app/my-config.yaml
ENV WORKER_CLASS="vumi_http_api.VumiApiWorker" \
    CONFIG_FILE="my-config.yaml" \
    AMQP_HOST="rabbitmq.service.consul"
EXPOSE 8000
```

Dockerfile for an image with custom arguments:
```dockerfile
FROM praekeltfoundation/vumi
RUN pip install go-metrics-api
COPY ./my-config.yaml /app/my-config.yaml
ENV TWISTD_COMMAND="cyclone"
EXPOSE 8000
CMD ["--app", "go_metrics.server.MetricsApi", \
     "--port", "8000", \
     "--app-opts", "my-config.yaml"]
```
