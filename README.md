# Flink REST Client

## What is it?
The Flink REST Client provides an easy-to-use python API for Flink REST API.
The client implements all available REST API endpoints that are documented on the [official Flink site](https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/ops/rest_api/).
Using this client, you can easily query your Flink cluster status, or you can upload and run arbitrary Flink jobs wrapped in a Java archive file.


## Installation
The source code is currently hosted on GitHub at: https://github.com/frego-dev/flink-rest-client

The installer for the latest released version is available at the [Python Package Index (PyPI)](https://pypi.org/project/flink-rest-client).

```sh
# via PyPI
pip install flink_rest_client
```

## Documentation

The official documentation is hosted on: [flink_rest_client.frego.dev](https://flink_rest_client.frego.dev/)


## License

[MIT](https://github.com/frego-dev/flink-rest-client/blob/master/LICENSE)

## Usage examples

The simplest way to create a new FlinkRestClient instance is using its static factory method:
```python
from flink_rest_client import FlinkRestClient

rest_client = FlinkRestClient.get(host="localhost", port=8082)
```

To check that the client can connect to Flink Jobmanager's webserver the overview method can be used, which returns an 
overview over the Flink cluster.

```python
from flink_rest_client import FlinkRestClient

rest_client = FlinkRestClient.get(host="localhost", port=8082)
result = rest_client.overview()

```

