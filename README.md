# Flink REST Client

[![PyPI Version][pypi-image]][pypi-url]
[![Build Status][build-image]][build-url]
[![Code Coverage][coverage-image]][coverage-url]

<!-- Badges -->

[pypi-image]: https://img.shields.io/pypi/v/flink_rest_client
[pypi-url]: https://pypi.org/project/flink_rest_client/
[build-image]: https://github.com/frego-dev/flink-rest-client/actions/workflows/build.yml/badge.svg
[build-url]: https://github.com/frego-dev/flink-rest-client/actions/workflows/build.yml
[coverage-image]: https://codecov.io/gh/frego-dev/flink-rest-client/branch/main/graph/badge.svg
[coverage-url]: https://codecov.io/gh/frego-dev/flink-rest-client

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

## API structure

In the client implementation, the API end points are categorized based on their functionality:
 - cluster level: API endpoints for managing the whole cluster.
 - jobmanager: API endpoints for managing the job manager(s).
 - taskmanager: API endpoints for managing the taskmanagers.
 - jars: API endpoints for managing the uploaded jars.
 - jobs: API endpoints for managing the submitted jobs.

You find the REST API endpoint -> python method mapping in the documentation: [link to mapping]

## Usage examples

### How to create a new FlinkRestClient instance
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
print(result)
```
**Output:**
```json
{
 "taskmanagers": 1,
 "slots-total": 4,
 "slots-available": 4,
 "jobs-running": 0,
 "jobs-finished": 0,
 "jobs-cancelled": 0,
 "jobs-failed": 0,
 "flink-version": "1.12.4",
 "flink-commit": "5364a6b"
}
```

### How to upload and run a Flink job

```python
from flink_rest_client import FlinkRestClient

rest_client = FlinkRestClient.get(host="localhost", port=8082)

# Path to the jar file on your file system
path_to_my_jar = "/path/to/StateMachineExample.jar"

# The upload_and_run method returns with the unique identifier of the already started Flink job
job_id = rest_client.jars.upload_and_run(path_to_jar=path_to_my_jar)

# Using the job_id, you can query the current status of the submitted job.
job = rest_client.jobs.get(job_id=job_id)
print(job)
```
**Output:**
```json
{
 "jid": "d8a3c7f257231678c1ca4b97d2316c45",
 "name": "State machine job",
 "isStoppable": false,
 "state": "RUNNING",
 "start-time": 1625758267958,
 "end-time": -1,
 "duration": 206345,
 "now": 1625758474303,
 "timestamps": {
  "FAILING": 0,
  "FINISHED": 0,
  "INITIALIZING": 1625758267958,
  "RESTARTING": 0,
  "CREATED": 1625758268002,
  "FAILED": 0,
  "SUSPENDED": 0,
  "CANCELLING": 0,
  "CANCELED": 0,
  "RECONCILING": 0,
  "RUNNING": 1625758268038
 },
 "vertices": [
   ...
 ],
 "status-counts": {
  "CREATED": 0,
  "CANCELING": 0,
  "FAILED": 0,
  "CANCELED": 0,
  "FINISHED": 0,
  "SCHEDULED": 0,
  "RUNNING": 2,
  "DEPLOYING": 0,
  "RECONCILING": 0
 },
 "plan": {
  "jid": "d8a3c7f257231678c1ca4b97d2316c45",
  "name": "State machine job",
  "nodes": [
    ...
   ]
 }
}

```

Sometimes you need to pass arguments/parameters to successfully start your Flink job.

For example, you have the following Java Main class:
```java

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyFlinkJob {
    
    private static final String PARAM_THRESHOLD = "my.flink.job.threshold";
    
    public static void main(String[] args) {
        // Reading configuration
        ParameterTool argsParams = ParameterTool.fromArgs(args);
        int threshold = argsParams.getInt(PARAM_THRESHOLD);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // ...
    }
}
```

The required argument can be passed via upload_and_run method's `arguments` parameter:
```python
from flink_rest_client import FlinkRestClient

rest_client = FlinkRestClient.get(host="localhost", port=8082)

# Path to the jar file on your file system
path_to_my_jar = "/path/to/StateMachineExample.jar"

# Put the arguments in a dictionary
job_id = rest_client.jars.upload_and_run(path_to_jar=path_to_my_jar, arguments={
    "my.flink.job.threshold": 55
})

```

