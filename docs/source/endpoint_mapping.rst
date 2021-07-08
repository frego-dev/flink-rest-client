API endpoint mapping
======================

In the client implementation, the API end points are categorized based on their functionality:

 - cluster level: API endpoints for managing the whole cluster.
 - jobmanager: API endpoints for managing the job manager(s).
 - taskmanager: API endpoints for managing the taskmanagers.
 - jars: API endpoints for managing the uploaded jars.
 - jobs: API endpoints for managing the submitted jobs.

You can find the original REST API documentation here: `REST API documentation <https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/ops/rest_api/>`_

In the following sections we provide the mapping REST API endpoint -> python method mapping.
We assume that we could already create the :code:`rest_client` instance:

.. code-block:: python

    from flink_rest_client import FlinkRestClient

    rest_client = FlinkRestClient.get(host="localhost", port=8082)


Cluster level
**************

+----------------------+-------------+----------------------------+
| REST API endpoint    | HTTP method | Python method              |
+======================+=============+============================+
| /overview            | GET         | rest_client.overview       |
+----------------------+-------------+----------------------------+
| /config              | GET         | rest_client.config         |
+----------------------+-------------+----------------------------+
| /cluster             | DELETE      | rest_client.delete_cluster |
+----------------------+-------------+----------------------------+
| /datasets            | GET         | rest_client.datasets       |
+----------------------+-------------+----------------------------+
| /datasets/:datasetid | DELETE      | rest_client.delete_dataset |
+----------------------+-------------+----------------------------+