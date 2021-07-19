API endpoint mapping
======================

In the client implementation, the API end points are categorized based on their functionality:

 - cluster level: API endpoints for managing the whole cluster.
 - jobmanager: API endpoints for managing the job manager(s).
 - taskmanager: API endpoints for managing the taskmanagers.
 - jars: API endpoints for managing the uploaded jars.
 - jobs: API endpoints for managing the submitted jobs.

You can find the original REST API documentation here: `REST API documentation <https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/ops/rest_api/>`_

In the following sections we provide the REST API endpoint -> python method mapping.

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

Jobmanager
**************

+----------------------------+-------------+--------------------------------+
| REST API endpoint          | HTTP method | Python method                  |
+============================+=============+================================+
| /jobmanager/config         | GET         | rest_client.jobmanager.config  |
+----------------------------+-------------+--------------------------------+
| /jobmanager/logs           | GET         | rest_client.jobmanager.logs    |
+----------------------------+-------------+--------------------------------+
| /jobmanager/logs/:log_file | GET         | rest_client.jobmanager.get_log |
+----------------------------+-------------+--------------------------------+
| /jobmanager/metrics        | GET         | rest_client.jobmanager.metrics |
+----------------------------+-------------+--------------------------------+


.. note::

    The :code:`[GET] /jobmanager/logs/:log_file` end point is not part of the official API documentation.


Taskmanager
**************

+------------------------------------------+-------------+----------------------------------------+
| REST API endpoint                        | HTTP method | Python method                          |
+==========================================+=============+========================================+
| /taskmanagers                            | GET         | rest_client.taskmanager.all            |
+------------------------------------------+-------------+----------------------------------------+
| /taskmanagers/metrics                    | GET         | rest_client.taskmanager.metrics        |
+------------------------------------------+-------------+----------------------------------------+
| /taskmanagers/:taskmanagerid             | GET         | rest_client.taskmanager.get            |
+------------------------------------------+-------------+----------------------------------------+
| /taskmanagers/:taskmanagerid/logs        | GET         | rest_client.taskmanager.get_logs       |
+------------------------------------------+-------------+----------------------------------------+
| /taskmanagers/:taskmanagerid/metrics     | GET         | rest_client.taskmanager.get_metrics    |
+------------------------------------------+-------------+----------------------------------------+
| /taskmanagers/:taskmanagerid/thread-dump | GET         | rest_client.taskmanager.get_thread_dump|
+------------------------------------------+-------------+----------------------------------------+

Jars
**************

.. note::

    We also provided the :code:`rest_client.jars.upload_and_run` helper method to upload and start a jar in one method call.

+-------------------+-------------+---------------------------+
| REST API endpoint | HTTP method | Python method             |
+===================+=============+===========================+
| /jars             | GET         | rest_client.jars.all      |
+-------------------+-------------+---------------------------+
| /jars/upload      | POST        | rest_client.jars.upload   |
+-------------------+-------------+---------------------------+
| /jars/:jarid/plan | POST        | rest_client.jars.get_plan |
+-------------------+-------------+---------------------------+
| /jars/:jarid/run  | POST        | rest_client.jars.run      |
+-------------------+-------------+---------------------------+
| /jars/:jarid      | DELETE      | rest_client.jars.delete   |
+-------------------+-------------+---------------------------+


Jobs
*********

+-------------------------------------------------------------------+-------------+--------------------------------------------------+
| REST API endpoint                                                 | HTTP method | Python method                                    |
+===================================================================+=============+==================================================+
| /jobs                                                             | GET         | rest_client.jobs.all                             |
+-------------------------------------------------------------------+-------------+--------------------------------------------------+
| /jobs/overview                                                    | GET         | rest_client.jobs.overview                        |
+-------------------------------------------------------------------+-------------+--------------------------------------------------+
| /jobs/metrics                                                     | GET         | rest_client.jobs.metrics                         |
+-------------------------------------------------------------------+-------------+--------------------------------------------------+
| /jobs/:jobid                                                      | GET         | rest_client.jobs.get                             |
+-------------------------------------------------------------------+-------------+--------------------------------------------------+
| /jobs/:jobid/config                                               | GET         | rest_client.jobs.get_config                      |
+-------------------------------------------------------------------+-------------+--------------------------------------------------+
| /jobs/:jobid/exceptions                                           | GET         | rest_client.jobs.get_exceptions                  |
+-------------------------------------------------------------------+-------------+--------------------------------------------------+
| /jobs/:jobid/execution-result                                     | GET         | rest_client.jobs.get_execution_results           |
+-------------------------------------------------------------------+-------------+--------------------------------------------------+
| /jobs/:jobid/metrics                                              | GET         | rest_client.jobs.get_metrics                     |
+-------------------------------------------------------------------+-------------+--------------------------------------------------+
| /jobs/:jobid/plan                                                 | GET         | rest_client.jobs.get_plan                        |
+-------------------------------------------------------------------+-------------+--------------------------------------------------+
| /jobs/:jobid/accumulators                                         | GET         | rest_client.jobs.get_accumulators                |
+-------------------------------------------------------------------+-------------+--------------------------------------------------+
| /jobs/:jobid/checkpoints/config                                   | GET         | rest_client.jobs.get_checkpointing_configuration |
+-------------------------------------------------------------------+-------------+--------------------------------------------------+
| /jobs/:jobid/checkpoints                                          | GET         | rest_client.jobs.get_checkpoints                 |
+-------------------------------------------------------------------+-------------+--------------------------------------------------+
| /jobs/:jobid/checkpoints/details/:checkpointid                    | GET         | rest_client.jobs.get_checkpoint_details          |
+-------------------------------------------------------------------+-------------+--------------------------------------------------+
| /jobs/:jobid/checkpoints/details/:checkpointid/subtasks/:vertexid | GET         | rest_client.jobs.get_checkpoint_details          |
+-------------------------------------------------------------------+-------------+--------------------------------------------------+
| /jobs/:jobid/rescaling                                            | GET         | rest_client.jobs.rescale                         |
+-------------------------------------------------------------------+-------------+--------------------------------------------------+
| /jobs/:jobid/savepoints                                           | GET         | rest_client.jobs.create_savepoint                |
+-------------------------------------------------------------------+-------------+--------------------------------------------------+
| /jobs/:jobid                                                      | PATCH       | rest_client.jobs.terminate                       |
+-------------------------------------------------------------------+-------------+--------------------------------------------------+
| /jobs/:jobid/stop                                                 | GET         | rest_client.jobs.stop                            |
+-------------------------------------------------------------------+-------------+--------------------------------------------------+
