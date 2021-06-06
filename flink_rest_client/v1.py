"""
cluster
config
dataset
"""
import os
import requests


class RestException(Exception):

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


def _execute_rest_request(url, http_method=None, accepted_status_code=None, files=None, params=None, data=None,
                          json=None):

    if http_method is None:
        http_method = 'GET'
    if params is None:
        params = {}
    if data is None:
        data = {}

    # If accepted_status_code is None then default value is set.
    if accepted_status_code is None:
        accepted_status_code = 200

    response = requests.request(method=http_method, url=url, files=files, params=params, data=data, json=json)
    if response.status_code == accepted_status_code:
        return response.json()
    else:
        if "errors" in response.json().keys():
            error_str = '\n'.join(response.json()["errors"])
        else:
            error_str = ''
        raise RestException(f"REST response error ({response.status_code}): {error_str}")


class TaskManagersClient:
    def __init__(self, prefix):
        """
        Constructor.

        Parameters
        ----------
        prefix: str
            REST API url prefix. It must contain the host, port pair.
        """
        self.prefix = f'{prefix}/taskmanagers'

    def all(self):
        """
        Returns an overview over all task managers.

        Endpoint: [GET] /taskmanagers

        Returns
        -------
        list
            List of taskmanagers. Each taskmanager is represented by a dictionary.
        """
        return _execute_rest_request(url=self.prefix)['taskmanagers']

    def taskmanager_ids(self):
        """
        Returns the list of taskmanager_ids.

        Returns
        -------
        list
            List of taskmanager ids.
        """
        return [elem['id'] for elem in self.all()]

    def metric_names(self):
        """
        Return the supported metric names.

        Returns
        -------
        list
            List of metric names.
        """
        return [elem['id'] for elem in _execute_rest_request(url=f'{self.prefix}/metrics')]

    def metrics(self, metric_names=None, agg_modes=None, taskmanager_ids=None):
        """
        Provides access to aggregated task manager metrics.
        By default it returns with all existing metric names.

        Endpoint: [GET] /taskmanagers/metrics

        Parameters
        ----------
        metric_names: list
            (optional) List of selected specific metric names. Default: <all metrics>

        agg_modes: list
            (optional) List of aggregation modes which should be calculated. Available aggregations are: "min, max,
            sum, avg". Default: <all modes>

        taskmanager_ids: list
            List of 32-character hexadecimal strings to select specific task managers. The list of valid taskmanager ids
            are available through the taskmanager_ids() method. Default: <all taskmanagers>.

        Returns
        -------
        dict
            Key-value pairs of metrics.
        """

        if metric_names is None:
            metric_names = self.metric_names()

        supported_agg_modes = ['min', 'max', 'sum', 'avg']
        if agg_modes is None:
            agg_modes = supported_agg_modes
        if len(set(agg_modes).difference(set(supported_agg_modes))) > 0:
            raise RestException(f"The provided aggregation modes list contains invalid value. Supported aggregation "
                                f"modes: {','.join(supported_agg_modes)}; given list: {','.join(agg_modes)}")

        if taskmanager_ids is None:
            taskmanager_ids = self.taskmanager_ids()

        params = {
            'get': ','.join(metric_names),
            'agg': ','.join(agg_modes),
            'taskmanagers': ','.join(taskmanager_ids)
        }
        query_result = _execute_rest_request(url=f'{self.prefix}/metrics', params=params)

        result = {}
        for elem in query_result:
            metric_name = elem.pop('id')
            result[metric_name] = elem

        return result

    def get(self, taskmanager_id):
        """
        Returns details for a task manager.

        Endpoint: [GET] /taskmanagers/:taskmanagerid

        Parameters
        ----------
        taskmanager_id: str
            32-character hexadecimal string that identifies a task manager.

        Returns
        -------
        dict
            Query result as a dict.
        """
        return _execute_rest_request(url=f'{self.prefix}/{taskmanager_id}')

    def get_logs(self, taskmanager_id):
        """
        Returns the list of log files on a TaskManager.

        Endpoint: [GET] /taskmanagers/:taskmanagerid/logs

        Parameters
        ----------
        taskmanager_id: str
            32-character hexadecimal string that identifies a task manager.

        Returns
        -------
        list
            List of log files in which each element contains a name and size fields.
        """
        return _execute_rest_request(url=f'{self.prefix}/{taskmanager_id}/logs')['logs']

    def get_metrics(self, taskmanager_id, metric_names=None):
        """
        Provides access to task manager metrics.

        Endpoint: [GET] /taskmanagers/:taskmanagerid/metrics

        Parameters
        ----------
        taskmanager_id: str
            32-character hexadecimal string that identifies a task manager.

        metric_names: list
            (optional) List of selected specific metric names. Default: <all metrics>

        Returns
        -------
        dict
            Metric name -> Metric value key-value pairs. The values are provided as strings.
        """

        if metric_names is None:
            metric_names = self.metric_names()
        params = {'get': ','.join(metric_names)}

        query_result = _execute_rest_request(url=f'{self.prefix}/{taskmanager_id}/metrics', params=params)
        return dict([(elem['id'], elem['value']) for elem in query_result])

    def get_thread_dump(self, taskmanager_id):
        """
        Returns the thread dump of the requested TaskManager.

        Endpoint: [GET] /taskmanagers/:taskmanagerid/thread-dump

        Parameters
        ----------
        taskmanager_id: str
            32-character hexadecimal string that identifies a task manager.

        Returns
        -------
        dict
            ThreadName -> StringifiedThreadInfo key-value pairs.
        """
        query_result = _execute_rest_request(url=f'{self.prefix}/{taskmanager_id}/thread-dump')['threadInfos']
        return dict([(elem['threadName'], elem['stringifiedThreadInfo']) for elem in query_result])


class JarsClient:
    def __init__(self, prefix):
        """
        Constructor.

        Parameters
        ----------
        prefix: str
            REST API url prefix. It must contain the host, port pair.
        """
        self.prefix = f'{prefix}/jars'

    def all(self):
        """
        Returns a list of all jars previously uploaded via '/jars/upload'.

        Endpoint: [GET] /jars

        Returns
        -------
        dict
            List all the jars were previously uploaded.
        """
        return _execute_rest_request(url=self.prefix)

    def upload(self, path_to_jar):
        """
        Uploads a jar to the cluster from the input path.

        Endpoint: [GET] /jars/upload

        Parameters
        ----------
        path_to_jar: str
            Path to the jar file.

        Returns
        -------
        dict
            Result of jar upload.
        """
        filename = os.path.basename(path_to_jar)
        files = {
            'file': (filename, (open(path_to_jar, 'rb')), 'application/x-java-archive')
        }
        return _execute_rest_request(url=f'{self.prefix}/upload', http_method='POST', files=files)

    def get_plan(self, jar_id):
        """
        Returns the dataflow plan of a job contained in a jar previously uploaded via '/jars/upload'.

        Endpoint: [GET] /jars/:jarid/plan

        Parameters
        ----------
        jar_id: str
            String value that identifies a jar. When uploading the jar a path is returned, where the filename is the ID.
            This value is equivalent to the `id` field in the list of uploaded jars.xe

        Returns
        -------
        dict
            Details of the jar_id's plan.

        Raises
        ------
        RestException
            If the jar_id does not exist.
        """
        return _execute_rest_request(url=f'{self.prefix}/{jar_id}/plan', http_method='POST')['plan']

    def run(self, jar_id, arguments=None, entry_class=None, parallelism=None, savepoint_path=None,
            allow_non_restored_state=None):
        """
        Submits a job by running a jar previously uploaded via '/jars/upload'.

        Endpoint: [GET] /jars/:jarid/run

        Parameters
        ----------
        jar_id: str
            String value that identifies a jar. When uploading the jar a path is returned, where the filename is the ID.
            This value is equivalent to the `id` field in the list of uploaded jars.

        arguments: dict
            (Optional) Comma-separated list of program arguments.

        entry_class: str
            (Optional) String value that specifies the fully qualified name of the entry point class. Overrides the
            class defined in the jar file manifest.

        parallelism: int
             (Optional) Positive integer value that specifies the desired parallelism for the job.

        savepoint_path: str
             (Optional) String value that specifies the path of the savepoint to restore the job from.

        allow_non_restored_state: bool
             (Optional) Boolean value that specifies whether the job submission should be rejected if the savepoint
             contains state that cannot be mapped back to the job.

        Returns
        -------
        str
            32-character hexadecimal string value that identifies a job.

        Raises
        ------
        RestException
            If the jar_id does not exist.
        """
        data = {}
        if arguments is not None:
            data['programArgs'] = " ".join([f"--{k} {v}" for k, v in arguments.items()])
        if entry_class is not None:
            data['entry-class'] = entry_class
        if parallelism is not None:
            if parallelism < 0:
                raise RestException("get_plan method's parallelism parameter must be a positive integer.")
            data['parallelism'] = parallelism
        if savepoint_path is not None:
            data['savepointPath'] = savepoint_path
        if allow_non_restored_state is not None:
            data['allowNonRestoredState'] = allow_non_restored_state

        return _execute_rest_request(url=f'{self.prefix}/{jar_id}/run', http_method='POST', json=data)

    def delete(self, jar_id):
        """
        Deletes a jar previously uploaded via '/jars/upload'.

        Endpoint: [DELETE] /jars/:jarid

        Parameters
        ----------
        jar_id: str
            String value that identifies a jar. When uploading the jar a path is returned, where the filename is the ID.
            This value is equivalent to the `id` field in the list of uploaded jars.

        Returns
        -------
        bool
            True, if jar_id has been successfully deleted, otherwise False.

        Raises
        ------
        RestException
            If the jar_id does not exist.
        """
        res = _execute_rest_request(url=f'{self.prefix}/{jar_id}', http_method='DELETE')
        if len(res.key()) < 1:
            return True
        else:
            return False


class JobmanagerClient:
    def __init__(self, prefix):
        """
        Constructor.

        Parameters
        ----------
        prefix: str
            REST API url prefix. It must contain the host, port pair.
        """
        self.prefix = f'{prefix}/jobmanager'

    def config(self):
        """
        Returns the cluster configuration.

        Endpoint: [GET] /jobmanager/config

        Returns
        -------
        dict
            Cluster configuration dictionary.
        """
        query_result = _execute_rest_request(url=f'{self.prefix}/config')
        return dict([(elem['key'], elem['value']) for elem in query_result])

    def logs(self):
        """
        Returns the list of log files on the JobManager.

        Endpoint: [GET] /jobmanager/logs

        Returns
        -------
        dict
            List of log files
        """
        return _execute_rest_request(url=f'{self.prefix}/logs')["logs"]

    def get_log(self, log_file):
        """
        Returns the content of the log_file.

        Endpoint: [GET] /jobmanager/logs/:log_file

        Parameters
        ----------
        log_file: str
            Name of the log file.
        Returns
        -------
        str
            The content of the log file as a string
        """
        response = requests.request(method="GET", url=f'{self.prefix}/logs/{log_file}')
        if response.status_code == 200:
            return response.content.decode()
        else:
            if "errors" in response.json().keys():
                error_str = '\n'.join(response.json()["errors"])
            else:
                error_str = ''
            raise RestException(f"REST response error ({response.status_code}): {error_str}")

    def metric_names(self):
        """
        Return the supported metric names.

        Returns
        -------
        list
            List of metric names.
        """
        return [elem['id'] for elem in _execute_rest_request(url=f'{self.prefix}/metrics')]

    def metrics(self):
        """
       Provides access to job manager metrics.

        Endpoint: [GET] /jobmanager/metrics

        Returns
        -------
        dict
            Jobmanager metrics
        """
        metric_names = self.metric_names()
        params = {'get': ','.join(metric_names)}
        query_result = _execute_rest_request(url=f'{self.prefix}/metrics', params=params)
        return dict([(elem['id'], elem['value']) for elem in query_result])


class JobsClient:
    def __init__(self, prefix):
        """
        Constructor.

        Parameters
        ----------
        prefix: str
            REST API url prefix. It must contain the host, port pair.
        """
        self.prefix = f'{prefix}/jobs'

    def all(self):
        """
        Returns an overview over all jobs and their current state.

        Endpoint: [GET] /jobs

        Returns
        -------
        dict

        """
        return _execute_rest_request(url=self.prefix)

    def submit(self):
        pass


class FlinkRestClientV1:

    def __init__(self, host, port):
        self.host = host
        self.port = port

    def overview(self):
        """
        Returns an overview over the Flink cluster.

        Endpoint: [GET] /overview

        Returns
        -------
        dict
            Key-value pairs of flink cluster infos.
        """
        return _execute_rest_request(self._assemble_url('/overview'))

    @property
    def api_url(self):
        return f'http://{self.host}:{self.port}/v1'

    @property
    def jobmanager(self):
        return JobmanagerClient(prefix=self.api_url)

    @property
    def taskmanagers(self):
        return TaskManagersClient(prefix=self.api_url)

    @property
    def jars(self):
        return JarsClient(prefix=self.api_url)

    def delete_cluster(self):
        """
        Shuts down the cluster.

        Returns
        -------

        """
        return self._execute_rest_request(url='/cluster', http_method='DELETE')

    def get_config(self):
        """
        Returns the configuration of the WebUI.

        Returns
        -------
        dict
            Query result as a dict.
        """
        return self._execute_rest_request(url='/config', http_method='GET')

    def get_datasets(self):
        """
        Returns all cluster data sets.

        Returns
        -------
        dict
            Query result as a dict.
        """
        return self._execute_rest_request(url='/datasets', http_method='GET')

    def delete_dataset(self, dataset_id):
        """
        Triggers the deletion of a cluster data set. This async operation would return a 'triggerid' for further query
        identifier.

        Parameters
        ----------
        dataset_id: str
             32-character hexadecimal string value that identifies a cluster data set.

        Returns
        -------
        dict
            Query result as a dict.
        """
        return self._execute_rest_request(url=f'/datasets/{dataset_id}', http_method='DELETE',
                                          accepted_status_code=202)

    def get_delete_dataset_status(self, trigger_id):
        """
        Returns the status for the delete operation of a cluster data set.

        Parameters
        ----------
        trigger_id: str
            32-character hexadecimal string that identifies an asynchronous operation trigger ID. The ID was returned
            then the operation was triggered.

        Returns
        -------
        dict
            Query result as a dict.
        """
        return self._execute_rest_request(url=f'/datasets/delete/{trigger_id}', http_method='GET')

    def get_jars(self):
        """
        Returns a list of all jars previously uploaded via '/jars/upload'.

        Returns
        -------
        dict
            Query result as a dict.
        """
        return self._execute_rest_request(url='/jars', http_method='GET')

    def upload_jar(self, path_to_jar):
        """
        Uploads a jar to the cluster.

        Parameters
        ----------
        path_to_jar: str
            Filepath to the jar file to upload.

        Returns
        -------
        dict
            Query result as a dict.
        """
        filename = os.path.basename(path_to_jar)
        files = {
            'file': (filename, (open(path_to_jar, 'rb')), 'application/x-java-archive')
        }
        return self._execute_rest_request(url='/jars/upload', http_method='POST', files=files)

    def upload_maven_jar(self):
        """

        Returns
        -------

        """
        # TODO

    def get_jar_plan(self, jar_id):
        """
        Returns the dataflow plan of a job contained in a jar previously uploaded via '/jars/upload'. Program arguments
        can be passed both via the JSON request (recommended) or query parameters.

        Parameters
        ----------
        jar_id: str
            String value that identifies a jar. When uploading the jar a path is returned, where the filename is the ID.
            This value is equivalent to the `id` field in the list of uploaded jars (/jars).

        Returns
        -------
        dict
            Query result as a dict.
        """
        # TODO: add optional params. The REST API has 2 end-point for handling this request. check which one should be
        #  used.
        return self._execute_rest_request(url=f'/jars/{jar_id}/plan', http_method='GET')

    def run_jar(self, jar_id):
        """

        Parameters
        ----------
        jar_id

        Returns
        -------

        """
        # TODO

    def delete_jar(self, jar_id):
        """
        Deletes a jar previously uploaded via '/jars/upload'.

        Parameters
        ----------
        jar_id: str
            String value that identifies a jar. When uploading the jar a path is returned, where the filename is the ID.
            This value is equivalent to the `id` field in the list of uploaded jars (/jars).

        Returns
        -------
        dict
            Query result as a dict.
        """
        return self._execute_rest_request(url=f'/jars/{jar_id}', http_method='DELETE')

    def get_jobmanager_config(self):
        """
        Returns the cluster configuration.

        Returns
        -------
        dict
            Query result as a dict.
        """
        return self._execute_rest_request(url='/jobmanager/logs', http_method='GET')

    def get_jobmanager_metrics(self):
        """
        Provides access to job manager metrics.

        Returns
        -------
        dict
            Query result as a dict.
        """
        return self._execute_rest_request(url='/jobmanager/metrics', http_method='GET')

    def submit_job(self, params):
        """
        Submits a job. This call is primarily intended to be used by the Flink client. This call expects a
        multipart/form-data request that consists of file uploads for the serialized JobGraph, jars and distributed
        cache artifacts and an attribute named "request" for the JSON payload.

        Returns
        -------
        dict
            Query result as a dict.
        """
        # TODO: it is not clear for me, that what should be attached to the request
        return self._execute_rest_request(url='/jobs', http_method='POST')

    def get_jobs(self):
        """
        Returns an overview over all jobs and their current state.

        Returns
        -------
        dict
            Query result as a dict.
        """
        return self._execute_rest_request(url='/jobs', http_method='GET')

    def get_job(self, job_id):
        """
        Returns details of a job.

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.

        Returns
        -------
        dict
            Query result as a dict.
        """
        return self._execute_rest_request(url=f'/jobs{job_id}', http_method='GET')

    def get_job_config(self, job_id):
        """
        Returns the configuration of a job.

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.

        Returns
        -------
        dict
            Query result as a dict.
        """
        return self._execute_rest_request(url=f'/jobs/{job_id}/config', http_method='GET')

    def get_job_exceptions(self, job_id):
        """
        Returns the most recent exceptions that have been handled by Flink for this job. The
        'exceptionHistory.truncated' flag defines whether exceptions were filtered out through the GET parameter. The
        backend collects only a specific amount of most recent exceptions per job. This can be configured through
        web.exception-history-size in the Flink configuration. The following first-level members are deprecated:
        'root-exception', 'timestamp', 'all-exceptions', and 'truncated'. Use the data provided through
        'exceptionHistory', instead.

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.

        Returns
        -------
        dict
            Query result as a dict.
        """
        # TODO: optional maxExceptions get parameters need to be added in future
        return self._execute_rest_request(url=f'/jobs/{job_id}/exceptions', http_method='GET')

    def get_job_execution_result(self, job_id):
        """
        Returns the result of a job execution. Gives access to the execution time of the job and to all accumulators
        created by this job.

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.

        Returns
        -------
        dict
            Query result as a dict.
        """
        return self._execute_rest_request(url=f'/jobs/{job_id}/execution-result', http_method='GET')

    def get_job_metrics(self, job_id):
        """
        Provides access to job metrics.

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.

        Returns
        -------
        dict
            Query result as a dict.
        """
        return self._execute_rest_request(url=f'/jobs/{job_id}/metrics', http_method='GET')

    def get_job_plan(self, job_id):
        """
        Returns the dataflow plan of a job.

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.

        Returns
        -------
        dict
            Query result as a dict.
        """
        return self._execute_rest_request(url=f'/jobs/{job_id}/plan', http_method='GET')

    def get_job_accumulators(self, job_id):
        """
        Returns the accumulators for all tasks of a job, aggregated across the respective subtasks.

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.

        Returns
        -------
        dict
            Query result as a dict.
        """
        return self._execute_rest_request(url=f'/jobs/{job_id}/accumulators', http_method='GET')

    def get_job_checkpoint_statistics(self, job_id, checkpoint_id=None, vertex_id=None):
        """
        Returns checkpointing statistics for a job. If the checkpoint_id parameter is also provided, then it returns
        details for the referenced checkpoint.

        If the checkpoint_id parameter AND vertex_id are provided, it returns checkpoint statistics for a task and its
        subtasks.

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.
        checkpoint_id: int
            Int value that identifies a checkpoint.
        vertex_id:str
            32-character hexadecimal string value that identifies a job vertex.

        Returns
        -------
        dict
            Query result as a dict.
        """
        if checkpoint_id is not None and vertex_id is not None:
            return self._execute_rest_request(
                url=f'/jobs/{job_id}/checkpoints/details/{checkpoint_id}/subtasks/{vertex_id}',
                http_method='GET')
        elif checkpoint_id is not None:
            return self._execute_rest_request(
                url=f'/jobs/{job_id}/checkpoints/details/{checkpoint_id}',
                http_method='GET')
        else:
            return self._execute_rest_request(url=f'/jobs/{job_id}/checkpoints', http_method='GET')

    def get_job_checkpoint_configuration(self, job_id):
        """
        Returns the checkpointing configuration.

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.

        Returns
        -------
        dict
            Query result as a dict.
        """
        return self._execute_rest_request(url=f'/jobs/{job_id}/checkpoints/config', http_method='GET')

    def get_jobs_metrics(self, metrics=None, aggs=None, job_ids=None):
        """
        Provides access to aggregated job metrics.

        Parameters
        ----------
        metrics: list
            List of string values to select specific metrics.
        aggs: list
            List of aggregation modes which should be calculated. Available aggregations are: "min,
            max, sum, avg".
        job_ids: list
            List of 32-character hexadecimal strings to select specific jobs.

        Returns
        -------
        dict
            Query result as a dict.
        """
        return self._execute_rest_request(url='/jobs/metrics', http_method='GET')

    def get_jobs_overview(self):
        """
        Returns an overview over all jobs.

        Returns
        -------

        """
        return self._execute_rest_request(url='/jobs/overview', http_method='GET')

    def terminate_job(self, job_id):
        """
        Terminates a job.

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.

        Returns
        -------
        dict
            Query result as a dict.
        """
        return self._execute_rest_request(url=f'/jobs{job_id}', http_method='PATCH', accepted_status_code=202)

    def _assemble_url(self, suffix):
        return f'http://{self.host}:{self.port}/v1{suffix}'

    def _execute_rest_request(self, url, http_method, accepted_status_code=None, files=None):

        # If accepted_status_code is None then default value is set.
        if accepted_status_code is None:
            accepted_status_code = 200

        response = requests.request(method=http_method, url=f'http://{self.host}:{self.port}/v1{url}', files=files)
        if response.status_code == accepted_status_code:
            return response.json()
        else:
            raise RestException(f"REST response error: {response.status_code}")
