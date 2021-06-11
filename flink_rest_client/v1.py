"""
cluster
config
dataset
"""
import os
import ntpath
from dataclasses import dataclass

import requests
from pip._internal.utils.deprecation import deprecated


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


class JobTrigger:
    def __init__(self, prefix, type_name, job_id, trigger_id):
        self._prefix = prefix
        self._type_name = type_name
        self.job_id = job_id
        self.trigger_id = trigger_id

    @property
    def status(self):
        return _execute_rest_request(url=f'{self._prefix}/{self.job_id}/{self._type_name}/{self.trigger_id}')


class DatasetTrigger:
    def __init__(self, prefix, trigger_id):
        self._prefix = prefix
        self.trigger_id = trigger_id

    @property
    def status(self):
        return _execute_rest_request(url=f'{self._prefix}/{self.trigger_id}')


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

        return _execute_rest_request(url=f'{self.prefix}/{jar_id}/run', http_method='POST', json=data)['jobid']

    def upload_and_run(self, path_to_jar, arguments=None, entry_class=None, parallelism=None, savepoint_path=None,
                       allow_non_restored_state=None):
        """
        Helper method to upload and start a jar in one method call.

        Parameters
        ----------
        path_to_jar: str
            Path to the jar file.

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
            If an error occurred during the upload of jar file.
        """
        result = self.upload(path_to_jar=path_to_jar)
        if not result['status'] == 'success':
            raise RestException('Could not upload the input jar file.', result)

        return self.run(ntpath.basename(result['filename']), arguments=arguments, entry_class=entry_class,
                        parallelism=parallelism, savepoint_path=savepoint_path,
                        allow_non_restored_state=allow_non_restored_state)

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


class JobVertexSubtaskClient:
    def __init__(self, prefix):
        """
        Constructor.

        Parameters
        ----------
        prefix: str
            REST API url prefix. It must contain the host, port pair.
        """
        self._prefix = prefix

    @property
    def prefix_url(self):
        return f"{self._prefix}/subtasks"

    def subtask_ids(self):
        """
        Returns the subtask identifiers.

        Returns
        -------
        list
            Positive integer list of subtask ids.
        """
        return [elem['subtask'] for elem in self.accumulators()['subtasks']]

    def accumulators(self):
        """
        Returns all user-defined accumulators for all subtasks of a task.

        Endpoint: [GET] /jobs/:jobid/vertices/:vertexid/accumulators

        Returns
        -------
        dict
            User-defined accumulators
        """
        return _execute_rest_request(url=f"{self.prefix_url}/accumulators")

    def metric_names(self):
        """
        Returns the supported metric names.

        Returns
        -------
        list
            List of metric names.
        """
        return [elem['id'] for elem in _execute_rest_request(url=f'{self.prefix_url}/metrics')]

    def metrics(self, metric_names=None, agg_modes=None, subtask_ids=None):
        """
        Provides access to aggregated subtask metrics.
        By default it returns with all existing metric names.

        Endpoint: [GET] /jobs/:jobid/vertices/:vertexid/subtasks/metrics

        Parameters
        ----------
        metric_names: list
            (optional) List of selected specific metric names. Default: <all metrics>

        agg_modes: list
            (optional) List of aggregation modes which should be calculated. Available aggregations are: "min, max,
            sum, avg". Default: <all modes>

        subtask_ids: list
            List of positive integers to select specific subtasks. The list of valid subtask ids is available through
            the subtask_ids() method. Default: <all subtasks>.

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

        if subtask_ids is None:
            subtask_ids = self.subtask_ids()

        params = {
            'get': ','.join(metric_names),
            'agg': ','.join(agg_modes),
            'subtasks': ','.join([str(elem) for elem in subtask_ids])
        }
        query_result = _execute_rest_request(url=f'{self.prefix_url}/metrics', params=params)

        result = {}
        for elem in query_result:
            metric_name = elem.pop('id')
            result[metric_name] = elem

        return result

    def get(self, subtask_id):
        """
        Returns details of the current or latest execution attempt of a subtask.

        Endpoint: [GET] /jobs/:jobid/vertices/:vertexid/subtasks/:subtaskindex

        Parameters
        ----------
        subtask_id: int
            Positive integer value that identifies a subtask.

        Returns
        -------
        dict

        """
        return _execute_rest_request(url=f"{self.prefix_url}/{subtask_id}")

    def get_attempt(self, subtask_id, attempt_id=None):
        """
        Returns details of an execution attempt of a subtask. Multiple execution attempts happen in case of
        failure/recovery.

        Endpoint: [GET] /jobs/:jobid/vertices/:vertexid/subtasks/:subtaskindex/attempts/:attempt

        Parameters
        ----------
        subtask_id: int
            Positive integer value that identifies a subtask.

        attempt_id: int
            (Optional) Positive integer value that identifies an execution attempt.
            Default: current execution attempt's id

        Returns
        -------
        dict
            Details of the selected attempt.
        """
        if attempt_id is None:
            return self.get(subtask_id)
        return _execute_rest_request(url=f"{self.prefix_url}/{subtask_id}/attempts/{attempt_id}")

    def get_attempt_accumulators(self, subtask_id, attempt_id=None):
        """
        Returns the accumulators of an execution attempt of a subtask. Multiple execution attempts happen in case of
        failure/recovery.

        Parameters
        ----------
        subtask_id: int
            Positive integer value that identifies a subtask.

        attempt_id: int
            (Optional) Positive integer value that identifies an execution attempt.
            Default: current execution attempt's id


        Returns
        -------
        dict
            The accumulators of the selected execution attempt of a subtask.
        """
        if attempt_id is None:
            attempt_id = self.get(subtask_id)['attempt']
        return _execute_rest_request(url=f"{self.prefix_url}/{subtask_id}/attempts/{attempt_id}/accumulators")


class JobVertexClient:
    def __init__(self, prefix, job_id, vertex_id):
        """
        Constructor.

        Parameters
        ----------
        prefix: str
            REST API url prefix. It must contain the host, port pair.
        """
        self._prefix = prefix
        self.job_id = job_id
        self.vertex_id = vertex_id

    @property
    def prefix_url(self):
        return f"{self._prefix}/{self.job_id}/vertices/{self.vertex_id}"

    @property
    def subtasks(self):
        return JobVertexSubtaskClient(self.prefix_url)

    def details(self):
        """
        Returns details for a task, with a summary for each of its subtasks.

        Endpoint: [GET] /jobs/:jobid/vertices/:vertexid

        Returns
        -------
        dict
            details for a task.
        """
        return _execute_rest_request(url=self.prefix_url)

    def backpressure(self):
        """
        Returns back-pressure information for a job, and may initiate back-pressure sampling if necessary.

        Endpoint: [GET] /jobs/:jobid/vertices/:vertexid/backpressure

        Notes
        -----
        The deprecated status means that the back pressure stats are not available.

        Returns
        -------
        dict
            Backpressure information
        """
        return _execute_rest_request(url=f"{self.prefix_url}/backpressure")

    def metric_names(self):
        """
        Returns the supported metric names.

        Returns
        -------
        list
            List of metric names.
        """
        return [elem['id'] for elem in _execute_rest_request(url=f'{self.prefix_url}/metrics')]

    def metrics(self, metric_names=None):
        """
        Provides access to task metrics.

        Endpoint: [GET] /jobs/:jobid/vertices/:vertexid/metrics

        Returns
        -------
        dict
            Task metrics.
        """
        if metric_names is None:
            metric_names = self.metric_names()

        params = {
            'get': ','.join(metric_names)
        }
        query_result = _execute_rest_request(url=f'{self.prefix_url}/metrics', params=params)
        result = {}
        for elem in query_result:
            metric_name = elem.pop('id')
            result[metric_name] = elem['value']
        return result

    def subtasktimes(self):
        """
        Returns time-related information for all subtasks of a task.

        Endpoint: [GET] /jobs/:jobid/vertices/:vertexid/subtasktimes

        Returns
        -------
        dict
            Time-related information for all subtasks
        """
        return _execute_rest_request(url=f"{self.prefix_url}/subtasktimes")

    def taskmanagers(self):
        """
        Returns task information aggregated by task manager.

        Endpoint: [GET] /jobs/:jobid/vertices/:vertexid/taskmanagers

        Returns
        -------
        dict
            Task information aggregated by task manager.
        """
        return _execute_rest_request(url=f"{self.prefix_url}/taskmanagers")

    def watermarks(self):
        """
        Returns the watermarks for all subtasks of a task.

        Endpoint: [GET] /jobs/:jobid/vertices/:vertexid/watermarks

        Returns
        -------
        list
            Watermarks for all subtasks of a task.
        """
        return _execute_rest_request(url=f"{self.prefix_url}/watermarks")


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
        list
            List of jobs and their current state.
        """
        return _execute_rest_request(url=self.prefix)['jobs']

    def job_ids(self):
        """
        Returns the list of job_ids.

        Returns
        -------
        list
            List of job ids.
        """
        return [elem['id'] for elem in self.all()]

    def overview(self):
        """
        Returns an overview over all jobs.

        Endpoint: [GET] /jobs/overview

        Returns
        -------
        list
            List of existing jobs.
        """
        return _execute_rest_request(url=f'{self.prefix}/overview')['jobs']

    def metric_names(self):
        """
        Returns the supported metric names.

        Returns
        -------
        list
            List of metric names.
        """
        return [elem['id'] for elem in _execute_rest_request(url=f'{self.prefix}/metrics')]

    def metrics(self, metric_names=None, agg_modes=None, job_ids=None):
        """
        Returns an overview over all jobs.

        Endpoint: [GET] /jobs/metrics

        Parameters
        ----------
        metric_names: list
            (optional) List of selected specific metric names. Default: <all metrics>

        agg_modes: list
            (optional) List of aggregation modes which should be calculated. Available aggregations are: "min, max,
            sum, avg". Default: <all modes>

        job_ids: list
            List of 32-character hexadecimal strings to select specific jobs. The list of valid jobs
            are available through the job_ids() method. Default: <all taskmanagers>.


        Returns
        -------
        dict
            Aggregated job metrics.
        """
        if metric_names is None:
            metric_names = self.metric_names()

        supported_agg_modes = ['min', 'max', 'sum', 'avg']
        if agg_modes is None:
            agg_modes = supported_agg_modes
        if len(set(agg_modes).difference(set(supported_agg_modes))) > 0:
            raise RestException(f"The provided aggregation modes list contains invalid value. Supported aggregation "
                                f"modes: {','.join(supported_agg_modes)}; given list: {','.join(agg_modes)}")

        if job_ids is None:
            job_ids = self.job_ids()

        params = {
            'get': ','.join(metric_names),
            'agg': ','.join(agg_modes),
            'jobs': ','.join(job_ids)
        }
        query_result = _execute_rest_request(url=f'{self.prefix}/metrics', params=params)

        result = {}
        for elem in query_result:
            metric_name = elem.pop('id')
            result[metric_name] = elem

        return result

    def get(self, job_id):
        """
        Returns details of a job.

        Endpoint: [GET] /jobs/:jobid

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.

        Returns
        -------
        dict
            Details of the selected job.
        """
        return _execute_rest_request(url=f'{self.prefix}/{job_id}')

    def get_config(self, job_id):
        """
        Returns the configuration of a job.

        Endpoint: [GET] /jobs/:jobid/config

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.

        Returns
        -------
        dict
            Job configuration
        """
        return _execute_rest_request(url=f'{self.prefix}/{job_id}/config')

    def get_exceptions(self, job_id):
        """
        Returns the most recent exceptions that have been handled by Flink for this job.

        Endpoint: [GET] /jobs/:jobid/exceptions

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.

        Returns
        -------
        dict
            The most recent exceptions.
        """
        return _execute_rest_request(url=f'{self.prefix}/{job_id}/exceptions')

    def get_execution_result(self, job_id):
        """
        Returns the result of a job execution. Gives access to the execution time of the job and to all accumulators
        created by this job.

        Endpoint: [GET] /jobs/:jobid/execution-result

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.

        Returns
        -------
        dict
            The execution result of the selected job.
        """
        return _execute_rest_request(url=f'{self.prefix}/{job_id}/execution-result')

    def get_metrics(self, job_id, metric_names=None):
        """
        Provides access to job metrics.

        Endpoint: [GET] /jobs/:jobid/metrics

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.

        metric_names: list
            (optional) List of selected specific metric names. Default: <all metrics>

        Returns
        -------
        dict
            Job metrics.
        """
        if metric_names is None:
            metric_names = self.metric_names()
        params = {
            'get': ','.join(metric_names)
        }
        query_result = _execute_rest_request(url=f'{self.prefix}/{job_id}/metrics', params=params)
        return dict([(elem['id'], elem['value']) for elem in query_result])

    def get_plan(self, job_id):
        """
        Returns the dataflow plan of a job.

        Endpoint: [GET] /jobs/:jobid/plan

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.

        Returns
        -------
        dict
            Dataflow plan
        """
        return _execute_rest_request(url=f'{self.prefix}/{job_id}/plan')['plan']

    def get_vertex_ids(self, job_id):
        """
        Returns the ids of vertices of the selected job.

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.

        Returns
        -------
        list
            List of identifiers.
        """
        return [elem['id'] for elem in self.get(job_id)['vertices']]

    def get_accumulators(self, job_id, include_serialized_value=None):
        """
        Returns the accumulators for all tasks of a job, aggregated across the respective subtasks.

        Endpoint: [GET] /jobs/:jobid/accumulators

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.

        include_serialized_value: bool
             (Optional) Boolean value that specifies whether serialized user task accumulators should be included in
             the response.

        Returns
        -------
        dict
            Accumulators for all task.
        """

        params = {}
        if include_serialized_value is not None:
            params['includeSerializedValue'] = 'true' if include_serialized_value else 'false'

        return _execute_rest_request(url=f'{self.prefix}/{job_id}/accumulators', http_method="GET", params=params)

    def get_checkpointing_configuration(self, job_id):
        """
        Returns the checkpointing configuration of the selected job_id

        Endpoint: [GET] /jobs/:jobid/checkpoints

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.

        Returns
        -------
        dict
            Checkpointing configuration of the selected job.
        """
        return _execute_rest_request(url=f'{self.prefix}/{job_id}/checkpoints/config', http_method="GET")

    def get_checkpoints(self, job_id):
        """
        Returns checkpointing statistics for a job.

        Endpoint: [GET] /jobs/:jobid/checkpoints

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.

        Returns
        -------
        dict
            Checkpointing statistics for the selected job: counts, summary, latest and history.
        """
        return _execute_rest_request(url=f'{self.prefix}/{job_id}/checkpoints', http_method="GET")

    def get_checkpoint_ids(self, job_id):
        """
        Returns checkpoint ids of the job_id.

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.

        Returns
        -------
        list
            List of checkpoint ids.
        """
        return [elem['id'] for elem in self.get_checkpoints(job_id=job_id)['history']]

    def get_checkpoint_details(self, job_id, checkpoint_id, show_subtasks=False):
        """
        Returns details for a checkpoint.

        Endpoint: [GET] /jobs/:jobid/checkpoints/details/:checkpointid

        If show_subtasks is true:
        Endpoint: [GET] /jobs/:jobid/checkpoints/details/:checkpointid/subtasks/:vertexid

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.

        checkpoint_id: int
            Long value that identifies a checkpoint.

        show_subtasks: bool
            If it is True, the details of the subtask are also returned.

        Returns
        -------
        dict

        """
        checkpoint_details = _execute_rest_request(url=f'{self.prefix}/{job_id}/checkpoints/details/{checkpoint_id}',
                                                   http_method="GET")
        if not show_subtasks:
            return checkpoint_details

        subtasks = {}
        for vertex_id in checkpoint_details['tasks'].keys():
            subtasks[vertex_id] = _execute_rest_request(
                url=f'{self.prefix}/{job_id}/checkpoints/details/{checkpoint_id}/subtasks/{vertex_id}',
                http_method="GET")
        checkpoint_details['subtasks'] = subtasks
        return checkpoint_details

    def rescale(self, job_id, parallelism):
        """
        Triggers the rescaling of a job. This async operation would return a 'triggerid' for further query identifier.

        Endpoint: [GET] /jobs/:jobid/rescaling

        Notes
        -----
        Using Flink version 1.12, the method will raise RestHandlerException because this rescaling is temporarily
        disabled. See FLINK-12312.

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.
        parallelism: int
            Positive integer value that specifies the desired parallelism.

        Returns
        -------
        JobTrigger
            Object that can be used to query the status of rescaling.
        """
        params = {
            'parallelism': parallelism
        }
        trigger_id = _execute_rest_request(
            url=f'{self.prefix}/{job_id}/rescaling',
            http_method="PATCH",
            params=params)['triggerid']
        return JobTrigger(self.prefix, 'rescaling', job_id, trigger_id)

    def create_savepoint(self, job_id, target_directory, cancel_job=False):
        """
        Triggers a savepoint, and optionally cancels the job afterwards. This async operation would return a
        JobTrigger for further query identifier.

        Attention: The target directory has to be a location accessible by both the JobManager(s) and TaskManager(s)
        e.g. a location on a distributed file-system or Object Store.

        Endpoint: [GET] /jobs/:jobid/savepoints

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.
        target_directory: str
            Savepoint target directory.
        cancel_job: bool
            If it is True, it also stops the job after the savepoint creation.

        Returns
        -------
        JobTrigger
            Object that can be used to query the status of savepoint.
        """
        trigger_id = _execute_rest_request(
            url=f'{self.prefix}/{job_id}/savepoints',
            http_method="POST",
            accepted_status_code=202,
            json={
                'cancel-job': cancel_job,
                'target-directory': target_directory
            })['request-id']
        return JobTrigger(self.prefix, 'savepoints', job_id, trigger_id)

    def terminate(self, job_id):
        """
        Terminates a job.

        Endpoint: [GET] /jobs/:jobid

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.

        Returns
        -------
        bool
            True if the job has been canceled, otherwise False.
        """
        res = _execute_rest_request(url=f'{self.prefix}/{job_id}', http_method="PATCH", accepted_status_code=202)
        if len(res) < 1:
            return True
        else:
            return False

    def stop(self, job_id, target_directory, drain=False):
        """
        Stops a job with a savepoint. This async operation would return a JobTrigger for further query identifier.

        Attention: The target directory has to be a location accessible by both the JobManager(s) and TaskManager(s)
        e.g. a location on a distributed file-system or Object Store.

        Draining emits the maximum watermark before stopping the job. When the watermark is emitted, all event time
        timers will fire, allowing you to process events that depend on this timer (e.g. time windows or process
        functions). This is useful when you want to fully shut down your job without leaving any unhandled events
        or state.

        Endpoint: [GET] /jobs/:jobid/stop

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.
        target_directory: str
            Savepoint target directory.
        drain: bool
            (Optional) If it is True, it emits the maximum watermark before stopping the job. default: False

        Returns
        -------
        JobTrigger
            Object that can be used to query the status of savepoint.
        """
        data = {
            'drain': False if drain is None else drain,
            'targetDirectory': target_directory
        }

        trigger_id = _execute_rest_request(
            url=f'{self.prefix}/{job_id}/stop',
            http_method="POST",
            accepted_status_code=202,
            json=data)['request-id']
        return JobTrigger(self.prefix, 'savepoints', job_id, trigger_id)

    def get_vertex(self, job_id, vertex_id):
        """
        Returns a JobVertexClient.

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.
        vertex_id: str
            32-character hexadecimal string value that identifies a vertex.

        Returns
        -------
        JobVertexClient
            JobVertexClient instance that can execute vertex related queries.
        """
        return JobVertexClient(self.prefix, job_id, vertex_id)


class FlinkRestClientV1:

    def __init__(self, host, port):
        self.host = host
        self.port = port

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

    @property
    def jobs(self):
        return JobsClient(prefix=self.api_url)

    def overview(self):
        """
        Returns an overview over the Flink cluster.

        Endpoint: [GET] /overview

        Returns
        -------
        dict
            Key-value pairs of flink cluster infos.
        """
        return _execute_rest_request(url=f'{self.api_url}/overview')

    def config(self):
        """
        Returns the configuration of the WebUI.

        Endpoint: [GET] /config

        Returns
        -------
        dict
            Query result as a dict.
        """
        return _execute_rest_request(url=f'{self.api_url}/config', http_method='GET')

    def delete_cluster(self):
        """
        Shuts down the cluster.

        Endpoint: [GET] /cluster

        Returns
        -------
        dict
            Result of delete operation.
        """
        return _execute_rest_request(url=f'{self.api_url}/cluster', http_method='DELETE')

    def datasets(self):
        """
        Returns all cluster data sets.

        Endpoint: [GET] /datasets

        Returns
        -------
        dict
            Query result as a dict.
        """
        return _execute_rest_request(url=f'{self.api_url}/datasets', http_method='GET')

    def delete_dataset(self, dataset_id):
        """
        Triggers the deletion of a cluster data set. This async operation would return a DatasetTrigger for further
        query identifier.

        Endpoint: [DELETE] /datasets/:datasetid

        Parameters
        ----------
        dataset_id: str
             32-character hexadecimal string value that identifies a cluster data set.

        Returns
        -------
        DatasetTrigger
            Object that can be used to query the status of delete operation.
        """
        trigger_id = _execute_rest_request(url=f'{self.api_url}/datasets/{dataset_id}', http_method='DELETE',
                                           accepted_status_code=202)['request-id']
        return DatasetTrigger(prefix=f'{self.api_url}/delete', trigger_id=trigger_id)
