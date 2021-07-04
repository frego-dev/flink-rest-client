from flink_rest_client.common import _execute_rest_request, RestException


class JobTrigger:
    def __init__(self, prefix, type_name, job_id, trigger_id):
        self._prefix = prefix
        self._type_name = type_name
        self.job_id = job_id
        self.trigger_id = trigger_id

    @property
    def status(self):
        return _execute_rest_request(
            url=f"{self._prefix}/{self.job_id}/{self._type_name}/{self.trigger_id}"
        )


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
        return [elem["subtask"] for elem in self.accumulators()["subtasks"]]

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
        return [
            elem["id"]
            for elem in _execute_rest_request(url=f"{self.prefix_url}/metrics")
        ]

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

        supported_agg_modes = ["min", "max", "sum", "avg"]
        if agg_modes is None:
            agg_modes = supported_agg_modes
        if len(set(agg_modes).difference(set(supported_agg_modes))) > 0:
            raise RestException(
                f"The provided aggregation modes list contains invalid value. Supported aggregation "
                f"modes: {','.join(supported_agg_modes)}; given list: {','.join(agg_modes)}"
            )

        if subtask_ids is None:
            subtask_ids = self.subtask_ids()

        params = {
            "get": ",".join(metric_names),
            "agg": ",".join(agg_modes),
            "subtasks": ",".join([str(elem) for elem in subtask_ids]),
        }
        query_result = _execute_rest_request(
            url=f"{self.prefix_url}/metrics", params=params
        )

        result = {}
        for elem in query_result:
            metric_name = elem.pop("id")
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
        return _execute_rest_request(
            url=f"{self.prefix_url}/{subtask_id}/attempts/{attempt_id}"
        )

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
            attempt_id = self.get(subtask_id)["attempt"]
        return _execute_rest_request(
            url=f"{self.prefix_url}/{subtask_id}/attempts/{attempt_id}/accumulators"
        )


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
        return [
            elem["id"]
            for elem in _execute_rest_request(url=f"{self.prefix_url}/metrics")
        ]

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

        params = {"get": ",".join(metric_names)}
        query_result = _execute_rest_request(
            url=f"{self.prefix_url}/metrics", params=params
        )
        result = {}
        for elem in query_result:
            metric_name = elem.pop("id")
            result[metric_name] = elem["value"]
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
        self.prefix = f"{prefix}/jobs"

    def all(self):
        """
        Returns an overview over all jobs and their current state.

        Endpoint: [GET] /jobs

        Returns
        -------
        list
            List of jobs and their current state.
        """
        return _execute_rest_request(url=self.prefix)["jobs"]

    def job_ids(self):
        """
        Returns the list of job_ids.

        Returns
        -------
        list
            List of job ids.
        """
        return [elem["id"] for elem in self.all()]

    def overview(self):
        """
        Returns an overview over all jobs.

        Endpoint: [GET] /jobs/overview

        Returns
        -------
        list
            List of existing jobs.
        """
        return _execute_rest_request(url=f"{self.prefix}/overview")["jobs"]

    def metric_names(self):
        """
        Returns the supported metric names.

        Returns
        -------
        list
            List of metric names.
        """
        return [
            elem["id"] for elem in _execute_rest_request(url=f"{self.prefix}/metrics")
        ]

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

        supported_agg_modes = ["min", "max", "sum", "avg"]
        if agg_modes is None:
            agg_modes = supported_agg_modes
        if len(set(agg_modes).difference(set(supported_agg_modes))) > 0:
            raise RestException(
                f"The provided aggregation modes list contains invalid value. Supported aggregation "
                f"modes: {','.join(supported_agg_modes)}; given list: {','.join(agg_modes)}"
            )

        if job_ids is None:
            job_ids = self.job_ids()

        params = {
            "get": ",".join(metric_names),
            "agg": ",".join(agg_modes),
            "jobs": ",".join(job_ids),
        }
        query_result = _execute_rest_request(
            url=f"{self.prefix}/metrics", params=params
        )

        result = {}
        for elem in query_result:
            metric_name = elem.pop("id")
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
        return _execute_rest_request(url=f"{self.prefix}/{job_id}")

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
        return _execute_rest_request(url=f"{self.prefix}/{job_id}/config")

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
        return _execute_rest_request(url=f"{self.prefix}/{job_id}/exceptions")

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
        return _execute_rest_request(url=f"{self.prefix}/{job_id}/execution-result")

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
        params = {"get": ",".join(metric_names)}
        query_result = _execute_rest_request(
            url=f"{self.prefix}/{job_id}/metrics", params=params
        )
        return dict([(elem["id"], elem["value"]) for elem in query_result])

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
        return _execute_rest_request(url=f"{self.prefix}/{job_id}/plan")["plan"]

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
        return [elem["id"] for elem in self.get(job_id)["vertices"]]

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
            params["includeSerializedValue"] = (
                "true" if include_serialized_value else "false"
            )

        return _execute_rest_request(
            url=f"{self.prefix}/{job_id}/accumulators", http_method="GET", params=params
        )

    def get_checkpointing_configuration(self, job_id):
        """
        Returns the checkpointing configuration of the selected job_id

        Endpoint: [GET] /jobs/:jobid/checkpoints/config

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.

        Returns
        -------
        dict
            Checkpointing configuration of the selected job.
        """
        return _execute_rest_request(
            url=f"{self.prefix}/{job_id}/checkpoints/config", http_method="GET"
        )

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
        return _execute_rest_request(
            url=f"{self.prefix}/{job_id}/checkpoints", http_method="GET"
        )

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
        return [elem["id"] for elem in self.get_checkpoints(job_id=job_id)["history"]]

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
        checkpoint_details = _execute_rest_request(
            url=f"{self.prefix}/{job_id}/checkpoints/details/{checkpoint_id}",
            http_method="GET",
        )
        if not show_subtasks:
            return checkpoint_details

        subtasks = {}
        for vertex_id in checkpoint_details["tasks"].keys():
            subtasks[vertex_id] = _execute_rest_request(
                url=f"{self.prefix}/{job_id}/checkpoints/details/{checkpoint_id}/subtasks/{vertex_id}",
                http_method="GET",
            )
        checkpoint_details["subtasks"] = subtasks
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
        params = {"parallelism": parallelism}
        trigger_id = _execute_rest_request(
            url=f"{self.prefix}/{job_id}/rescaling", http_method="PATCH", params=params
        )["triggerid"]
        return JobTrigger(self.prefix, "rescaling", job_id, trigger_id)

    def create_savepoint(self, job_id, target_directory, cancel_job=False):
        """
        Triggers a savepoint, and optionally cancels the job afterwards. This async operation would return a
        JobTrigger for further query identifier.

        Endpoint: [GET] /jobs/:jobid/savepoints

        Notes
        -----
        The target directory has to be a location accessible by both the JobManager(s) and TaskManager(s)
        e.g. a location on a distributed file-system or Object Store.

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
            url=f"{self.prefix}/{job_id}/savepoints",
            http_method="POST",
            accepted_status_code=202,
            json={"cancel-job": cancel_job, "target-directory": target_directory},
        )["request-id"]
        return JobTrigger(self.prefix, "savepoints", job_id, trigger_id)

    def terminate(self, job_id):
        """
        Terminates a job.

        Endpoint: [PATCH] /jobs/:jobid

        Parameters
        ----------
        job_id: str
            32-character hexadecimal string value that identifies a job.

        Returns
        -------
        bool
            True if the job has been canceled, otherwise False.
        """
        res = _execute_rest_request(
            url=f"{self.prefix}/{job_id}", http_method="PATCH", accepted_status_code=202
        )
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
            "drain": False if drain is None else drain,
            "targetDirectory": target_directory,
        }

        trigger_id = _execute_rest_request(
            url=f"{self.prefix}/{job_id}/stop",
            http_method="POST",
            accepted_status_code=202,
            json=data,
        )["request-id"]
        return JobTrigger(self.prefix, "savepoints", job_id, trigger_id)

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
