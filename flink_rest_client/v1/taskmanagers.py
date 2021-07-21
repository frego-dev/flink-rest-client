from flink_rest_client.common import _execute_rest_request, RestException


class TaskManagersClient:
    def __init__(self, prefix):
        """
        Constructor.

        Parameters
        ----------
        prefix: str
            REST API url prefix. It must contain the host, port pair.
        """
        self.prefix = f"{prefix}/taskmanagers"

    def all(self):
        """
        Returns an overview over all task managers.

        Endpoint: [GET] /taskmanagers

        Returns
        -------
        list
            List of taskmanagers. Each taskmanager is represented by a dictionary.
        """
        return _execute_rest_request(url=self.prefix)["taskmanagers"]

    def taskmanager_ids(self):
        """
        Returns the list of taskmanager_ids.

        Returns
        -------
        list
            List of taskmanager ids.
        """
        return [elem["id"] for elem in self.all()]

    def metric_names(self):
        """
        Return the supported metric names.

        Returns
        -------
        list
            List of metric names.
        """
        return [
            elem["id"] for elem in _execute_rest_request(url=f"{self.prefix}/metrics")
        ]

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

        supported_agg_modes = ["min", "max", "sum", "avg"]
        if agg_modes is None:
            agg_modes = supported_agg_modes
        if len(set(agg_modes).difference(set(supported_agg_modes))) > 0:
            raise RestException(
                f"The provided aggregation modes list contains invalid value. Supported aggregation "
                f"modes: {','.join(supported_agg_modes)}; given list: {','.join(agg_modes)}"
            )

        if taskmanager_ids is None:
            taskmanager_ids = self.taskmanager_ids()

        params = {
            "get": ",".join(metric_names),
            "agg": ",".join(agg_modes),
            "taskmanagers": ",".join(taskmanager_ids),
        }
        query_result = _execute_rest_request(
            url=f"{self.prefix}/metrics", params=params
        )

        result = {}
        for elem in query_result:
            metric_name = elem.pop("id")
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
        return _execute_rest_request(url=f"{self.prefix}/{taskmanager_id}")

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
        return _execute_rest_request(url=f"{self.prefix}/{taskmanager_id}/logs")["logs"]

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
        params = {"get": ",".join(metric_names)}

        query_result = _execute_rest_request(
            url=f"{self.prefix}/{taskmanager_id}/metrics", params=params
        )
        return dict([(elem["id"], elem["value"]) for elem in query_result])

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
        query_result = _execute_rest_request(
            url=f"{self.prefix}/{taskmanager_id}/thread-dump"
        )["threadInfos"]
        return dict(
            [
                (elem["threadName"], elem["stringifiedThreadInfo"])
                for elem in query_result
            ]
        )
