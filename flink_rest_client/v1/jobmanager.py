import requests

from flink_rest_client.common import _execute_rest_request, RestException


class JobmanagerClient:
    def __init__(self, prefix):
        """
        Constructor.

        Parameters
        ----------
        prefix: str
            REST API url prefix. It must contain the host, port pair.
        """
        self.prefix = f"{prefix}/jobmanager"

    def config(self):
        """
        Returns the cluster configuration.

        Endpoint: [GET] /jobmanager/config

        Returns
        -------
        dict
            Cluster configuration dictionary.
        """
        query_result = _execute_rest_request(url=f"{self.prefix}/config")
        return dict([(elem["key"], elem["value"]) for elem in query_result])

    def logs(self):
        """
        Returns the list of log files on the JobManager.

        Endpoint: [GET] /jobmanager/logs

        Returns
        -------
        dict
            List of log files
        """
        return _execute_rest_request(url=f"{self.prefix}/logs")["logs"]

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
        response = requests.request(method="GET", url=f"{self.prefix}/logs/{log_file}")
        if response.status_code == 200:
            return response.content.decode()
        else:
            if "errors" in response.json().keys():
                error_str = "\n".join(response.json()["errors"])
            else:
                error_str = ""
            raise RestException(
                f"REST response error ({response.status_code}): {error_str}"
            )

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
        params = {"get": ",".join(metric_names)}
        query_result = _execute_rest_request(
            url=f"{self.prefix}/metrics", params=params
        )
        return dict([(elem["id"], elem["value"]) for elem in query_result])
