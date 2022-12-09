from flink_rest_client.common import _execute_rest_request
from flink_rest_client.v1.jars import JarsClient
from flink_rest_client.v1.jobmanager import JobmanagerClient
from flink_rest_client.v1.jobs import JobsClient
from flink_rest_client.v1.taskmanagers import TaskManagersClient


class DatasetTrigger:
    def __init__(self, prefix, trigger_id):
        self._prefix = prefix
        self.trigger_id = trigger_id

    @property
    def status(self):
        return _execute_rest_request(url=f"{self._prefix}/{self.trigger_id}")


class FlinkRestClientV1:
    def __init__(self, host, port, auth, verify):
        self.host = host
        self.port = port
        self.auth = auth
        self.verify = verify

    @property
    def api_url(self):
        if self.port == "443":
            return f"https://{self.host}:{str(self.port)}/v1"
        else:
            return f"http://{self.host}:{str(self.port)}/v1"

    @property
    def jobmanager(self):
        return JobmanagerClient(prefix=self.api_url, auth=self.auth, verify=self.verify)

    @property
    def taskmanagers(self):
        return TaskManagersClient(prefix=self.api_url, auth=self.auth, verify=self.verify)

    @property
    def jars(self):
        return JarsClient(prefix=self.api_url, auth=self.auth, verify=self.verify)

    @property
    def jobs(self):
        return JobsClient(prefix=self.api_url, auth=self.auth, verify=self.verify)

    def overview(self):
        """
        Returns an overview over the Flink cluster.

        Endpoint: [GET] /overview

        Returns
        -------
        dict
            Key-value pairs of flink cluster infos.
        """
        return _execute_rest_request(url=f"{self.api_url}/overview", auth=self.auth, verify=self.verify)

    def config(self):
        """
        Returns the configuration of the WebUI.

        Endpoint: [GET] /config

        Returns
        -------
        dict
            Query result as a dict.
        """
        return _execute_rest_request(url=f"{self.api_url}/config", http_method="GET",
                                     auth=self.auth, verify=self.verify)

    def delete_cluster(self):
        """
        Shuts down the cluster.

        Endpoint: [DELETE] /cluster

        Returns
        -------
        dict
            Result of delete operation.
        """
        return _execute_rest_request(
            url=f"{self.api_url}/cluster", http_method="DELETE", auth=self.auth, verify=self.verify
        )

    def datasets(self):
        """
        Returns all cluster data sets.

        Endpoint: [GET] /datasets

        Returns
        -------
        list
            Query result as a list of datasets.
        """
        return _execute_rest_request(url=f"{self.api_url}/datasets", http_method="GET",
                                     auth=self.auth, verify=self.verify)[
            "dataSets"
        ]

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
        trigger_id = _execute_rest_request(
            url=f"{self.api_url}/datasets/{dataset_id}",
            http_method="DELETE",
            accepted_status_code=202,
            auth=self.auth,
            verify=self.verify,
        )["request-id"]
        return DatasetTrigger(
            prefix=f"{self.api_url}/datasets/delete", trigger_id=trigger_id
        )
