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


class FlinkRestClientV1:

    def __init__(self, host, port):
        self.host = host
        self.port = port

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
        return self._execute_rest_request(url=f'/jobs{job_id}/accumulators', http_method='GET')

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
        return self._execute_rest_request(url=f'/jobs{job_id}/checkpoints/config', http_method='GET')

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

    def _execute_rest_request(self, url, http_method, accepted_status_code=None, files=None):

        # If accepted_status_code is None then default value is set.
        if accepted_status_code is None:
            accepted_status_code = 200

        response = requests.request(method=http_method, url=f'http://{self.host}:/v1{self.port}{url}', files=files)
        if response.status_code == accepted_status_code:
            return response.json()
        else:
            raise RestException(f"REST response error: {response.status_code}")
