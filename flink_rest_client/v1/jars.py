import ntpath
import os

from flink_rest_client.common import _execute_rest_request, RestException


class JarsClient:
    def __init__(self, prefix):
        """
        Constructor.

        Parameters
        ----------
        prefix: str
            REST API url prefix. It must contain the host, port pair.
        """
        self.prefix = f"{prefix}/jars"

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
        Uploads a jar to the cluster from the input path. The jar's name will be the original filename from the input
        path.

        Endpoint: [POST] /jars/upload

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
            "file": (filename, (open(path_to_jar, "rb")), "application/x-java-archive")
        }
        return _execute_rest_request(
            url=f"{self.prefix}/upload", http_method="POST", files=files
        )

    def get_plan(self, jar_id):
        """
        Returns the dataflow plan of a job contained in a jar previously uploaded via '/jars/upload'.

        Endpoint: [POST] /jars/:jarid/plan

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
        return _execute_rest_request(
            url=f"{self.prefix}/{jar_id}/plan", http_method="POST"
        )["plan"]

    def run(
        self,
        jar_id,
        arguments=None,
        entry_class=None,
        parallelism=None,
        savepoint_path=None,
        allow_non_restored_state=None,
    ):
        """
        Submits a job by running a jar previously uploaded via '/jars/upload'.

        Endpoint: [POST] /jars/:jarid/run

        Parameters
        ----------
        jar_id: str
            String value that identifies a jar. When uploading the jar a path is returned, where the filename is the ID.
            This value is equivalent to the `id` field in the list of uploaded jars.

        arguments: dict
            (Optional) Dict of program arguments.

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
            data["programArgs"] = " ".join([f"--{k} {v}" for k, v in arguments.items()])
        if entry_class is not None:
            data["entry-class"] = entry_class
        if parallelism is not None:
            if parallelism < 0:
                raise RestException(
                    "get_plan method's parallelism parameter must be a positive integer."
                )
            data["parallelism"] = parallelism
        if savepoint_path is not None:
            data["savepointPath"] = savepoint_path
        if allow_non_restored_state is not None:
            data["allowNonRestoredState"] = allow_non_restored_state

        return _execute_rest_request(
            url=f"{self.prefix}/{jar_id}/run", http_method="POST", json=data
        )["jobid"]

    def upload_and_run(
        self,
        path_to_jar,
        arguments=None,
        entry_class=None,
        parallelism=None,
        savepoint_path=None,
        allow_non_restored_state=None,
    ):
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
        if not result["status"] == "success":
            raise RestException("Could not upload the input jar file.", result)

        return self.run(
            ntpath.basename(result["filename"]),
            arguments=arguments,
            entry_class=entry_class,
            parallelism=parallelism,
            savepoint_path=savepoint_path,
            allow_non_restored_state=allow_non_restored_state,
        )

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
        res = _execute_rest_request(url=f"{self.prefix}/{jar_id}", http_method="DELETE")
        if len(res.keys()) < 1:
            return True
        else:
            return False
