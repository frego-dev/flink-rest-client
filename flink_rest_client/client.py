from flink_rest_client.common import RestException
from flink_rest_client.v1.client import FlinkRestClientV1

VERSIONS = {"v1": FlinkRestClientV1}


class FlinkRestClient:
    @staticmethod
    def get(host, port=None, version=None):
        """
        Constructs a new rest client instance.

        Parameters
        ----------
        host: str
            Hostname of Flink Jobmanager
        port: int
            Port number. Default value: 8081
        version: str
            Version of the REST API. Default value: v1
        version
        """
        port = 8081 if port is None else port
        version = "v1" if version is None else version

        if version not in VERSIONS.keys():
            raise RestException(f"Unknown REST API version: {version}")
        api_client_cls = VERSIONS[version]
        return api_client_cls(host=host, port=port)
