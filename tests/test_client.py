import pytest

from flink_rest_client import FlinkRestClient
from flink_rest_client.common import RestException
from flink_rest_client.v1.client import FlinkRestClientV1


class TestClient:

    def test_default_port(self):
        client = FlinkRestClient.get("test-host")
        assert client.port == 8081

    def test_unknown_version(self):
        with pytest.raises(RestException):
            FlinkRestClient.get("test-host", version="v2")

    def test_client_v1(self):
        client = FlinkRestClient.get("test-host", 8082)
        assert isinstance(client, FlinkRestClientV1)
        assert client.host == "test-host"
        assert client.port == 8082
