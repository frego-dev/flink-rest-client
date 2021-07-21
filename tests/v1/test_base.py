import pytest

from flink_rest_client.v1.client import FlinkRestClientV1


class TestBase:

    @pytest.fixture(scope="class")
    def simple_client(self):
        return FlinkRestClientV1("host", 8081)
