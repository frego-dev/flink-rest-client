
from flink_rest_client.v1.client import DatasetTrigger
from tests.v1.test_base import TestBase


class TestClientV1(TestBase):

    def test_api_url(self, simple_client):
        assert simple_client.api_url == 'http://host:8081/v1'

    def test_jobmanager(self, simple_client):
        assert simple_client.jobmanager.prefix == 'http://host:8081/v1/jobmanager'

    def test_taskmanagers(self, simple_client):
        assert simple_client.taskmanagers.prefix == 'http://host:8081/v1/taskmanagers'

    def test_jars(self, simple_client):
        assert simple_client.jars.prefix == 'http://host:8081/v1/jars'

    def test_overview(self, simple_client, requests_mock):
        requests_mock.get(f'{simple_client.api_url}/overview', json={
            'taskmanagers': 1,
            'flink-version': '1.12.4'
        })
        response = simple_client.overview()

        assert isinstance(response, dict)
        assert response['taskmanagers'] == 1
        assert response['flink-version'] == '1.12.4'

    def test_config(self, simple_client, requests_mock):
        requests_mock.get(f'{simple_client.api_url}/config', json={
            'refresh-interval': 3000,
            'flink-version': '1.12.4'
        })
        response = simple_client.config()

        assert isinstance(response, dict)
        assert response['refresh-interval'] == 3000
        assert response['flink-version'] == '1.12.4'

    def test_delete_cluster(self, simple_client, requests_mock):
        requests_mock.delete(f'{simple_client.api_url}/cluster', json={})
        response = simple_client.delete_cluster()

        assert isinstance(response, dict)
        assert len(response) == 0

    def test_datasets(self, simple_client, requests_mock):
        requests_mock.get(f'{simple_client.api_url}/datasets', json={
            'dataSets': []
        })
        response = simple_client.datasets()

        assert isinstance(response, list)
        assert len(response) == 0

    def test_delete_dataset(self, simple_client, requests_mock):
        dataset_id = 'test_id'
        trigger_id = 5555
        requests_mock.delete(f'{simple_client.api_url}/datasets/{dataset_id}', json={
            'request-id': trigger_id
        }, status_code=202)
        response = simple_client.delete_dataset(dataset_id)

        assert isinstance(response, DatasetTrigger)
        assert response.trigger_id == trigger_id

        requests_mock.get(f'{simple_client.api_url}/datasets/delete/{trigger_id}', json={
            'status': 'finished'
        })
        status = response.status
        assert isinstance(status, dict)
        assert status['status'] == 'finished'



