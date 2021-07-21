
from tests.v1.test_base import TestBase


class TestJobmanagerClient(TestBase):

    def test_config(self, simple_client, requests_mock):
        requests_mock.get(f'{simple_client.jobmanager.prefix}/config', json=[
            {
                'key': 'blob.server.port',
                'value': '6124'
            },
            {
                'key': 'taskmanager.memory.process.size',
                'value': '1728m'
            },
        ])
        response = simple_client.jobmanager.config()

        assert isinstance(response, dict)
        assert response['blob.server.port'] == '6124'
        assert response['taskmanager.memory.process.size'] == '1728m'

    def test_logs(self, simple_client, requests_mock):
        requests_mock.get(f'{simple_client.jobmanager.prefix}/logs', json={
            'logs': [{'name': 'flink--standalonesession-1-64c17dab68c5.log', 'size': 19222}]
        })
        response = simple_client.jobmanager.logs()

        assert isinstance(response, list)
        assert response[0]['name'] == 'flink--standalonesession-1-64c17dab68c5.log'

    def test_get_log(self, simple_client, requests_mock):

        log_file = "flink--standalonesession-1-64c17dab68c5.log"
        content = "content_of_the_log_file"

        requests_mock.get(f'{simple_client.jobmanager.prefix}/logs/{log_file}', text=content)
        response = simple_client.jobmanager.get_log(log_file)

        assert isinstance(response, str)
        assert response == content

    def test_metric_names(self, simple_client, requests_mock):
        requests_mock.get(f'{simple_client.jobmanager.prefix}/metrics', json=[
            {'id': 'Status.JVM.GarbageCollector.PS_MarkSweep.Time'},
            {'id': 'Status.JVM.Memory.Mapped.TotalCapacity'},

        ])
        response = simple_client.jobmanager.metric_names()

        assert isinstance(response, list)
        assert response[0] == 'Status.JVM.GarbageCollector.PS_MarkSweep.Time'
        assert response[1] == 'Status.JVM.Memory.Mapped.TotalCapacity'

    def test_metrics(self, simple_client, requests_mock):
        requests_mock.get(f'{simple_client.jobmanager.prefix}/metrics', json=[
            {'id': 'Status.JVM.GarbageCollector.PS_MarkSweep.Time',
             'value': '33'},
            {'id': 'Status.JVM.Memory.Mapped.TotalCapacity',
             'value': '0'},
        ])
        response = simple_client.jobmanager.metrics()

        assert isinstance(response, dict)
        assert response['Status.JVM.GarbageCollector.PS_MarkSweep.Time'] == '33'
        assert response['Status.JVM.Memory.Mapped.TotalCapacity'] == '0'
