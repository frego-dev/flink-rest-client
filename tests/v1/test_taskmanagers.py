import math

from tests.v1.test_base import TestBase


class TestTaskmanagerClient(TestBase):

    def test_all(self, simple_client, requests_mock):
        requests_mock.get(f'{simple_client.taskmanagers.prefix}', json={'taskmanagers': [
            {
                'id': '172.18.0.3:42073-c8a6ca',
                'path': 'akka.tcp://flink@172.18.0.3:42073/user/rpc/taskmanager_0',
                'dataPort': 45075
            }
        ]})
        response = simple_client.taskmanagers.all()

        assert isinstance(response, list)
        assert len(response) == 1
        assert response[0]['id'] == '172.18.0.3:42073-c8a6ca'

    def test_taskmanager_ids(self, simple_client, requests_mock):
        requests_mock.get(f'{simple_client.taskmanagers.prefix}', json={'taskmanagers': [
            {
                'id': '172.18.0.3:42073-c8a6ca',
                'path': 'akka.tcp://flink@172.18.0.3:42073/user/rpc/taskmanager_0',
                'dataPort': 45075
            }
        ]})
        response = simple_client.taskmanagers.taskmanager_ids()

        assert isinstance(response, list)
        assert len(response) == 1
        assert response[0] == '172.18.0.3:42073-c8a6ca'

    def test_metric_names(self, simple_client, requests_mock):
        requests_mock.get(f'{simple_client.taskmanagers.prefix}/metrics', json=[
            {'id': 'Status.JVM.GarbageCollector.PS_MarkSweep.Time'},
            {'id': 'Status.JVM.Memory.Mapped.TotalCapacity'},

        ])
        response = simple_client.taskmanagers.metric_names()

        assert isinstance(response, list)
        assert response[0] == 'Status.JVM.GarbageCollector.PS_MarkSweep.Time'
        assert response[1] == 'Status.JVM.Memory.Mapped.TotalCapacity'

    def test_metrics(self, simple_client, requests_mock):
        requests_mock.get(f'{simple_client.taskmanagers.prefix}', json={'taskmanagers': [
            {
                'id': '172.18.0.3:42073-c8a6ca',
                'path': 'akka.tcp://flink@172.18.0.3:42073/user/rpc/taskmanager_0',
                'dataPort': 45075
            }
        ]})

        requests_mock.get(f'{simple_client.taskmanagers.prefix}/metrics', json=[
            {'id': 'Status.Network.AvailableMemorySegments',
             'min': 4092.0, 'max': 4092.0, 'avg': 4092.0, 'sum': 4092.0},
            {'id': 'Status.JVM.Memory.Mapped.TotalCapacity',
             'min': 0.0, 'max': 0.0, 'avg': 0.0, 'sum': 0.0},
        ])
        response = simple_client.taskmanagers.metrics()

        assert isinstance(response, dict)
        assert math.isclose(response['Status.Network.AvailableMemorySegments']['min'], 4092.0)
        assert math.isclose(response['Status.JVM.Memory.Mapped.TotalCapacity']['min'], 0.0)

    def test_get(self, simple_client, requests_mock):
        tid = '172.18.0.3:42073-c8a6ca'
        requests_mock.get(f'{simple_client.taskmanagers.prefix}/{tid}', json={
            'id': '172.18.0.3:42073-c8a6ca',
            'path': 'akka.tcp://flink@172.18.0.3:42073/user/rpc/taskmanager_0',
            'dataPort': 45075,
        })
        response = simple_client.taskmanagers.get(tid)

        assert isinstance(response, dict)
        assert response['id'] == '172.18.0.3:42073-c8a6ca'
        assert response['path'] == 'akka.tcp://flink@172.18.0.3:42073/user/rpc/taskmanager_0'

    def test_get_logs(self, simple_client, requests_mock):
        tid = '172.18.0.3:42073-c8a6ca'
        requests_mock.get(f'{simple_client.taskmanagers.prefix}/{tid}/logs', json={
            'logs': [
                {'name': 'flink--taskexecutor-0-88d2501520f8.log', 'size': 18965}]

        })
        response = simple_client.taskmanagers.get_logs(tid)

        assert isinstance(response, list)
        assert len(response) == 1
        assert response[0]['name'] == 'flink--taskexecutor-0-88d2501520f8.log'

    def test_get_metrics(self, simple_client, requests_mock):
        tid = '172.18.0.3:42073-c8a6ca'

        requests_mock.get(f'{simple_client.taskmanagers.prefix}/metrics', json=[
            {'id': 'Status.JVM.GarbageCollector.PS_MarkSweep.Time'},
            {'id': 'Status.JVM.Memory.Mapped.TotalCapacity'},

        ])

        requests_mock.get(f'{simple_client.taskmanagers.prefix}/{tid}/metrics', json=[
            {
                'id': 'Status.Network.AvailableMemorySegments',
                'value': '4092'
            },
            {
                'id': 'Status.JVM.Memory.Mapped.TotalCapacity',
                'value': '0'
            },
        ])
        response = simple_client.taskmanagers.get_metrics(tid)

        assert isinstance(response, dict)
        assert len(response) == 2
        assert response['Status.Network.AvailableMemorySegments'] == '4092'

    def test_get_thread_dump(self, simple_client, requests_mock):
        tid = '172.18.0.3:42073-c8a6ca'
        requests_mock.get(f'{simple_client.taskmanagers.prefix}/{tid}/thread-dump', json={
            'threadInfos': [{
                'threadName': 'flink-taskexecutor-io-thread-1',
                'stringifiedThreadInfo': '"flink-taskexecutor-io-thread-1"'
            }]

        })
        response = simple_client.taskmanagers.get_thread_dump(tid)

        assert isinstance(response, dict)
        assert len(response) == 1
        assert response['flink-taskexecutor-io-thread-1'] == '"flink-taskexecutor-io-thread-1"'
