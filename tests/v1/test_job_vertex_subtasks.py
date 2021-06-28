import math

from tests.v1.test_base import TestBase


class TestJobVertexSubtaskClient(TestBase):

    def test_accumulators(self, simple_client, requests_mock):
        jid = "efjbcepigfvrui235324ui"
        vid = "12hfkbro943rri235324ui"
        jvc = simple_client.jobs.get_vertex(jid, vid).subtasks

        requests_mock.get(f'{jvc.prefix_url}/accumulators', json={
            'id': 'bc764cd8ddf7a0cff126f51c16239658',
            'parallelism': 1,
            'subtasks': [{'subtask': 0, 'attempt': 0, 'host': 'flink-taskmanager', 'user-accumulators': []}]})
        response = jvc.accumulators()

        assert isinstance(response, dict)
        assert response['id'] == 'bc764cd8ddf7a0cff126f51c16239658'
        assert response['parallelism'] == 1
        assert isinstance(response['subtasks'], list)

    def test_subtask_ids(self, simple_client, requests_mock):
        jid = "efjbcepigfvrui235324ui"
        vid = "12hfkbro943rri235324ui"
        jvc = simple_client.jobs.get_vertex(jid, vid).subtasks

        requests_mock.get(f'{jvc.prefix_url}/accumulators', json={
            'id': 'bc764cd8ddf7a0cff126f51c16239658',
            'parallelism': 1,
            'subtasks': [{'subtask': 0, 'attempt': 0, 'host': 'flink-taskmanager', 'user-accumulators': []}]})
        response = jvc.subtask_ids()

        assert isinstance(response, list)
        assert len(response) == 1
        assert response[0] == 0

    def test_metric_names(self, simple_client, requests_mock):
        jid = "efjbcepigfvrui235324ui"
        vid = "12hfkbro943rri235324ui"
        jvc = simple_client.jobs.get_vertex(jid, vid).subtasks

        requests_mock.get(f'{jvc.prefix_url}/metrics', json=[
            {'id': 'Shuffle.Netty.Input.numBytesInLocal'},
            {'id': 'Shuffle.Netty.Output.Buffers.outPoolUsage'},
            {'id': 'checkpointStartDelayNanos'},
        ])
        response = jvc.metric_names()

        assert isinstance(response, list)
        assert len(response) == 3
        assert response[0] == 'Shuffle.Netty.Input.numBytesInLocal'
        assert response[1] == 'Shuffle.Netty.Output.Buffers.outPoolUsage'
        assert response[2] == 'checkpointStartDelayNanos'

    def test_metrics(self, simple_client, requests_mock):
        jid = "efjbcepigfvrui235324ui"
        vid = "12hfkbro943rri235324ui"
        jvc = simple_client.jobs.get_vertex(jid, vid).subtasks

        requests_mock.get(f'{jvc.prefix_url}/accumulators', json={
            'id': 'bc764cd8ddf7a0cff126f51c16239658',
            'parallelism': 1,
            'subtasks': [{'subtask': 0, 'attempt': 0, 'host': 'flink-taskmanager', 'user-accumulators': []}]})

        requests_mock.get(f'{jvc.prefix_url}/metrics', json=[
            {'id': 'Shuffle.Netty.Input.numBytesInLocal',
             'min': 0.0, 'max': 0.0, 'avg': 0.0, 'sum': 0.0},
            {'id': 'Shuffle.Netty.Output.Buffers.outPoolUsage',
             'min': 8482.0, 'max': 8482.0, 'avg': 8482.0, 'sum': 8482.0},
        ])
        response = jvc.metrics()

        assert isinstance(response, dict)
        assert math.isclose(response['Shuffle.Netty.Input.numBytesInLocal']['min'], 0.0)
        assert math.isclose(response['Shuffle.Netty.Output.Buffers.outPoolUsage']['min'], 8482.0)

    def test_get(self, simple_client, requests_mock):
        jid = "efjbcepigfvrui235324ui"
        vid = "12hfkbro943rri235324ui"
        subtask_id = 0
        jvc = simple_client.jobs.get_vertex(jid, vid).subtasks

        requests_mock.get(f'{jvc.prefix_url}/{subtask_id}', json={
            'subtask': 0,
            'status': 'RUNNING',
            'attempt': 0,
            'host': 'flink-taskmanager',
            'start-time': 101010402, 'end-time': -1, 'duration': 2136900,
            'metrics': {'read-bytes': 0, 'read-bytes-complete': True, 'write-bytes': 7862928,
                        'write-bytes-complete': True, 'read-records': 0, 'read-records-complete': True,
                        'write-records': 564821, 'write-records-complete': True},
            'taskmanager-id': '172.18.0.3:40027-9e32e4',
            'start_time': 101010402})
        response = jvc.get(subtask_id)

        assert isinstance(response, dict)
        assert response['status'] == 'RUNNING'
        assert response['taskmanager-id'] == '172.18.0.3:40027-9e32e4'

    def test_get_attempt(self, simple_client, requests_mock):
        jid = "efjbcepigfvrui235324ui"
        vid = "12hfkbro943rri235324ui"
        subtask_id = 0
        jvc = simple_client.jobs.get_vertex(jid, vid).subtasks

        requests_mock.get(f'{jvc.prefix_url}/{subtask_id}', json={
            'subtask': 0,
            'status': 'RUNNING',
            'attempt': 0,
            'host': 'flink-taskmanager',
            'start-time': 101010402, 'end-time': -1, 'duration': 2136900,
            'metrics': {'read-bytes': 0, 'read-bytes-complete': True, 'write-bytes': 7862928,
                        'write-bytes-complete': True, 'read-records': 0, 'read-records-complete': True,
                        'write-records': 564821, 'write-records-complete': True},
            'taskmanager-id': '172.18.0.3:40027-9e32e4',
            'start_time': 101010402})

        requests_mock.get(f'{jvc.prefix_url}/{subtask_id}/attempts/{subtask_id}', json={
            'subtask': 0,
            'status': 'RUNNING',
            'attempt': 0,
            'host': 'flink-taskmanager',
            'start-time': 101010402, 'end-time': -1, 'duration': 2136900,
            'metrics': {'read-bytes': 0, 'read-bytes-complete': True, 'write-bytes': 7862928,
                        'write-bytes-complete': True, 'read-records': 0, 'read-records-complete': True,
                        'write-records': 564821, 'write-records-complete': True},
            'taskmanager-id': '172.18.0.3:40027-9e32e4',
            'start_time': 101010402})
        response = jvc.get_attempt(subtask_id)

        assert isinstance(response, dict)
        assert response['status'] == 'RUNNING'
        assert response['taskmanager-id'] == '172.18.0.3:40027-9e32e4'

    def test_get_attempt_accumulators(self, simple_client, requests_mock):
        jid = "efjbcepigfvrui235324ui"
        vid = "12hfkbro943rri235324ui"
        subtask_id = 0
        jvc = simple_client.jobs.get_vertex(jid, vid).subtasks

        requests_mock.get(f'{jvc.prefix_url}/{subtask_id}', json={
            'subtask': 0,
            'status': 'RUNNING',
            'attempt': 0,
            'host': 'flink-taskmanager',
            'start-time': 101010402, 'end-time': -1, 'duration': 2136900,
            'metrics': {'read-bytes': 0, 'read-bytes-complete': True, 'write-bytes': 7862928,
                        'write-bytes-complete': True, 'read-records': 0, 'read-records-complete': True,
                        'write-records': 564821, 'write-records-complete': True},
            'taskmanager-id': '172.18.0.3:40027-9e32e4',
            'start_time': 101010402})

        requests_mock.get(f'{jvc.prefix_url}/{subtask_id}/attempts/{subtask_id}/accumulators', json={
            'subtask': 0,
            'attempt': 0,
            'id': 'b0a52b89389fe646b47eb9632bd7f1f8',
            'user-accumulators': []})
        response = jvc.get_attempt_accumulators(subtask_id)

        assert isinstance(response, dict)
        assert response['subtask'] == 0
        assert response['attempt'] == 0
        assert response['id'] == 'b0a52b89389fe646b47eb9632bd7f1f8'
