import math

from flink_rest_client.v1.jobs import JobVertexSubtaskClient
from tests.v1.test_base import TestBase


class TestJobsClient(TestBase):

    def test_subtasks(self, simple_client, requests_mock):
        jid = "efjbcepigfvrui235324ui"
        vid = "12hfkbro943rri235324ui"
        response = simple_client.jobs.get_vertex(jid, vid).subtasks

        assert isinstance(response, JobVertexSubtaskClient)

    def test_details(self, simple_client, requests_mock):
        jid = "efjbcepigfvrui235324ui"
        vid = "12hfkbro943rri235324ui"
        vc = simple_client.jobs.get_vertex(jid, vid)

        requests_mock.get(f'{vc.prefix_url}', json={
            'id': 'bc764cd8ddf7a0cff126f51c16239658',
            'name': 'Source: Custom Source',
            'parallelism': 1
        })
        response = vc.details()

        assert isinstance(response, dict)
        assert response['id'] == 'bc764cd8ddf7a0cff126f51c16239658'
        assert response['name'] == 'Source: Custom Source'
        assert response['parallelism'] == 1

    def test_backpressure(self, simple_client, requests_mock):
        jid = "efjbcepigfvrui235324ui"
        vid = "12hfkbro943rri235324ui"
        vc = simple_client.jobs.get_vertex(jid, vid)

        requests_mock.get(f'{vc.prefix_url}/backpressure', json={'status': 'deprecated'})
        response = vc.backpressure()

        assert isinstance(response, dict)
        assert response['status'] == 'deprecated'

    def test_metric_names(self, simple_client, requests_mock):
        jid = "efjbcepigfvrui235324ui"
        vid = "12hfkbro943rri235324ui"
        vc = simple_client.jobs.get_vertex(jid, vid)

        requests_mock.get(f'{vc.prefix_url}/metrics', json=[
            {'id': '0.numBytesInRemote'},
            {'id': '0.Source__Custom_Source.numRecordsIn'},
            {'id': '0.numBytesInRemotePerSecond'},
        ])
        response = vc.metric_names()

        assert isinstance(response, list)
        assert len(response) == 3
        assert response[0] == '0.numBytesInRemote'
        assert response[1] == '0.Source__Custom_Source.numRecordsIn'
        assert response[2] == '0.numBytesInRemotePerSecond'

    def test_metrics(self, simple_client, requests_mock):
        jid = "efjbcepigfvrui235324ui"
        vid = "12hfkbro943rri235324ui"
        vc = simple_client.jobs.get_vertex(jid, vid)

        requests_mock.get(f'{vc.prefix_url}/metrics', json=[
            {'id': '0.numBytesInRemote', 'value': 0.0},
            {'id': '0.Source__Custom_Source.numRecordsIn', 'value': 40.0},
        ])
        response = vc.metrics(['0.numBytesInRemote', '0.Source__Custom_Source.numRecordsIn'])

        assert isinstance(response, dict)
        assert math.isclose(response['0.numBytesInRemote'], 0.0)
        assert math.isclose(response['0.Source__Custom_Source.numRecordsIn'], 40.0)

    def test_subtasktimes(self, simple_client, requests_mock):
        jid = "efjbcepigfvrui235324ui"
        vid = "12hfkbro943rri235324ui"
        vc = simple_client.jobs.get_vertex(jid, vid)

        requests_mock.get(f'{vc.prefix_url}/subtasktimes', json={
            'id': 'bc764cd8ddf7a0cff126f51c16239658',
            'name': 'Source: Custom Source',
            'now': 101010,
            'subtasks': [{'subtask': 0,
                          'host': 'flink-taskmanager', 'duration': 5127315,
                          'timestamps': {'SCHEDULED': 1625895875, 'CANCELED': 0, 'FINISHED': 0, 'RECONCILING': 0,
                                         'CREATED': 124815827, 'RUNNING': 1215896082, 'CANCELING': 0,
                                         'FAILED': 0, 'DEPLOYING': 6281535}}]})
        response = vc.subtasktimes()

        assert isinstance(response, dict)
        assert response['subtasks'][0]['timestamps']['SCHEDULED'] == 1625895875

    def test_watermarks(self, simple_client, requests_mock):
        jid = "efjbcepigfvrui235324ui"
        vid = "12hfkbro943rri235324ui"
        vc = simple_client.jobs.get_vertex(jid, vid)

        requests_mock.get(f'{vc.prefix_url}/watermarks', json=[])
        response = vc.watermarks()

        assert isinstance(response, list)
        assert len(response) == 0

