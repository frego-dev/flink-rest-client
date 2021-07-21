import math

from flink_rest_client.v1.jobs import JobTrigger, JobVertexClient
from tests.v1.test_base import TestBase


class TestJobsClient(TestBase):

    def test_all(self, simple_client, requests_mock):
        requests_mock.get(f'{simple_client.jobs.prefix}', json={'jobs': [
            {'id': 'a0d4b5b51065202b788bbd0a80251a3c', 'status': 'RUNNING'}
        ]})
        response = simple_client.jobs.all()

        assert isinstance(response, list)
        assert len(response) == 1
        assert response[0]['id'] == 'a0d4b5b51065202b788bbd0a80251a3c'

    def test_job_ids(self, simple_client, requests_mock):
        requests_mock.get(f'{simple_client.jobs.prefix}', json={'jobs': [
            {'id': 'a0d4b5b51065202b788bbd0a80251a3c', 'status': 'RUNNING'}
        ]})
        response = simple_client.jobs.job_ids()

        assert isinstance(response, list)
        assert len(response) == 1
        assert response[0] == 'a0d4b5b51065202b788bbd0a80251a3c'

    def test_overview(self, simple_client, requests_mock):
        requests_mock.get(f'{simple_client.jobs.prefix}/overview', json={'jobs': [
            {
                'jid': 'a0d4b5b51065202b788bbd0a80251a3c',
                'name': 'State machine job',
                'state': 'RUNNING'
            }
        ]})
        response = simple_client.jobs.overview()

        assert isinstance(response, list)
        assert len(response) == 1
        assert response[0]['jid'] == 'a0d4b5b51065202b788bbd0a80251a3c'
        assert response[0]['name'] == 'State machine job'
        assert response[0]['state'] == 'RUNNING'

    def test_metric_names(self, simple_client, requests_mock):
        requests_mock.get(f'{simple_client.jobs.prefix}/metrics', json=[
            {'id': 'numberOfFailedCheckpoints'},
            {'id': 'lastCheckpointSize'},
            {'id': 'lastCheckpointExternalPath'},
        ])
        response = simple_client.jobs.metric_names()

        assert isinstance(response, list)
        assert response[0] == 'numberOfFailedCheckpoints'
        assert response[1] == 'lastCheckpointSize'
        assert response[2] == 'lastCheckpointExternalPath'

    def test_metrics(self, simple_client, requests_mock):
        requests_mock.get(f'{simple_client.jobs.prefix}', json={'jobs': [
            {'id': 'a0d4b5b51065202b788bbd0a80251a3c', 'status': 'RUNNING'}
        ]})

        requests_mock.get(f'{simple_client.jobs.prefix}/metrics', json=[
            {'id': 'numberOfFailedCheckpoints',
             'min': 0.0, 'max': 0.0, 'avg': 0.0, 'sum': 0.0},
            {'id': 'lastCheckpointSize',
             'min': 8482.0, 'max': 8482.0, 'avg': 8482.0, 'sum': 8482.0},
        ])
        response = simple_client.jobs.metrics()

        assert isinstance(response, dict)
        assert math.isclose(response['lastCheckpointSize']['min'], 8482.0)
        assert math.isclose(response['numberOfFailedCheckpoints']['min'], 0.0)

    def test_get(self, simple_client, requests_mock):
        jid = 'a0d4b5b51065202b788bbd0a80251a3c'
        requests_mock.get(f'{simple_client.jobs.prefix}/{jid}', json={
            'jid': 'a0d4b5b51065202b788bbd0a80251a3c',
            'name': 'State machine job',
            'isStoppable': False
        })
        response = simple_client.jobs.get(jid)

        assert isinstance(response, dict)
        assert response['jid'] == jid
        assert response['name'] == 'State machine job'
        assert response['isStoppable'] is False

    def test_config(self, simple_client, requests_mock):
        jid = 'a0d4b5b51065202b788bbd0a80251a3c'
        requests_mock.get(f'{simple_client.jobs.prefix}/{jid}/config', json={
            'jid': 'a0d4b5b51065202b788bbd0a80251a3c',
            'name': 'State machine job',
            'execution-config': {
                'execution-mode': 'PIPELINED',
                'restart-strategy': 'Cluster level default restart strategy',
                'job-parallelism': 1,
                'object-reuse-mode': False,
                'user-config': {}
            }
        })
        response = simple_client.jobs.get_config(jid)

        assert isinstance(response, dict)
        assert response['jid'] == jid
        assert response['name'] == 'State machine job'
        assert isinstance(response['execution-config'], dict)

    def test_exceptions(self, simple_client, requests_mock):
        jid = 'a0d4b5b51065202b788bbd0a80251a3c'
        requests_mock.get(f'{simple_client.jobs.prefix}/{jid}/exceptions', json={
            'root-exception': None, 'timestamp': None,
            'all-exceptions': []
        })
        response = simple_client.jobs.get_exceptions(jid)

        assert isinstance(response, dict)
        assert response['root-exception'] is None
        assert response['timestamp'] is None
        assert isinstance(response['all-exceptions'], list)

    def test_execution_result(self, simple_client, requests_mock):
        jid = 'a0d4b5b51065202b788bbd0a80251a3c'
        requests_mock.get(f'{simple_client.jobs.prefix}/{jid}/execution-result', json={
            'status': {'id': 'IN_PROGRESS'}
        })
        response = simple_client.jobs.get_execution_result(jid)

        assert isinstance(response, dict)
        assert response['status']['id'] == 'IN_PROGRESS'

    def test_get_metrics(self, simple_client, requests_mock):
        jid = 'a0d4b5b51065202b788bbd0a80251a3c'

        requests_mock.get(f'{simple_client.jobs.prefix}/metrics', json=[
            {'id': 'numberOfFailedCheckpoints'},
            {'id': 'lastCheckpointSize'},
        ])

        requests_mock.get(f'{simple_client.jobs.prefix}/{jid}/metrics', json=[
            {'id': 'numberOfFailedCheckpoints',
             'value': 0.0},
            {'id': 'lastCheckpointSize',
             'value': 8500.0},
        ])
        response = simple_client.jobs.get_metrics(jid)

        assert isinstance(response, dict)
        assert math.isclose(response['lastCheckpointSize'], 8500.0)
        assert math.isclose(response['numberOfFailedCheckpoints'], 0.0)

    def test_get_plan(self, simple_client, requests_mock):
        jid = 'a0d4b5b51065202b788bbd0a80251a3c'
        requests_mock.get(f'{simple_client.jobs.prefix}/{jid}/plan', json={
            'plan': {
                'jid': 'a0d4b5b51065202b788bbd0a80251a3c',
                'name': 'State machine job',
                'nodes': []
            }
        })
        response = simple_client.jobs.get_plan(jid)

        assert isinstance(response, dict)
        assert response['jid'] == 'a0d4b5b51065202b788bbd0a80251a3c'
        assert response['name'] == 'State machine job'
        assert isinstance(response['nodes'], list)

    def test_get_vertex_ids(self, simple_client, requests_mock):
        jid = 'a0d4b5b51065202b788bbd0a80251a3c'
        requests_mock.get(f'{simple_client.jobs.prefix}/{jid}', json={
            'jid': 'a0d4b5b51065202b788bbd0a80251a3c',
            'name': 'State machine job',
            'isStoppable': False,
            'vertices': [
                {'id': 'bc764cd8ddf7a0cff126f51c16239658',
                 'name': 'Source: Custom Source', 'parallelism': 1},
                {'id': '20ba6b65f97481d5570070de90e4e791',
                 'name': 'Flat Map -> Sink: Print to Std. Out', 'parallelism': 1}]

        })
        response = simple_client.jobs.get_vertex_ids(jid)

        assert isinstance(response, list)
        assert response[0] == 'bc764cd8ddf7a0cff126f51c16239658'
        assert response[1] == '20ba6b65f97481d5570070de90e4e791'

    def test_get_accumulators(self, simple_client, requests_mock):
        jid = 'a0d4b5b51065202b788bbd0a80251a3c'
        requests_mock.get(f'{simple_client.jobs.prefix}/{jid}/accumulators', json={
            'job-accumulators': [],
            'user-task-accumulators': [],
            'serialized-user-task-accumulators': {}})
        response = simple_client.jobs.get_accumulators(jid)

        assert isinstance(response, dict)
        assert isinstance(response['job-accumulators'], list)
        assert isinstance(response['user-task-accumulators'], list)
        assert isinstance(response['serialized-user-task-accumulators'], dict)

    def test_get_checkpointing_configuration(self, simple_client, requests_mock):
        jid = 'a0d4b5b51065202b788bbd0a80251a3c'
        requests_mock.get(f'{simple_client.jobs.prefix}/{jid}/checkpoints/config', json={
            'mode': 'exactly_once',
            'interval': 2000,
            'timeout': 600000})
        response = simple_client.jobs.get_checkpointing_configuration(jid)

        assert isinstance(response, dict)
        assert response['mode'] == 'exactly_once'
        assert response['interval'] == 2000
        assert response['timeout'] == 600000

    def test_get_checkpoints(self, simple_client, requests_mock):
        jid = 'a0d4b5b51065202b788bbd0a80251a3c'
        requests_mock.get(f'{simple_client.jobs.prefix}/{jid}/checkpoints', json={
            'counts': {'restored': 0, 'total': 22645},
            'summary': {}})
        response = simple_client.jobs.get_checkpoints(jid)

        assert isinstance(response, dict)
        assert isinstance(response['counts'], dict)
        assert isinstance(response['summary'], dict)
        assert response['counts']['restored'] == 0

    def test_get_checkpoint_ids(self, simple_client, requests_mock):
        jid = 'a0d4b5b51065202b788bbd0a80251a3c'
        requests_mock.get(f'{simple_client.jobs.prefix}/{jid}/checkpoints', json={
            'counts': {'restored': 0, 'total': 22645},
            'summary': {},
            'history': [
                {'@class': 'completed', 'id': 22819, 'status': 'COMPLETED'},
                {'@class': 'completed', 'id': 22818, 'status': 'COMPLETED'},
                {'@class': 'completed', 'id': 22817, 'status': 'COMPLETED'},
                {'@class': 'completed', 'id': 22816, 'status': 'COMPLETED'}
            ]
        })
        response = simple_client.jobs.get_checkpoint_ids(jid)

        assert isinstance(response, list)
        assert len(response) == 4
        assert response[0] == 22819

    def test_get_checkpoint_details(self, simple_client, requests_mock):
        jid = 'a0d4b5b51065202b788bbd0a80251a3c'
        cid = 23342
        requests_mock.get(f'{simple_client.jobs.prefix}/{jid}/checkpoints/details/{cid}', json={
            '@class': 'completed',
            'id': cid,
            'status': 'COMPLETED',
            'is_savepoint': False,
            'state_size': 8356,
            'end_to_end_duration': 2
        })
        response = simple_client.jobs.get_checkpoint_details(jid, cid)

        assert isinstance(response, dict)
        assert response['id'] == cid

    def test_get_checkpoint_details_with_subtasks(self, simple_client, requests_mock):
        jid = 'a0d4b5b51065202b788bbd0a80251a3c'
        cid = 23342
        requests_mock.get(f'{simple_client.jobs.prefix}/{jid}/checkpoints/details/{cid}', json={
            '@class': 'completed',
            'id': cid,
            'status': 'COMPLETED',
            'is_savepoint': False,
            'state_size': 8356,
            'end_to_end_duration': 2,
            'tasks': {
                '20ba6b65f97481d5570070de90e4e791': {'id': 23522, 'status': 'COMPLETED'}
            }
        })

        requests_mock.get(
            f'{simple_client.jobs.prefix}/{jid}/checkpoints/details/{cid}/subtasks/20ba6b65f97481d5570070de90e4e791',
            json={
                '@class': 'completed',
                'id': cid,
                'status': 'COMPLETED',
                'is_savepoint': False,
                'state_size': 8356,
                'end_to_end_duration': 2,
                'tasks': {
                    '20ba6b65f97481d5570070de90e4e791': {'id': 23522, 'status': 'COMPLETED'},
                    'bc764cd8ddf7a0cff126f51c16239658': {'id': 23522, 'status': 'COMPLETED'}
                }
            })

        response = simple_client.jobs.get_checkpoint_details(jid, cid, show_subtasks=True)

        assert isinstance(response, dict)
        assert isinstance(response['subtasks'], dict)
        assert response['id'] == cid
        assert len(response['subtasks']) == 1

    def test_rescale(self, simple_client, requests_mock):
        jid = 'a0d4b5b51065202b788bbd0a80251a3c'
        tid = 'test_trigger_id'
        parallelism = 2

        requests_mock.patch(f'{simple_client.jobs.prefix}/{jid}/rescaling', json={
            'triggerid': tid,
        })

        response = simple_client.jobs.rescale(jid, parallelism)

        assert isinstance(response, JobTrigger)
        assert response.trigger_id == tid

    def test_savepoints(self, simple_client, requests_mock):
        jid = 'a0d4b5b51065202b788bbd0a80251a3c'
        tid = 'test_trigger_id'
        path = 'test'

        requests_mock.get(f'{simple_client.jobs.prefix}/{jid}/savepoints/{tid}', json={
            'status': {'id': 'COMPLETED'},
            'operation': {'location': 'file:/opt/flink/test/savepoint-0754b5-9e6dc83c00ac'}
        })
        requests_mock.post(f'{simple_client.jobs.prefix}/{jid}/savepoints', json={
            'request-id': tid,
        }, status_code=202)

        response = simple_client.jobs.create_savepoint(jid, path)

        assert isinstance(response, JobTrigger)
        assert response.trigger_id == tid

        assert response.status['status']['id'] == 'COMPLETED'

    def test_terminate(self, simple_client, requests_mock):
        jid = 'a0d4b5b51065202b788bbd0a80251a3c'
        requests_mock.patch(f'{simple_client.jobs.prefix}/{jid}', json={}, status_code=202)
        response = simple_client.jobs.terminate(jid)
        assert response is True

    def test_stop(self, simple_client, requests_mock):
        jid = 'a0d4b5b51065202b788bbd0a80251a3c'
        tid = 'test_trigger_id'
        path = 'test'

        requests_mock.get(f'{simple_client.jobs.prefix}/{jid}/savepoints/{tid}', json={
            'status': {'id': 'COMPLETED'},
            'operation': {'location': 'file:/opt/flink/test/savepoint-0754b5-9e6dc83c00ac'}
        })
        requests_mock.post(f'{simple_client.jobs.prefix}/{jid}/stop', json={
            'request-id': tid,
        }, status_code=202)

        response = simple_client.jobs.stop(jid, path)

        assert isinstance(response, JobTrigger)
        assert response.trigger_id == tid

        assert response.status['status']['id'] == 'COMPLETED'

    def test_get_vertex(self, simple_client, requests_mock):
        jid = 'a0d4b5b51065202b788bbd0a80251a3c'
        vertex_id = 'test_vertex_id'

        response = simple_client.jobs.get_vertex(jid, vertex_id)

        assert isinstance(response, JobVertexClient)
        assert response.job_id == jid
        assert response.vertex_id == vertex_id
