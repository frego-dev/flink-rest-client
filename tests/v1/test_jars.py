import importlib_resources

from tests.v1.test_base import TestBase


class TestJarClient(TestBase):

    def test_all(self, simple_client, requests_mock):
        requests_mock.get(f'{simple_client.jars.prefix}', json={
            'address': 'http://jobmanager:8081',
            'files': []})
        response = simple_client.jars.all()

        assert isinstance(response, dict)
        assert response['address'] == 'http://jobmanager:8081'

    def test_upload(self, simple_client, requests_mock):
        with importlib_resources.path("tests.data.sample_jars", "StateMachineExample.jar") as fpath:
            requests_mock.post(f'{simple_client.jars.prefix}/upload', json={
                'filename': '/tmp/flink-web-dc68d2c5/flink-web-upload/c17af8f2-_StateMachineExample.jar',
                'status': 'success'})
            response = simple_client.jars.upload(fpath)

        assert isinstance(response, dict)
        assert response['status'] == 'success'
        assert response['filename'] == '/tmp/flink-web-dc68d2c5/flink-web-upload/c17af8f2-_StateMachineExample.jar'

    def test_run(self, simple_client, requests_mock):
        jar_id = 'test_jar_id'
        job_id = "bdtg564"
        requests_mock.post(f'{simple_client.jars.prefix}/{jar_id}/run', json={
            'jobid': job_id
        })
        response = simple_client.jars.run(jar_id)

        assert isinstance(response, str)
        assert response == job_id

    def test_get_plan(self, simple_client, requests_mock):
        jar_id = 'test_jar_id'
        requests_mock.post(f'{simple_client.jars.prefix}/{jar_id}/plan', json={
            'plan': {
                'jid': 'test_jar_id', 'name': 'State machine job',
                'nodes': []
            }
        })
        response = simple_client.jars.get_plan(jar_id)

        assert isinstance(response, dict)
        assert response['jid'] == jar_id

    def test_upload_and_run(self, simple_client, requests_mock):
        with importlib_resources.path("tests.data.sample_jars", "StateMachineExample.jar") as fpath:
            jar_id = "c17af8f2-_StateMachineExample.jar"
            job_id = "bdtg564"
            requests_mock.post(f'{simple_client.jars.prefix}/upload', json={
                'filename': f'/tmp/flink-web-dc68d2c5/flink-web-upload/{jar_id}',
                'status': 'success'})
            requests_mock.post(f'{simple_client.jars.prefix}/{jar_id}/run', json={
                'jobid': job_id
            })

            response = simple_client.jars.upload_and_run(fpath)
            assert isinstance(response, str)
            assert response == job_id

    def test_delete(self, simple_client, requests_mock):
        jar_id = 'test_jar_id'
        requests_mock.delete(f'{simple_client.jars.prefix}/{jar_id}', json={})
        response = simple_client.jars.delete(jar_id)

        assert isinstance(response, bool)
        assert response is True
