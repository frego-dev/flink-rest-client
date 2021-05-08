"""
cluster
config
dataset
"""
import requests


class RestException(Exception):

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class FlinkRestClientV1:

    def __init__(self, host, port):
        self.host = host
        self.port = port

    def get_config(self):
        return self._execute_rest_request(url='/config', http_method='GET')

    def get_jars(self):
        return self._execute_rest_request(url='/jars', http_method='GET')

    def upload_jar(self, path_to_jar):
        # TODO: finish the file uploading
        return self._execute_rest_request(url='/jars/upload', http_method='POST')

    def _execute_rest_request(self, url, http_method):
        response = requests.request(method=http_method, url=f'http://{self.host}:/v1{self.port}{url}')
        if response.status_code == 200:
            return response.json()
        else:
            raise RestException(f"REST response error: {response.status_code}")
