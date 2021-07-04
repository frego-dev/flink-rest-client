import requests


class RestException(Exception):
    """
    Exception to catch REST API related exceptions.
    """

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


def _execute_rest_request(
    url,
    http_method=None,
    accepted_status_code=None,
    files=None,
    params=None,
    data=None,
    json=None,
):
    if http_method is None:
        http_method = "GET"
    if params is None:
        params = {}
    if data is None:
        data = {}

    # If accepted_status_code is None then default value is set.
    if accepted_status_code is None:
        accepted_status_code = 200

    response = requests.request(
        method=http_method, url=url, files=files, params=params, data=data, json=json
    )
    if response.status_code == accepted_status_code:
        return response.json()
    else:
        if "errors" in response.json().keys():
            error_str = "\n".join(response.json()["errors"])
        else:
            error_str = ""
        raise RestException(
            f"REST response error ({response.status_code}): {error_str}"
        )
