import os
import pytest
import requests

@pytest.fixture
def http_url():
    env_name = "LIBSQL_CLIENT_TEST_URL"
    env_url = os.getenv(env_name)
    if env_url is not None:
        return env_url

    default_url = "http://localhost:8080"
    if is_libsql_alive(default_url):
        return default_url

    pytest.skip(f"Skipping HTTP test because environment variable {env_name} is not defined "
        f"and we could not reach the default server at {default_url}")

@pytest.fixture
def file_url(tmp_path):
    return f"file://{tmp_path.absolute() / 'test.db'}"

@pytest.fixture(params=["http", "file"])
def url(request):
    if request.param == "http":
        return request.getfixturevalue("http_url")
    elif request.param == "file":
        return request.getfixturevalue("file_url")
    else:
        assert False, f"Bad param: {request.param!r}"

def is_libsql_alive(url: str) -> bool:
    try:
        return requests.get(f"{url}/health").status_code == 200
    except requests.RequestException:
        return False
