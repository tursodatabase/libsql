import os
import pytest

@pytest.fixture(params=["http", "file"])
def url(request, tmp_path):
    if request.param == "http":
        return os.getenv("LIBSQL_CLIENT_TEST_URL", "http://localhost:8080")
    elif request.param == "file":
        return f"file://{tmp_path.absolute() / 'test.db'}"
    else:
        assert False, f"Bad param: {request.param!r}"

