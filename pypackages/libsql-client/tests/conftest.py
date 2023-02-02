import os
import pytest

@pytest.fixture
def url():
    return os.getenv("LIBSQL_CLIENT_TEST_URL", "http://localhost:8080")

