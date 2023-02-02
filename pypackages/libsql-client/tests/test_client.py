import libsql_client
import pytest

@pytest.fixture
def url():
    return "http://localhost:8080"

@pytest.mark.asyncio
async def test_it_works(url):
    async with libsql_client.Client(url) as client:
        result_set = await client.execute("SELECT 42")
        assert len(result_set.columns) == 1
        assert len(result_set.rows) == 1

        row = result_set.rows[0]
        assert len(row) == 1
        assert row[0] == 42

@pytest.mark.asyncio
async def test_result_basic_types(url):
    async with libsql_client.Client(url) as client:
        result_set = await client.execute("SELECT 42, 0.5, 'brontosaurus', NULL")
        assert len(result_set.columns) == 4
        assert len(result_set.rows) == 1

        row = result_set.rows[0]
        assert len(row) == 4
        assert row[0] == 42
        assert row[1] == 0.5
        assert row[2] == "brontosaurus"
        assert row[3] is None

@pytest.mark.asyncio
async def test_result_big_integer(url):
    n = 2**63-1
    async with libsql_client.Client(url) as client:
        result_set = await client.execute(f"SELECT {n}")
        assert result_set.rows[0][0] == n

# this test fails, because sqld returns blobs as base64-encoded strings, but we cannot distinguish them from
# normal strings!
@pytest.mark.asyncio
@pytest.mark.xfail
async def test_result_blob(url):
    async with libsql_client.Client(url) as client:
        result_set = await client.execute(f"SELECT X'deadbeef'")
        assert result_set.rows[0][0] == b"\xde\xad\xbe\xef"

@pytest.mark.asyncio
async def test_result_columns(url):
    async with libsql_client.Client(url) as client:
        result_set = await client.execute("SELECT 1 AS a, 2 AS b, 3 AS c")
        assert result_set.columns == ("a", "b", "c")
        row = result_set.rows[0]
        assert row["a"] == 1
        assert row["b"] == 2
        assert row["c"] == 3

@pytest.mark.asyncio
async def test_result_rows(url):
    async with libsql_client.Client(url) as client:
        result_set = await client.execute("VALUES (1, 'one'), (2, 'two'), (3, 'three')")
        assert len(result_set.rows) == 3
        assert result_set.rows[0][0] == 1
        assert result_set.rows[0][1] == "one"
        assert result_set.rows[1][0] == 2
        assert result_set.rows[1][1] == "two"
        assert result_set.rows[2][0] == 3
        assert result_set.rows[2][1] == "three"

@pytest.mark.asyncio
async def test_params_basic_types(url):
    async with libsql_client.Client(url) as client:
        result_set = await client.execute("SELECT ?, ?, ?, ?", [42, 0.5, "brontosaurus", None])
        row = result_set.rows[0]
        assert row[0] == 42
        assert row[1] == 0.5
        assert row[2] == "brontosaurus"
        assert row[3] is None

@pytest.mark.asyncio
async def test_params_big_integer(url):
    n = 2**63-1
    async with libsql_client.Client(url) as client:
        result_set = await client.execute("SELECT ?", [n])
        assert result_set.rows[0][0] == n

@pytest.mark.asyncio
async def test_params_blob(url):
    async with libsql_client.Client(url) as client:
        for l in range(5):
            blob = b"\xde\xad\xbe\xef"[:l]
            result_set = await client.execute("SELECT length(?)", [blob])
            assert result_set.rows[0][0] == l

# TODO: test errors
