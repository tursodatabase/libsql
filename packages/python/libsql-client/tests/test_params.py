import libsql_client
import pytest

@pytest.mark.asyncio
async def test_basic_types(url):
    async with libsql_client.Client(url) as client:
        result_set = await client.execute("SELECT ?, ?, ?, ?", [42, 0.5, "brontosaurus", None])
        row = result_set.rows[0]
        assert row[0] == 42
        assert row[1] == 0.5
        assert row[2] == "brontosaurus"
        assert row[3] is None

@pytest.mark.asyncio
async def test_big_integer(url):
    n = 2**63-1
    async with libsql_client.Client(url) as client:
        result_set = await client.execute("SELECT ?", [n])
        assert result_set.rows[0][0] == n

@pytest.mark.asyncio
async def test_blob(url):
    async with libsql_client.Client(url) as client:
        for l in range(5):
            blob = b"\xde\xad\xbe\xef"[:l]
            result_set = await client.execute("SELECT length(?)", [blob])
            assert result_set.rows[0][0] == l

@pytest.mark.asyncio
async def test_named(url):
    async with libsql_client.Client(url) as client:
        result_set = await client.execute("SELECT :a, @b, $c", {":a": 42, "@b": 0.5, "$c": "brontosaurus"})
        row = result_set.rows[0]
        assert row[0] == 42
        assert row[1] == 0.5
        assert row[2] == "brontosaurus"

