import pandas
import libsql_client
import pytest

@pytest.mark.asyncio
async def test_basic_types(url):
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
async def test_big_integer(url):
    n = 2**63-1
    async with libsql_client.Client(url) as client:
        result_set = await client.execute(f"SELECT {n}")
        assert result_set.rows[0][0] == n

# this test fails, because sqld returns blobs as base64-encoded strings, but we cannot distinguish them from
# normal strings!
@pytest.mark.asyncio
async def test_blob(url):
    async with libsql_client.Client(url) as client:
        result_set = await client.execute(f"SELECT X'deadbeef'")
        assert result_set.rows[0][0] == b"\xde\xad\xbe\xef"

@pytest.mark.asyncio
async def test_columns(url):
    async with libsql_client.Client(url) as client:
        result_set = await client.execute("SELECT 1 AS a, 2 AS b, 3 AS c")
        assert result_set.columns == ("a", "b", "c")
        row = result_set.rows[0]
        assert row["a"] == 1
        assert row["b"] == 2
        assert row["c"] == 3

@pytest.mark.asyncio
async def test_rows(url):
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
async def test_row_repr(url):
    async with libsql_client.Client(url) as client:
        result_set = await client.execute("SELECT 42, 0.5, 'brontosaurus', NULL")
        assert repr(result_set.rows[0]) == "(42, 0.5, 'brontosaurus', None)"

@pytest.mark.asyncio
async def test_row_slice(url):
    async with libsql_client.Client(url) as client:
        result_set = await client.execute("SELECT 'one', 'two', 'three', 'four', 'five'")
        assert result_set.rows[0][1:3] == ("two", "three")

@pytest.mark.asyncio
async def test_pandas_from_records(url):
    async with libsql_client.Client(url) as client:
        result_set = await client.execute("SELECT 1, 'two', 3.0")
        data_frame = pandas.DataFrame.from_records(result_set.rows)
        assert data_frame.shape == (1, 3)

@pytest.mark.asyncio
async def test_pandas_ctor(url):
    async with libsql_client.Client(url) as client:
        result_set = await client.execute("SELECT 1 AS one, 'two' AS two, 3.0 AS three")
        data_frame = pandas.DataFrame(result_set)
        assert data_frame.shape == (1, 3)
        assert tuple(data_frame.columns) == ("one", "two", "three")
