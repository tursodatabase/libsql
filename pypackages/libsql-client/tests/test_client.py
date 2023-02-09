import concurrent.futures
import libsql_client
import pytest

@pytest.mark.asyncio
async def test_execute(url):
    async with libsql_client.Client(url) as client:
        result_set = await client.execute("SELECT 42")
        assert len(result_set.columns) == 1
        assert len(result_set.rows) == 1

        row = result_set.rows[0]
        assert len(row) == 1
        assert row[0] == 42

@pytest.mark.asyncio
async def test_batch(url):
    async with libsql_client.Client(url) as client:
        result_sets = await client.batch(["SELECT 42", ("VALUES (?, ?)", [1, "one"])])
        assert len(result_sets) == 2
        assert result_sets[0].rows[0][0] == 42
        assert result_sets[1].rows[0][0] == 1
        assert result_sets[1].rows[0][1] == "one"

@pytest.mark.asyncio
async def test_transaction(url):
    async with libsql_client.Client(url) as client:
        result_sets = await client.transaction(["SELECT 42", "SELECT 'two'"])
        assert len(result_sets) == 2
        assert result_sets[0].rows[0][0] == 42
        assert result_sets[1].rows[0][0] == "two"

@pytest.mark.asyncio
async def test_response_error(url):
    async with libsql_client.Client(url) as client:
        with pytest.raises(libsql_client.ClientResponseError) as excinfo:
            await client.execute("SELECT foo")
        assert "no such column: foo" in str(excinfo.value)

@pytest.mark.asyncio
async def test_http_invalid_url_error(http_url):
    invalid_url = http_url + "/invalid/url"
    async with libsql_client.Client(invalid_url) as client:
        with pytest.raises(libsql_client.ClientHttpError) as excinfo:
            await client.execute("SELECT 1")
        assert "404" in str(excinfo.value)

@pytest.mark.asyncio
async def test_http_invalid_sql_error(http_url):
    async with libsql_client.Client(http_url) as client:
        with pytest.raises(libsql_client.ClientHttpError) as excinfo:
            await client.execute("SELECT")
        assert "unexpected end of input" in str(excinfo.value)

@pytest.mark.asyncio
async def test_http_multiple_statements_error(http_url):
    async with libsql_client.Client(http_url) as client:
        with pytest.raises(libsql_client.ClientHttpError) as excinfo:
            await client.execute("SELECT 1; SELECT 2")
        assert "more than one command" in str(excinfo.value)

@pytest.mark.asyncio
async def test_http_interactive_transaction_error(http_url):
    async with libsql_client.Client(http_url) as client:
        with pytest.raises(libsql_client.ClientHttpError) as excinfo:
            await client.execute("BEGIN")
        assert "interactive transaction" in str(excinfo.value)

@pytest.mark.asyncio
async def test_custom_executor(url):
    with concurrent.futures.ThreadPoolExecutor(1) as executor:
        async with libsql_client.Client(url, executor=executor) as client:
            result_set = await client.execute("SELECT 42")
            assert result_set.rows[0][0] == 42
