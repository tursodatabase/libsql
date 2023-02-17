from typing import Any, Dict, List, Optional, Tuple, TypeAlias
from urllib.parse import urljoin
import aiohttp
import base64
import json

from .driver import _Driver, _RawStmt
from .errors import ClientResponseError, ClientHttpError, ClientError
from .result import ResultSet, Row, Value

Version: TypeAlias = Tuple[int, int, int]

class _HttpDriver(_Driver):
    _session: aiohttp.ClientSession
    _url: str
    _version: Optional[Version]

    def __init__(self, url: str) -> None:
        self._session = aiohttp.ClientSession()
        self._url = url
        self._version = None

    async def _get_version(self) -> Version:
        url = urljoin(self._url, "version")
        async with await self._session.get(url) as resp:
            if not resp.ok:
                if resp.status == 404:
                    # pre /version, return dummy 0.0.0 version
                    return 0, 0, 0
                resp_body = await resp.read()
                try:
                    message = json.loads(resp_body).get("error")
                except ValueError:
                    message = None
                raise ClientHttpError(resp.status, message)
            version_string = await resp.text()
            parts = [int(x) for x in version_string.split(".")]
            if len(parts) != 3:
                raise ClientError("server returned invalid version number")
            return tuple(parts)

    async def _get_or_load_cached_version(self) -> Version:
        if self._version is None:
            self._version = await self._get_version()
        return self._version



    async def batch(self, stmts: List[_RawStmt]) -> List[ResultSet]:
        req_body = {
            "statements": [_encode_stmt(stmt) for stmt in stmts],
        }

        version = await self._get_or_load_cached_version()
        url = self._queries_url(version)
        async with await self._session.post(url, json=req_body) as resp:
            if not resp.ok:
                resp_body = await resp.read()
                try:
                    message = json.loads(resp_body).get("error")
                except ValueError:
                    message = None
                raise ClientHttpError(resp.status, message)

            resp_json = await resp.json(content_type=None)

        result_sets = [
            _decode_result_set(result_set_json)
            for result_set_json in resp_json
        ]
        return result_sets

    async def close(self) -> None:
        await self._session.close()

    # TODO: use version to dispatch query route
    def _queries_url(self, _version: Version):
        return urljoin(self._url, "")

def _encode_stmt(stmt: _RawStmt) -> Any:
    params_json: Any
    if isinstance(stmt.params, dict):
        params_json = {name: _encode_value(value) for name, value in stmt.params.items()}
    else:
        params_json = [_encode_value(value) for value in stmt.params]

    return {
        "q": stmt.sql,
        "params": params_json,
    }

def _encode_value(value: Value) -> Any:
    if isinstance(value, str) or isinstance(value, float) or isinstance(value, int):
        return value
    elif isinstance(value, bytes):
        return {"base64": base64.b64encode(value).strip(b"=").decode()}
    elif value is None:
        return None
    else:
        raise ValueError(f"Value of type {type(value)} is not supported in libsql client")

def _decode_result_set(result_set_json: Any) -> ResultSet:
    if "error" in result_set_json:
        raise ClientResponseError(result_set_json["error"]["message"])

    results_json = result_set_json["results"]
    columns = tuple(str(col_json) for col_json in results_json["columns"])
    column_idxs = {name: idx for (idx, name) in enumerate(columns)}
    rows = [_decode_row(row_json, len(columns), column_idxs) for row_json in results_json["rows"]]
    return ResultSet(columns, rows)

def _decode_row(row_json: Any, column_count: int, column_idxs: Dict[str, int]) -> Row:
    values = tuple(_decode_value(value_json) for value_json in row_json)
    if len(values) != column_count:
        raise RuntimeError(f"Received {len(values)} values, expected {column_count} columns")
    return Row(column_idxs, values)

def _decode_value(value_json: Any) -> Value:
    if isinstance(value_json, int) or isinstance(value_json, float):
        return value_json
    elif isinstance(value_json, str):
        return value_json
    elif value_json is None:
        return None
    elif "base64" in value_json:
        return base64.b64decode(value_json["base64"] + "===")
    else:
        raise RuntimeError(f"Received unexpected JSON value of type {type(value_json)}")
