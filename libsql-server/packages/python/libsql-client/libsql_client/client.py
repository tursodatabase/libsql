from collections import namedtuple
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union, TYPE_CHECKING
import urllib.parse

from .driver import _Driver, _RawParams, _RawStmt
from .http_driver import _HttpDriver
from .sqlite_driver import _SqliteDriver

if TYPE_CHECKING:
    import concurrent.futures
    from .result import ResultSet, Value

Params = Union[Sequence["Value"], Dict[str, "Value"]]
Stmt = Union[str, Tuple[str, Params]]

class Client:
    """A client for sqld that can also work with a local SQLite database."""

    _driver: _Driver

    def __init__(
        self, url: str, *,
        executor: Optional["concurrent.futures.ThreadPoolExecutor"] = None,
    ) -> None:
        """Create a client that connects to the given URL.

        The following URL schemes are supported:

        - `http` and `https` connect to sqld via the HTTP interface
        - `file` opens a local SQLite database

        The optional `executor` is used to execute database operations for `file` URLs.
        """

        parsed_url = urllib.parse.urlparse(url)
        if parsed_url.scheme in ("http", "https"):
            self._driver = _HttpDriver(url)
        elif parsed_url.scheme == "file":
            self._driver = _SqliteDriver(parsed_url.path, executor=executor)
        else:
            raise ValueError(f"Unsupported URL scheme: {parsed_url.scheme!r}")

    async def execute(self, sql: str, params: Params = ()) -> "ResultSet":
        """Execute a single SQL statement with optional parameters."""
        return (await self.batch([(sql, params)]))[0]

    async def batch(self, stmts: Sequence[Stmt]) -> List["ResultSet"]:
        """Execute a batch of SQL statements with optional parameters.

        The returned list contains a `ResultSet` for every statement in `stmts`.
        """

        raw_stmts = []
        for stmt in stmts:
            if isinstance(stmt, tuple):
                raw_params: _RawParams
                if isinstance(stmt[1], dict):
                    raw_params = stmt[1]
                else:
                    raw_params = list(stmt[1])
                raw_stmts.append(_RawStmt(stmt[0], raw_params))
            else:
                raw_stmts.append(_RawStmt(stmt, []))
        return await self._driver.batch(raw_stmts)

    async def transaction(self, stmts: Sequence[Stmt]) -> List["ResultSet"]:
        """Execute a batch of SQL statements between BEGIN and COMMIT."""
        return (await self.batch(["BEGIN"] + list(stmts) + ["COMMIT"]))[1:-1]

    async def close(self) -> None:
        """Close the client and release resources."""
        await self._driver.close()

    async def __aenter__(self) -> "Client":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()

