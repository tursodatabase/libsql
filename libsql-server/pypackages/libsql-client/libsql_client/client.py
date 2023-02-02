from collections import namedtuple
from typing import Any, List, Optional, Sequence, Tuple, Union, TYPE_CHECKING
import urllib.parse

from .driver import _Driver, _RawStmt
from .http_driver import _HttpDriver
from .sqlite_driver import _SqliteDriver

if TYPE_CHECKING:
    import concurrent.futures
    from .result import ResultSet, Value

Stmt = Union[str, Tuple[str, Sequence["Value"]]]

class Client:
    _driver: _Driver

    def __init__(
        self, url: str, *,
        executor: Optional["concurrent.futures.ThreadPoolExecutor"] = None,
    ) -> None:
        parsed_url = urllib.parse.urlparse(url)
        if parsed_url.scheme in ("http", "https"):
            self._driver = _HttpDriver(url)
        elif parsed_url.scheme == "file":
            self._driver = _SqliteDriver(parsed_url.path, executor=executor)
        else:
            raise ValueError(f"Unsupported URL scheme: {parsed_url.scheme!r}")

    async def execute(self, sql: str, params: Sequence["Value"] = ()) -> "ResultSet":
        return (await self.batch([(sql, params)]))[0]

    async def batch(self, stmts: Sequence[Stmt]) -> List["ResultSet"]:
        raw_stmts = []
        for stmt in stmts:
            if isinstance(stmt, tuple):
                raw_stmts.append(_RawStmt(stmt[0], stmt[1]))
            else:
                raw_stmts.append(_RawStmt(stmt, ()))
        return await self._driver.batch(raw_stmts)

    async def close(self) -> None:
        await self._driver.close()

    async def __aenter__(self) -> "Client":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()

