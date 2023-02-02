from collections import namedtuple
from typing import Any, List, Sequence, Tuple, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from .result import ResultSet, Value
from .driver import _Driver, _RawStmt
from .http_driver import _HttpDriver

Stmt = Union[str, Tuple[str, Sequence["Value"]]]

class Client:
    _driver: _Driver

    def __init__(self, url: str) -> None:
        self._driver = _HttpDriver(url)

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

