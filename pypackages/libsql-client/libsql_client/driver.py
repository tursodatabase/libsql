from abc import ABC, abstractmethod
from typing import NamedTuple, List, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from .result import ResultSet, Value

class _RawStmt(NamedTuple):
    sql: str
    params: Sequence["Value"]

class _Driver:
    @abstractmethod
    async def batch(self, stmts: List[_RawStmt]) -> List["ResultSet"]:
        raise NotImplementedError()

    async def close(self) -> None:
        pass

