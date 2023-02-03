from abc import ABC, abstractmethod
from typing import Dict, NamedTuple, List, Sequence, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from .result import ResultSet, Value

_RawParams = Union[List["Value"], Dict[str, "Value"]]

class _RawStmt(NamedTuple):
    sql: str
    params: _RawParams

class _Driver:
    @abstractmethod
    async def batch(self, stmts: List[_RawStmt]) -> List["ResultSet"]:
        raise NotImplementedError()

    async def close(self) -> None:
        pass

