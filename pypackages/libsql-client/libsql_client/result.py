from typing import Dict, List, Tuple, Union

Value = Union[str, float, int, bytes, None]

class ResultSet:
    def __init__(self, columns: Tuple[str], rows: List["Row"]) -> None:
        self._columns = columns
        self._rows = rows

    @property
    def columns(self) -> Tuple[str]:
        return self._columns

    @property
    def rows(self) -> List["Row"]:
        return self._rows

class Row:
    _column_idxs: Dict[str, int]
    _values: Tuple[Value]

    def __init__(self, column_idxs: Dict[str, int], values: Tuple[Value]) -> None:
        self._column_idxs = column_idxs
        self._values = values

    def __getitem__(self, key: Union[int, str]) -> Value:
        if isinstance(key, str):
            idx = self._column_idxs[key]
        else:
            idx = key
        return self._values[idx]

    def __len__(self) -> int:
        return len(self._values)
