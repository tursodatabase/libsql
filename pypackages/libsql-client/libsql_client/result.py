from typing import Dict, List, Tuple, Union

Value = Union[str, float, int, bytes, None]

class ResultSet:
    """Result of an SQL statement.

    The result is composed of columns and rows. Every row is represented as a `Row` object and the length of
    every row is equal to the number of columns.
    """

    _columns: Tuple[str, ...]
    _rows: List["Row"]

    def __init__(self, columns: Tuple[str, ...], rows: List["Row"]) -> None:
        self._columns = columns
        self._rows = rows

    @property
    def columns(self) -> Tuple[str, ...]:
        """The column names in the result set."""
        return self._columns

    @property
    def rows(self) -> List["Row"]:
        """List of all rows in the result set."""
        return self._rows

class Row:
    """A row returned by an SQL statement.

    The row values can be accessed with an index or by name.
    """

    _column_idxs: Dict[str, int]
    _values: Tuple[Value, ...]

    def __init__(self, column_idxs: Dict[str, int], values: Tuple[Value, ...]) -> None:
        self._column_idxs = column_idxs
        self._values = values

    def __getitem__(self, key: Union[int, str]) -> Value:
        """Access a value by index or by name."""
        if isinstance(key, str):
            idx = self._column_idxs[key]
        else:
            idx = key
        return self._values[idx]

    def __len__(self) -> int:
        return len(self._values)

    def __repr__(self) -> str:
        return repr(self._values)
