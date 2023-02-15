from typing import Dict, Iterator, List, Tuple, Union, overload
import collections

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

    def __iter__(self) -> Iterator["Row"]:
        return self._rows.__iter__()

class Row(collections.abc.Sequence):
    """A row returned by an SQL statement.

    The row values can be accessed with an index or by name.
    """

    _column_idxs: Dict[str, int]
    _values: Tuple[Value, ...]

    def __init__(self, column_idxs: Dict[str, int], values: Tuple[Value, ...]) -> None:
        self._column_idxs = column_idxs
        self._values = values

    @overload
    def __getitem__(self, key: Union[int, str]) -> Value:
        pass

    @overload
    def __getitem__(self, key: slice) -> Tuple[Value, ...]:
        pass

    def __getitem__(self, key: Union[int, str, slice]) -> Union[Value, Tuple[Value, ...]]:
        """Access a value by index or by name."""
        tuple_key: Union[int, slice]
        if isinstance(key, str):
            tuple_key = self._column_idxs[key]
        else:
            tuple_key = key
        return self._values[tuple_key]

    def __len__(self) -> int:
        return len(self._values)

    def __repr__(self) -> str:
        return repr(self._values)

    @property
    def _fields(self) -> Tuple[str, ...]:
        return tuple(self._column_idxs.keys())
