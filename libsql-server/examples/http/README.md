# client.sh

`client.sh` script is a simple wrapper over curl and [jtbl](https://github.com/kellyjonbrazil/jtbl)
which mimics libSQL shell, but speaks over the HTTP interface.

## Example
```
./client.sh 
Connected to localhost:8000
sqld> EXPLAIN SELECT * FROM sqlite_master;
  addr  comment    opcode         p1    p2    p3    p4    p5
------  ---------  -----------  ----  ----  ----  ----  ----
     0             Init            0    11     0           0
     1             OpenRead        0     1     0     5     0
     2             Rewind          0    10     0           0
     3             Column          0     0     1           0
     4             Column          0     1     2           0
     5             Column          0     2     3           0
     6             Column          0     3     4           0
     7             Column          0     4     5           0
     8             ResultRow       1     5     0           0
     9             Next            0     3     0           1
    10             Halt            0     0     0           0
    11             Transaction     0     0     1     0     1
    12             Goto            0     1     0           0
```

## Shell history
For convenient shell history, use `rlwrap`:
```sh
rlwrap ./client.sh
```
