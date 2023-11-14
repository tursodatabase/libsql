# Sqld consistency model

## Building on top of sqlite

sqlite offers a strictly serializable consistency model. Since sqld is built on top of it, it inherits some of its properties.

## Transactional consistency

Any transaction in sqld is equivalent to sqlite transaction. When a transaction is opened, on the primary or replicas alike, the view that the transaction get is "frozen" is time. any write performed by a transaction is at the same time immediately visible to itself, as well as completely isolated from any other ongoing transactions. Therefore, sqld offers serializable transactions

## Real-time guarantees

All operations occurring on the primary are linearizable. However, there is no guarantee that changes made to the primary are immediately visible to all replicas. Sqld guarantees that a process (connection) will always see its write. Given that the primary is linearizable, it means that a process is guaranteed to see all writes that happened on the primary up until (at least) the last write performed by the process. This is not true for two distinct processes on the same replica, however, that can potentially read two different points in time. For example, a read for process A on the replica might return immediately returning some state, while a read on process B issued at the same time would need to wait to sync with the primary.

Note that reads on a replica are monotonical: once a value has been witnessed, only a value at least as recent can be witnessed on any subsequent read.

There are no global ordering guarantees provided by sqld: any two instances needn't be in sync at any time.
