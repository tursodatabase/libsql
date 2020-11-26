
20-11-2020

# Memory Allocation In vdbesort.c

Memory allocation is slightly different depending on:

  * whether or not SQLITE_CONFIG_SMALL_MALLOC is set, and
  * whether or not worker threads are enabled.

## SQLITE_CONFIG_SMALL_MALLOC=0

Assuming SQLITE_CONFIG_SMALL_MALLOC is not set, keys passed to the sorter are
added to an in-memory buffer. This buffer is grown using sqlite3Realloc() as
required it reaches the size configured for the main pager cache using "PRAGMA
cache_size". i.e. if the user has executed "PRAGMA main.cache_size = -2048",
then this buffer is allowed to grow up to 2MB in size.

Once the buffer has grown to its threshold, keys are sorted and written to
a temp file. If worker threads are not enabled, this is the only significant
allocation the sorter module makes. After keys are sorted and flushed out to
the temp file, the buffer is reused to accumulate the next batch of keys.

If worker threads are available, then the buffer is passed to a worker thread
to sort and flush once it is full, and a new buffer allocated to allow the
main thread to continue to accumulate keys. Buffers are reused once they
have been flushed, so in this case at most (nWorker+1) buffers are allocated
and used, where nWorker is the number of configured worker threads.

There are no other significant users of heap memory in the sorter module. 
Once sorted buffers of keys have been flushed to disk, they are read back
either by mapping the file (via sqlite3_file.xFetch()) or else read back
in one page at a time.

All buffers are allocated by the main thread. A sorter object is associated
with a single database connection, to which it holds a pointer.

## SQLITE_CONFIG_SMALL_MALLOC=1

This case is similar to the above, except that instead of accumulating
multiple keys in a single large buffer, sqlite3VdbeSorterWrite() stores
keys in a regular heap-memory linked list (one allocation per element).
List elements are freed as they are flushed to disk, either by the main
thread or by a worker thread.

Each time a key is added the sorter (and an allocation made),
sqlite3HeapNearlyFull() is called. If it returns true, the current
list of keys is flushed to a temporary file, even if it has not yet
reached the size threshold.
