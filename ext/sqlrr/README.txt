
sqlrr - 10/19/2009

SQL Replay Recording

SUMMARY
-------------------------------------------------------
This extension enables recording sqlite API calls that access the database so that they can be replayed or examined.

USAGE
------------------------------------------------------
Recording is enabled by compiling sqlite with symbolic constant SQLITE_ENABLE_SQLRR defined.  
By default logs are written to /tmp/<databasename>_<pid>_<connection_number>.sqlrr, to choose another directory, set the environment variable SQLITE_REPLAY_RECORD_DIR to that path.

FILE FORMAT
-----------------------------------------------------
 file:			<header>[<sql-command>]*
 
 header:			<signature><format-version>
   signature: 		SQLRR (5 bytes)
   format-version:	n (1 byte)
 
 sql-command:		<timestamp><type><arg-data>
   timestamp:       n (16 bytes)
   type:			n (1 byte)
        open		0
        close		1
        exec		8
        bind-text	16
        bind-double	17
        bind-int	18
        bind-null	19
        bind-value	20
        bind-clear	21
        prep		32
        step		33
        reset		34
        finalize	35

  open-arg-data:		<connection><path><flags>
  close-arg-data:		<connection>
  exec-arg-data:		<connection><len><statement-text>
  bind-text-arg-data:	<statement-ref><index><len><data>
  bind-double-arg-data:	<statement-ref><index><data>
  bind-int-arg-data:	<statement-ref><index><data>
  bind-null-arg-data:	<statement-ref><index>
  bind-value-arg-data:	<statement-ref><index><len><data> ???
  bind-clear-arg-data:	<statement-ref>
  prep-arg-data:		<connection><len><statement-text>
  step-arg-data:		<statement-ref>
  reset-arg-data:		<statement-ref>
  finalize-arg-data:	<statement-ref>

NOTES
