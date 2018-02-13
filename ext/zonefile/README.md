
# The Zonefile Extension

## Functionality

### Creating Zonefile Files

To create a new zonefile, first create a database table with the following
schema:

>     CREATE TABLE data(
>       k INTEGER PRIMARY KEY,
>       frame INTEGER DEFAULT -1,   -- frame number.  Automatic if -1
>       idx INTEGER DEFAULT -1,     -- index of entry within frame.  Auto if -1
>       v BLOB
>     );

The table may be created in a persistent or temporary database and may
take any name, but must contain the columns above. The table must be 
populated with a row for each key intended to appear in the new zonefile
file.

Once the table is populated, a zonefile is created using the following
SQL:

>     SELECT zonefile_write(<file>, <table> [, <parameters>]);

where &lt;file&gt; is the name of the file to create on disk, &lt;table&gt; 
is the name of the database table to read and optional argument 
&lt;parameters&gt; is a JSON object containing various attributes that
influence creation of the zonefile file. 

Currently the only &lt;parameters&gt; attribute supported is 
<i>maxAutoFrameSize</i> (default value 65536), which sets the maximum 
uncompressed frame size in bytes for automatically generated zonefile 
frames.

For example, to create a zonefile named "test.zonefile" based on the
contents of database table "test_input" and with a maximum automatic
frame size of 4096 bytes:

>     SELECT zonefile_write('test.zonefile', 'test_input',
>       '{"maxAutoFrameSize":4096}'
>     );

### Using (Reading) Zonefile Files

To create a new zonefile table:

>     CREATE VIRTUAL TABLE z1 USING zonefile;

This creates two virtual tables in the database schema. One read-only table
named "z1", with a schema equivalent to:

>     CREATE TABLE z1(  -- this whole table is read-only
>       k INTEGER PRIMARY KEY,
>       v BLOB,
>       fileid INTEGER,
>       frame INTEGER,
>       ofst INTEGER,
>       sz INTEGER
>     );

And a read-write table named "z1_files" with a schema like:

>     CREATE TABLE z1_files(
>       filename TEXT PRIMARY KEY,
>       ekey BLOB,         -- encryption key
>       fileid INTEGER,    -- read-only
>       header JSON HIDDEN -- read-only
>     );

Both tables are initially empty. To add a zonefile to the index, insert a
row into the "z1_files" table:

>     INSERT INTO z1_files(filename) VALUES(<filename>);

Currently, any value provided for any column other than "filename" is 
ignored. Files are removed from the index by deleting rows from the
z1_files table:

>     DELETE FROM z1_files WHERE filename = <filename>;

Once zonefile files have been added to the index, their contents are 
visible in table "z1". To retrieve the value associated with a single
key from one of the zonefile files in the index:

>     SELECT v FROM z1 WHERE k = <key>;


## Notes

  *  Contrary to the spec, the implementation uses 32-bit (not 16-bit) frame
     numbers. So the KeyOffsetPair structure becomes:

     KeyOffsetPair
     {
       uint64  key;
       uint32  frameNo;
       uint32  frameByteOffset;
     };

     Also, the ZonefileHeader.numFrames field is now 32-bit. Which makes
     the ZonefileHeader structure 26 bytes in size. The implementation
     pads this out to 32 bytes so that the ZoneFileIndex is 8-byte aligned.

  *  Multi-byte integer values are big-endian.

  *  The offsets in the ZoneFileIndex.byteOffsetZoneFrame[] array are
     relative to the offset in ZoneFileHeader.byteOffsetFrames. This is
     necessary as we may not know the offset of the start of the frame data
     until after the ZoneFileIndex structure is compressed.

  *  Currently there is no support at all for encryption or compression.

  *  Zonefile currently uses json1 to parse the json argument to
     zonefile_write(). And so must be used with an SQLITE_ENABLE_JSON1
     or otherwise json1-enabled SQLite.


