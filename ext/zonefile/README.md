
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

Currently, the following &lt;parameters&gt; attributes are supported:

<table border=1>
<tr align=left><th>Attribute<th>Default<th>Interpretation
<tr valign=top><td>maxAutoFrameSize<td>65536
<td>The maximum uncompressed frame size in bytes for automatically generated
zonefile frames.

<tr valign=top><td>compressionTypeContent<td>"none"
<td>The compression type used to compress each frame in the zonefile. 
Valid values are "none" (no compression), "zstd", "zstd_global_dict",
"zlib", "brotli", "lz4" and "lz4hc". Not all compression methods are
supported by all builds. The compression method supported by a build
depends on the combination of SQLITE_HAVE_ZSTD, SQLITE_HAVE_ZLIB,
SQLITE_HAVE_BROTLI and SQLITE_HAVE_LZ4 pre-processor symbols defined
at build time.

<tr valign=top><td>compressionTypeIndexData<td>"none"
<td>The compression type used to compress the zonefile index structure.
All values that are valid for the <i>compressionTypeContent</i> parameter,
except for "zstd_global_dict", are also valid for this option.

<tr valign=top><td>encryptionType<td>"none"
<td>The encryption type to use. At present the only valid values are
"none" (no encryption) and "xor" (an insecure mock encryption method
useful for testing only). Enhanced implementations may support any or
all of the following encryption schemes:
<ul>
  <li> "AES_128_CTR"
  <li> "AES_128_CBC"
  <li> "AES_256_CTR"
  <li> "AES_256_CBC"
</ul>

<tr valign=top><td>encryptionKey<td>""
<td>The encryption key to use. The encryption key must be specified as an
even number of hexadecimal that will be converted to a binary key before
use. It is the responsibility of the caller to specify a key of the optimal
length for each encryption algorithm (e.g. 16 bytes (32 hex digits) for
a 128-bit encryption, or 32 bytes (64 digits) for a 256-bit method).
This option is ignored if <i>encryptionType</i> is set to "none".
</table>

For example, to create a zonefile named "test.zonefile" based on the
contents of database table "test_input", with a maximum automatic
frame size of 4096 bytes and using "xor" encryption with a 128-bit key:

>     SELECT zonefile_write('test.zonefile', 'test_input',
>       '{"maxAutoFrameSize":4096,
>         "encryptionType":"xor",
>         "encryptionKey":"e6e600bc063aad12f6387beab650c48a"
>       }'
>     );

### Using (Reading) Zonefile Files

To create a new zonefile table, one of the following:

>     CREATE VIRTUAL TABLE z1 USING zonefile;
>     CREATE VIRTUAL TABLE z1 USING zonefile(cachesize=N);

where <i>N</i> is any non-zero positive integer. If the zonefile is used
to access any files containing compressed or encrypted data, it maintains
an LRU cache of uncompressed frame data <i>N</i> frames in size. The
default value of <i>N</i> is 1.

Creating a "zonefile" virtual table actually creates two virtual tables in the
database schema. One read-only table named "z1", with a schema equivalent to:

>     CREATE TABLE z1(  -- this whole table is read-only
>       k INTEGER PRIMARY KEY,     -- key value
>       v BLOB,                    -- associated blob of data
>       fileid INTEGER,            -- file id (rowid value for z1_files)
>       sz INTEGER                 -- size of blob of data in bytes
>     );

And a read-write table named "z1_files" with a schema like:

>     CREATE TABLE z1_files(
>       filename TEXT PRIMARY KEY,
>       ekey BLOB,         -- encryption key
>       header JSON HIDDEN -- read-only
>     );

Both tables are initially empty. To add a zonefile to the index, insert a
row into the "z1_files" table:

>     INSERT INTO z1_files(filename) VALUES(<filename>);

If the file is an encrypted file, then the encryption key (a blob) must
be inserted into the "ekey" column. Encryption keys are not stored in the
database, they are held in main-memory only. This means that each new
connection must configure encryption key using UPDATE statements before
accessing any encrypted files. For example:

>     -- Add new encrypted file to database:
>     INSERT INTO z1_files(filename, ekey) VALUES(<filename>, <ekey>);
>
>     -- Configure encryption key for existing file after opening database:
>     UPDATE z1_files SET ekey = <ekey> WHERE filename = <filename>;

Currently, values provided for any columns other than "filename" and
"ekey" are ignored. Files are removed from the index by deleting rows 
from the z1_files table:

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

  *  The offsets in the ZoneFileIndex.byteOffsetZoneFrame[] array are the
     offsets for the first byte past the end of the corresponding frame.
     For example, byteOffsetZoneFrame[] identifies the first byte of the
     second frame, and byteOffsetZoneFrame[numFrames-1] is one byte past
     the end of the last frame in the file.

     This is better as if we store the starting offset of each frame, there
     is no way to determine the size of the last frame in the file without
     trusting the filesize itself.

  *  Currently there is no support at all for encryption.

  *  Zonefile currently uses json1 to parse the json argument to
     zonefile\_write(). And so must be used with an SQLITE\_ENABLE\_JSON1
     or otherwise json1-enabled SQLite.


