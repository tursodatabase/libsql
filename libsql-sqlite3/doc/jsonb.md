# The JSONB Format

This document describes SQLite's JSONB binary encoding of
JSON.

## 1.0 What Is JSONB?

Beginning with version 3.45.0 (circa 2024-01-01), SQLite supports an
alternative binary encoding of JSON which we call "JSONB".  JSONB is
a binary format that stored as a BLOB.

The advantage of JSONB over ordinary text RFC 8259 JSON is that JSONB
is both slightly smaller (by between 5% and 10% in most cases) and
can be processed in less than half the number of CPU cycles.  The built-in
[JSON SQL functions] of SQLite can accept either ordinary text JSON
or the binary JSONB encoding for any of their JSON inputs.

The "JSONB" name is inspired by [PostgreSQL](https://postgresql.org), but the
on-disk format for SQLite's JSONB is not the same as PostgreSQL's.
The two formats have the same name, but they have wildly different internal
representations and are not in any way binary compatible.

The central idea behind this JSONB specification is that each element
begins with a header that includes the size and type of that element.
The header takes the place of punctuation such as double-quotes,
curly-brackes, square-brackets, commas, and colons.  Since the size
and type of each element is contained in its header, the element can
be read faster since it is no longer necessary to carefully scan forward
looking for the closing delimiter.  The payload of JSONB is the same
as for corresponding text JSON.  The same payload bytes occur in the
same order.  The only real difference between JSONB and ordinary text
JSON is that JSONB includes a binary header on
each element and omits delimiter and separator punctuation.

### 1.1 Internal Use Only

The details of the JSONB are not intended to be visible to application
developers.  Application developers should look at JSONB as an opaque BLOB
used internally by SQLite.  Nevertheless, we want the format to be backwards
compatible across all future versions of SQLite.  To that end, the format
is documented by this file in the source tree.  But this file should be
used only by SQLite core developers, not by developers of applications
that only use SQLite.

## 2.0 The Purpose Of This Document

JSONB is not intended as an external format to be used by
applications.  JSONB is designed for internal use by SQLite only.
Programmers do not need to understand the JSONB format in order to
use it effectively.
Applications should access JSONB only through the [JSON SQL functions],
not by looking at individual bytes of the BLOB.

However, JSONB is intended to be portable and backwards compatible
for all future versions of SQLite.  In other words, you should not have
to export and reimport your SQLite database files when you upgrade to
a newer SQLite version.  For that reason, the JSONB format needs to
be well-defined.

This document is therefore similar in purpose to the
[SQLite database file format] document that describes the on-disk
format of an SQLite database file.  Applications are not expected
to directly read and write the bits and bytes of SQLite database files.
The SQLite database file format is carefully documented so that it
can be stable and enduring.  In the same way, the JSONB representation
of JSON is documented here so that it too can be stable and enduring,
not so that applications can read or writes individual bytes.

## 3.0 Encoding

JSONB is a direct translation of the underlying text JSON. The difference
is that JSONB uses a binary encoding that is faster to parse compared to
the detailed syntax of text JSON.

Each JSON element is encoded as a header and a payload.  The header
determines type of element (string, numeric, boolean, null, object, or
array) and the size of the payload.  The header can be between 1 and
9 bytes in size.  The payload can be any size from zero bytes up to the
maximum allowed BLOB size.

### 3.1 Payload Size

The upper four bits of the first byte of the header determine size of the
header and possibly also the size of the payload.
If the upper four bits have a value between 0 and 11, then the header is
exactly one byte in size and the payload size is determined by those
upper four bits.  If the upper four bits have a value between 12 and 15,
that means that the total header size is 2, 3, 5, or 9 bytes and the
payload size is unsigned big-endian integer that is contained in the
subsequent bytes.  The size integer is the one byte that following the
initial header byte if the upper four bits
are 12, two bytes if the upper bits are 13, four bytes if the upper bits
are 14, and eight bytes if the upper bits are 15.  The current design
of SQLite does not support BLOB values larger than 2GiB, so the eight-byte
variant of the payload size integer will never be used by the current code.
The eight-byte payload size integer is included in the specification
to allow for future expansion.

The header for an element does *not* need to be in its simplest
form.  For example, consider the JSON numeric value "`1`".
That element can be encode in five different ways:

  *  `0x13 0x31`
  *  `0xc3 0x01 0x31`
  *  `0xd3 0x00 0x01 0x31`
  *  `0xe3 0x00 0x00 0x00 0x01 0x31`
  *  `0xf3 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x01 0x31`

The shortest encoding is preferred, of course, and usually happens with
primitive elements such as numbers.  However the total size of an array
or object might not be known exactly when the header of the element is
first generated.  It is convenient to reserve space for the largest
possible header and then go back and fill in the correct payload size
at the end.  This technique can result in array or object headers that
are larger than absolutely necessary.

### 3.2 Element Type

The least-significant four bits of the first byte of the header (the first
byte masked against 0x0f) determine element type.  The following codes are
used:

<ol>
<li type="0"><p><b>NULL</b> &rarr;
The element is a JSON "null".  The payload size for a true JSON NULL must
must be zero.  Future versions of SQLite might extend the JSONB format
with elements that have a zero element type but a non-zero size.  In that
way, legacy versions of SQLite will interpret the element as a NULL 
for backwards compatibility while newer versions will interpret the
element in some other way.

<li value="1"><p><b>TRUE</b> &rarr;
The element is a JSON "true".  The payload size must be zero for a actual
"true" value.  Elements with type 1 and a non-zero payload size are
reserved for future expansion.  Legacy implementations that see an element
type of 1 with a non-zero payload size should continue to interpret that
element as "true" for compatibility.

<li value="2"><p><b>FALSE</b> &rarr;
The element is a JSON "false".  The payload size must be zero for a actual
"false" value.  Elements with type 2 and a non-zero payload size are
reserved for future expansion.  Legacy implementations that see an element
type of 2 with a non-zero payload size should continue to interpret that
element as "false" for compatibility.

<li value="3"><p><b>INT</b> &rarr;
The element is a JSON integer value in the canonical
RFC 8259 format, without extensions.  The payload is the ASCII
text representation of that numeric value.

<li value="4"><p><b>INT5</b> &rarr;
The element is a JSON integer value that is not in the
canonical format.   The payload is the ASCII
text representation of that numeric value.  Because the payload is in a
non-standard format, it will need to be translated when the JSONB is
converted into RFC 8259 text JSON.

<li value="5"><p><b>FLOAT</b> &rarr;
The element is a JSON floating-point value in the canonical
RFC 8259 format, without extensions.  The payload is the ASCII
text representation of that numeric value.

<li value="6"><p><b>FLOAT5</b> &rarr;
The element is a JSON floating-point value that is not in the
canonical format.   The payload is the ASCII
text representation of that numeric value.  Because the payload is in a
non-standard format, it will need to be translated when the JSONB is
converted into RFC 8259 text JSON.

<li value="7"><p><b>TEXT</b> &rarr;
The element is a JSON string value that does not contain
any escapes nor any characters that need to be escaped for either SQL or
JSON.  The payload is the UTF8 text representation of the string value.
The payload does <i>not</i> include string delimiters.

<li value="8"><p><b>TEXTJ</b> &rarr;
The element is a JSON string value that contains
RFC 8259 character escapes (such as "<tt>\n</tt>" or "<tt>\u0020</tt>").
Those escapes will need to be translated into actual UTF8 if this element
is [json_extract|extracted] into SQL.
The payload is the UTF8 text representation of the escaped string value.
The payload does <i>not</i> include string delimiters.

<li value="9"><p><b>TEXT5</b> &rarr;
The element is a JSON string value that contains
character escapes, including some character escapes that part of JSON5
and which are not found in the canonical RFC 8259 spec.
Those escapes will need to be translated into standard JSON prior to
rendering the JSON as text, or into their actual UTF8 characters if this
element is [json_extract|extracted] into SQL.
The payload is the UTF8 text representation of the escaped string value.
The payload does <i>not</i> include string delimiters.

<li value="10"><p><b>TEXTRAW</b> &rarr;
The element is a JSON string value that contains
UTF8 characters that need to be escaped if this string is rendered into
standard JSON text.
The payload does <i>not</i> include string delimiters.

<li value="11"><p><b>ARRAY</b> &rarr;
The element is a JSON array.  The payload contains
JSONB elements that comprise values contained within the array.

<li value="12"><p><b>OBJECT</b> &rarr;
The element is a JSON object.  The payload contains
pairs of JSONB elements that comprise entries for the JSON object.
The first element in each pair must be a string (types 7 through 10).
The second element of each pair may be any types, including nested
arrays or objects.

<li value="13"><p><b>RESERVED-13</b> &rarr;
Reserved for future expansion.  Legacy implements that encounter this
element type should raise an error.

<li value="14"><p><b>RESERVED-14</b> &rarr;
Reserved for future expansion.  Legacy implements that encounter this
element type should raise an error.

<li value="15"><p><b>RESERVED-15</b> &rarr;
Reserved for future expansion.  Legacy implements that encounter this
element type should raise an error.
</ol>

Element types outside the range of 0 to 12 are reserved for future
expansion.  The current implement raises an error if see an element type
other than those listed above.  However, future versions of SQLite might
use of the three remaining element types to implement indexing or similar
optimizations, to speed up lookup against large JSON arrays and/or objects.

### 3.3 Design Rationale For Element Types

A key goal of JSONB is that it should be quick to translate
to and from text JSON and/or be constructed from SQL values.
When converting from text into JSONB, we do not want the
converter subroutine to burn CPU cycles converting elements
values into some standard format which might never be used.
Format conversion is "lazy" - it is deferred until actually
needed.  This has implications for the JSONB format design:

  1.   Numeric values are stored as text, not a numbers.  The values are
       a direct copy of the text JSON values from which they are derived.

  2.   There are multiple element types depending on the details of value
       formats.  For example, INT is used for pure RFC-8259 integer
       literals and INT5 exists for JSON5 extensions such as hexadecimal
       notation.  FLOAT is used for pure RFC-8259 floating point literals
       and FLOAT5 is used for JSON5 extensions.  There are four different
       representations of strings, depending on where the string came from
       and how special characters within the string are escaped.

A second goal of JSONB is that it should be capable of serving as the
"parse tree" for JSON when a JSON value is being processed by the
various [JSON SQL functions] built into SQLite.  Before JSONB was
developed, operations such [json_replace()] and [json_patch()]
and similar worked in three stages:


  1.  Translate the text JSON into a internal format that is
      easier to scan and edit.
  2.  Perform the requested operation on the JSON.
  3.  Translate the internal format back into text.

JSONB seeks to serve as the internal format directly - bypassing
the first and third stages of that process.  Since most of the CPU
cycles are spent on the first and third stages, that suggests that
JSONB processing will be much faster than text JSON processing.

So when processing JSONB, only the second stage of the three-stage
process is required.  But when processing text JSON, it is still necessary
to do stages one and three.  If JSONB is to be used as the internal
binary representation, this is yet another reason to store numeric
values as text.  Storing numbers as text minimizes the amount of
conversion work needed for stages one and three.  This is also why
there are four different representations of text in JSONB.  Different
text representations are used for text coming from different sources
(RFC-8259 JSON, JSON5, or SQL string values) and conversions only
happen if and when they are actually needed.

### 3.4 Valid JSONB BLOBs

A valid JSONB BLOB consists of a single JSON element.  The element must
exactly fill the BLOB.  This one element is often a JSON object or array
and those usually contain additional elements as its payload, but the
element can be a primite value such a string, number, boolean, or null.

When the built-in JSON functions are attempting to determine if a BLOB
argument is a JSONB or just a random BLOB, they look at the header of
the outer element to see that it is well-formed and that the element
completely fills the BLOB.  If these conditions are met, then the BLOB
is accepted as a JSONB value.
