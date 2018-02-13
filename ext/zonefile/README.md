
Notes:

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

  *  Currently there is no support for encryption or compression.


