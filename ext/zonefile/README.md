
Notes:

  *  Using 32-bit frame numbers (not 16-bit).

  *  The ZonefileHeader object is 26 bytes in size. Which means that the
     ZoneFileIndex will not be 8-byte aligned. Problem?

  *  The offsets in the ZoneFileIndex.byteOffsetZoneFrame[] array are
     relative to the offset in ZoneFileHeader.byteOffsetFrames. This is
     necessary as we may not know the offset of the start of the frame data
     until after the ZoneFileIndex structure is compressed.


