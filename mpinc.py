#! /usr/bin/env python3

from collections import namedtuple

ShortType = namedtuple('ShortType', ['tag', 'tag_mask', 'name'])

short_types = [ShortType(t) for t in [
    (0b00000000, 0b10000000, "positive fixint"),
    (0b10100000, 0b11100000, "fixstr"),
    (0b10010000, 0b11110000, "fixarray"),
]]

LongType = namedtuple('LongType', ['tag', 'size_bytes', 'name'])

long_types = [LongType(t) for t in [
    (0xdc, 2, "array16"),
]]


def get_type(buf):
    '''Return a type tuple and a size, or (None, None) to indicate that more
    bytes are required to determine the type/size.'''
    # Short circuit for empty bytes.
    if len(buf) == 0:
        return None, None
    # Check to see if we have a short tag (just 1 byte).
    for t in short_types:
        if (buf[0] & t.tag_mask) == t.tag:
            size_mask = 0b11111111 ^ t.tag_mask
            size = buf[0] & size_mask
            return t, size
    # Check to see if we have a long tag, and if so whether we have the size.
    for t in long_types:
        if buf[0] == t.tag:
            if len(buf) >= t.size_bytes+1:
                size = int.from_bytes(buf[1:t.size_bytes+1], byteorder='big')
                return t, size
            else:
                # We have the tag, but we need more bytes for the size.
                return None, None
    raise ValueError('No type recognized for ' + repr(buf))


class Decoder:
    def __init__(self):
        self.has_value = False
        self.value = None
        self._buffer = bytearray()
        self._type = None
        self._size = 0

    def write(self, buf):
        '''Feed bytes into the decoder. Returns the number of bytes accepted,
        which is zero if this decoder already has a value.'''
        # If we don't have a type yet, try to derive one from the bytes we have
        # so far plus the first 18 bytes of buf. (The largest possible
        # MessagePack tag is fixext16, which is 18 bytes long.)
        if self.type is None:
            self._type, self._size = get_type(self._buffer + buf[:18])
        # If we still don't have a type, accept all of buf and return.
        if self.type is None:
            self._buffer.extend(buf)
            return len(buf)
        # Now figure out how many total bytes we're supposed to need...
