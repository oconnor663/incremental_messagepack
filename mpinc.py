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


def get_type(b):
    '''Return a type tuple and a size, or (None, None) to indicate that more
    bytes are required to determine the type/size.'''
    # Short circuit for empty bytes.
    if len(b) == 0:
        return None, None
    # Check to see if we have a short tag (just 1 byte).
    for t in short_types:
        if (b[0] & t.tag_mask) == t.tag:
            size_mask = 0b11111111 ^ t.tag_mask
            size = b[0] & size_mask
            return t, size
    # Check to see if we have a long tag, and if so whether we have the size.
    for t in long_types:
        if b[0] == t.tag:
            if len(b) >= t.size_bytes+1:
                size = int.from_bytes(b[1:t.size_bytes+1], byteorder='big')
                return t, size
            else:
                # We have the tag, but we need more bytes for the size.
                return None, None
    raise ValueError('No type recognized for ' + repr(b))


class Decoder:
    def __init__(self):
        self.buffer = bytearray()
        self.objects = []

    def write(self, b):
        self.buffer.extend(b)
