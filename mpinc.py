#! /usr/bin/env python3

from collections import namedtuple

ShortType = namedtuple('ShortType', ['tag', 'tag_mask', 'name'])
ShortType.size_bytes = 0

short_types = [ShortType(*t) for t in [
    (0b00000000, 0b10000000, "positive fixint"),
    (0b10100000, 0b11100000, "fixstr"),
    (0b10010000, 0b11110000, "fixarray"),
]]

LongType = namedtuple('LongType', ['tag', 'size_bytes', 'name'])

long_types = [LongType(*t) for t in [
    (0xdc, 2, "array16"),
]]


def get_type(byte):
    for type in short_types:
        if (byte & type.tag_mask) == type.tag:
            return type
    for type in long_types:
        if byte == type.tag:
            return type
    raise ValueError('No type recognized for {} ({}).'
                     .format(hex(byte), bin(byte)))


class Decoder:
    def __init__(self):
        self.has_value = False
        self.value = None
        self._size_buffer = bytearray()
        self._type = None
        self._size = None

    def write(self, buf):
        '''Feed bytes into the decoder. Returns the number of bytes used. Once
        an object has been parsed, no more bytes will be used.'''
        if len(buf) == 0:
            return 0
        used = 0
        used += self._read_type(buf)
        used += self._try_read_size(buf, used)

    def _read_type(self, buf):
        '''Set self._type, and return 1 if we consumed a byte to do that,
        otherwise, 0.'''
        if self._type is not None:
            return 0
        else:
            self._type = get_type(buf[0])
            return 1

    def _try_read_size(self, buf, buf_start):
        '''Read bytes into self._size_buffer until it's has all the size bytes
        for the current type. We might not have enough to fill it completely.
        (The caller will have to check self._size after this to see if we set
        it.) Return the number of bytes we used.'''
        if self._size is not None:
            return 0
        bytes_needed = self._type.size_bytes - len(self._size_buffer)
        bytes_to_use = buf[buf_start:buf_start+bytes_needed]
        self._size_buffer.extend(bytes_to_use)
        if len(self._size_buffer) == self._type.size_bytes:
            self._size = int.from_bytes(self._size_buffer, byteorder='big')
        return len(bytes_to_use)
