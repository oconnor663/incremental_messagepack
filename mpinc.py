#! /usr/bin/env python3


def make_type(name, tag, builder_fn, size_len=0, tag_bits=8,
              is_container=False):

    tag_mask = (255 << (8-tag_bits)) % 256
    size_mask = 255 ^ tag_mask

    class Type:
        @staticmethod
        def matches_tag(byte):
            if tag_mask is not None:
                byte = tag_mask & byte
            return byte == tag

        def __init__(self, tag_byte):
            self.tag_byte = tag_byte

        def is_container(self):
            return is_container

        def size(self, size_buffer):
            assert len(size_buffer) == size_len
            if size_len == 0:
                # The size is stored in the tag byte. Mask it out.
                return size_mask & self.tag_byte
            else:
                # TODO: map and ext types will behave differently here.
                return int.from_bytes(size_buffer, byteorder='big')

        def build(self, buffer_or_list):
            return builder_fn(self.tag_byte, buffer_or_list)

    return Type


def return_size(size, payload):
    return size


def return_payload(size, payload):
    return payload


def build_int(_, payload):
    return int.from_bytes(payload, signed=True, byteorder='big')


def build_uint(_, payload):
    return int.from_bytes(payload, signed=False, byteorder='big')


def build_str(_, payload):
    return payload.encode()


MessagePackTypes = [
    make_type("positive fixint", 0x00, return_size, tag_bits=1),
    make_type("fixstr",          0xa0, build_str, tag_bits=3),
    make_type("fixarray",        0x90, return_payload, tag_bits=4),
    make_type("array16",         0xdc, return_payload, size_len=2),
]


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
        if self._size is None:
            return None

    def _read_type(self, buf):
        '''Set self._type, and return 1 if we consumed a byte to do that,
        otherwise, 0.'''
        if self._type is not None:
            return 0
        tag_byte = buf[0]
        for T in MessagePackTypes:
            if T.match_tag(tag_byte):
                self._type = T(tag_byte)
                return 1
        raise TypeError('unknown tag byte: ' + repr(tag_byte))

    def _try_read_size(self, buf, buf_start):
        '''Read bytes into self._size_buffer until it's has all the size bytes
        for the current type. We might not have enough to fill it completely.
        (The caller will have to check self._size after this to see if we set
        it.) Return the number of bytes we used.'''
        if self._size is not None:
            return 0
        bytes_needed = self._type.size_len - len(self._size_buffer)
        bytes_to_use = buf[buf_start:buf_start+bytes_needed]
        self._size_buffer.extend(bytes_to_use)
        if len(self._size_buffer) == self._type.size_len:
            self._size = self._type.size(self._size_buffer)
        return len(bytes_to_use)
