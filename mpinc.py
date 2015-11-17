#! /usr/bin/env python3

from collections import namedtuple
from struct import pack


# tag: The expected first byte when masked with tag_mask, e.g. 0xc0 for nil.
# name: e.g. "fixint"
# tag_mask: The bits of the first byte to use for the tag, e.g. 0b11100000.
# N_mask: The bits of the first byte to read for N, if any, e.g. 0b00011111.
# N_size: The number of bytes to read for N, when not using an N_mask.
# is_container: True if the type contains objects, like array16.
# len: The number of payload bytes to read, if it is fixed.
# len_fn: The number of payload bytes to read, if it is a function of N.
# build_fn: The final value constructor. Its argument will be N if len=0, else
#           a list of objects if is_container=True, else an array of bytes.
MessagePackType = namedtuple('MessagePackType', [
    'tag', 'name', 'tag_mask', 'N_mask', 'N_size', 'is_container', 'len',
    'len_fn', 'build_fn'])

types = []

def make_type(tag, name, build_fn, tag_bits=8, N_size=0, is_container=False,
              len=None, len_fn=lambda N: N):
    assert len is not None or len_fn is not None
    # The first tag_bits bits. So if tag_bits=3, then 0b11100000. If
    # tag_bits=8, then the tag is the whole byte.
    tag_mask = (255 << (8-tag_bits)) % 256
    # The inverse of tag_mask. So if tag_bits=3, then 0b00011111. If
    # tag_bits=8, then N is derived from the N_buf instead of the tag byte.
    N_mask = 255 ^ tag_mask
    t = MessagePackType(tag, name, tag_mask, N_mask, N_size, is_container, len,
                        len_fn, build_fn)
    types.append(t)

def get_type(tag_byte):
    for mptype in types:
        if tag_byte & mptype.tag_mask == mptype.tag:
            return mptype
    raise TypeError("Unknown tag byte: " + repr(tag_byte))

def get_N(mptype, tag_byte, N_buf):
    assert len(N_buf) == mptype.N_size
    # N is either derived from the bytes in N_buf, or from the N_mask.
    if len(N_buf) > 0:
        return int.from_bytes(N_buf, byteorder='big')
    else:
        return tag_byte & mptype.N_mask

def get_len(mptype, tag_byte, N_buf):
    # The length is either fixed, or a function of N.
    if mptype.len is not None:
        return mptype.len
    else:
        N = get_N(mptype, tag_byte, N_buf)
        return mptype.len_fn(N)

def build(mptype, tag_byte, N_buf, buf_or_items):
    # The build_fn takes one of three arguments:
    #   1) N, if len is fixed at 0.
    #   2) a list of objects
    #   3) a bytearray
    if mptype.len == 0:
        N = get_N(mptype, tag_byte, N_buf)
        return mptype.build_fn(N)
    expected_type = list if mptype.is_container else bytearray
    assert isinstance(buf_or_items, expected_type)
    return mptype.build_fn(buf_or_items)


# nil family
# ==========

make_type(0xc0, "nil", lambda _: None)


# bool family
# ===========

make_type(0xc2, "false", lambda _: False)
make_type(0xc3, "false", lambda _: True)


# int family
# ==========

def build_uint(buf):
    return int.from_bytes(buf, byteorder='big', signed=False)

def build_int(buf):
    return int.from_bytes(buf, byteorder='big', signed=True)

make_type(0x00, "positive fixint", lambda N: N, tag_bits=1, len=0)
make_type(0xe0, "negative fixint", lambda N: -N, tag_bits=3, len=0)
make_type(0xcc, "uint8", build_uint, len=1)
make_type(0xcd, "uint16", build_uint, len=2)
make_type(0xce, "uint32", build_uint, len=4)
make_type(0xcf, "uint64", build_uint, len=8)
make_type(0xcc, "int8", build_int, len=1)
make_type(0xcd, "int16", build_int, len=2)
make_type(0xce, "int32", build_int, len=4)
make_type(0xcf, "int64", build_int, len=8)


# float family
# ============

make_type(0xca, "float32", lambda buf: pack('f', buf), len=4)
make_type(0xcb, "float64", lambda buf: pack('d', buf), len=8)


# str family
# ==========

def build_str(buf):
    return buf.decode('utf8')

make_type(0xa0, "fixstr", build_str, tag_bits=3)
make_type(0xd9, "str8", build_str, N_size=1)
make_type(0xda, "str16", build_str, N_size=2)
make_type(0xdb, "str32", build_str, N_size=4)


# bin family
# ==========

def build_bin(buf):
    return bytes(buf)

make_type(0xc4, "bin8", build_bin, N_size=1)
make_type(0xc5, "bin16", build_bin, N_size=2)
make_type(0xc6, "bin32", build_bin, N_size=4)


# array family
# ============

def build_array(items):
    return list(items)

make_type(0x90, "fixarray", build_array, tag_bits=4, is_container=True)
make_type(0xdc, "array16", build_array, N_size=2, is_container=True)
make_type(0xdd, "array32", build_array, N_size=4, is_container=True)


# map family
# ==========

def map_len(N):
    return 2*N

def build_map(items):
    assert len(items) % 2 == 0
    return {items[2*i]: items[2*i+1] for i in range(len(items)//2)}

make_type(0x80, "fixmap", build_map, tag_bits=4, len_fn=map_len,
          is_container=True)
make_type(0xde, "map16", build_map, N_size=2, len_fn=map_len,
          is_container=True)
make_type(0xdf, "map32", build_map, N_size=4, len_fn=map_len,
          is_container=True)


# ext family
# ==========

Ext = namedtuple('Ext', ['type', 'data'])

def ext_len(N):
    return N+1

def build_ext(buf):
    return Ext(buf[0], bytes(buf[1:]))

make_type(0xd4, "fixext1", build_ext, len=2)
make_type(0xd5, "fixext2", build_ext, len=3)
make_type(0xd6, "fixext4", build_ext, len=5)
make_type(0xd7, "fixext8", build_ext, len=9)
make_type(0xd8, "fixext16", build_ext, len=17)
make_type(0xc7, "ext8", build_ext, len_fn=ext_len)
make_type(0xc8, "ext16", build_ext, len_fn=ext_len)
make_type(0xc9, "ext32", build_ext, len_fn=ext_len)


# --------- Decoder ---------------


# TODO: Can we avoid using a big class for all this?
def make_type(tag, name, builder_fn, size_len=0, tag_bits=8,
              is_container=False, size_fn=lambda x: x):

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
            self.size_len = size_len
            self.name = name
            self.is_container = is_container

        def get_N(self, size_buffer):
            assert len(size_buffer) == size_len
            if size_len == 0:
                # The size is stored in the tag byte. Mask it out.
                return size_mask & self.tag_byte
            else:
                # TODO: map and ext types will behave differently here.
                return int.from_bytes(size_buffer, byteorder='big')

        def size(self, size_buffer):
            N = self.get_N(size_buffer)
            return size_fn(N)

        def build(self, size_buffer, payload):
            N = self.get_N(size_buffer)
            return builder_fn(N, payload)

    return Type


def size_const(const):
    return lambda N: const


def size_map(N):
    return 2*N


def size_ext(N):
    return N + 1


def build_const(const):
    return lambda N, payload: const


def build_return_N(N, payload):
    return N


def build_bytes(N, payload):
    return bytes(payload)


def build_tuple(N, payload):
    return tuple(payload)


def build_int(N, payload):
    return int.from_bytes(payload, signed=True, byteorder='big')


def build_uint(N, payload):
    return int.from_bytes(payload, signed=False, byteorder='big')


def build_str(N, payload):
    return payload.decode('utf8')


def build_map(N, payload):
    return {payload[2*i]: payload[2*i+1] for i in range(len(payload)//2)}


def build_ext(N, payload):
    return Ext(payload[0], bytes(payload[1:]))


MessagePackTypes = [
    make_type(0x00, "positive fixint", build_return_N, tag_bits=1,
              size_fn=size_const(0)),
    make_type(0x80, "fixmap", build_map, tag_bits=4, is_container=True,
              size_fn=size_map),
    make_type(0x90, "fixarray", build_tuple, tag_bits=4, is_container=True),
    make_type(0xa0, "fixstr", build_str, tag_bits=3),
    make_type(0xc0, "nil", build_const(None), size_fn=size_const(0)),
    make_type(0xc2, "false", build_const(False), size_fn=size_const(0)),
    make_type(0xc3, "true", build_const(True), size_fn=size_const(0)),
    make_type(0xc4, "bin8", build_bytes, size_len=1),
    make_type(0xc5, "bin16", build_bytes, size_len=2),
    make_type(0xc6, "bin32", build_bytes, size_len=4),
    make_type(0xc7, "ext8", build_ext, size_len=1, size_fn=size_ext),
    make_type(0xc8, "ext16", build_ext, size_len=2, size_fn=size_ext),
    make_type(0xc9, "ext32", build_ext, size_len=4, size_fn=size_ext),
    make_type(0xdc, "array16", build_tuple, size_len=2, is_container=True),
]


class Decoder:
    def __init__(self):
        self.has_value = False
        self.value = None
        self._size_buffer = bytearray()
        self._type = None
        self._size = None
        self._payload_buffer = bytearray()
        self._payload_list = []
        self._child_decoder = None

    def write(self, buf, buf_start=0):
        '''Feed bytes into the decoder. Returns the number of bytes used. Once
        an object has been parsed, no more bytes will be used.'''
        if len(buf) == 0 or self.has_value:
            return 0
        used = 0
        used += self._read_type(buf, buf_start)
        used += self._try_read_size(buf, buf_start + used)
        if self._size is None:
            return used
        if self._type.is_container:
            used += self._write_into_container(buf, buf_start + used)
        else:
            used += self._write_into_bytes(buf, buf_start + used)
        return used

    def _read_type(self, buf, buf_start):
        '''Set self._type, and return 1 if we consumed a byte to do that,
        otherwise, 0.'''
        if self._type is not None:
            return 0
        tag_byte = buf[buf_start]
        for T in MessagePackTypes:
            if T.matches_tag(tag_byte):
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

    def _write_into_container(self, buf, buf_start):
        used = 0
        while len(self._payload_list) < self._size:
            if len(buf) <= buf_start + used:
                break
            if self._child_decoder is None:
                self._child_decoder = Decoder()
            used += self._child_decoder.write(buf, buf_start + used)
            if self._child_decoder.has_value:
                self._payload_list.append(self._child_decoder.value)
                self._child_decoder = None
        else:
            self.has_value = True
            self.value = self._type.build(
                self._size_buffer, self._payload_list)
        return used

    def _write_into_bytes(self, buf, buf_start):
        # TODO: Some duplicated code here.
        bytes_needed = self._size - len(self._payload_buffer)
        bytes_to_use = buf[buf_start:buf_start+bytes_needed]
        self._payload_buffer.extend(bytes_to_use)
        if len(self._payload_buffer) == self._size:
            self.has_value = True
            self.value = self._type.build(
                self._size_buffer, self._payload_buffer)
        return len(bytes_to_use)


def main():
    import umsgpack

    tests = [
        0,
        1,
        "",
        "foo",
        (),
        (1, "two", 3),
        (0) * 100,
        {},
        {'a': 1, 'b': (2,)},
        None,
        True,
        False,
        b"",
        b"1",
        b"1" * (2**8),
        b"1" * (2**16),
        umsgpack.Ext(5, b''),
    ]

    for val in tests:
        print('Testing ' + repr(val)[:40] + ' ...')
        b = umsgpack.packb(val)
        d = Decoder()
        used = d.write(b)
        assert d.has_value
        new_val = d.value
        if isinstance(new_val, Ext):
            new_val = umsgpack.Ext(*new_val)
        assert val == new_val, '{} != {}'.format(repr(val), repr(new_val))
        assert used == len(b), 'only used {} bytes out of {}'.format(len(b), b)


if __name__ == '__main__':
    main()
