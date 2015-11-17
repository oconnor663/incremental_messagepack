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

def get_len(mptype, N):
    # The length is either fixed, or a function of N.
    if mptype.len is not None:
        return mptype.len
    else:
        return mptype.len_fn(N)

def build(mptype, N, buf_or_items):
    # The build_fn takes one of three arguments:
    #   1) N, if len is fixed at 0.
    #   2) a list of objects
    #   3) a bytearray
    if mptype.len == 0:
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
make_type(0xc7, "ext8", build_ext, N_size=1, len_fn=ext_len)
make_type(0xc8, "ext16", build_ext, N_size=2, len_fn=ext_len)
make_type(0xc9, "ext32", build_ext, N_size=4, len_fn=ext_len)


# --------- Decoder ---------------

class OffsetReader:
    def __init__(self, buffer):
        self.buffer = buffer
        self.offset = 0

    def read(self, n):
        output = self.buffer[self.offset:self.offset+n]
        self.offset += n
        return output

    def empty(self):
        return self.offset >= len(self.buffer)

class FixedBufferWriter:
    def __init__(self, capacity):
        self.buffer = bytearray()
        self.capacity = capacity

    def write(self, oreader):
        needed = self.capacity - len(self.buffer)
        self.buffer.extend(oreader.read(needed))

    def full(self):
        return len(self.buffer) >= self.capacity

    def __len__(self):
        return len(self.buffer)

class SpilloverWriter:
    def __init__(self, writers):
        self._writers = iter(writers)
        self._full = False
        self._current_writer = None

    def write(self, buf):
        oreader = OffsetReader(buf)
        while not self._full:
            try:
                if self._current_writer is None:
                    self._current_writer = next(self._writers)
                self._current_writer.write(oreader)
                if self._current_writer.full():
                    self._current_writer = None
                    continue
                if oreader.empty():
                    break
            except StopIteration:
                self._full = True
        return oreader.offset

    def full(self):
        return self._full

def decode_generator(value_holder):
    type_buf = FixedBufferWriter(1)
    yield type_buf
    tag_byte = type_buf.buffer[0]
    mptype = get_type(tag_byte)
    N_buf = FixedBufferWriter(mptype.N_size)
    yield N_buf
    N = get_N(mptype, tag_byte, N_buf.buffer)
    L = get_len(mptype, N)
    if not mptype.is_container:
        value_buf = FixedBufferWriter(L)
        yield value_buf
        value = build(mptype, N, value_buf.buffer)
        value_holder.append(value)
    else:
        items = []
        for i in range(L):
            item_holder = []
            yield from decode_generator(item_holder)
            items.append(item_holder[0])
        value = build(mptype, N, items)
        value_holder.append(value)

class MessagePackDecoder:
    def __init__(self):
        self.value = None
        self._value_holder = []
        self._spillover = SpilloverWriter(decode_generator(self._value_holder))

    def write(self, buf):
        used = self._spillover.write(buf)
        if self._spillover.full():
            self.value = self._value_holder[0]
        return used

    def full(self):
        return self._spillover.full()

tests = [
    (b'\x00', 0),
    (b'\x01', 1),
    (b'\xcf\x81h2O\x91\xac\xca\x99', 9324758345798437529),
    (b'\xa0', ""),
    (b'\xa3foo', "foo"),
    (b'\x90', []),
    (b'\x93\x01\xa3two\x03', [1, "two", 3]),
    (b'\xdc\x00d' + b'\x00'*100, [0]*100),
    (b'\x80', {}),
    (b'\x82\xa1a\x01\xa1b\x91\x02', {'a': 1, 'b': [2]}),
    (b'\xc0', None),
    (b'\xc3', True),
    (b'\xc2', False),
    (b'\xc4\x00', b''),
    (b'\xc4\x011', b'1'),
    (b'\xc5\x01\x00' + b'1' * 2**8, b'1' * 2**8),
    (b'\xc6\x00\x01\x00\x00' + b'1' * 2**16, b'1' * 2**16),
    (b'\xc7\x00\x05', Ext(5, b'')),
]

def main():
    for b, val in tests:
        d = MessagePackDecoder()
        used = d.write(b)
        assert d.full()
        assert val == d.value, '{} != {}'.format(repr(val), repr(d.value))
        assert used == len(b), 'only used {} bytes out of {}'.format(len(b), b)
        # Do it again one byte at a time.
        d = MessagePackDecoder()
        for i in range(len(b)):
            d.write(b[i:i+1])
        assert d.full()
        assert val == d.value, '{} != {}'.format(repr(val), repr(d.value))

if __name__ == '__main__':
    main()
