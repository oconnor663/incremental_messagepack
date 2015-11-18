#! /usr/bin/env python3


# This implementation is in two major sections. First, we define a convention
# for declaring MessagePack formats, and we declare all the formats from the
# spec. Second, we implement a decoder based on those declarations.


import collections
import struct


# We break down each MessagePack format into four different parts:
#
# 1) A tag byte. For the shorter formats (like fixmap), we have to mask the
# first byte to get the tag. For simplicy we mask everything, and just use a
# no-op mask for the longer types.
#
# 2) A number N, the first thing encoded after the tag. This is either the
# remaining bits after the tag mask, or a series of whole bytes after the tag.
# For constants like nil/true/false, we let N be 0 and ignore it.
#
# 3) A length L. For many types (like array16), L is equal to N. For some (like
# map16), L is a function of N. For the rest (like int16), L is a constant.
#
# 4) A payload of length L. This is either bytes (as in fixstr) or a list of
# objects (as in fixarray).
#
# Each format describes these parts with a bunch of constants, so that the
# decoder always works the same way. The only detail the decoder has to be
# careful with is whether the payload at the end is bytes or objects.

class Format:
    def __init__(self, tag, name, build_fn, tag_bits=8, N_size=0,
                 holds_objects=False, L_const=None, L_fn=lambda N: N):
        self.tag = tag
        self.name = name
        self.build_fn = build_fn
        # If tag_bits is 3, then tag_mask is 0b11100000. If tag_bits is 8, then
        # the tag takes up the whole byte.
        self.tag_mask = (255 << (8-tag_bits)) % 256
        # The inverse of tag_mask. So if tag_bits is 3, then N_mask is
        # 0b00011111. If N_mask is zero, we defer to the N_size bytes.
        self.N_mask = 255 ^ self.tag_mask
        self.N_size = N_size
        self.holds_objects = holds_objects
        self.L_const = L_const
        self.L_fn = L_fn

    def get_N_and_L(self, tag_byte, N_buf):
        assert len(N_buf) == self.N_size
        if len(N_buf) > 0:
            N = int.from_bytes(N_buf, byteorder='big')
        else:
            # This is 0 for constant types like nil, which is fine.
            N = tag_byte & self.N_mask
        if self.L_const is not None:
            L = self.L_const
        else:
            L = self.L_fn(N)
        return N, L

    def build(self, N, payload):
        # The build_fn takes a single argument. If L_const is 0, that argument
        # is N. Otherwise that argument is the payload (which the decoder must
        # assemble as either bytes or objects, depending on holds_objects.)
        return self.build_fn(N if self.L_const == 0 else payload)


formats = []

def make_format(*args, **kwargs):
    format = Format(*args, **kwargs)
    formats.append(format)

def get_format(tag_byte):
    for format in formats:
        if tag_byte & format.tag_mask == format.tag:
            return format
    assert False, 'unknown tag: ' + hex(tag_byte)


# nil family
# ==========

make_format(0xc0, "nil", lambda _: None)


# bool family
# ===========

make_format(0xc2, "false", lambda _: False)
make_format(0xc3, "false", lambda _: True)


# int family
# ==========

def build_uint(buf):
    return int.from_bytes(buf, byteorder='big', signed=False)

def build_int(buf):
    return int.from_bytes(buf, byteorder='big', signed=True)

make_format(0x00, "positive fixint", lambda N: N, tag_bits=1, L_const=0)
make_format(0xe0, "negative fixint", lambda N: -N, tag_bits=3, L_const=0)
make_format(0xcc, "uint8", build_uint, L_const=1)
make_format(0xcd, "uint16", build_uint, L_const=2)
make_format(0xce, "uint32", build_uint, L_const=4)
make_format(0xcf, "uint64", build_uint, L_const=8)
make_format(0xcc, "int8", build_int, L_const=1)
make_format(0xcd, "int16", build_int, L_const=2)
make_format(0xce, "int32", build_int, L_const=4)
make_format(0xcf, "int64", build_int, L_const=8)


# float family
# ============

def build_float(buf):
    # Big-endian.
    return struct.unpack('>f', buf)[0]

def build_double(buf):
    return struct.unpack('>d', buf)[0]

make_format(0xca, "float32", build_float, L_const=4)
make_format(0xcb, "float64", build_double, L_const=8)


# str family
# ==========

def build_str(buf):
    return buf.decode('utf8')

make_format(0xa0, "fixstr", build_str, tag_bits=3)
make_format(0xd9, "str8", build_str, N_size=1)
make_format(0xda, "str16", build_str, N_size=2)
make_format(0xdb, "str32", build_str, N_size=4)


# bin family
# ==========

def build_bin(buf):
    return bytes(buf)

make_format(0xc4, "bin8", build_bin, N_size=1)
make_format(0xc5, "bin16", build_bin, N_size=2)
make_format(0xc6, "bin32", build_bin, N_size=4)


# array family
# ============

def build_array(items):
    return list(items)

make_format(0x90, "fixarray", build_array, tag_bits=4, holds_objects=True)
make_format(0xdc, "array16", build_array, N_size=2, holds_objects=True)
make_format(0xdd, "array32", build_array, N_size=4, holds_objects=True)


# map family
# ==========

def map_len(N):
    return 2*N

def build_map(items):
    assert len(items) % 2 == 0
    return {items[2*i]: items[2*i+1] for i in range(len(items)//2)}

make_format(0x80, "fixmap", build_map, tag_bits=4, L_fn=map_len,
            holds_objects=True)
make_format(0xde, "map16", build_map, N_size=2, L_fn=map_len,
            holds_objects=True)
make_format(0xdf, "map32", build_map, N_size=4, L_fn=map_len,
            holds_objects=True)


# ext family
# ==========

Ext = collections.namedtuple('Ext', ['type', 'data'])

def ext_len(N):
    return N+1

def build_ext(buf):
    return Ext(buf[0], bytes(buf[1:]))

make_format(0xd4, "fixext1", build_ext, L_const=2)
make_format(0xd5, "fixext2", build_ext, L_const=3)
make_format(0xd6, "fixext4", build_ext, L_const=5)
make_format(0xd7, "fixext8", build_ext, L_const=9)
make_format(0xd8, "fixext16", build_ext, L_const=17)
make_format(0xc7, "ext8", build_ext, N_size=1, L_fn=ext_len)
make_format(0xc8, "ext16", build_ext, N_size=2, L_fn=ext_len)
make_format(0xc9, "ext32", build_ext, N_size=4, L_fn=ext_len)


# ---------- Decoding ----------

class SpilloverWriter:
    '''A writer that takes bytes as input. It starts with a collection of
    (writer, capacity) pairs. Calls to write() are forwarded to the writers in
    the collection. When a writer reaches capacity, the current write spills
    over to the next writer. The return value is the total number of bytes
    written.

    It's expected that the collection of writers will actually be a generator.
    In that case, when control reenters the generator using next(), the
    previously yielded writer is guaranteed to be full. Also any value on
    StopIteration (that is, any `return` at the end of the generator) gets set
    as the `value` of the SpilloverWriter itself.'''
    def __init__(self, writer_capacity_pairs):
        self.full = False
        self.value = None
        self._iterator = iter(writer_capacity_pairs)
        self._writer = None
        self._capacity = None

    def write(self, buf):
        bytes_written = 0
        while True:
            # If there's no current writer, grab one. If we've run out of
            # writers, we're done.
            if self._writer is None:
                try:
                    self._writer, self._capacity = next(self._iterator)
                except StopIteration as stop:
                    self.full = True
                    self.value = stop.value
                    break
            # Take as many bytes as we can, up to the current writer's
            # remaining capacity. We might get less than that.
            available = buf[bytes_written:bytes_written+self._capacity]
            # Write what we got.
            self._writer.extend(available)
            bytes_written += len(available)
            self._capacity -= len(available)
            # If we filled the current writer drop it. If not, then we're out
            # of bytes, and we're done. Note that if we *both* filled the
            # current writer *and* ran out of bytes, then we keep going. That
            # way we find out if there are no more writers left, or if the next
            # writer has capacity 0.
            if self._capacity == 0:
                self._writer = None
            else:
                break
        return bytes_written

def decoder_coroutine():
    '''A generator yielding a bunch of (buffer, capacity) pairs, for use with
    SpilloverWriter. This relies on the guarantee that after returning from
    yield, the yielded buffer will be full. At the end, return the decoded
    object (Python sticks returns on the StopIteration).'''
    # Get the type.
    type_buf = bytearray()
    yield type_buf, 1
    tag_byte = type_buf[0]
    format = get_format(tag_byte)
    # Get the N and L values.
    N_buf = bytearray()
    yield N_buf, format.N_size
    N, L = format.get_N_and_L(tag_byte, N_buf)
    # Now if we have a container of objects, defer to child decoders to read
    # its payload. Otherwise, read its payload as more bytes.
    if format.holds_objects:
        payload = []
        for i in range(L):
            obj = yield from decoder_coroutine()
            payload.append(obj)
    else:
        payload = bytearray()
        yield payload, L
    # Build and return the final object.
    return format.build(N, payload)

def Decoder():
    'Here it is!'
    writer_capacity_pairs = decoder_coroutine()
    return SpilloverWriter(writer_capacity_pairs)

# ---------- Tests ----------

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
    (b'\xc5\x00\x01A', b'A'),
    (b'\xc6\x00\x00\x00\x01A', b'A'),
    (b'\xc7\x00\x05', Ext(5, b'')),
    (b'\xca?\x80\x00\x00', 1.0),
    (b'\xcb@\x16\x00\x00\x00\x00\x00\x00', 5.5),
]

def main():
    for b, val in tests:
        d = Decoder()
        used = d.write(b)
        assert d.full
        assert val == d.value, '{} != {}'.format(repr(val), repr(d.value))
        assert used == len(b), 'only used {} bytes out of {}'.format(len(b), b)
        # Do it again one byte at a time.
        d = Decoder()
        for i in range(len(b)):
            d.write(b[i:i+1])
        assert d.full
        assert val == d.value, '{} != {}'.format(repr(val), repr(d.value))

if __name__ == '__main__':
    main()
