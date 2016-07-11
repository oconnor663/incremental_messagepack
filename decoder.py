#! /usr/bin/env python3


# This implementation is in two major sections. First, we define a convention
# for declaring MessagePack formats, and we declare all the formats from the
# spec. Second, we implement a decoder based on those declarations.


import collections
import io
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
        return self.build_fn(N, payload)


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

make_format(0xc0, "nil", lambda N, buf: None)


# bool family
# ===========

make_format(0xc2, "false", lambda N, buf: False)
make_format(0xc3, "true", lambda N, buf: True)


# int family
# ==========

def build_posfixint(N, buf):
    return N

def build_negfixint(N, buf):
    return -N

def build_uint(N, buf):
    return int.from_bytes(buf, byteorder='big', signed=False)

def build_int(N, buf):
    return int.from_bytes(buf, byteorder='big', signed=True)

make_format(0x00, "positive fixint", build_posfixint, tag_bits=1, L_const=0)
make_format(0xe0, "negative fixint", build_negfixint, tag_bits=3, L_const=0)
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

def build_float(N, buf):
    # Big-endian.
    return struct.unpack('>f', buf)[0]

def build_double(N, buf):
    return struct.unpack('>d', buf)[0]

make_format(0xca, "float32", build_float, L_const=4)
make_format(0xcb, "float64", build_double, L_const=8)


# str family
# ==========

def build_str(N, buf):
    return buf.decode('utf8')

make_format(0xa0, "fixstr", build_str, tag_bits=3)
make_format(0xd9, "str8", build_str, N_size=1)
make_format(0xda, "str16", build_str, N_size=2)
make_format(0xdb, "str32", build_str, N_size=4)


# bin family
# ==========

def build_bin(N, buf):
    return bytes(buf)

make_format(0xc4, "bin8", build_bin, N_size=1)
make_format(0xc5, "bin16", build_bin, N_size=2)
make_format(0xc6, "bin32", build_bin, N_size=4)


# array family
# ============

def build_array(N, items):
    return list(items)

make_format(0x90, "fixarray", build_array, tag_bits=4, holds_objects=True)
make_format(0xdc, "array16", build_array, N_size=2, holds_objects=True)
make_format(0xdd, "array32", build_array, N_size=4, holds_objects=True)


# map family
# ==========

def map_len(N):
    return 2*N

def build_map(N, items):
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

def build_ext(N, buf):
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

class MessagePackDecoder:
    def __init__(self):
        self._result = None
        self._full = False
        self._tag_byte = None
        self._format = None
        self._N_buf = bytearray()
        self._N = None
        self._L = None
        self._payload_buf = bytearray()
        self._payload_list = []
        self._payload_decoder = None

    def full(self):
        return self._full

    def result(self):
        assert self.full()
        return self._result

    def write(self, buf):
        bytesio = io.BytesIO(buf)
        self._write_bytesio(bytesio)
        return bytesio.tell()

    def _write_bytesio(self, bytesio):
        if self._full:
            return

        if self._format is None:
            read = bytesio.read(1)
            if len(read) == 0:
                return
            self._tag_byte = read[0]
            self._format = get_format(self._tag_byte)

        if self._N is None:
            needed = self._format.N_size - len(self._N_buf)
            read = bytesio.read(needed)
            self._N_buf.extend(read)
            if len(read) < needed:
                return
            self._N, self._L = self._format.get_N_and_L(
                self._tag_byte, self._N_buf)

        if self._format.holds_objects:
            while True:
                # See if we're full. Do this first, in case L is actually 0.
                if len(self._payload_list) == self._L:
                    self._full = True
                    self._result = self._format.build(
                        self._N, self._payload_list)
                    return
                # Create a sub-decoder if there isn't one already going.
                if self._payload_decoder is None:
                    self._payload_decoder = MessagePackDecoder()
                # Let the sub-decoder suck up as many bytes as it wants/can.
                self._payload_decoder._write_bytesio(bytesio)
                # If the sub-decoder is full, take its result and loop again.
                # Otherwise we must be out of bytes, so just exit.
                if self._payload_decoder.full():
                    self._payload_list.append(self._payload_decoder.result())
                    self._payload_decoder = None
                else:
                    return
        else:
            needed = self._L - len(self._payload_buf)
            read = bytesio.read(needed)
            self._payload_buf.extend(read)
            if len(read) < needed:
                return
            self._full = True
            self._result = self._format.build(
                self._N, self._payload_buf)


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

def run_tests_with_decoder_constructor(constructor):
    for b, val in tests:
        d = constructor()
        used = d.write(b)
        assert d.full()
        assert val == d.result(), '{} != {}'.format(
            repr(val), repr(d.result()))
        assert used == len(b), 'only used {} bytes out of {}'.format(len(b), b)
        # Do it again one byte at a time.
        d = constructor()
        for i in range(len(b)):
            d.write(b[i:i+1])
        assert d.full()
        assert val == d.result(), '{} != {}'.format(
            repr(val), repr(d.result()))


def main():
    run_tests_with_decoder_constructor(MessagePackDecoder)


if __name__ == '__main__':
    main()
