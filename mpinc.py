#! /usr/bin/env python3


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


def build_const(const):
    return lambda N, payload: const


def build_return_N(N, payload):
    return N


def build_return_payload(N, payload):
    return payload


def build_int(N, payload):
    return int.from_bytes(payload, signed=True, byteorder='big')


def build_uint(N, payload):
    return int.from_bytes(payload, signed=False, byteorder='big')


def build_str(N, payload):
    return payload.decode('utf8')


def build_map(N, payload):
    return {payload[2*i]: payload[2*i+1] for i in range(len(payload)//2)}


MessagePackTypes = [
    make_type(0x00, "positive fixint", build_return_N, tag_bits=1,
              size_fn=size_const(0)),
    make_type(0x80, "fixmap", build_map, tag_bits=4, is_container=True,
              size_fn=size_map),
    make_type(0x90, "fixarray", build_return_payload, tag_bits=4,
              is_container=True),
    make_type(0xa0, "fixstr", build_str, tag_bits=3),
    make_type(0xc0, "nil", build_const(None), size_fn=size_const(0)),
    make_type(0xc2, "false", build_const(False), size_fn=size_const(0)),
    make_type(0xc3, "true", build_const(True), size_fn=size_const(0)),
    make_type(0xc4, "bin8", build_return_payload, size_len=1),
    make_type(0xc5, "bin16", build_return_payload, size_len=2),
    make_type(0xc6, "bin32", build_return_payload, size_len=4),
    make_type(0xdc, "array16", build_return_payload, size_len=2,
              is_container=True),
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


tests = [
    0,
    1,
    "",
    "foo",
    [],
    [1, "two", 3],
    [0] * 100,
    {},
    {'a': 1, 'b': [2]},
    None,
    True,
    False,
    b"",
    b"1",
    b"1" * (2**8),
    b"1" * (2**16),
]


def main():
    from umsgpack import packb
    for val in tests:
        print('Testing ' + repr(val)[:20] + ' ...')
        b = packb(val)
        d = Decoder()
        used = d.write(b)
        assert d.has_value
        assert val == d.value, '{} != {}'.format(repr(val), repr(d.value))
        assert used == len(b), 'only used {} bytes out of {}'.format(len(b), b)


if __name__ == '__main__':
    main()
