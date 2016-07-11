#! /usr/bin/env python3

# The main decoder.py implementation doesn't use generators, to make it easy to
# translate into languages that don't have them. However, generators are a
# wonderful way to represent interruptible functions that can pick up where
# they left off. This is an alternative implementation of the same incremental
# decoder using generators.

import io

# Reuse definitions and tests from the first decoder.
import decoder

class DelegatableBuffer:
    def __init__(self, size):
        self.buffer = bytearray()
        self.size = size

    def fill(self, bytesio=io.BytesIO()):
        '''The contract of the fill() generator is this:
        1) The caller calls fill() with an optional initial BytesIO reader.
        2) The caller begins the generator with next().
        3) Until StopIteration is raised, the caller uses send() to pass in
           further BytesIO readers.
        4) When the caller catches StopIteration, the buffer member is full.
           The attached value on the StopIteration will be the last BytesIO
           reader passed in.

        This contract is designed so that another generator with the same
        contract may delegate to this one with `yield from`.
        '''
        while True:
            needed = self.size - len(self.buffer)
            bytes_received = bytesio.read(needed)
            self.buffer.extend(bytes_received)
            if len(bytes_received) < needed:
                bytesio = yield
            else:
                return bytesio

class MessagePackDecoder:
    def __init__(self):
        self._full = False
        self._result = None
        self._generator = self.fill()
        next(self._generator)

    def full(self):
        return self._full

    def result(self):
        return self._result

    def write(self, buf):
        bytesio = io.BytesIO(buf)
        try:
            self._generator.send(bytesio)
        except StopIteration:
            pass
        return bytesio.tell()

    def fill(self, bytesio=io.BytesIO()):
        tag_buf = DelegatableBuffer(1)
        bytesio = yield from tag_buf.fill(bytesio)
        tag_byte = tag_buf.buffer[0]
        format = decoder.get_format(tag_byte)
        N_buf = DelegatableBuffer(format.N_size)
        bytesio = yield from N_buf.fill(bytesio)
        N, L = format.get_N_and_L(tag_byte, N_buf.buffer)
        if format.holds_objects:
            payload_list = []
            while len(payload_list) < L:
                subdecoder = MessagePackDecoder()
                bytesio = yield from subdecoder.fill(bytesio)
                payload_list.append(subdecoder.result())
            self._result = format.build(N, payload_list)
        else:
            payload_buf = DelegatableBuffer(L)
            bytesio = yield from payload_buf.fill(bytesio)
            self._result = format.build(N, payload_buf.buffer)
        self._full = True
        return bytesio

def main():
    decoder.run_tests_with_decoder_constructor(MessagePackDecoder)

if __name__ == '__main__':
    main()
