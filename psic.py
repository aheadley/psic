#!/usr/bin/env python

"""
Heavily based on the following code:
http://pastebin.com/GZzRaYiP
http://code.google.com/p/pspshrink/
"""

__author__  = 'Alex Headley <aheadley@waysaboutstuff.com>'
__license__ = 'GPLv2'
__version__ = '0.1'

import zlib
import struct
import logging
import os

# constrain the value to 1-9
ZLIB_DEFAULT_LEVEL  = max(1, min(9, int(os.environ.get('ZLIB_DEFAULT_LEVEL', '1'))))
ZLIB_WINDOW_SIZE    = -15

class CisoWorker(object):
    CISO_HEADER_FMT     = ''.join([
        '<',    # ensure little endian
        'I',    # file format magic
        'I',    # header size
        'Q',    # size of uncompressed file
        'I',    # compressed block size
        'B',    # version number
        'B',    # alignment of index value
        'H',    # reserved
    ])
    CISO_HEADER_SIZE    = 0x18
    CISO_MAGIC      = 0x4F534943 # 'CISO'
    CISO_VER        = 0x01
    CISO_BLOCK_SIZE = 0x0800
    CISO_INDEX_FMT  = '<%dI'

    UNCOMPRESSED_BITMASK    = 0x80000000
    INDEX_BITMASK           = 0x7FFFFFFF

assert CisoWorker.CISO_HEADER_SIZE == struct.calcsize(CisoWorker.CISO_HEADER_FMT)

class CisoDecompressor(CisoWorker):
    def decompress(self, input_handle, output_handle):
        """Decompress a CSO
        """
        header = self._read_header(input_handle.read(self.CISO_HEADER_SIZE))
        block_count = header['file_size'] / header['block_size']
        index_buffer_fmt = self.CISO_INDEX_FMT % (block_count + 1)
        index_buffer_size = struct.calcsize(index_buffer_fmt)
        index_buffer = struct.unpack(index_buffer_fmt,
            input_handle.read(index_buffer_size))

        decompress_block = lambda i, bi: \
            self._decompress_block(input_handle.read, i, bi,
                header['align'], header['block_size'])

        for block_i in xrange(block_count):
            output_handle.write(decompress_block(index_buffer, block_i))

    def _decompress_block(self, read, index_buffer, block_i, align, block_size):
        index = index_buffer[block_i] & self.INDEX_BITMASK
        compressed = not (index_buffer[block_i] & self.UNCOMPRESSED_BITMASK)
        # read_pos = index << align

        real_block_size = block_size
        if compressed:
            next_index = index_buffer[block_i+1] & self.INDEX_BITMASK
            if align:
                real_block_size = (next_index - index + 1) << align
            else:
                real_block_size = (next_index - index) << align

        block = read(real_block_size)
        # block = read(real_block_size, read_pos)

        if not compressed:
            return block
        else:
            # TODO: error handling here
            return zlib.decompress(block, ZLIB_WINDOW_SIZE)

    def _read_header(self, header_bytes):
        header_struct = struct.unpack(self.CISO_HEADER_FMT,
            header_bytes[:self.CISO_HEADER_SIZE])
        header_data = {
            'magic':        header_struct[0],
            'header_size':  header_struct[1],
            'file_size':    header_struct[2],
            'block_size':   header_struct[3],
            'version':      header_struct[4],
            'align':        header_struct[5],
            'reserved':     header_struct[6],
        }
        return header_data

class CisoCompressor(CisoWorker):
    PADDING_BYTE            = b'X'
    COMPRESSION_THRESHOLD   = 90
    COMPRESSION_LEVEL       = ZLIB_DEFAULT_LEVEL

    def __init__(self, level=COMPRESSION_LEVEL, threshold=COMPRESSION_THRESHOLD,
            padding_byte=PADDING_BYTE):
        self.COMPRESSION_LEVEL = level
        self.COMPRESSION_THRESHOLD = threshold
        self.PADDING_BYTE = padding_byte

    def compress(self, input_handle, output_handle, level=ZLIB_DEFAULT_LEVEL):
        """Compress a ISO into a CSO
        """
        file_size = self._get_stream_size(input_handle)
        if file_size >= 2 ** 31:
            align = 1
        else:
            align = 0
        header = self._build_header(file_size, self.CISO_BLOCK_SIZE, align)
        output_handle.write(header)

        block_count = file_size / self.CISO_BLOCK_SIZE
        index_buffer = [0] * (block_count + 1)
        output_handle.write(b'\x00\x00\x00\x00' * len(index_buffer))
        compress_block = lambda write_pos: self._compress_block(input_handle.read,
            write_pos, self.COMPRESSION_THRESHOLD, level, align, self.CISO_BLOCK_SIZE)
        for block_i in xrange(block_count):
            index, block = compress_block(output_handle.tell())
            index_buffer[block_i] = index
            output_handle.write(block)
        index_buffer[-1] = output_handle.tell() >> align

        output_handle.seek(self.CISO_HEADER_SIZE)
        output_handle.write(struct.pack(self.CISO_INDEX_FMT % len(index_buffer),
            *index_buffer))

    def _compress_block(self, read, write_pos, threshold, level, align, block_size):
        uncompressed_block = read(block_size)
        # TODO: error handling here
        compressed_block = zlib.compress(uncompressed_block, level)[2:]
        padding = self._get_align_padding(write_pos, align)
        index = (write_pos + len(padding)) >> align
        if (100 * len(compressed_block)) / len(uncompressed_block) >= threshold:
            block = uncompressed_block
            index |= self.UNCOMPRESSED_BITMASK
        elif index & self.UNCOMPRESSED_BITMASK:
            raise Exception('Align error')
        else:
            block = compressed_block
        return index, padding + block

    def _zlib_compress(self, data, level):
        return zlib.compress(data, level)[2:]

    def _get_align_padding(self, write_pos, align):
        align_shift = 1 << align
        if write_pos % align_shift:
            padding = PADDING_BYTE * (align_shift - write_pos % align_shift)
        else:
            padding = ''
        return padding

    def _get_stream_size(self, stream):
        current_pos = stream.tell()
        stream.seek(0, os.SEEK_END)
        size = stream.tell()
        stream.seek(current_pos)
        return size

    def _build_header(self, file_size, block_size, align):
        return struct.pack(self.CISO_HEADER_FMT,
            self.CISO_MAGIC,
            self.CISO_HEADER_SIZE,
            file_size,
            block_size,
            self.CISO_VER,
            align,
            0
        )

def decompress(input_handle, output_handle):
    worker = CisoDecompressor()
    return worker.decompress(input_handle, output_handle)

def compress(input_handle, output_handle, level=ZLIB_DEFAULT_LEVEL):
    worker = CisoCompressor(level)
    return worker.compress(input_handle, output_handle, level)

if __name__ == '__main__':
    import sys

    if '-d' in ' '.join(sys.argv[1:]):
        decompress(sys.stdin, sys.stdout)
    else:
        compress(sys.stdin, sys.stdout)
