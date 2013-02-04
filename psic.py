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
import itertools
try:
    import multiprocessing as mp
except ImportError:
    import multiprocessing.dummy as mp

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
    CISO_BLOCK_SIZE = 0x0800 # 2048, ISO sector size (maybe?)
    CISO_INDEX_FMT  = '<%dI'

    UNCOMPRESSED_BITMASK    = 0x80000000
    INDEX_BITMASK           = 0x7FFFFFFF

assert CisoWorker.CISO_HEADER_SIZE == struct.calcsize(CisoWorker.CISO_HEADER_FMT)

class IndexedBlockIterator(object):
    def __init__(self, input_handle, align, index_buffer):
        self.input_handle = input_handle
        self.align = align
        self.index_buffer = index_buffer
        self._read_lock = mp.Semaphore()

    def __len__(self):
        return len(self.index_buffer) - 1

    def __call__(self):
        for block_i in xrange(len(self.index_buffer)-1):
            raw_index = self.index_buffer[block_i]
            block_start = raw_index & CisoWorker.INDEX_BITMASK
            compressed = not (raw_index & CisoWorker.UNCOMPRESSED_BITMASK)
            if compressed:
                next_index = self.index_buffer[block_i+1] & CisoWorker.INDEX_BITMASK
                if self.align:
                    block_size = (next_index - block_start + 1) << self.align
                else:
                    block_size = next_index - block_start
            else:
                block_size = CisoWorker.CISO_BLOCK_SIZE

            self._read_lock.acquire()
            self.input_handle.seek(block_start)
            data = self.input_handle.read(block_size)
            self._read_lock.release()
            yield compressed, data

    __iter__ = __call__

class CisoDecompressor(CisoWorker):
    def __init__(self, threads=None):
        self._pool = mp.Pool(threads)

    def decompress(self, input_handle, output_handle):
        """Decompress a CSO
        """
        header = self._read_header(input_handle.read(self.CISO_HEADER_SIZE))
        block_count = header['file_size'] / header['block_size']
        index_buffer_fmt = self.CISO_INDEX_FMT % (block_count + 1)
        index_buffer = struct.unpack(index_buffer_fmt,
            input_handle.read(struct.calcsize(index_buffer_fmt)))

        block_iter = IndexedBlockIterator(
            input_handle, header['align'], index_buffer)
        for block_data in self._pool.map(_decompress_mp_helper, block_iter):
            output_handle.write(block_data)

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
        if header_struct[0] != self.CISO_MAGIC:
            raise Exception('CISO magic not found')
        return header_data

def _decompress_mp_helper(args):
    compressed, data = args
    if compressed:
        return zlib.decompress(data, ZLIB_WINDOW_SIZE)
    else:
        return data

class CisoCompressor(CisoWorker):
    PADDING_BYTE            = b'X'
    COMPRESSION_THRESHOLD   = 90
    COMPRESSION_LEVEL       = ZLIB_DEFAULT_LEVEL

    def __init__(self, threads=None, level=COMPRESSION_LEVEL,
            threshold=COMPRESSION_THRESHOLD, padding_byte=PADDING_BYTE):
        self._pool = mp.Pool(threads)
        self.COMPRESSION_LEVEL = level
        self.COMPRESSION_THRESHOLD = threshold
        self.PADDING_BYTE = padding_byte

    def compress(self, input_handle, output_handle):
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

        for (block_i, (compressed, data)) in enumerate(self._pool.map(
                _compress_mp_helper,
                ((input_handle.read(self.CISO_BLOCK_SIZE),
                        self.COMPRESSION_LEVEL, self.COMPRESSION_THRESHOLD)
                    for _ in xrange(block_count)))):
            write_pos = output_handle.tell()
            padding = self._get_align_padding(write_pos, align)
            index = (write_pos + len(padding)) >> align
            if not compressed:
                index |= self.UNCOMPRESSED_BITMASK
            elif index & self.UNCOMPRESSED_BITMASK:
                raise Exception('Align error')

            index_buffer[block_i] = index
            output_handle.write(padding + data)

        index_buffer[-1] = output_handle.tell() >> align

        output_handle.seek(self.CISO_HEADER_SIZE)
        output_handle.write(struct.pack(self.CISO_INDEX_FMT % len(index_buffer),
            *index_buffer))

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

def _compress_mp_helper(args):
    data, level, threshold = args
    compressed_data = zlib.compress(data, level)[2:]
    if (100 * len(compressed_data)) / len(data) >= threshold:
        return False, data
    else:
        return True, compressed_data

def decompress(input_handle, output_handle, threads):
    worker = CisoDecompressor(threads)
    return worker.decompress(input_handle, output_handle)

def compress(input_handle, output_handle, threads, level=ZLIB_DEFAULT_LEVEL):
    worker = CisoCompressor(threads, level=level)
    return worker.compress(input_handle, output_handle)


if __name__ == '__main__':
    import sys
    import optparse

    parser = optparse.OptionParser()
    parser.add_option('-d', '--decompress',
        action='store_true', default=False,
        help='Decompress a CSO file')
    parser.add_option('-l', '--level',
        action='store', type='int', default=ZLIB_DEFAULT_LEVEL,
        help='Compression level to use (1-9)')
    parser.add_option('-t', '--threads',
        action='store', type='int', default=None,
        help='Number of threads/processes to use')

    opts, args = parser.parse_args()
    if opts.decompress:
        worker = lambda i, o: decompress(i, o, opts.threads)
    else:
        worker = lambda i, o: compress(i, o, opts.threads, opts.level)

    if len(args) == 0:
        worker(sys.stdin, sys.stdout)
    else:
        if len(args) == 1:
            in_filename = args[0]
            parts = os.path.splitext(in_filename)
            if opts.decompress:
                if parts[1].lower() == '.cso':
                    out_filename = parts[0] + '.iso'
                else:
                    out_filename = in_filename + '.iso'
            else:
                if parts[1].lower() == '.iso':
                    out_filename = parts[0] + '.cso'
                else:
                    out_filename = in_filename + '.cso'

            if os.path.exists(out_filename):
                raise Exception('Output file already exists, not clobbering!')
        elif len(args) == 2:
            in_filename = args[0]
            out_filename = args[1]
        with open(in_filename, 'rb') as in_file:
            with open(out_filename, 'wb') as out_file:
                worker(in_file, out_file)
