#!/usr/bin/env python

import zlib
import struct
import os

ZLIB_DEFAULT_LEVEL  = max(1, min(9, int(os.environ.get('ZLIB_DEFAULT_LEVEL', '1'))))
ZLIB_WINDOW_SIZE = -15

class GczError(Exception): pass

class GczHeader(object):
    MAGIC_COOKIE        = 0xB10BC001;
    HEADER_STRUCT       = struct.Struct('IIQQII')
    POINTERS_STRUCT_FMT = '%dQ'
    HASHES_STRUCT_FMT   = '%dI'

    def __init__(self, handle=None):
        if handle is not None:
            self.load(handle)

    def load(self, handle):
        handle.seek(0)
        unpacked_data = self.HEADER_STRUCT.unpack(handle.read(self.size))
        (self.magic_cookie, self.sub_type, self.compressed_data_size, \
            self.data_size, self.block_size, self.num_blocks) = unpacked_data
        self._load_pointers_and_hashes(handle)

    @property
    def size(self):
        return self.HEADER_STRUCT.size

    @property
    def block_pointers_size(self):
        return struct.calcsize(self.POINTERS_STRUCT_FMT % len(self.block_pointers))

    @property
    def block_hashes_size(self):
        return struct.calcsize(self.HASHES_STRUCT_FMT % len(self.block_hashes))

    @property
    def full_size(self):
        return self.size + self.block_pointers_size + self.block_hashes_size

    def _load_pointers_and_hashes(self, handle):
        pointers_struct = struct.Struct(self.POINTERS_STRUCT_FMT % self.num_blocks)
        handle.seek(self.size)
        self.block_pointers = list(pointers_struct.unpack(handle.read(pointers_struct.size)))
        hashes_struct = struct.Struct(self.HASHES_STRUCT_FMT % self.num_blocks)
        self.block_hashes = list(hashes_struct.unpack(handle.read(hashes_struct.size)))

class GczFile(object):
    def __init__(self, handle):
        self._handle = handle
        self.header = GczHeader(handle)

    def __len__(self):
        return len(self.header.block_pointers)

    def get_block_compressed_size(self, block_num):
        start_pos = self.header.block_pointers[block_num]
        if block_num < self.header.num_blocks - 1:
            return self.header.block_pointers[block_num + 1] - start_pos
        elif block_num == header.num_blocks - 1:
            return self.header.compressed_data_size - start_pos
        else:
            raise GczError('Illegal block number: %d' % block_num)

    def compute_block_hash(self, block_data):
        return zlib.adler32(block_data)

    def check_block_hash(self, block_num, block_data):
        return self.compute_block_hash(block_data) == self.handle.block_hashes[block_num]

    def get_block(self, block_num):
        compressed = True
        compressed_block_size = self.get_block_compressed_size(block_num)

        block_offset = self.header.block_pointers[block_num] + self.header.full_size

        if block_offset & (1 << 63):
            if compressed_block_size != self.header.block_size:
                raise GczError('Uncompressed block with wrong size')
            compressed = False
            block_offset &= ~(1 << 63)

        self._handle.seek(block_offset)
        block_data = self._handle.read(compressed_block_size)

        block_hash = self.compute_block_hash(block_data)
        if block_hash != self.header.block_hashes[block_num]:
            raise GczError('Hash of block %d is wrong: %08x != %08x' % \
                (block_num, block_hash, self.header.block_hashes[block_num]))

        if not compressed:
            return block_data
        else:
            decompressed_block_data = zlib.decompress(block_data, ZLIB_WINDOW_SIZE)
            if len(decompressed_block_data) != self.header.block_size:
                raise GczError('Decompressed block is wrong size: %d != %d' % \
                    (len(decompressed_block_data), self.header.block_size))
            return decompressed_block_data

class GczWorker(object):
    def __init__(self):
        pass

class GczDecompressor(GczWorker):
    def decompress(self, input_handle, output_handle):
        gcz_file = GczFile(input_handle)

        for i in xrange(len(gcz_file)):
            output_handle.write(gcz_file.get_block(i))

if __name__ == '__main__':
    import sys

    input_filename = sys.argv[1]
    output_filename = sys.argv[2]

    with open(output_filename, 'wb') as output_handle:
        with open(input_filename, 'rb') as input_handle:
            GczDecompressor().decompress(input_handle, output_handle)
