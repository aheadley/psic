#!/usr/bin/env python

import zlib
import struct
import os

ZLIB_DEFAULT_LEVEL  = max(1, min(9, int(os.environ.get('ZLIB_DEFAULT_LEVEL', '1'))))
ZLIB_WINDOW_SIZE = -15

class GczError(Exception): pass

class GczHeader(object):
    GCZ_MAGIC           = 0xB10BC001;
    HEADER_STRUCT       = struct.Struct('IIQQII')
    POINTERS_STRUCT_FMT = '%dQ'
    HASHES_STRUCT_FMT   = '%dI'

    def __init__(self, handle=None):
        if handle is not None:
            self.load(handle)

    def load(self, handle):
        handle.seek(0)
        unpacked_data = self.HEADER_STRUCT.unpack(handle.read(self.size))
        self.magic_cookie           = unpacked_data[0]
        self.sub_type               = unpacked_data[1]
        self.compressed_data_size   = unpacked_data[2]
        self.data_size              = unpacked_data[3]
        self.block_size             = unpacked_data[4]
        self.num_blocks             = unpacked_data[5]

        if self.magic_cookie != self.GCZ_MAGIC:
            raise GczError('File magic value is wrong: %08X != %08X' % \
                (self.magic_cookie, self.GCZ_MAGIC))
        if (self.block_size * self.num_blocks) != self.data_size:
            raise GczError('Decompressed data size does not match expected size: %d != %d' % \
                self.block_size * self.num_blocks, self.data_size)

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
    UNCOMPRESSED_BLOCK_FLAG     = (1 << 63)
    COMPRESSED_BLOCK_BITMASK    = ~(1 << 63)

    def __init__(self, handle):
        self._handle = handle
        self.header = GczHeader(handle)

    def __len__(self):
        return self.header.num_blocks

    def get_block_size(self, block_num):
        try:
            block_start = self.get_block_start(block_num)
        except IndexError:
            raise GczError('Illegal block number: %d' % block_num)

        if block_num == (self.header.num_blocks - 1):
            block_end = self.header.compressed_data_size
        else:
            block_end = self.get_block_start(block_num+1)

        return block_end - block_start

    def get_block_start(self, block_num):
        return self.header.block_pointers[block_num] & self.COMPRESSED_BLOCK_BITMASK

    def get_block_is_uncompressed(self, block_num):
        return bool(self.header.block_pointers[block_num] & self.UNCOMPRESSED_BLOCK_FLAG)

    def compute_block_hash(self, block_data):
        return zlib.adler32(block_data) & 0xFFFFFFFF

    def check_block_hash(self, block_num, block_data):
        return self.compute_block_hash(block_data) == self.handle.block_hashes[block_num]

    def get_block(self, block_num):
        block_size = self.get_block_size(block_num)
        if block_size > self.header.block_size:
            raise GczError('Block [%d] larger than largest possible block size: %d > %d' % \
                (block_num, block_size, self.header.block_size))

        self._handle.seek(self.header.full_size + self.get_block_start(block_num))
        block_data = self._handle.read(block_size)

        block_hash = self.compute_block_hash(block_data)
        if block_hash != self.header.block_hashes[block_num]:
            raise GczError('Hash of block [%d] is wrong: %08X != %08X' % \
                (block_num, block_hash, self.header.block_hashes[block_num]))

        if self.get_block_is_uncompressed(block_num):
            if block_size != self.header.block_size:
                raise GczError('Uncompressed block [%d] is wrong size: %d != %d' % \
                    (block_num, block_size, self.header.block_size))
        else:
            block_data = zlib.decompress(block_data)
            if len(block_data) != self.header.block_size:
                raise GczError('Decompressed block [%d] is wrong size: %d != %d' % \
                    (block_num, len(block_data), self.header.block_size))

        return block_data

class GczWorker(object):
    def __init__(self):
        pass

class GczDecompressor(GczWorker):
    def decompress(self, input_handle, output_handle, observer=None, skip_broken=False):
        gcz_file = GczFile(input_handle)
        if observer is not None:
            observer.maxval = len(gcz_file)
            observer.start()
        for i in xrange(len(gcz_file)):
            try:
                block = gcz_file.get_block(i)
            except GczError as err:
                if skip_broken:
                    print err
                    block = '\0' * gcz_file.handle.block_size
                else:
                    raise err
            output_handle.write(block)
            if observer is not None and i % 10 == 0:
                observer.update(i)
        if observer is not None:
            observer.update(len(gcz_file))

if __name__ == '__main__':
    import sys
    try:
        import progressbar
    except ImportError:
        progressbar = None

    input_filename = sys.argv[1]
    output_filename = sys.argv[2]

    if progressbar is not None:
        obs = progressbar.ProgressBar()
    else:
        obs = None

    with open(output_filename, 'wb') as output_handle:
        with open(input_filename, 'rb') as input_handle:
            GczDecompressor().decompress(input_handle, output_handle, obs)
