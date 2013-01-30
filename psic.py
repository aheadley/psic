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

ZLIB_DEFAULT_LEVEL  = 1

class CisoWorker(object):
    CISO_HEADER_FMT     = ''.join([
        '<',    # ensure little endian
        '4B',   # magic ('C', 'I', 'S', 'O')
        'I',    # header size
        'Q',    # size of uncompressed file
        'I',    # compressed block size
        'B',    # version number
        'B',    # alignment of index value
        '2B',   # reserved
    ])
    CISO_HEADER_SIZE    = 0x18
    # CISO_MAGIC      = ('C', 'I', 'S', 'O')
    CISO_MAGIC      = (chr(c) for c in 'CISO')
    # CISO_MAGIC      = 0x4F534943
    CISO_VER        = 0x01
    CISO_BLOCK_SIZE = 0x0800
    CISO_INDEX      = '<I'

    PLAIN_BITMASK   = 0x80000000
    INDEX_BITMASK   = 0x7FFFFFFF

    def __init__(self):
        pass

    def decompress(self, in_file, out_file):
        with open(in_file, 'rb') as in_file_handle:
            header = self._read_header(in_file_handle.read(self.CISO_HEADER_SIZE))
            block_count = header['file_size'] / header['block_size']
            index_buffer_fmt = '<%dI' % (block_count + 1)
            index_buffer_size = struct.calcsize(index_buffer_fmt)
            index_buffer = struct.unpack(index_buffer_fmt,
                in_file_handle.read(index_buffer_size))
            with open(out_file, 'wb') as out_file_handle:
                for block_i in xrange(block_count):
                    index = index_buffer[block_i]
                    uncompressed = index & self.PLAIN_BITMASK
                    index &= self.INDEX_BITMASK
                    jump = index << header['align']

                    if uncompressed:
                        read_size = header['block_size']
                    else:
                        extra_index = index_buffer[block_i+1] & self.INDEX_BITMASK
                        if header['align']:
                            read_size = (extra_index - index + 1) << header['align']
                        else:
                            read_size = (extra_index - index) << header['align']
                    in_file_handle.seek(jump)
                    block = in_file_handle.read(read_size)

                    if not uncompressed:
                        try:
                            block = zlib.decompress(block, -15)
                        except zlib.error as err:
                            # do nothing for now
                            raise err

                    out_file_handle.write(block)

    def compress(self, in_file, out_file, level=ZLIB_DEFAULT_LEVEL):
        pass

    def _read_header(self, data):
        header = struct.unpack(self.CISO_HEADER_FMT,
            data[:self.CISO_HEADER_SIZE])
        header_data = {
            'magic':        header[0:4],
            'header_size':  header[4],
            'file_size':    header[5],
            'block_size':   header[6],
            'version':      header[7],
            'align':        header[8],
            'reserved':     header[9:],
        }
        return header_data

    def _build_header(self, header_data):
        return struct.pack(self.CISO_HEADER_FMT,
            header_data['magic'],
            header_data['header_size'],
            header_data['file_size'],
            header_data['block_size'],
            header_data['version'],
            header_data['align'],
            header_data['reserved']
        )

assert CisoWorker.CISO_HEADER_SIZE == struct.calcsize(CisoWorker.CISO_HEADER_FMT)

if __name__ == '__main__':
    import sys

    worker = CisoWorker()
    worker.decompress(sys.argv[1], sys.argv[2])
