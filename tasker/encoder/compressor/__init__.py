from . import bzip2
from . import dummy
from . import gzip
from . import lzma
from . import zlib

from . import _compressor


__compressors__ = [
    bzip2.Compressor,
    dummy.Compressor,
    gzip.Compressor,
    lzma.Compressor,
    zlib.Compressor,
]
