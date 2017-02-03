# MIT License

# Copyright (c) 2016 Diogo Dutra <dutradda@gmail.com>

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


from swaggerit.utils import set_logger
from myreco.engines.cores.utils import build_engine_key_prefix
import zlib
import numpy as np


class RedisObjectBase(object):

    def __init__(self, engine_core):
        self._engine_core = engine_core
        self._set_redis_key()
        set_logger(self, self._redis_key)

    def _set_redis_key(self):
        self._redis_key = build_engine_key_prefix(self._engine_core.engine)

    def _pack_array(self, array, compress=True, level=-1):
        if compress:
            return zlib.compress(array.tobytes(), level)
        else:
            return array.tobytes()

    def _unpack_array(self, array, dtype, compress=True):
        if array is not None:
            if compress:
                return np.fromstring(zlib.decompress(array), dtype=dtype)
            else:
                return np.fromstring(array, dtype=dtype)
        else:
            return None
