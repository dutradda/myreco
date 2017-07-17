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
from myreco.utils import build_engine_object_key, makedirs, run_coro
from myreco.exceptions import EngineError
from abc import abstractmethod, ABCMeta
from glob import glob
from gzip import GzipFile
import os.path
import asyncio
import zlib
import numpy as np


class EngineObjectBase(metaclass=ABCMeta):

    def __init__(self, engine_object, data_path=None):
        self._engine_object = engine_object
        self._set_redis_key()
        set_logger(self, self._redis_key)
        if data_path is not None:
            self._set_data_path(data_path)

    def _set_redis_key(self):
        self._redis_key = build_engine_object_key(self._engine_object)

    def _set_data_path(self, data_path):
        self._data_path = os.path.join(data_path, self._redis_key)
        makedirs(self._data_path)

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

    @abstractmethod
    def export(self, items_model, session):
        pass

    def _build_csv_readers(self, pattern=''):
        readers = []
        pattern = os.path.join(self._data_path, '{}*.gz'.format(pattern))

        for filename in glob(pattern):
            file_ = GzipFile(filename, 'r')
            readers.append(file_)

        return readers

    async def _get_items_indices_map_dict(self, items_indices_map, session):
        items_indices_map_dict = await items_indices_map.get_all(session)

        if not items_indices_map_dict.values():
            raise EngineError(
                "The Indices Map for '{}' is empty. Please update these items"
                .format(self._engine_object['item_type']['name']))

        return items_indices_map_dict

    def _run_coro(self, coro, session):
        return run_coro(coro, session)
    
    @abstractmethod
    def get_data(self, items_model, session):
        pass

    def _log_get_data_started(self):
        self._logger.info("Started import data")

    def _log_get_data_finished(self):
        self._logger.info("Finished import data")
