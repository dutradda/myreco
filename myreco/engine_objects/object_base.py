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
from abc import abstractmethod, ABCMeta
from glob import glob
from gzip import GzipFile
import os.path


class EngineObjectBase(metaclass=ABCMeta):
    CHUNK_SIZE = 100000

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

    def _run_coro(self, coro, session):
        return run_coro(coro, session)
    
    @abstractmethod
    def get_data(self, items_model, session):
        pass

    def _log_get_data_started(self):
        self._logger.info("Started import data")

    def _log_get_data_finished(self):
        self._logger.info("Finished import data")

    async def _del_old_keys(self, name, actual_keys, session):
        setted_keys = set([k.decode() for k in await session.redis_bind.hkeys(name)])
        keys_to_remove = list(set(setted_keys) - actual_keys)

        while keys_to_remove:
            await session.redis_bind.hdel(name, *keys_to_remove[:type(self).CHUNK_SIZE])
            keys_to_remove = keys_to_remove[type(self).CHUNK_SIZE:]
