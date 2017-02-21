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


from myreco.engines.cores.base import EngineCoreBase
from myreco.engines.exceptions import EngineError
from abc import abstractmethod
from glob import glob
from aiofiles import gzip_open
import os.path
import asyncio


class EngineCoreObjectsExporter(EngineCoreBase):

    @abstractmethod
    def export_objects(self, session):
        pass

    async def _build_csv_readers(self, pattern=''):
        readers = []
        pattern = os.path.join(self._data_path, '{}*.gz'.format(pattern))

        for filename in glob(pattern):
            file_ = await gzip_open(filename, 'rt')
            readers.append(file_)

        return readers

    async def _get_items_indices_map_dict(self, session):
        items_indices_map_dict = await self._items_indices_map.get_all(session)

        if not items_indices_map_dict.values():
            raise EngineError(
                "The Indices Map for '{}' is empty. Please update these items"
                .format(self.engine['item_type']['name']))

        return items_indices_map_dict

    def _run_coro(self, coro, loop):
        if loop.is_running():
            return asyncio.run_coroutine_threadsafe(coro, loop).result()
        else:
            return loop.run_until_complete(coro)
