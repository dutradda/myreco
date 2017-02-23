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


from myreco.engines.cores.redis_object import RedisObjectBase
from myreco.engines.exceptions import EngineError
import numpy as np
import ujson


class TopSellerRedisObject(RedisObjectBase):

    async def update(self, readers, session, items_indices_map_dict):
        await self._build_top_seller_vector(readers, session, items_indices_map_dict)
        await session.redis_bind.set(
            self._redis_key,
            self._pack_array(self.numpy_array, compress=False)
        )

        return {
            'length': int(self.numpy_array.size),
            'max_sells': int(max(self.numpy_array)),
            'min_sells': int(min(self.numpy_array))
        }

    async def _build_top_seller_vector(self, readers, session, items_indices_map_dict):
        error_message = "No data found for engine '{}'".format(self._engine_core.engine['name'])
        if not len(readers):
            raise EngineError(error_message)

        indices_values_map = dict()

        for reader in readers:
            await self._set_indices_values_map(indices_values_map, items_indices_map_dict, reader)

        if not indices_values_map:
            raise EngineError(error_message)

        vector = np.zeros(len(items_indices_map_dict), dtype=np.int32)
        indices = np.array(list(indices_values_map.keys()), dtype=np.int32)
        vector[indices] = np.array(list(indices_values_map.values()), dtype=np.int32)
        self.numpy_array = vector

    async def _set_indices_values_map(self, indices_values_map, items_indices_map_dict, reader):
        async for line in reader:
            line = ujson.loads(line)
            index = items_indices_map_dict.get(line['item_key'])
            if index is not None:
                indices_values_map[index] = int(line['value'])

    async def get_numpy_array(self, session):
        items_vector = await session.redis_bind.get(self._redis_key)
        if items_vector is not None:
            return self._unpack_array(items_vector, np.int32, compress=False)
