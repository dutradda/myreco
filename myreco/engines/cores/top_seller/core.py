# MIT License

# Copyright (c) 2016 Diogo Dutra

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


from myreco.engines.cores.base import EngineCore, EngineError, RedisObjectBase
from falconswagger.utils import get_model_schema
from falconswagger.json_builder import JsonBuilder
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import os.path
import zlib
import json


class TopSellerEngineCore(EngineCore):
    __configuration_schema__ = get_model_schema(__file__)

    def export_objects(self, session, items_indices_map):
        readers = self._build_csv_readers('top_seller')
        items_indices_map_dict = self._get_items_indices_map_dict(items_indices_map, session)

        top_seller = TopSellerRedisObject(self)
        return top_seller.update(readers, session, items_indices_map_dict)

    def _build_rec_vector(self, session, **variables):
        return TopSellerRedisObject(self).get_numpy_array(session)
        rec_vector = session.redis_bind.get(redis_key)
        if rec_vector:
            return np.fromstring(zlib.decompress(rec_vector), dtype=np.int32)


class TopSellerRedisObject(RedisObjectBase):

    def update(self, readers, session, items_indices_map_dict):
        self._build_top_seller_vector(readers, session, items_indices_map_dict)
        session.redis_bind.set(
            self._redis_key,
            zlib.compress(self.numpy_array.tobytes())
        )

        return {
            'length': int(self.numpy_array.size),
            'max_sells': int(max(self.numpy_array)),
            'min_sells': int(min(self.numpy_array))
        }

    def _build_top_seller_vector(self, readers, session, items_indices_map_dict):
        error_message = "No data found for engine '{}'".format(self._engine_core.engine['name'])
        if not len(readers):
            raise EngineError(error_message)

        indices_values_map = dict()

        for reader in readers:
            self._set_indices_values_map(indices_values_map, items_indices_map_dict, reader)

        if not indices_values_map:
            raise EngineError(error_message)

        vector = np.zeros(max(indices_values_map.keys())+1, dtype=np.int32)
        indices = np.array(list(indices_values_map.keys()), dtype=np.int32)
        vector[indices] = np.array(list(indices_values_map.values()), dtype=np.int32)
        self.numpy_array = vector

    def _set_indices_values_map(self, indices_values_map, items_indices_map_dict, reader):
        for line in reader:
            line = json.loads(line)
            index = items_indices_map_dict.get(line['item_key'])
            if index is not None:
                indices_values_map[int(index)] = int(line['value'])

    def get_numpy_array(self, session):
        rec_vector = session.redis_bind.get(self._redis_key)
        if rec_vector:
            return np.fromstring(zlib.decompress(rec_vector), dtype=np.int32)
