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


from myreco.engines.cores.base import EngineCore, EngineError
from myreco.engines.cores.utils import build_csv_readers, build_engine_data_path
from falconswagger.models.base import get_model_schema
from falconswagger.json_builder import JsonBuilder
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import os.path
import zlib


class TopSellerEngine(EngineCore):
    __configuration_schema__ = get_model_schema(__file__)

    def export_objects(self, session, items_indices_map):
        data_path = build_engine_data_path(self.engine)
        readers = build_csv_readers(data_path, 'top_seller')

        top_seller_vector = self._build_top_seller_vector(readers, items_indices_map, session)
        redis_key = self._build_redis_key()
        session.redis_bind.set(redis_key, zlib.compress(top_seller_vector.tobytes()))

        result = sorted(enumerate(top_seller_vector), key=(lambda x: (x[1], x[0])), reverse=True)
        indices_items_map = items_indices_map.get_indices_items_map(session)
        return [{self._format_output(indices_items_map, r): int(r[1])} for r in result]

    def _format_output(self, indices_items_map, r):
        return ' | '.join([str(i) for i in eval(indices_items_map[r[0]])])

    def _build_top_seller_vector(self, readers, items_indices_map, session):
        error_message = "No data found for engine '{}'".format(self.engine['name'])
        if not len(readers):
            raise EngineError(error_message)

        executor = ThreadPoolExecutor(len(readers))
        jobs = []
        indices_values_map = dict()
        for reader in readers:
            job = executor.submit(self._set_indices_values_map, indices_values_map,
                                reader, items_indices_map, session)
            jobs.append(job)

        [job.result() for job in jobs]

        if not indices_values_map:
            raise EngineError(error_message)

        vector = np.zeros(max(indices_values_map.keys())+1, dtype=np.int32)
        indices = np.array(list(indices_values_map.keys()), dtype=np.int32)
        vector[indices] = np.array(list(indices_values_map.values()), dtype=np.int32)

        return vector

    def _set_indices_values_map(self, indices_values_map, reader, items_indices_map, session):
        items_indices_map = items_indices_map.get_all(session)

        if not items_indices_map:
            raise EngineError(
                "The Indices Map for '{}' is empty. Please update these items"
                .format(self.engine['item_type']['name']))

        for line in reader:
            value = line.pop('value')
            for k in line:
                schema = self.engine['item_type']['schema']['properties'].get(k)
                if schema is None:
                    raise EngineError('Invalid Line {}'.format(line))

                line[k] = JsonBuilder(line[k], schema)
            index = items_indices_map.get(line)
            if index is not None:
                indices_values_map[int(index)] = int(value)

    def _build_redis_key(self):
        return '{}_{}'.format(self.engine['core']['name'], self.engine['id'])

    def _build_rec_vector(self, session, **variables):
        rec_vector = session.redis_bind.get(self._build_redis_key())
        if rec_vector:
            return np.fromstring(zlib.decompress(rec_vector), dtype=np.int32)
