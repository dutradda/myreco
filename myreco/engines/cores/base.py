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


from swaggerit.utils import build_validator, get_module_path, set_logger
from swaggerit.json_builder import JsonBuilder
from myreco.engines.cores.items_indices_map import ItemsIndicesMap
from myreco.engines.cores.utils import build_engine_data_path, build_engine_key_prefix
from jsonschema import Draft4Validator
from abc import ABCMeta, abstractmethod
from bottleneck import argpartition
from glob import glob
from aiofiles import gzip_open
import ujson
import os.path


class EngineError(Exception):
    pass


class EngineCoreMeta(ABCMeta):

    def __init__(cls, name, bases_classes, attributes):
        if name != 'EngineCore':
            schema = cls.__configuration_schema__
            Draft4Validator.check_schema(schema)
            cls.__config_validator__ = build_validator(schema, get_module_path(cls))


class EngineCore(metaclass=EngineCoreMeta):

    def __init__(self, engine=None, items_model=None):
        self.engine = engine
        self.items_model = items_model
        set_logger(self, build_engine_key_prefix(self.engine))

    def get_variables(self):
        return []

    def validate_config(self):
        self.__config_validator__.validate(self.engine['configuration'])
        self._validate_config(self.engine)

    def _validate_config(self, engine):
        pass

    async def get_recommendations(self, session, filters, max_recos, show_details, **variables):
        rec_vector = await self._build_rec_vector(session, **variables)

        if rec_vector is not None:
            for filter_, ids in filters.items():
                await filter_.filter(session, rec_vector, ids)

            return await self._build_rec_list(session, rec_vector, max_recos, show_details)

        return []

    @abstractmethod
    async def _build_rec_vector(self, session, **variables):
        pass

    async def _build_rec_list(self, session, rec_vector, max_recos, show_details):
        items_indices_map = ItemsIndicesMap(self.items_model)
        best_indices = self._get_best_indices(rec_vector, max_recos)
        best_items_keys = await items_indices_map.get_items(best_indices, session)

        if show_details and best_items_keys:
            return [ujson.loads(item) for item in await session.redis_bind.hmget(
                            self.items_model.__key__, *best_items_keys) if item is not None]

        else:
            items_ids = []
            for key in best_items_keys:
                item = {}
                self.items_model.set_instance_ids(item, key)
                self._set_item_values(item)
                items_ids.append(item)

            return items_ids

    def _get_best_indices(self, rec_vector, max_recos):
        if max_recos > rec_vector.size:
            max_recos = rec_vector.size

        best_indices = argpartition(-rec_vector, max_recos-1)[:max_recos]
        best_values = rec_vector[best_indices]
        return [int(i) for i, v in
            sorted(zip(best_indices, best_values), key=lambda x: x[1], reverse=True) if v > 0.0]

    def _set_item_values(self, item):
        for k in item:
            schema = self.engine['item_type']['schema']['properties'].get(k)
            if schema is None:
                raise EngineError('Invalid Item {}'.format(item))

            item[k] = JsonBuilder.build(item[k], schema)

    def export_objects(self, session):
        pass

    async def _build_csv_readers(self, pattern):
        path = build_engine_data_path(self.engine)
        readers = []
        for filename in glob(os.path.join(path, '{}*.gz'.format(pattern))):
            file_ = await gzip_open(filename, 'rt')
            readers.append(file_)
        return readers

    async def _get_items_indices_map_dict(self, items_indices_map, session):
        items_indices_map_dict = await items_indices_map.get_all(session)

        if not items_indices_map_dict.values():
            raise EngineError(
                "The Indices Map for '{}' is empty. Please update these items"
                .format(self.engine['item_type']['name']))

        return items_indices_map_dict


class AbstractDataImporter(metaclass=ABCMeta):

    def __init__(self, engine):
        self._engine = engine

    @abstractmethod
    def get_data(cls, items_indices_map, session):
        pass


class RedisObjectBase(object):

    def __init__(self, engine_core):
        self._engine_core = engine_core
        self._set_redis_key()
        set_logger(self, self._redis_key)

    def _set_redis_key(self):
        self._redis_key = build_engine_key_prefix(self._engine_core.engine)
