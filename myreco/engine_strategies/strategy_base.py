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


from swaggerit.utils import build_validator, get_module_path
from jsonschema import Draft4Validator
from swaggerit.json_builder import JsonBuilder
from abc import ABCMeta, abstractmethod
import heapq


class EngineStrategyMetaBase(ABCMeta):

    def __init__(cls, name, bases_classes, attributes):
        if hasattr(cls, 'configuration_schema'):
            schema = cls.configuration_schema
            Draft4Validator.check_schema(schema)
            cls.__config_validator__ = build_validator(schema, get_module_path(cls))


class EngineStrategyBase(metaclass=EngineStrategyMetaBase):
    object_types = {}

    def __init__(self, engine, items_model=None):
        self._engine = engine
        self._set_objects(engine['objects'])
        self._set_config(engine['objects'])
        self._items_model = items_model

    def _set_objects(self, objects):
        self.objects = {
            obj['type']: self._get_object_instance(obj) \
                for obj in objects
        }

    def _get_object_instance(self, obj):
        return self.object_types[obj['type']](obj)

    def _set_config(self, objects):
        self._config = {obj['type']: obj['configuration'] for obj in objects}

    def validate_config(self):
        type(self).__config_validator__.validate(self._config)
        self._validate_config()

    def _validate_config(self):
        pass

    def get_variables(self):
        return []

    async def get_items(self, session, max_items, show_details, filters, engine_variables):
        recos = await self._build_recommendations(session, filters, engine_variables)
        recos_keys = self._sort_recommendations(recos, max_items)

        if show_details and recos_keys:
            return await self._items_model.get(session, recos_keys)
        else:
            return [self._build_item_ids(key) for key in recos_keys]

    def _build_item_ids(self, key):
        item = {}
        self._items_model.set_instance_ids(item, key)

        for k in item:
            schema = self._engine['item_type']['schema']['properties'].get(k)
            if schema is None:
                raise EngineError('Invalid Item {}'.format(item))

            item[k] = JsonBuilder.build(item[k], schema)

        return item

    @abstractmethod
    async def _build_recommendations(self, session, filters, engine_variables):
        pass

    def _sort_recommendations(self, recos, max_items):
        heapq.heapify(recos)
        return [reco[0] for reco in heapq.nlargest(max_items, recos, key=lambda r: r[1])]
