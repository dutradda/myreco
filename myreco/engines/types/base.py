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


from falconswagger.models.base import build_validator, get_module_path
from jsonschema import Draft4Validator
from abc import ABCMeta, abstractmethod
from bottleneck import argpartition
import msgpack


class EngineError(Exception):
    pass


class EngineTypeMeta(type):

    def __init__(cls, name, bases_classes, attributes):
        if name != 'EngineType':
            schema = cls.__configuration_schema__
            Draft4Validator.check_schema(schema)
            cls.__config_validator__ = build_validator(schema, get_module_path(cls))


class EngineType(metaclass=EngineTypeMeta):

    def __init__(self, engine=None, items_model=None):
        self.engine = engine
        self.items_model = items_model

    def get_variables(self):
        return []

    def validate_config(self):
        self.__config_validator__.validate(self.engine['configuration'])
        self._validate_config(self.engine)

    def _validate_config(self, engine):
        pass

    def get_recommendations(self, session, filters, max_recos, **variables):
        rec_vector = self._build_rec_vector(session, **variables)

        if rec_vector is not None:
            [filter_.filter(session, rec_vector, ids) for filter_, ids in filters.items()]
            return self._build_rec_list(session, rec_vector, max_recos)

        return []

    def _build_rec_vector(self, session, **variables):
        pass

    def _build_rec_list(self, session, rec_vector, max_recos):
        items_indices_map = self.items_model.build_items_indices_map()
        best_indices = self._get_best_indices(rec_vector, max_recos)
        best_items_keys = items_indices_map.get_items(session, best_indices)
        return [msgpack.loads(item, encoding='utf-8') for item in session.redis_bind.hmget(
                            self.items_model.__key__, best_items_keys) if item is not None]

    def _get_best_indices(self, rec_vector, max_recos):
        if max_recos > rec_vector.size:
            max_recos = rec_vector.size

        best_indices = argpartition(-rec_vector, max_recos-1)[:max_recos]
        best_values = rec_vector[best_indices]
        return [i for i, v in
            sorted(zip(best_indices, best_values), key=lambda x: x[1], reverse=True) if v > 0.0]

    def export_objects(self, session):
        pass


class AbstractDataImporter(metaclass=ABCMeta):

    @classmethod
    @abstractmethod
    def get_data(cls, engine, items_indices_map, session):
        pass


from myreco.engines.types.neighborhood.engine import NeighborhoodEngine
from myreco.engines.types.top_seller.engine import TopSellerEngine
from myreco.engines.types.visual_similarity.engine import VisualSimilarityEngine


class EngineTypeChooser(object):

    def __new__(cls, name):
        if name == 'neighborhood':
            return NeighborhoodEngine

        elif name == 'top_seller':
            return TopSellerEngine

        elif name == 'visual_similarity':
            return VisualSimilarityEngine
