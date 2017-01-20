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
from myreco.engines.cores.utils import build_engine_data_path, build_engine_key_prefix, makedirs
from myreco.engines.cores.items_indices_map import ItemsIndicesMap
from jsonschema import Draft4Validator
from abc import ABCMeta


class EngineCoreMetaBase(ABCMeta):

    def __init__(cls, name, bases_classes, attributes):
        if hasattr(cls, '__configuration_schema__'):
            schema = cls.__configuration_schema__
            Draft4Validator.check_schema(schema)
            cls.__config_validator__ = build_validator(schema, get_module_path(cls))


class EngineCoreBase(metaclass=EngineCoreMetaBase):

    def __init__(self, engine, items_model):
        self.engine = engine
        self._items_model = items_model
        self._key = build_engine_key_prefix(self.engine)
        self._data_path = build_engine_data_path(self.engine)
        self._items_indices_map = ItemsIndicesMap(self._items_model)
        makedirs(self._data_path)
        set_logger(self, self._key)

    def validate_config(self):
        type(self).__config_validator__.validate(self.engine['configuration'])
        self._validate_config()

    def _validate_config(self):
        pass
