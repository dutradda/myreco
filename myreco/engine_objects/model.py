# MIT License

# Copyright (c) 2017 Diogo Dutra <dutradda@gmail.com>

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

from swaggerit.utils import get_swagger_json
from swaggerit.exceptions import SwaggerItModelError
from sqlalchemy.ext.declarative import AbstractConcreteBase, declared_attr
import sqlalchemy as sa
import ujson
import jsonschema


class EngineObjectsModelBase(AbstractConcreteBase):
    __tablename__ = 'engine_objects'
    __swagger_json__ = get_swagger_json(__file__)

    id = sa.Column(sa.Integer, primary_key=True)
    type = sa.Column(sa.String(255), nullable=False)
    name = sa.Column(sa.String(255), unique=True, nullable=False)
    configuration_json = sa.Column(sa.Text, nullable=False)

    @declared_attr
    def strategy_id(cls):
        return sa.Column(sa.ForeignKey('engine_strategies.id'), nullable=False)

    @declared_attr
    def store_id(cls):
        return sa.Column(sa.ForeignKey('stores.id'), nullable=False)

    @declared_attr
    def item_type_id(cls):
        return sa.Column(sa.ForeignKey('item_types.id'), nullable=False)

    @declared_attr
    def strategy(cls):
        return sa.orm.relationship('EngineStrategiesModel')

    @declared_attr
    def item_type(cls):
        return sa.orm.relationship('ItemTypesModel')

    @declared_attr
    def store(cls):
        return sa.orm.relationship('StoresModel')

    @property
    def configuration(self):
        if not hasattr(self, '_configuration'):
            self._configuration = ujson.loads(self.configuration_json)
        return self._configuration

    async def _setattr(self, attr_name, value, session, input_):
        if attr_name == 'configuration':
            value = ujson.dumps(value)
            attr_name = 'configuration_json'

        if attr_name == 'strategy_id':
            value = {'id': value}
            attr_name = 'strategy'

        await super()._setattr(attr_name, value, session, input_)

    def _format_output_json(self, dict_inst, schema):
        if schema.get('configuration') is not False:
            dict_inst.pop('configuration_json')
            dict_inst['configuration'] = self.configuration

    async def _validate(self, session, input_):
        # disable validation when the operation was did by the engine model
        if (isinstance(input_, list) and 'objects' in input_[0]) or \
                (isinstance(input_, dict) and 'objects' in input_):
            return

        strategy_class = self.strategy.get_class()

        if self.type not in strategy_class.object_types:
            raise SwaggerItModelError(
                "Invalid object type '{}'".format(self.type),
                instance=input_
            )

        object_schema = strategy_class.configuration_schema['properties'][self.type]

        if 'definitions' in strategy_class.configuration_schema:
            object_schema['definitions'] = strategy_class.configuration_schema['definitions']

        jsonschema.validate(self.configuration, object_schema)
