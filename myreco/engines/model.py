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


from swaggerit.utils import get_swagger_json
from swaggerit.exceptions import SwaggerItModelError
from sqlalchemy.ext.declarative import AbstractConcreteBase, declared_attr
import sqlalchemy as sa


class EnginesModelBase(AbstractConcreteBase):
    __tablename__ = 'engines'
    __swagger_json__ = get_swagger_json(__file__)
    _jobs = dict()
    __table_args__ = (sa.UniqueConstraint('name', 'store_id'),)

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(255), nullable=False)

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
    def item_type(cls):
        return sa.orm.relationship('ItemTypesModel')

    @declared_attr
    def store(cls):
        return sa.orm.relationship('StoresModel')

    @declared_attr
    def strategy(cls):
        return sa.orm.relationship('EngineStrategiesModel')

    @declared_attr
    def objects(cls):
        return sa.orm.relationship('EngineObjectsModel', uselist=True,
                                   secondary='engines_objects')

    @property
    def variables(self):
        return self.strategy_instance.get_variables()

    @property
    def strategy_instance(self):
        if not hasattr(self, '_strategy_instance'):
            self._strategy_instance = \
                type(self.strategy).get_instance(self._todict_when_new())

        return self._strategy_instance

    def _todict_when_new(self):
        objects = []
        self_dict = self.todict({'variables': False})
        strategy_dict = self_dict['strategy']

        for obj in self.objects:
            obj_dict = obj.todict()
            obj_dict['strategy'] = strategy_dict
            objects.append(obj_dict)

        self_dict['objects'] = objects
        return self_dict

    async def _setattr(self, attr_name, value, session, input_):
        if attr_name == 'item_type_id':
            value = {'id': value}
            attr_name = 'item_type'

        if attr_name == 'strategy_id':
            value = {'id': value}
            attr_name = 'strategy'

        if attr_name == 'store_id':
            value = {'id': value}
            attr_name = 'store'

        await super()._setattr(attr_name, value, session, input_)

    async def _validate(self, session, input_):
        strategy_class = self.strategy.get_class()

        for obj in self.objects:
            if obj.type not in strategy_class.object_types:
                raise SwaggerItModelError(
                    "Invalid object type '{}'".format(obj.type),
                    instance=input_
                )

        props_sequence = [
            ('strategy_id', self.strategy.id),
            ('item_type_id',self.item_type.id),
            ('store_id', self.store.id)
        ]

        for obj in self.objects:
            for obj_prop_name, self_prop_value in props_sequence:
                value = getattr(obj, obj_prop_name)

                if value is None:
                    setattr(obj, obj_prop_name, self_prop_value)

                elif value != self_prop_value:
                    raise SwaggerItModelError(
                    "Invalid object '{}' with value '{}'. "\
                    "This value must be the same as '{}'".format(
                        obj_prop_name,
                        value,
                        self_prop_value
                    ),
                    instance=input_
                )

        self.strategy_instance.validate_config()

    async def init(self, session, input_=None, **kwargs):
        for object_ in kwargs.get('objects', []):
            object_['item_type_id'] = kwargs.get('item_type_id', self.item_type_id)
            object_['strategy_id'] = kwargs.get('strategy_id', self.strategy_id)
            object_['store_id'] = kwargs.get('store_id', self.store_id)

        await super().init(session, input_=input_, **kwargs)


    def _format_output_json(self, dict_inst, schema):
        if schema.get('variables') is not False:
            dict_inst['variables'] = self.variables


def build_engines_objects_table(metadata, **kwargs):
    return sa.Table(
        'engines_objects', metadata,
        sa.Column('engine_id', sa.Integer, sa.ForeignKey('engines.id', onupdate='CASCADE', ondelete='CASCADE'), primary_key=True),
        sa.Column('object_id', sa.Integer, sa.ForeignKey('engine_objects.id', onupdate='CASCADE', ondelete='CASCADE'), primary_key=True),
        **kwargs)
