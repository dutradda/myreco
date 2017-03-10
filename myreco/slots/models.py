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


from myreco.engines.cores.filters.factory import FiltersFactory
from swaggerit.utils import get_swagger_json
from swaggerit.exceptions import SwaggerItModelError
from jsonschema import ValidationError
from sqlalchemy.ext.declarative import AbstractConcreteBase, declared_attr
import sqlalchemy as sa
import ujson


class SlotsVariablesModelBase(AbstractConcreteBase):
    __tablename__ = 'slots_variables'
    __use_redis__ = False
    __id_names__ = ['id']

    # To autoincrement works a alter table with autoincrement is necessary
    id = sa.Column(sa.Integer, nullable=False, unique=True, autoincrement=True, index=True)
    override = sa.Column(sa.Boolean, default=False)
    override_value_json = sa.Column(sa.Text)
    engine_variable_name = sa.Column(sa.String(255), primary_key=True)

    @property
    def override_value(self):
        if not hasattr(self, '_override_value'):
            self._override_value = \
                ujson.loads(self.override_value_json) if self.override_value_json is not None else None
        return self._override_value

    async def _setattr(self, attr_name, value, session, input_):
        if attr_name == 'override_value':
            value = ujson.dumps(value)
            attr_name = 'override_value_json'

        await super()._setattr(attr_name, value, session, input_)

    def _format_output_json(self, dict_inst, schema):
        if schema.get('override_value') is not False:
            dict_inst.pop('override_value_json')
            dict_inst['override_value'] = self.override_value

    @declared_attr
    def external_variable_name(cls):
        return sa.Column(sa.ForeignKey('external_variables.name', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)

    @declared_attr
    def external_variable_store_id(cls):
        return sa.Column(sa.ForeignKey('external_variables.store_id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)

    @declared_attr
    def slot_id(cls):
        return sa.Column(sa.ForeignKey('slots.id', ondelete='CASCADE', onupdate='CASCADE'), primary_key=True)

    @declared_attr
    def external_variable(cls):
        return sa.orm.relationship('ExternalVariablesModel',
            foreign_keys=[cls.external_variable_name, cls.external_variable_store_id],
            primaryjoin='and_(SlotsVariablesModel.external_variable_name == ExternalVariablesModel.name, '\
                        'SlotsVariablesModel.external_variable_store_id == ExternalVariablesModel.store_id)')


class SlotsFiltersModelBase(AbstractConcreteBase):
    __tablename__ = 'slots_filters'
    __use_redis__ = False
    __factory__ = FiltersFactory
    __id_names__ = ['id']

    # To autoincrement works a alter table with autoincrement is necessary
    id = sa.Column(sa.Integer, nullable=False, unique=True, autoincrement=True, index=True)
    is_inclusive = sa.Column(sa.Boolean, default=True, primary_key=True)
    property_name = sa.Column(sa.String(255), nullable=False, primary_key=True)
    type_id = sa.Column(sa.String(255), nullable=False, primary_key=True)
    override = sa.Column(sa.Boolean, default=False)
    override_value_json = sa.Column(sa.Text)
    skip_values_json = sa.Column(sa.Text)

    @property
    def override_value(self):
        if not hasattr(self, '_override_value'):
            self._override_value = \
                ujson.loads(self.override_value_json) if self.override_value_json is not None else None
        return self._override_value

    @property
    def skip_values(self):
        if not hasattr(self, '_skip_values'):
            self._skip_values = \
                ujson.loads(self.skip_values_json) if self.skip_values_json is not None else None
        return self._skip_values

    async def _setattr(self, attr_name, value, session, input_):
        if attr_name == 'skip_values':
            value = ujson.dumps(value)
            attr_name = 'skip_values_json'

        if attr_name == 'override_value':
            value = ujson.dumps(value)
            attr_name = 'override_value_json'

        await super()._setattr(attr_name, value, session, input_)

    def _format_output_json(self, dict_inst, schema):
        if schema.get('skip_values') is not False:
            dict_inst.pop('skip_values_json')
            dict_inst['skip_values'] = self.skip_values

        if schema.get('override_value') is not False:
            dict_inst.pop('override_value_json')
            dict_inst['override_value'] = self.override_value

    @declared_attr
    def external_variable_name(cls):
        return sa.Column(sa.ForeignKey('external_variables.name', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)

    @declared_attr
    def external_variable_store_id(cls):
        return sa.Column(sa.ForeignKey('external_variables.store_id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)

    @declared_attr
    def slot_id(cls):
        return sa.Column(sa.ForeignKey('slots.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)

    @declared_attr
    def external_variable(cls):
        return sa.orm.relationship('ExternalVariablesModel',
            foreign_keys=[cls.external_variable_name, cls.external_variable_store_id],
            primaryjoin='and_(SlotsFiltersModel.external_variable_name == ExternalVariablesModel.name, '\
                        'SlotsFiltersModel.external_variable_store_id == ExternalVariablesModel.store_id)')

    @property
    def type(self):
        return type(self).__factory__.get_filter_type(self.type_id)


class SlotsModelBase(AbstractConcreteBase):
    __tablename__ = 'slots'
    __swagger_json__ = get_swagger_json(__file__)

    id = sa.Column(sa.Integer, primary_key=True)
    max_items = sa.Column(sa.Integer, nullable=False)
    name = sa.Column(sa.String(255), nullable=False)

    @declared_attr
    def engine_id(cls):
        return sa.Column(sa.ForeignKey('engines.id'), nullable=False)

    @declared_attr
    def store_id(cls):
        return sa.Column(sa.ForeignKey('stores.id'), nullable=False)

    @declared_attr
    def engine(cls):
        return sa.orm.relationship('EnginesModel')

    @declared_attr
    def slot_variables(cls):
        return sa.orm.relationship('SlotsVariablesModel', uselist=True, passive_deletes=True)

    @declared_attr
    def slot_filters(cls):
        return sa.orm.relationship('SlotsFiltersModel', uselist=True, passive_deletes=True)

    @declared_attr
    def fallbacks(cls):
        return sa.orm.relationship('SlotsModel',
                                   uselist=True, remote_side='SlotsModel.id',
                                   secondary='slots_fallbacks',
                                   primaryjoin='slots_fallbacks.c.slot_id == SlotsModel.id',
                                   secondaryjoin='slots_fallbacks.c.fallback_id == SlotsModel.id')

    async def init(self, session, input_=None, **kwargs):
        await super().init(session, input_=input_, **kwargs)
        self._validate_fallbacks(input_)
        self._validate_slot_variables(input_)
        self._validate_slot_filters(input_)

    def _validate_fallbacks(self, input_):
        for fallback in self.fallbacks:
            if fallback.id == self.id:
                raise SwaggerItModelError(
                    "a Engine Manager can't fallback itself", input_)

            if fallback.engine.item_type_id != self.engine.item_type_id:
                raise SwaggerItModelError(
                    "Cannot set a fallback with different items types", input_)

    def _validate_slot_variables(self, input_):
        if self.engine is not None:
            engine = self.engine.todict()
            engine_variables_set = set([var['name'] for var in engine['variables']])
            message = "Invalid slot variable with 'engine_variable_name' attribute value '{}'"
            schema = {'available_variables': engine['variables']}

            for slot_variable in self.slot_variables:
                var_name = slot_variable.engine_variable_name
                if var_name not in engine_variables_set:
                    raise ValidationError(
                        message.format(var_name),
                        instance=input_, schema=schema
                    )

    def _validate_slot_filters(self, input_):
        if self.engine is not None:
            engine = self.engine.todict()
            available_filters = engine['item_type']['available_filters']
            available_filters_set = set([filter_['name'] for filter_ in available_filters])
            schema = {'available_filters': available_filters}
            message = "Invalid slot filter with 'property_name' attribute value '{}'"

            for slot_filter in self.slot_filters:
                filter_name = slot_filter.property_name
                if filter_name not in available_filters_set:
                    raise ValidationError(
                        message.format(filter_name),
                        instance=input_, schema=schema
                    )

    async def _setattr(self, attr_name, value, session, input_):
        if attr_name == 'engine_id':
            value = {'id': value}
            attr_name = 'engine'

        if attr_name == 'slot_variables':
            for engine_var in value:
                if 'external_variable_id' in engine_var:
                    var = {'id': engine_var.pop('external_variable_id')}
                    engine_var['external_variable'] = var

        await super()._setattr(attr_name, value, session, input_)

    def _format_output_json(self, dict_inst, schema):
        if schema.get('fallbacks') is not False:
            for fallback in dict_inst.get('fallbacks'):
                fallback.pop('fallbacks')


def build_slots_fallbacks_table(metadata, **kwargs):
    return sa.Table("slots_fallbacks", metadata,
        sa.Column("slot_id", sa.Integer, sa.ForeignKey(
            "slots.id", ondelete='CASCADE', onupdate='CASCADE'), primary_key=True),
        sa.Column("fallback_id", sa.Integer, sa.ForeignKey(
            "slots.id", ondelete='CASCADE', onupdate='CASCADE'), primary_key=True),
        **kwargs)
