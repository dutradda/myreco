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


from falconswagger.utils import get_model_schema
from falconswagger.exceptions import ModelBaseError
from jsonschema import ValidationError
from sqlalchemy.ext.declarative import AbstractConcreteBase, declared_attr
import sqlalchemy as sa
import json


class SlotsVariablesModelBase(AbstractConcreteBase):
    __tablename__ = 'slots_variables'
    __use_redis__ = False

    id = sa.Column(sa.Integer, primary_key=True)
    is_filter = sa.Column(sa.Boolean, default=False)
    filter_type = sa.Column(sa.String(255))
    is_inclusive_filter = sa.Column(sa.Boolean)
    override = sa.Column(sa.Boolean, default=False)
    override_value_json = sa.Column(sa.Text)
    inside_engine_name = sa.Column(sa.String(255), nullable=False)
    skip_values_json = sa.Column(sa.Text)

    @property
    def override_value(self):
        if not hasattr(self, '_override_value'):
            self._override_value = \
                json.loads(self.override_value_json) if self.override_value_json is not None else None
        return self._override_value

    @property
    def skip_values(self):
        if not hasattr(self, '_skip_values'):
            self._skip_values = \
                json.loads(self.skip_values_json) if self.skip_values_json is not None else None
        return self._skip_values

    def _setattr(self, attr_name, value, session, input_):
        if attr_name == 'skip_values':
            value = json.dumps(value)
            attr_name = 'skip_values_json'

        if attr_name == 'override_value':
            value = json.dumps(value)
            attr_name = 'override_value_json'

        super()._setattr(attr_name, value, session, input_)

    def _format_output_json(self, dict_inst, schema):
        if schema.get('skip_values') is not False:
            dict_inst.pop('skip_values_json')
            dict_inst['skip_values'] = self.skip_values

        if schema.get('override_value') is not False:
            dict_inst.pop('override_value_json')
            dict_inst['override_value'] = self.override_value

    @declared_attr
    def variable_name(cls):
        return sa.Column(sa.ForeignKey('variables.name', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)

    @declared_attr
    def variable_store_id(cls):
        return sa.Column(sa.ForeignKey('variables.store_id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)

    @declared_attr
    def slot_id(cls):
        return sa.Column(sa.ForeignKey('slots.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)

    @declared_attr
    def variable(cls):
        return sa.orm.relationship('VariablesModel',
            foreign_keys=[cls.variable_name, cls.variable_store_id],
            primaryjoin='and_(SlotsVariablesModel.variable_name == VariablesModel.name, '\
                        'SlotsVariablesModel.variable_store_id == VariablesModel.store_id)')


class SlotsModelBase(AbstractConcreteBase):
    __tablename__ = 'slots'
    __schema__ = get_model_schema(__file__)

    id = sa.Column(sa.Integer, primary_key=True)
    max_recos = sa.Column(sa.Integer, nullable=False)
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
    def fallbacks(cls):
        return sa.orm.relationship('SlotsModel',
                                   uselist=True, remote_side='SlotsModel.id',
                                   secondary='slots_fallbacks',
                                   primaryjoin='slots_fallbacks.c.slot_id == SlotsModel.id',
                                   secondaryjoin='slots_fallbacks.c.fallback_id == SlotsModel.id')

    def __init__(self, session, input_=None, **kwargs):
        super().__init__(session, input_=input_, **kwargs)
        self._validate_fallbacks(input_)
        self._validate_slot_variables(input_)

    def _validate_fallbacks(self, input_):
        for fallback in self.fallbacks:
            if fallback.id == self.id:
                raise ModelBaseError(
                    "a Engine Manager can't fallback itself", input_)

            if fallback.engine.item_type_id != self.engine.item_type_id:
                raise ModelBaseError(
                    "Cannot set a fallback with different items types", input_)

    def _validate_slot_variables(self, input_):
        if self.engine is not None:
            engine = self.engine.todict()

            for engine_variable in self.slot_variables:
                var_name = engine_variable.inside_engine_name
                engines_variables_map = {var['name']: var[
                    'schema'] for var in engine['variables']}
                available_filters_map = {fil['name']: fil['schema']
                                         for fil in engine['item_type']['available_filters']}
                key_func = lambda v: v['name']
                message = 'Invalid {}' + \
                    " with 'inside_engine_name' value '{}'".format(var_name)

                if not engine_variable.is_filter:
                    if var_name not in engines_variables_map:
                        message = message.format('engine variable')
                        schema = {'available_variables': sorted(
                            engine['variables'], key=key_func)}
                        raise ValidationError(
                            message, instance=input_, schema=schema)

                else:
                    if engine_variable.is_inclusive_filter is None \
                            or engine_variable.filter_type is None:
                        raise ModelBaseError(
                            "When 'is_filter' is 'true' the properties 'is_inclusive_filter'"
                            " and 'filter_type' must be setted", input_)

                    elif var_name not in available_filters_map:
                        message = message.format('filter')
                        schema = {
                            'available_filters':
                            sorted(engine['item_type'][
                                   'available_filters'], key=key_func)
                        }
                        raise ValidationError(
                            message, instance=input_, schema=schema)

    def _setattr(self, attr_name, value, session, input_):
        if attr_name == 'engine_id':
            value = {'id': value}
            attr_name = 'engine'

        if attr_name == 'slot_variables':
            for engine_var in value:
                if 'variable_id' in engine_var:
                    var = {'id': engine_var.pop('variable_id')}
                    engine_var['variable'] = var

        super()._setattr(attr_name, value, session, input_)

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
