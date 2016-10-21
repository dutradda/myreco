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


from falconswagger.models.base import get_model_schema
from falconswagger.json_builder import JsonBuilder
from falconswagger.exceptions import ModelBaseError
from myreco.engines.types.base import EngineTypeChooser
from myreco.engines.types.filters.factory import FiltersFactory
from myreco.items_types.models import build_item_key
from falcon.errors import HTTPNotFound
from sqlalchemy.ext.declarative import AbstractConcreteBase, declared_attr
import sqlalchemy as sa
import hashlib
import json


class PlacementsModelBase(AbstractConcreteBase):
    __tablename__ = 'placements'

    hash = sa.Column(sa.String(255), primary_key=True)
    small_hash = sa.Column(sa.String(255), unique=True, nullable=False)
    name = sa.Column(sa.String(255), unique=True, nullable=False)
    ab_testing = sa.Column(sa.Boolean, default=False)

    @declared_attr
    def store_id(cls):
        return sa.Column(sa.ForeignKey('stores.id'), nullable=False)

    @declared_attr
    def variations(cls):
        return sa.orm.relationship('VariationsModel', uselist=True, passive_deletes=True)

    def __init__(self, session, input_=None, **kwargs):
        super().__init__(session, input_=input_, **kwargs)
        self._set_hash()

    def _set_hash(self):
        if self.name and not self.hash:
            hash_ = hashlib.new('ripemd160')
            hash_.update(self.name.encode())
            self.hash = hash_.hexdigest()

    def __setattr__(self, name, value):
        if name == 'hash':
            self.small_hash = value[:5]

        super().__setattr__(name, value)


class PlacementsModelRecommenderBase(PlacementsModelBase):
    __schema__ = get_model_schema(__file__)

    @classmethod
    def get_recommendations(cls, req, resp):
        placement = cls._get_placement(req, resp)
        recommendations = []
        session = req.context['session']
        input_variables = req.context['parameters']['query_string']

        for engine_manager in placement['variations'][0]['engines_managers']:
            engine = engine_manager['engine']
            items_model = cls._get_items_model(engine)
            engine_vars, filters = \
                cls._get_variables_and_filters(engine_manager, items_model, input_variables)
            engine_type = EngineTypeChooser(engine['type_name']['name'])(items_model=items_model)
            max_recos = engine_manager['max_recos']
            eng_recos = engine_type.get_recommendations(session, filters, max_recos, **engine_vars)
            recommendations.extend(eng_recos)

        if not recommendations:
            raise HTTPNotFound()

        resp.body = json.dumps(recommendations)

    @classmethod
    def _get_placement(cls, req, resp):
        small_hash = req.context['parameters']['uri_template']['small_hash']
        session = req.context['session']
        placements = cls.get(session, {'small_hash': small_hash})

        if not placements:
            raise HTTPNotFound()

        return placements[0]

    @classmethod
    def _get_variables_and_filters(cls, engine_manager, items_model, input_variables):
        engine_vars = dict()
        filters = dict()
        engine = engine_manager['engine']

        for engine_var in engine_manager['engine_variables']:
            var_name = engine_var['variable']['name']
            var_engine_name = engine_var['inside_engine_name']

            if var_name in input_variables:
                schema, schema_ids = cls._get_variable_schema(engine, engine_var)

                schema_ids = schema if schema_ids is None else schema_ids
                var_value = JsonBuilder(input_variables[var_name], schema_ids)

                if not engine_var['is_filter']:
                    engine_vars[var_engine_name] = var_value
                else:
                    filter_ = FiltersFactory.make(items_model, engine_var, schema)
                    filters[filter_] = var_value

        return engine_vars, filters

    @classmethod
    def _get_variable_schema(cls, engine, engine_var):
        if engine_var['is_filter']:
            variables = engine['item_type']['available_filters']
        else:
            variables = engine['variables']

        for var in variables:
            if var['name'] == engine_var['inside_engine_name']:
                if engine_var['is_filter'] and engine_var['filter_type'] == 'Property Of':
                    return var['schema'], cls._build_id_names_schema(engine)
                else:
                    return var['schema'], None

    @classmethod
    def _build_id_names_schema(cls, engine):
        return {
            'type': 'array',
            'items': [engine['item_type']['schema']['properties'][id_name] \
                for id_name in engine['item_type']['schema']['id_names']]
        }

    @classmethod
    def _get_items_model(cls, engine):
        items_types_model_key = build_item_key(engine['item_type']['name'])
        return cls.__api__.models[items_types_model_key].__models__[engine['store_id']]


class VariationsModelBase(AbstractConcreteBase):
    __tablename__ = 'variations'
    __use_redis__ = False

    id = sa.Column(sa.Integer, primary_key=True)
    weight = sa.Column(sa.Float)

    @declared_attr
    def placement_hash(cls):
        return sa.Column(sa.ForeignKey('placements.hash', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)

    @declared_attr
    def engines_managers(cls):
        return sa.orm.relationship('EnginesManagersModel',
                uselist=True, secondary='variations_engines_managers')


class ABTestUsersModelBase(AbstractConcreteBase):
    __tablename__ = 'ab_test_users'
    __use_redis__ = False

    id = sa.Column(sa.Integer, primary_key=True)

    @declared_attr
    def variation_id(cls):
        return sa.Column(sa.ForeignKey('variations.id'), nullable=False)


def build_variations_engines_managers_table(metadata, **kwargs):
    return sa.Table(
        'variations_engines_managers', metadata,
        sa.Column('variation_id', sa.Integer, sa.ForeignKey('variations.id', ondelete='CASCADE', onupdate='CASCADE'), primary_key=True),
        sa.Column('engine_manager_id', sa.Integer, sa.ForeignKey('engines_managers.id', ondelete='CASCADE', onupdate='CASCADE'), primary_key=True),
        **kwargs)
