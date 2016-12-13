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
from falconswagger.json_builder import JsonBuilder
from falconswagger.exceptions import ModelBaseError
from myreco.engines.cores.filters.factory import FiltersFactory
from myreco.utils import ModuleClassLoader, get_items_model_from_api
from falcon.errors import HTTPNotFound
from sqlalchemy.ext.declarative import AbstractConcreteBase, declared_attr
import random as random_
import sqlalchemy as sa
import hashlib
import json


class PlacementsModelBase(AbstractConcreteBase):
    __tablename__ = 'placements'
    __schema__ = get_model_schema(__file__)

    hash = sa.Column(sa.String(255), unique=True, nullable=False)
    small_hash = sa.Column(sa.String(255), primary_key=True)
    name = sa.Column(sa.String(255), nullable=False)
    ab_testing = sa.Column(sa.Boolean, default=False)
    show_details = sa.Column(sa.Boolean, default=True)
    distribute_recos = sa.Column(sa.Boolean, default=False)

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
            hash_.update(self.name.encode() + bytes(self.store_id))
            self.hash = hash_.hexdigest()

    def __setattr__(self, name, value):
        if name == 'hash':
            self.small_hash = value[:5]

        super().__setattr__(name, value)

    @classmethod
    def get_recommendations(cls, req, resp):
        cls._get_recommendations(req, resp)

    @classmethod
    def get_slots(cls, req, resp):
        cls._get_recommendations(req, resp, True)

    @classmethod
    def _get_recommendations(cls, req, resp, by_slots=False):
        placement = cls._get_placement(req, resp)
        session = req.context['session']
        input_variables = req.context['parameters']['query_string']
        show_details = placement.get('show_details')
        distribute_recos = placement.get('distribute_recos')
        recos = []
        recos_key = 'slots' if by_slots else 'recommendations'

        for slot in placement['variations'][0]['slots']:
            slot_recos = {'fallbacks': []}
            slot_recos['main'] = \
                cls._get_recos_by_slot(slot, input_variables, session, show_details)

            cls._get_fallbacks_recos(slot_recos, slot, input_variables, session, show_details)

            if by_slots:
                slot = {'name': slot['name'], 'item_type': slot['engine']['item_type']['name']}
                slot['recommendations'] = slot_recos
                recos.append(slot)
            else:
                cls._get_recos(recos, slot_recos, slot, distribute_recos)

        if not recos:
            raise HTTPNotFound()

        if not by_slots:
            if distribute_recos:
                recos = cls._distribute_recos(recos)

            recos = cls._unique_recos(recos)

        placement = {'name': placement['name'], 'small_hash': placement['small_hash']}
        placement[recos_key] = recos

        resp.body = json.dumps(placement)

    @classmethod
    def _get_placement(cls, req, resp):
        small_hash = req.context['parameters']['path']['small_hash']
        session = req.context['session']
        placements = cls.get(session, {'small_hash': small_hash})

        if not placements:
            raise HTTPNotFound()

        return placements[0]

    @classmethod
    def _get_recos_by_slot(cls, slot, input_variables, session, show_details, max_recos=None):
        engine = slot['engine']
        items_model = get_items_model_from_api(cls.__api__, engine)
        engine_vars, filters = \
            cls._get_variables_and_filters(slot, items_model, input_variables)
        core_config = engine['core']['configuration']['core_module']
        core_instance = ModuleClassLoader.load(core_config)(engine, items_model)
        max_recos = slot['max_recos'] if max_recos is None else max_recos

        return core_instance.get_recommendations(
            session, filters, max_recos, show_details, **engine_vars)

    @classmethod
    def _get_variables_and_filters(cls, slot, items_model, input_variables):
        engine_vars = dict()
        filters = dict()
        engine = slot['engine']

        for engine_var in slot['slot_variables']:
            var_name = engine_var['variable']['name']
            var_engine_name = engine_var['inside_engine_name']

            if var_name in input_variables:
                schema, filter_input_schema = cls._get_variable_schema(engine, engine_var)

                filter_input_schema = schema if filter_input_schema is None else filter_input_schema
                var_value = JsonBuilder.build(input_variables[var_name], filter_input_schema)

                if not engine_var['is_filter']:
                    engine_vars[var_engine_name] = var_value
                else:
                    filter_ = FiltersFactory.make(items_model, engine_var, schema)
                    filters[filter_] = var_value

        return engine_vars, filters

    @classmethod
    def _get_variable_schema(cls, engine, engine_var):
        filter_input_schema = None

        if engine_var['is_filter']:
            if engine_var['filter_type'].endswith('Of'):
                filter_input_schema = {'type': 'array', 'items': engine['item_type']['schema']}

            variables = engine['item_type']['available_filters']
        else:
            variables = engine['variables']

        for var in variables:
            if var['name'] == engine_var['inside_engine_name']:
                if engine_var['is_filter'] and var['schema'].get('type') != 'array' \
                        and not engine_var['filter_type'].endswith('Of'):
                    filter_input_schema = {'type': 'array', 'items': var['schema']}

                return var['schema'], filter_input_schema

    @classmethod
    def _get_fallbacks_recos(cls, slot_recos, slot, input_variables, session, show_details):
        if len(slot_recos['main']) != slot['max_recos']:
            for fallback in slot['fallbacks']:
                fallbacks_recos_size = \
                    sum([len(fallback) for fallback in slot_recos['fallbacks']])
                max_recos = slot['max_recos'] - len(slot_recos['main']) - fallbacks_recos_size
                if max_recos == 0:
                    break

                fallback_recos = cls._get_recos_by_slot(
                    fallback, input_variables, session, show_details, max_recos)
                slot_recos['fallbacks'].append(fallback_recos)

    @classmethod
    def _get_recos(cls, recos, slot_recos, slot, distribute_recos):
        fallbacks_recos = []
        [fallbacks_recos.extend(recos) for recos in slot_recos['fallbacks']]
        slot_recos = slot_recos['main'] + fallbacks_recos

        for reco in slot_recos:
            reco['type'] = slot['engine']['item_type']['name']

        if distribute_recos:
            recos.append(slot_recos)
        else:
            recos.extend(slot_recos)

    @classmethod
    def _distribute_recos(cls, recos_list, random=True):
        total_length = sum([len(recos) for recos in recos_list])
        total_items = []
        initial_pos = 0

        for recos in recos_list:
            if not recos:
                continue

            step = int(total_length / len(recos))

            if random:
                initial_pos = random_.randint(0, step-1)

            positions = range(initial_pos, total_length, step)
            zip_items_positions = zip(recos, positions)
            total_items.extend(zip_items_positions)

        random_.shuffle(total_items)
        sorted_items = sorted(total_items, key=(lambda each: each[1]))

        return [i[0] for i in sorted_items]

    @classmethod
    def _unique_recos(cls, recos):
        unique = list()
        [unique.append(reco) for reco in recos if not unique.count(reco)]
        return unique

    @classmethod
    def _build_id_names_schema(cls, engine):
        return {
            'type': 'array',
            'items': [engine['item_type']['schema']['properties'][id_name] \
                for id_name in engine['item_type']['schema']['id_names']]
        }


class VariationsModelBase(AbstractConcreteBase):
    __tablename__ = 'variations'
    __use_redis__ = False

    id = sa.Column(sa.Integer, primary_key=True)
    weight = sa.Column(sa.Float)

    @declared_attr
    def placement_hash(cls):
        return sa.Column(sa.ForeignKey('placements.hash', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)

    @declared_attr
    def slots(cls):
        return sa.orm.relationship('SlotsModel',
                uselist=True, secondary='variations_slots')


class ABTestUsersModelBase(AbstractConcreteBase):
    __tablename__ = 'ab_test_users'
    __use_redis__ = False

    id = sa.Column(sa.Integer, primary_key=True)

    @declared_attr
    def variation_id(cls):
        return sa.Column(sa.ForeignKey('variations.id'), nullable=False)


def build_variations_slots_table(metadata, **kwargs):
    return sa.Table(
        'variations_slots', metadata,
        sa.Column('variation_id', sa.Integer, sa.ForeignKey('variations.id', ondelete='CASCADE', onupdate='CASCADE'), primary_key=True),
        sa.Column('slot_id', sa.Integer, sa.ForeignKey('slots.id', ondelete='CASCADE', onupdate='CASCADE'), primary_key=True),
        **kwargs)
