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
from swaggerit.json_builder import JsonBuilder
from swaggerit.exceptions import SwaggerItModelError
from myreco.utils import ModuleObjectLoader, get_items_model
from sqlalchemy.ext.declarative import AbstractConcreteBase, declared_attr
import random as random_
import sqlalchemy as sa
import hashlib
import ujson


class PlacementsModelBase(AbstractConcreteBase):
    __tablename__ = 'placements'
    __swagger_json__ = get_swagger_json(__file__)

    hash = sa.Column(sa.String(255), unique=True, nullable=False)
    small_hash = sa.Column(sa.String(255), primary_key=True)
    name = sa.Column(sa.String(255), nullable=False)
    ab_testing = sa.Column(sa.Boolean, default=False)
    show_details = sa.Column(sa.Boolean, default=True)
    distribute_items = sa.Column(sa.Boolean, default=False)

    @declared_attr
    def store_id(cls):
        return sa.Column(sa.ForeignKey('stores.id'), nullable=False)

    @declared_attr
    def variations(cls):
        return sa.orm.relationship('VariationsModel', uselist=True, passive_deletes=True)

    async def init(self, session, input_=None, **kwargs):
        await super().init(session, input_=input_, **kwargs)
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
    async def get_items(cls, req, session):
        placement = await cls._get_placement(req, session)
        if placement is None:
            return cls._build_recos_response(None)

        explict_fallbacks = req.query.pop('explict_fallbacks', False)
        input_external_variables = req.query
        show_details = req.query.pop('show_details', placement.get('show_details'))
        distribute_items = placement.get('distribute_items')
        recos = slots = []
        recos_key = 'slots'

        for slot in placement['variations'][0]['slots']:
            slot_recos = {'fallbacks': []}
            slot_recos['main'] = \
                await cls._get_recos_by_slot(slot, input_external_variables, session, show_details)

            await cls._get_fallbacks_recos(slot_recos, slot, input_external_variables, session, show_details)

            slot = {'name': slot['name'], 'item_type': slot['engine']['item_type']['name']}
            slot['items'] = slot_recos
            if slot_recos['main'] or slot_recos['fallbacks']:
                slots.append(slot)

        if not slots:
            return cls._build_recos_response(None)

        if not explict_fallbacks:
            for slot in slots:
                slot['items'] = cls._get_all_slot_recos(slot['items'])

            if distribute_items:
                recos_key = 'distributed_items'
                recos = cls._get_all_recos_from_slots(slots)
                recos = cls._distribute_items(recos)

        placement = {'name': placement['name'], 'small_hash': placement['small_hash']}
        placement[recos_key] = recos

        return cls._build_recos_response(placement)

    @classmethod
    async def _get_placement(cls, req, session):
        small_hash = req.path_params['small_hash']
        placements = await cls.get(session, {'small_hash': small_hash})

        if not placements:
            return None

        return placements[0]

    @classmethod
    async def _get_recos_by_slot(cls, slot, input_external_variables, session, show_details, max_items=None):
        try:
            engine = slot['engine']
            items_model = get_items_model(engine)
            engine_vars = cls._get_slot_variables(slot, input_external_variables)
            filters = cls._get_slot_filters(slot, input_external_variables, items_model)
            core_config = engine['core']['configuration']['core_module']
            core_instance = ModuleObjectLoader.load(core_config)(engine, items_model)
            max_items = slot['max_items'] if max_items is None else max_items

            return await core_instance.get_items(
                session, filters, max_items, show_details, **engine_vars)

        except Exception as error:
            cls._logger.debug('Slot:\n' + ujson.dumps(slot, indent=4))
            cls._logger.debug('Input Variables:\n' + ujson.dumps(input_external_variables, indent=4))
            raise error

    @classmethod
    def _get_slot_variables(cls, slot, input_external_variables):
        engine_vars = dict()

        for slot_var in slot['slot_variables']:
            var_name = slot_var['external_variable']['name']
            var_engine_name = slot_var['engine_variable_name']

            if var_name in input_external_variables:
                var_value = input_external_variables[var_name]
                schema = cls._get_external_variable_schema(slot, var_engine_name)

                if schema is not None:
                    engine_vars[var_engine_name] = JsonBuilder.build(var_value, schema)

        return engine_vars

    @classmethod
    def _get_external_variable_schema(cls, slot, var_name):
        for var in slot['engine']['variables']:
            if var['name'] == var_name:
                return var['schema']

    @classmethod
    def _get_slot_filters(cls, slot, input_external_variables, items_model):
        filters = dict()
        factory = cls.get_model('slots_filters').__factory__

        for slot_filter in slot['slot_filters']:
            var_name = slot_filter['external_variable']['name']
            prop_name = slot_filter['property_name']

            if var_name in input_external_variables:
                var_value = input_external_variables[var_name]
                filter_schema, input_schema = \
                    cls._get_filter_and_input_schema(slot['engine'], slot_filter)

                if filter_schema is not None and input_schema is not None:
                    filter_ = factory.make(items_model, slot_filter, filter_schema)
                    filters[filter_] = JsonBuilder.build(var_value, input_schema)

        return filters

    @classmethod
    def _get_filter_and_input_schema(cls, engine, slot_filter):
        for var in engine['item_type']['available_filters']:
            if var['name'] == slot_filter['property_name']:
                if slot_filter['type_id'] == 'item_property_value':
                    input_schema = {'type': 'array', 'items': {'type': 'string'}}

                elif var['schema'].get('type') != 'array':
                    input_schema = {'type': 'array', 'items': var['schema']}

                else:
                    input_schema = var['schema']

                filter_schema = var['schema']
                return filter_schema, input_schema

        return None, None

    @classmethod
    async def _get_fallbacks_recos(cls, slot_recos, slot, input_external_variables, session, show_details):
        if len(slot_recos['main']) != slot['max_items']:
            for fallback in slot['fallbacks']:
                fallbacks_recos_size = \
                    sum([len(fallback) for fallback in slot_recos['fallbacks']])
                max_items = slot['max_items'] - len(slot_recos['main']) - fallbacks_recos_size
                if max_items == 0:
                    break

                fallback_recos = await cls._get_recos_by_slot(
                    fallback, input_external_variables, session, show_details, max_items)
                all_recos = cls._get_all_slot_recos(slot_recos)
                fallback_recos = cls._unique_recos(fallback_recos, all_recos)
                slot_recos['fallbacks'].append(fallback_recos)

    @classmethod
    def _get_all_slot_recos(cls, slot_recos):
        all_recos = list(slot_recos['main'])
        [all_recos.extend(fallback_recos) for fallback_recos in slot_recos['fallbacks']]
        return all_recos

    @classmethod
    def _get_all_recos_from_slots(cls, slots):
        recos = []
        for slot in slots:
            for reco in slot['items']:
                reco['type'] = slot['item_type']

            recos.append(slot['items'])
        return recos

    @classmethod
    def _distribute_items(cls, recos_list, random=True):
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
    def _unique_recos(cls, recos, all_recos):
        unique = list()
        [unique.append(reco) for reco in recos if not all_recos.count(reco)]
        return unique

    @classmethod
    def _build_recos_response(cls, recos):
        if recos is None:
            return cls._build_response(404)
        else:
            headers = {'Content-Type': 'application/json'}
            return cls._build_response(200, body=cls._pack_obj(recos), headers=headers)


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
