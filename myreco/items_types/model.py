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


from myreco.items_types._store_items_model_meta import _StoreItemsModelBaseMeta
from myreco.utils import build_item_key, build_class_name, ModuleObjectLoader
from myreco.engines.cores.filters.filters import BooleanFilterBy
from swaggerit.utils import get_swagger_json, get_dir_path
from swaggerit.method import SwaggerMethod
from swaggerit.models.orm.factory import FactoryOrmModels
from sqlalchemy.ext.declarative import declared_attr, AbstractConcreteBase
from jsonschema import ValidationError, Draft4Validator
from copy import deepcopy
import sqlalchemy as sa
import ujson


class _ItemsTypesModelBase(AbstractConcreteBase):
    __tablename__ = 'items_types'
    __swagger_json__ = get_swagger_json(__file__)
    __schema_dir__ = get_dir_path(__file__)

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(255), unique=True, nullable=False)
    schema_json = sa.Column(sa.Text, nullable=False)
    store_items_base_class_json = sa.Column(sa.Text)

    @declared_attr
    def stores(cls):
        return sa.orm.relationship('StoresModel', uselist=True, secondary='items_types_stores')

    @property
    def store_items_base_class(self):
        if not hasattr(self, '_store_items_base_class'):
            self._store_items_base_class = \
                ujson.loads(self.store_items_base_class_json) if self.store_items_base_class_json \
                    is not None else None
        return self._store_items_base_class

    async def _setattr(self, attr_name, value, session, input_):
        if attr_name == 'schema':
            self._validate_input(value)
            value = ujson.dumps(value)
            attr_name = 'schema_json'

        if attr_name == 'store_items_base_class':
            value = ujson.dumps(value)
            attr_name = 'store_items_base_class_json'

        await super()._setattr(attr_name, value, session, input_)

    def _validate_input(self, schema):
        Draft4Validator.check_schema(schema)

        for id_name in schema['id_names']:
            if id_name not in schema.get('properties', {}):
                raise ValidationError(
                    "id_name '{}' was not found in schema properties".format(id_name),
                    instance=schema['id_names'], schema=schema)

    def _format_output_json(self, dict_inst, todict_schema):
        if todict_schema.get('schema') is not False:
            if 'schema_json' in dict_inst:
                dict_inst['schema'] = ujson.loads(dict_inst.pop('schema_json'))

                schema_properties = dict_inst['schema'].get('properties', {})
                schema_properties_names = sorted(schema_properties.keys())
                dict_inst['available_filters'] = \
                    [{'name': name, 'schema': schema_properties[name]} \
                        for name in schema_properties_names]

        if todict_schema.get('store_items_base_class') is not False:
            dict_inst.pop('store_items_base_class_json')
            dict_inst['store_items_base_class'] = self.store_items_base_class


class _StoreItemsOperationsMixin(object):

    @classmethod
    async def swagger_insert_items(cls, req, session):
        store_items_model = await cls._get_store_items_model(req, session)
        if store_items_model is None:
            return cls._build_response(404)

        resp = await store_items_model.swagger_insert(req, session)
        await cls._set_stock_filter(store_items_model, session)
        return resp

    @classmethod
    async def _get_store_items_model(cls, req, session):
        id_ = req.path_params['id']
        store_id = req.query.pop('store_id')
        item_type = await cls._get_item_type(id_, store_id, session)
        if item_type is None:
            return None

        return cls.get_store_items_model(item_type, store_id)

    @classmethod
    async def _get_item_type(cls, id_, store_id, session):
        items_types = await cls.get(session, {'id': id_})
        if not items_types or not (items_types and cls._has_store(items_types[0], store_id)):
            return None

        return items_types[0]

    @classmethod
    def _has_store(cls, item_type, store_id):
        for store in item_type['stores']:
            if store['id'] == store_id:
                return True

        return False

    @classmethod
    def get_store_items_model(cls, item_type, store_id):
        store_items_model_key = build_item_key('store_items', item_type['name'], store_id)
        store_items_model = cls.get_model(store_items_model_key)

        if store_items_model is None:
            store_items_model = \
                cls._set_store_items_model(item_type, store_items_model_key, store_id)

        return store_items_model

    @classmethod
    def _set_store_items_model(cls, item_type, store_items_model_key, store_id):
        class_name = build_class_name(item_type['name'], str(store_id))
        base_class = cls._get_store_items_base_class(item_type)
        store_items_model = FactoryOrmModels.make_redis_elsearch(
            class_name, item_type['schema']['id_names'],
            store_items_model_key, use_elsearch=True,
            metaclass=_StoreItemsModelBaseMeta,
            base=base_class,
            extra_attributes={
                'insert_validator': cls._build_insert_validator(item_type),
                'update_validator': cls._build_update_validator(item_type),
                'item_type': item_type
            }
        )
        return store_items_model

    @classmethod
    def _get_store_items_base_class(cls, item_type):
        return ModuleObjectLoader.load({
            'path': item_type['store_items_base_class']['module'],
            'object_name': item_type['store_items_base_class']['class_name']
        }) if item_type['store_items_base_class'] else object

    @classmethod
    def _build_insert_validator(cls, item_type):
        return Draft4Validator({
            'type': 'array',
            'minItems': 1,
            'items': item_type['schema']
        })

    @classmethod
    def _build_update_validator(cls, item_type):
        schema = deepcopy(item_type['schema'])
        properties = schema.get('properties')
        if properties:
            properties['_operation'] = {'enum': ['delete', 'update']}

        return Draft4Validator({
            'type': 'array',
            'minItems': 1,
            'items': schema
        })

    @classmethod
    async def swagger_update_items(cls, req, session):
        store_items_model = await cls._get_store_items_model(req, session)
        if store_items_model is None:
            return cls._build_response(404)

        resp = await store_items_model.swagger_update_many(req, session)
        await cls._set_stock_filter(store_items_model, session)
        return resp

    @classmethod
    async def swagger_get_item(cls, req, session):
        store_items_model = await cls._get_store_items_model(req, session)
        if store_items_model is None:
            return cls._build_response(404)

        req = SwaggerRequest(
            req.url, req.method,
            path_params=req.path_params['item_key'],
            query=req.query,
            headers=req.headers,
            body=req.body,
            body_schema=req.body_schema,
            context=req.context
        )
        return await store_items_model.swagger_get(req, session)

    @classmethod
    async def swagger_get_all_items(cls, req, session):
        store_items_model = await cls._get_store_items_model(req, session)
        if store_items_model is None:
            return cls._build_response(404)

        return await store_items_model.swagger_get_all(req, session)

    @classmethod
    async def swagger_search_items(cls, req, session):
        store_items_model = await cls._get_store_items_model(req, session)
        if store_items_model is None:
            return cls._build_response(404)

        return await store_items_model.swagger_search(req, session)

    @classmethod
    async def _set_stock_filter(cls, store_items_model, session):
        items_indices_map_dict = await store_items_model.items_indices_map.get_all(session)
        items_indices_map_len = await store_items_model.items_indices_map.get_length(session)

        if items_indices_map_dict.values():
            items_keys = set(await session.redis_bind.hkeys(store_items_model.__key__))
            items_indices_keys = set(items_indices_map_dict.keys())
            remaining_keys = items_indices_keys.intersection(items_keys)
            old_keys = items_indices_keys.difference(items_keys)

            items = []
            cls._set_stock_item(
                store_items_model, remaining_keys,
                items_indices_map_dict, True, items
            )
            cls._set_stock_item(store_items_model, old_keys, items_indices_map_dict, False, items)

            stock_filter = BooleanFilterBy(store_items_model, 'stock')
            await stock_filter.update(session, items, items_indices_map_len)

    @classmethod
    def _set_stock_item(cls, store_items_model, keys, items_indices_map_dict, value, items):
        for key in keys:
            item = {}
            store_items_model.set_instance_ids(item, key)
            item.update({'stock': value, 'index': int(items_indices_map_dict[key])})
            items.append(item)


class ItemsTypesModelBase(_ItemsTypesModelBase, _StoreItemsOperationsMixin):
    pass


def build_items_types_stores_table(metadata, **kwargs):
    return sa.Table(
        'items_types_stores', metadata,
        sa.Column('item_type_id', sa.Integer, sa.ForeignKey('items_types.id', ondelete='CASCADE', onupdate='CASCADE'), primary_key=True),
        sa.Column('store_id', sa.Integer, sa.ForeignKey('stores.id', ondelete='CASCADE', onupdate='CASCADE'), primary_key=True),
        **kwargs)
