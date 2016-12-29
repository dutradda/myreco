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


from myreco.items_types.items_model import (
    ItemsModelBaseMeta, ItemsModelCollection, build_items_model_collection_schema_base)
from myreco.utils import build_item_key
from swaggerit.models.orm.factory import FactoryOrmModels
from swaggerit.utils import get_model_schema, get_dir_path
from swaggerit.response import SwaggerResponse
from swaggerit.method import SwaggerMethod
from sqlalchemy.ext.declarative import AbstractConcreteBase, declared_attr
from jsonschema import ValidationError, Draft4Validator
from copy import deepcopy
import sqlalchemy as sa
import ujson


class ItemsTypesModelBase(AbstractConcreteBase):
    __tablename__ = 'items_types'
    __schema__ = get_model_schema(__file__)
    __schema_dir__ = get_dir_path(__file__)
    __items_models_colletions__ = dict()

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(255), unique=True, nullable=False)
    schema_json = sa.Column(sa.Text, nullable=False)

    @declared_attr
    def stores(cls):
        return sa.orm.relationship('StoresModel', uselist=True, secondary='items_types_stores')

    async def _setattr(self, attr_name, value, session, input_):
        if attr_name == 'schema':
            self._validate_input(value)
            value = ujson.dumps(value)
            attr_name = 'schema_json'

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

    @classmethod
    async def build_all_items_models_collections(cls, session):
        items_types = await cls.get(session)
        return cls._build_items_models_collections(items_types)

    @classmethod
    def _build_items_models_collections(cls, items_types):
        return [cls._build_items_model_collection(item_type) for item_type in items_types]

    @classmethod
    def _build_items_model_collection(cls, item_type):
        name, schema = item_type['name'], item_type['schema']
        id_names = schema['id_names']
        class_name = cls._build_class_name(name) + 'Collection'
        key = build_item_key(name, 'collection')
        [cls._build_items_model(item_type, store) for store in item_type['stores']]
        item_model_key = key.replace('_collection', '')
        items_model_collection_class = cls._get_items_model_collection_class()
        items_model_collection_class.__key__ = key
        base_uri = '/{}'.format(item_model_key)
        items_model_collection_class.__schema__ = \
            cls._build_items_model_collection_schema(base_uri, schema, id_names)
        items_model_collection_class.__item_type__ = item_type
        items_model_collection_class.__item_type_model__ = cls
        items_model_collection = items_model_collection_class()
        cls.__items_models_colletions__[item_model_key] = items_model_collection

        if cls.__api__ is not None:
            cls._set_items_model_collection_methods(items_model_collection, base_uri)
            cls.__api__.update_swagger_paths(items_model_collection)

        return items_model_collection

    @classmethod
    def _build_class_name(cls, *names):
        final_name = ''
        for name in names:
            name = name.split(' ')
            for in_name in name:
                final_name += in_name.capitalize()

        return final_name + 'Model'

    @classmethod
    def _build_items_model(cls, item_type, store):
        class_name = cls._build_class_name(item_type['name'], store['name'])
        key = build_item_key(item_type['name'], store['id'])
        id_names = item_type['schema']['id_names']
        items_model = \
            FactoryOrmModels.make_redis(class_name, id_names, key, metaclass=ItemsModelBaseMeta)
        items_model.__item_type__ = item_type
        return items_model

    @classmethod
    def _get_items_model_collection_class(cls):
        return ItemsModelCollection

    @classmethod
    def _build_items_model_collection_schema(cls, base_uri, schema, id_names):
        id_names_uri = base_uri + '/{item_key}'
        patch_schema = deepcopy(schema)
        required = patch_schema.get('required') # why it?
        if required:
            patch_schema['required'] = [req for req in required if req in id_names]
        properties = patch_schema.get('properties')
        if properties:
            properties['_operation'] = {'enum': ['delete', 'update']}

        swagger_schema = \
            build_items_model_collection_schema_base(base_uri, schema, patch_schema, id_names_uri)

        return swagger_schema

    @classmethod
    def _set_items_model_collection_methods(cls, items_model_collection, base_uri):
        for path, method, handler in cls.__api__.get_model_methods(items_model_collection):
            key = cls._get_items_model_colletion_method_key(path, (path == base_uri))
            items_model_collection.__methods__[key][method] = handler

    @classmethod
    def _get_items_model_colletion_method_key(cls, path, first_condition):
        if first_condition:
            key = 'base'
        elif path.endswith('import_data_file'):
            key = 'import_data_file'
        elif path.endswith('update_filters'):
            key = 'update_filters'
        else:
            key = 'id'

        return key

    @classmethod
    async def insert(cls, session, objs, commit=True, todict=True, **kwargs):
        objs = await type(cls).insert(cls, session, objs, commit=commit, todict=todict, **kwargs)
        cls._build_items_models_collections(objs)
        return objs

    @classmethod
    async def update(cls, session, objs, commit=True, todict=True, ids=None, **kwargs):
        old_items_types = await cls.get(session, ids=ids)
        objs = await type(cls).update(cls, session, objs, commit=commit, todict=todict, ids=ids, **kwargs)
        cls._rebuild_items_models_collections(old_items_types, objs)
        return objs

    @classmethod
    def _rebuild_items_models_collections(cls, old_items_types, new_items_types):
        new_names = [item_type['name'] for item_type in new_items_types]

        old_items_types_remove = \
            [item_type for item_type in old_items_types if item_type['name'] not in new_names]

        old_items_types_rebuild = \
            [item_type for item_type in old_items_types if item_type['name'] in new_names]

        old_names = [item_type['name']
            for item_type in old_items_types_remove+old_items_types_rebuild]

        new_items_types = \
            [item_type for item_type in new_items_types if item_type['name'] not in old_names]

        for item_type in old_items_types_remove:
            cls._remove_items_model_collection(item_type)

        for item_type in old_items_types_rebuild:
            cls._rebuild_items_model_collection(item_type)

        for item_type in new_items_types:
            cls._build_items_model_collection(item_type)

    @classmethod
    def _rebuild_items_model_collection(cls, item_type):
        cls._remove_items_model_collection(item_type)
        cls._build_items_model_collection(item_type)

    @classmethod
    def _remove_items_model_collection(cls, item_type):
        for store in item_type['stores']:
            key = build_item_key(item_type['name'], store['id'])
            type(cls).__all_models__.pop(key)

        model = type(cls).__all_models__.pop(build_item_key(item_type['name'], 'collection'))
        cls.__api__.remove_swagger_paths(model)

    @classmethod
    async def delete(cls, session, ids, commit=True, **kwargs):
        items_types = await cls.get(session, ids=ids)
        await type(cls).delete(cls, session, ids, commit=commit, **kwargs)
        [cls._remove_items_model_collection(item_type) for item_type in items_types]

    @classmethod
    async def items_models_handler(cls, req, session):
        items_model_name = req.path_params['items_model_name']
        items_model_collection = cls.__items_models_colletions__.get(items_model_name)

        if items_model_collection is None:
            methods = None
        else:
            key = cls._get_items_model_colletion_method_key(
                req.url, (not 'item_key' in req.path_params))
            methods = items_model_collection.__methods__.get(key)

        if methods is None:
            return SwaggerResponse(404)

        method = methods.get(req.method)

        if method is None:
            methods = [k.upper() for k in methods.keys()]
            return SwaggerResponse(405, headers={'Allow', ', '.join(methods)})

        return await method(req, session)



def build_items_types_stores_table(metadata, **kwargs):
    return sa.Table(
        'items_types_stores', metadata,
        sa.Column('item_type_id', sa.Integer, sa.ForeignKey('items_types.id', ondelete='CASCADE', onupdate='CASCADE'), primary_key=True),
        sa.Column('store_id', sa.Integer, sa.ForeignKey('stores.id', ondelete='CASCADE', onupdate='CASCADE'), primary_key=True),
        **kwargs)
