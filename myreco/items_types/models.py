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


from myreco.items_types.items_model import ItemsModelBaseMeta, ItemsCollectionsModelBaseMeta
from falconswagger.models.orm.redis import ModelRedisFactory
from falconswagger.models.orm.redis_base import ModelRedisBase
from falconswagger.utils import get_model_schema, get_dir_path
from sqlalchemy.ext.declarative import AbstractConcreteBase, declared_attr
from jsonschema import ValidationError, Draft4Validator
from copy import deepcopy
import sqlalchemy as sa
import json


def build_item_key(name, store_id=None):
    name = name.lower().replace(' ', '_')
    if store_id:
        return '{}_{}'.format(name, store_id)
    return name


class ItemsTypesModelBase(AbstractConcreteBase):
    __tablename__ = 'items_types'
    __schema__ = get_model_schema(__file__)
    __build_items_models__ = True
    __schema_dir__ = get_dir_path(__file__)

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(255), unique=True, nullable=False)
    schema_json = sa.Column(sa.Text, nullable=False)

    @declared_attr
    def stores(cls):
        return sa.orm.relationship('StoresModel', uselist=True, secondary='items_types_stores')

    def _setattr(self, attr_name, value, session, input_):
        if attr_name == 'schema':
            self._validate_input(value)
            value = json.dumps(value)
            attr_name = 'schema_json'

        super()._setattr(attr_name, value, session, input_)

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
                dict_inst['schema'] = json.loads(dict_inst.pop('schema_json'))

                schema_properties = dict_inst['schema'].get('properties', {})
                schema_properties_names = sorted(schema_properties.keys())
                dict_inst['available_filters'] = \
                    [{'name': name, 'schema': schema_properties[name]} \
                        for name in schema_properties_names]

    @classmethod
    def associate_all_items(cls, session):
        items_types = cls.get(session)
        cls.associate_items(items_types)

    @classmethod
    def associate_items(cls, items_types):
        [cls._build_items_collections_model(item_type) for item_type in items_types]

    @classmethod
    def _build_items_collections_model(cls, item_type):
        if cls.__api__:
            name, schema = item_type['name'], item_type['schema']
            id_names = schema['id_names']
            class_name = cls._build_class_name(name)
            key = build_item_key(name)
            items_models = {store['id']: cls._build_items_model(item_type, store) \
                for store in item_type['stores']}
            attributes = {
                '__key__': key,
                '__schema__': cls._build_items_collections_schema(key, schema, id_names),
                '__models__': items_models,
                '__all_models__': cls.__all_models__,
                '__item_type__': item_type,
                '__authorizer__': cls.__authorizer__
            }
            items_collections = \
                cls._get_items_collections_metaclass()(class_name, (ModelRedisBase,), attributes)
            cls.__api__.associate_model(items_collections)

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
            ModelRedisFactory.make(class_name, key, id_names, metaclass=ItemsModelBaseMeta)
        items_model.__item_type__ = item_type
        return items_model

    @classmethod
    def _build_items_collections_schema(cls, key, schema, id_names):
        base_uri = '/{}'.format(key)
        id_names_uri = base_uri + '/' + '/'.join(['{{{}}}'.format(id_name) for id_name in id_names])
        patch_schema = deepcopy(schema)
        required = patch_schema.get('required')
        if required:
            patch_schema['required'] = [req for req in required if req in id_names]
        properties = patch_schema.get('properties')
        if properties:
            properties['_operation'] = {'enum': ['delete', 'update']}

        swagger_schema = {
            base_uri: {
                'parameters': [{
                    'name': 'Authorization',
                    'in': 'header',
                    'required': True,
                    'type': 'string'
                },{
                    'name': 'store_id',
                    'in': 'query',
                    'required': True,
                    'type': 'integer'
                }],
                'post': {
                    'parameters': [{
                        'name': 'body',
                        'in': 'body',
                        'required': True,
                        'schema': {
                            'type': 'array',
                            'minItems': 1,
                            'items': schema
                        }
                    }],
                    'operationId': 'post_by_body',
                    'responses': {'201': {'description': 'Created'}}
                },
                'patch': {
                    'parameters': [{
                        'name': 'body',
                        'in': 'body',
                        'required': True,
                        'schema': {
                            'type': 'array',
                            'minItems': 1,
                            'items': patch_schema
                        }
                    }],
                    'operationId': 'patch_by_body',
                    'responses': {'200': {'description': 'Updated'}}
                },
                'get': {
                    'parameters': [{
                        'name': 'page',
                        'in': 'query',
                        'type': 'integer',
                        'default': 1
                    },{
                        'name': 'items_per_page',
                        'in': 'query',
                        'type': 'integer',
                        'default': 1000
                    }],
                    'operationId': 'get_by_body',
                    'responses': {'200': {'description': 'Got'}}
                },
            },
            id_names_uri: {
                'parameters': [{
                    'name': 'Authorization',
                    'in': 'header',
                    'required': True,
                    'type': 'string'
                },{
                    'name': 'store_id',
                    'in': 'query',
                    'required': True,
                    'type': 'integer'
                }],
                'get': {
                    'operationId': 'get_by_uri_template',
                    'responses': {'200': {'description': 'Got'}}
                }
            }
        }

        base_uri_get_parameters = cls._build_id_names_parameters_schema(id_names, schema, 'query')
        swagger_schema[base_uri]['get']['parameters'].extend(base_uri_get_parameters)

        id_names_uri_parameters = cls._build_id_names_parameters_schema(id_names, schema, 'path')
        swagger_schema[id_names_uri]['parameters'].extend(id_names_uri_parameters)

        return swagger_schema

    @classmethod
    def _build_id_names_parameters_schema(cls, id_names, schema, in_):
        parameters = []
        for id_name in id_names:
            parameter = deepcopy(schema['properties'][id_name])
            parameter.update({
                'name': id_name,
                'in': in_
            })
            if in_ == 'path':
                parameter['required'] = True
            parameters.append(parameter)

        return parameters

    @classmethod
    def _get_items_collections_metaclass(cls):
        return ItemsCollectionsModelBaseMeta

    @classmethod
    def insert(cls, session, objs, commit=True, todict=True, **kwargs):
        objs = type(cls).insert(cls, session, objs, commit=commit, todict=todict, **kwargs)
        if cls.__build_items_models__:
            cls.associate_items(objs)

        return objs

    @classmethod
    def update(cls, session, objs, commit=True, todict=True, ids=None, **kwargs):
        old_items_types = cls.get(session, ids=ids)
        objs = type(cls).update(cls, session, objs, commit=commit, todict=todict, ids=ids, **kwargs)
        cls.reassociate_items(old_items_types, objs)
        return objs

    @classmethod
    def reassociate_items(cls, old_items_types, new_items_types):
        new_names = [item_type['name'] for item_type in new_items_types]
        old_items_types = \
            [item_type for item_type in old_items_types if item_type['name'] not in new_names]

        for item_type in old_items_types:
            cls._disassociate_item(item_type)

        for item_type in new_items_types:
            cls._disassociate_item(item_type)
            cls._build_items_collections_model(item_type)

    @classmethod
    def _disassociate_item(cls, item_type):
        name = item_type['name']
        if name in cls.__api__.models:
            cls.__api__.disassociate_model(cls.__api__.models[name])

    @classmethod
    def delete(cls, session, ids, commit=True, **kwargs):
        items_types = cls.get(session, ids=ids)
        type(cls).delete(cls, session, ids, commit=commit, **kwargs)
        [cls._disassociate_item(item_type) for item_type in items_types]

    @staticmethod
    def get_module_path():
        return get_dir_path(__file__)


def build_items_types_stores_table(metadata, **kwargs):
    return sa.Table(
        'items_types_stores', metadata,
        sa.Column('item_type_id', sa.Integer, sa.ForeignKey('items_types.id', ondelete='CASCADE', onupdate='CASCADE'), primary_key=True),
        sa.Column('store_id', sa.Integer, sa.ForeignKey('stores.id', ondelete='CASCADE', onupdate='CASCADE'), primary_key=True),
        **kwargs)
