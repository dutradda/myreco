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


from falconswagger.models.redis import RedisModelMeta, RedisModelBuilder
from falconswagger.models.base import get_model_schema
from sqlalchemy.ext.declarative import AbstractConcreteBase
from jsonschema import ValidationError
from copy import deepcopy
import sqlalchemy as sa
import json


class ItemsTypesModelBase(AbstractConcreteBase):
    __tablename__ = 'items_types'
    __schema__ = get_model_schema(__file__)
    __build_items_models__ = True

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(255), unique=True, nullable=False)
    id_names_json = sa.Column(sa.String(255), nullable=False)
    schema_json = sa.Column(sa.Text, nullable=False)

    def _setattr(self, attr_name, value, session, input_):
        if attr_name == 'id_names':
            value = json.dumps(value)
            attr_name = 'id_names_json'

        elif attr_name == 'schema':
            value = json.dumps(value)
            attr_name = 'schema_json'

        super()._setattr(attr_name, value, session, input_)

    def __setattr__(self, name, value):
        if name == 'id_names_json' and getattr(self, 'schema_json', None):
            id_names = json.loads(value)
            schema = json.loads(self.schema_json)
            self._validate_input(id_names, schema)

        elif name == 'schema_json' and getattr(self, 'id_names_json', None):
            schema = json.loads(value)
            id_names = json.loads(self.id_names_json)
            self._validate_input(id_names, schema)

        super().__setattr__(name, value)

    def _validate_input(self, id_names, schema):
        for id_name in id_names:
            if id_name not in schema.get('properties', {}):
                raise ValidationError(
                    "Invalid id_name '{}'".format(id_name),
                    instance=id_names,
                    schema=schema)

    def _format_output_json(self, dict_inst):
        if 'id_names_json' in dict_inst:
            dict_inst['id_names'] = json.loads(dict_inst.pop('id_names_json'))

        if 'schema_json' in dict_inst:
            dict_inst['schema'] = json.loads(dict_inst.pop('schema_json'))

        schema_properties = dict_inst['schema'].get('properties', {})
        schema_properties_names = sorted(schema_properties.keys())
        dict_inst['available_filters'] = [{'name': name, 'schema': schema_properties[name]} \
            for name in schema_properties_names]

    @classmethod
    def associate_all_items(cls, session):
        items_types = cls.get(session)
        cls.associate_items(items_types)

    @classmethod
    def associate_items(cls, items_types):
        [cls._build_item_model(item_type) for item_type in items_types]

    @classmethod
    def _build_item_model(cls, item_type):
        if cls.__api__:
            item_type['name'] = item_type['name'].lower().replace(' ', '_')
            schema = cls._build_item_model_schema(item_type)
            model = \
                RedisModelBuilder(
                    item_type['name'], item_type['id_names'], schema, metaclass=ItemsModelBaseMeta)
            cls.__api__.associate_model(model)

    @classmethod
    def _build_item_model_schema(cls, item_type):
        name, schema, id_names = item_type['name'], item_type['schema'], item_type['id_names']
        base_uri = '/{}'.format(name)
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
                        'type': 'integer'
                    },{
                        'name': 'items_per_page',
                        'in': 'query',
                        'type': 'integer'
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
            cls._build_item_model(item_type)

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


class ItemsModelBaseMeta(RedisModelMeta):

    def get(cls, session, ids=None, limit=None, offset=None, **kwargs):
        items_per_page, page = kwargs.get('items_per_page', 1000), kwargs.get('page', 1)
        limit = items_per_page * page
        offset = items_per_page * (page-1)
        return RedisModelMeta.get(cls, session, ids=ids, limit=limit, offset=offset, **kwargs)
