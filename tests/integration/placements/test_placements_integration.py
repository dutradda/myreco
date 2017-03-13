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


from tests.integration.fixtures import EngineCoreTestWithVars, EngineCoreTest
from swaggerit.models._base import _all_models
from tempfile import TemporaryDirectory
from time import sleep
from unittest import mock
import pytest
import ujson
import random
import asyncio
import tempfile
import zipfile
import os


@pytest.fixture
def temp_dir():
    dir_ = TemporaryDirectory()
    yield dir_
    dir_.cleanup()


@pytest.fixture
def init_db(models, session, api, temp_dir):
    user = {
        'name': 'test',
        'email': 'test',
        'password': 'test',
        'admin': True
    }
    session.loop.run_until_complete(models['users'].insert(session, user))

    store = {
        'name': 'test',
        'country': 'test',
        'configuration': {'data_path': temp_dir.name}
    }
    session.loop.run_until_complete(models['stores'].insert(session, store))

    schema = {
        'type': 'object',
        'id_names': ['item_id'],
        'properties': {
            'filter_test': {'type': 'string'},
            'item_id': {'type': 'integer'}
        }
    }

    item_type = {
        'name': 'products',
        'stores': [{'id': 1}],
        'schema': schema
    }
    session.loop.run_until_complete(models['item_types'].insert(session, item_type))
    item_type = {
        'name': 'categories',
        'stores': [{'id': 1}],
        'schema': schema
    }
    session.loop.run_until_complete(models['item_types'].insert(session, item_type))
    item_type = {
        'name': 'invalid',
        'stores': [{'id': 1}],
        'schema': {
            'type': 'object',
            'id_names': ['item_id'],
            'properties': {'item_id': {'type': 'string'}}
        }
    }
    session.loop.run_until_complete(models['item_types'].insert(session, item_type))
    item_type = {
        'name': 'new_products',
        'stores': [{'id': 1}],
        'store_items_class': {
            'module': 'tests.integration.fixtures',
            'class_name': 'MyProducts'
        },
        'schema': {
            'type': 'object',
            'id_names': ['item_id', 'sku'],
            'properties': {
                'filter_string': {'type': 'string'},
                'filter_integer': {'type': 'integer'},
                'filter_boolean': {'type': 'boolean'},
                'base_prop': {'type': 'integer'},
                'filter_array': {
                    'type': 'array',
                    'items': {'type': 'string'}
                },
                'filter_object': {
                    'type': 'object',
                    'id_names': ['id'],
                    'properties': {
                        'id': {'type': 'integer'}
                    }
                },
                'item_id': {'type': 'integer'},
                'sku': {'type': 'string'}
            }
        }
    }
    session.loop.run_until_complete(models['item_types'].insert(session, item_type))


    core = {
        'name': 'test with vars',
        'strategy_class': {
            'module': 'tests.integration.fixtures',
            'class_name': 'EngineCoreTestWithVars'
        }
    }
    session.loop.run_until_complete(models['engine_cores'].insert(session, core))

    core = {
        'name': 'test',
        'strategy_class': {
            'module': 'tests.integration.fixtures',
            'class_name': 'EngineCoreTest'
        }
    }
    session.loop.run_until_complete(models['engine_cores'].insert(session, core))

    engine = {
        'name': 'Visual Similarity',
        'configuration_json': ujson.dumps({
            'item_id_name': 'item_id',
            'aggregators_ids_name': 'filter_test',
            'data_importer_path': 'test.test'
        }),
        'store_id': 1,
        'item_type_id': 1,
        'core_id': 1
    }
    session.loop.run_until_complete(models['engines'].insert(session, engine))
    engine = {
        'name': 'Categories Visual Similarity',
        'configuration_json': ujson.dumps({
            'item_id_name': 'item_id',
            'aggregators_ids_name': 'filter_test'
        }),
        'store_id': 1,
        'item_type_id': 2,
        'core_id': 1
    }
    session.loop.run_until_complete(models['engines'].insert(session, engine))
    engine = {
        'name': 'Invalid Top Seller',
        'configuration_json': ujson.dumps({
            'days_interval': 7
        }),
        'store_id': 1,
        'item_type_id': 3,
        'core_id': 2
    }
    session.loop.run_until_complete(models['engines'].insert(session, engine))
    engine = {
        'name': 'Top Seller',
        'configuration_json': ujson.dumps({
            'days_interval': 7
        }),
        'store_id': 1,
        'item_type_id': 4,
        'core_id': 2
    }
    session.loop.run_until_complete(models['engines'].insert(session, engine))
    engine = {
        'name': 'With Fallback',
        'store_id': 1,
        'item_type_id': 4,
        'configuration': {
            'item_id_name': 'item_id',
            'aggregators_ids_name': 'filter_string',
            'data_importer_path': 'test.test'
        },
        'core_id': 1
    }
    session.loop.run_until_complete(models['engines'].insert(session, engine))

    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'test', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'test2', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'test3', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_string_inclusive', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_integer_inclusive', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_base_prop_inclusive', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_boolean_inclusive', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_array_inclusive', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_object_inclusive', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_string_exclusive', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_integer_exclusive', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_base_prop_exclusive', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_boolean_exclusive', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_array_exclusive', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_object_exclusive', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_string_inclusive_of', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_integer_inclusive_of', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_boolean_inclusive_of', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_array_inclusive_of', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_object_inclusive_of', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_string_exclusive_of', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_integer_exclusive_of', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_boolean_exclusive_of', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_array_exclusive_of', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_object_exclusive_of', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'index_inclusive_of', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'index_exclusive_of', 'store_id': 1}))

    slot = {
        'max_items': 10,
        'name': 'test',
        'store_id': 1,
        'engine_id': 1,
        'slot_variables': [{
            '_operation': 'insert',
            'external_variable_name': 'test2',
            'external_variable_store_id': 1,
            'engine_variable_name': 'item_id'
        }],
        'slot_filters': [{
            '_operation': 'insert',
            'external_variable_name': 'test',
            'external_variable_store_id': 1,
            'is_inclusive': True,
            'type_id': 'property_value',
            'property_name': 'filter_test'
        },{
            '_operation': 'insert',
            'external_variable_name': 'test3',
            'external_variable_store_id': 1,
            'is_inclusive': True,
            'type_id': 'item_property_value',
            'property_name': 'filter_test'
        }]
    }
    session.loop.run_until_complete(models['slots'].insert(session, slot))
    slot = {
        'max_items': 10,
        'name': 'test2',
        'store_id': 1,
        'engine_id': 4,
        'slot_filters': [{
            '_operation': 'insert',
            'external_variable_name': 'filter_string_inclusive',
            'external_variable_store_id': 1,
            'is_inclusive': True,
            'type_id': 'property_value',
            'property_name': 'filter_string'
        },{
            '_operation': 'insert',
            'external_variable_name': 'filter_integer_inclusive',
            'external_variable_store_id': 1,
            'is_inclusive': True,
            'type_id': 'property_value',
            'property_name': 'filter_integer'
        },{
            '_operation': 'insert',
            'external_variable_name': 'filter_base_prop_inclusive',
            'external_variable_store_id': 1,
            'is_inclusive': True,
            'type_id': 'property_value',
            'property_name': 'base_prop'
        },{
            '_operation': 'insert',
            'external_variable_name': 'filter_boolean_inclusive',
            'external_variable_store_id': 1,
            'is_inclusive': True,
            'type_id': 'property_value',
            'property_name': 'filter_boolean'
        },{
            '_operation': 'insert',
            'external_variable_name': 'filter_array_inclusive',
            'external_variable_store_id': 1,
            'is_inclusive': True,
            'type_id': 'property_value',
            'property_name': 'filter_array'
        },{
            '_operation': 'insert',
            'external_variable_name': 'filter_object_inclusive',
            'external_variable_store_id': 1,
            'is_inclusive': True,
            'type_id': 'property_value',
            'property_name': 'filter_object'
        },{
            '_operation': 'insert',
            'external_variable_name': 'filter_string_exclusive',
            'external_variable_store_id': 1,
            'is_inclusive': False,
            'type_id': 'property_value',
            'property_name': 'filter_string'
        },{
            '_operation': 'insert',
            'external_variable_name': 'filter_integer_exclusive',
            'external_variable_store_id': 1,
            'is_inclusive': False,
            'type_id': 'property_value',
            'property_name': 'filter_integer'
        },{
            '_operation': 'insert',
            'external_variable_name': 'filter_base_prop_exclusive',
            'external_variable_store_id': 1,
            'is_inclusive': False,
            'type_id': 'property_value',
            'property_name': 'base_prop'
        },{
            '_operation': 'insert',
            'external_variable_name': 'filter_boolean_exclusive',
            'external_variable_store_id': 1,
            'is_inclusive': False,
            'type_id': 'property_value',
            'property_name': 'filter_boolean'
        },{
            '_operation': 'insert',
            'external_variable_name': 'filter_array_exclusive',
            'external_variable_store_id': 1,
            'is_inclusive': False,
            'type_id': 'property_value',
            'property_name': 'filter_array'
        },{
            '_operation': 'insert',
            'external_variable_name': 'filter_object_exclusive',
            'external_variable_store_id': 1,
            'is_inclusive': False,
            'type_id': 'property_value',
            'property_name': 'filter_object'
        },{
            '_operation': 'insert',
            'external_variable_name': 'filter_string_inclusive_of',
            'external_variable_store_id': 1,
            'is_inclusive': True,
            'type_id': 'item_property_value',
            'property_name': 'filter_string'
        },{
            '_operation': 'insert',
            'external_variable_name': 'filter_integer_inclusive_of',
            'external_variable_store_id': 1,
            'is_inclusive': True,
            'type_id': 'item_property_value',
            'property_name': 'filter_integer'
        },{
            '_operation': 'insert',
            'external_variable_name': 'filter_boolean_inclusive_of',
            'external_variable_store_id': 1,
            'is_inclusive': True,
            'type_id': 'item_property_value',
            'property_name': 'filter_boolean'
        },{
            '_operation': 'insert',
            'external_variable_name': 'filter_array_inclusive_of',
            'external_variable_store_id': 1,
            'is_inclusive': True,
            'type_id': 'item_property_value',
            'property_name': 'filter_array'
        },{
            '_operation': 'insert',
            'external_variable_name': 'filter_object_inclusive_of',
            'external_variable_store_id': 1,
            'is_inclusive': True,
            'type_id': 'item_property_value',
            'property_name': 'filter_object'
        },{
            '_operation': 'insert',
            'external_variable_name': 'filter_string_exclusive_of',
            'external_variable_store_id': 1,
            'is_inclusive': False,
            'type_id': 'item_property_value',
            'property_name': 'filter_string'
        },{
            '_operation': 'insert',
            'external_variable_name': 'filter_integer_exclusive_of',
            'external_variable_store_id': 1,
            'is_inclusive': False,
            'type_id': 'item_property_value',
            'property_name': 'filter_integer'
        },{
            '_operation': 'insert',
            'external_variable_name': 'filter_boolean_exclusive_of',
            'external_variable_store_id': 1,
            'is_inclusive': False,
            'type_id': 'item_property_value',
            'property_name': 'filter_boolean'
        },{
            '_operation': 'insert',
            'external_variable_name': 'filter_array_exclusive_of',
            'external_variable_store_id': 1,
            'is_inclusive': False,
            'type_id': 'item_property_value',
            'property_name': 'filter_array'
        },{
            '_operation': 'insert',
            'external_variable_name': 'filter_object_exclusive_of',
            'external_variable_store_id': 1,
            'is_inclusive': False,
            'type_id': 'item_property_value',
            'property_name': 'filter_object'
        },{
            '_operation': 'insert',
            'external_variable_name': 'index_inclusive_of',
            'external_variable_store_id': 1,
            'is_inclusive': True,
            'type_id': 'property_value_index',
            'property_name': 'sku'
        },{
            '_operation': 'insert',
            'external_variable_name': 'index_exclusive_of',
            'external_variable_store_id': 1,
            'is_inclusive': False,
            'type_id': 'property_value_index',
            'property_name': 'sku'
        }]
    }
    session.loop.run_until_complete(models['slots'].insert(session, slot))
    slot = {
        'max_items': 10,
        'name': 'test',
        'store_id': 1,
        'engine_id': 5,
        'fallbacks': [{'id': 2}]
    }
    session.loop.run_until_complete(models['slots'].insert(session, slot))

    yield None

    _all_models.pop('store_items_products_1')
    _all_models.pop('store_items_categories_1')
    _all_models.pop('store_items_invalid_1')
    _all_models.pop('store_items_new_products_1')


class TestPlacementsModelPost(object):

    async def test_post_without_body(self, init_db, client, headers):
        client = await client
        resp = await client.post('/placements/', headers=headers)
        assert resp.status == 400
        assert (await resp.json()) == {'message': 'Request body is missing'}

    async def test_post_with_invalid_body(self, init_db, client, headers):
        client = await client
        resp = await client.post('/placements/', headers=headers, data='[{}]')
        assert resp.status == 400
        assert (await resp.json()) ==  {
            'message': "'name' is a required property. "\
                       "Failed validating instance['0'] for schema['items']['required']",
            'schema': {
                'type': 'object',
                'additionalProperties': False,
                'required': ['name', 'variations', 'store_id'],
                'properties': {
                    'ab_testing': {'type': 'boolean'},
                    'show_details': {'type': 'boolean'},
                    'distribute_items': {'type': 'boolean'},
                    'name': {'type': 'string'},
                    'store_id': {'type': 'integer'},
                    'variations': {'$ref': '#/definitions/PlacementsModel.variations'}
                }
            }
        }

    async def test_post_valid(self, init_db, client, headers, temp_dir):
        client = await client
        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 1}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        assert resp.status == 201
        assert (await resp.json()) ==  [{
            'ab_testing': False,
            'show_details': True,
            'distribute_items': False,
            'hash': '941e021d7ae6ca23f8969870ffe48b87a315e05c',
            'name': 'Placement Test',
            'small_hash': '941e0',
            'store_id': 1,
            'variations': [{
                'slots': [{
                    'max_items': 10,
                    'name': 'test',
                    'engine': {
                        'configuration': {
                            'aggregators_ids_name': 'filter_test',
                            'item_id_name': 'item_id',
                            'data_importer_path': 'test.test'
                        },
                        'id': 1,
                        'item_type': {
                            'id': 1,
                            'store_items_class': None,
                            'stores': [{
                                'configuration': {'data_path': temp_dir.name},
                                'country': 'test',
                                'id': 1,
                                'name': 'test'
                            }],
                            'schema': {
                                'type': 'object',
                                'id_names': ['item_id'],
                                'properties': {
                                    'filter_test': {'type': 'string'},
                                    'item_id': {'type': 'integer'}
                                }
                            },
                            'available_filters': [{
                                'name': 'filter_test',
                                'schema': {'type': 'string'}
                            },{
                                'name': 'item_id',
                                'schema': {'type': 'integer'}
                            }],
                            'name': 'products'
                        },
                        'item_type_id': 1,
                        'name': 'Visual Similarity',
                        'store': {
                            'country': 'test',
                            'id': 1,
                            'name': 'test',
                            'configuration': {'data_path': temp_dir.name}
                        },
                        'store_id': 1,
                        'core_id': 1,
                        'core': {
                            'id': 1,
                            'name': 'test with vars',
                            'strategy_class': {
                                'module': 'tests.integration.fixtures',
                                'class_name': 'EngineCoreTestWithVars'
                            }
                        },
                        'variables': [{
                            'name': 'item_id', 'schema': {'type': 'integer'}
                        },{
                            'name': 'filter_test', 'schema': {'type': 'string'}
                        }],
                    },
                    'engine_id': 1,
                    'slot_variables': [{
                        'slot_id': 1,
                        'id': 1,
                        'engine_variable_name': 'item_id',
                        'override': False,
                        'override_value': None,
                        'external_variable': {
                            'name': 'test2',
                            'store_id': 1
                        },
                        'external_variable_name': 'test2',
                        'external_variable_store_id': 1
                    }],
                    'slot_filters': [{
                        'is_inclusive': True,
                        'type_id': 'item_property_value',
                        'slot_id': 1,
                        'id': 2,
                        'property_name': 'filter_test',
                        'override': False,
                        'override_value': None,
                        'skip_values': None,
                        'external_variable': {
                            'name': 'test3',
                            'store_id': 1
                        },
                        'external_variable_name': 'test3',
                        'external_variable_store_id': 1
                    },{
                        'is_inclusive': True,
                        'type_id': 'property_value',
                        'slot_id': 1,
                        'id': 1,
                        'property_name': 'filter_test',
                        'override': False,
                        'override_value': None,
                        'skip_values': None,
                        'external_variable': {
                            'name': 'test',
                            'store_id': 1
                        },
                        'external_variable_name': 'test',
                        'external_variable_store_id': 1
                    }],
                    'fallbacks': [],
                    'id': 1,
                    'store_id': 1
                }],
                'id': 1,
                'placement_hash': '941e021d7ae6ca23f8969870ffe48b87a315e05c',
                'weight': None
            }]
        }]

    async def test_post_with_invalid_grant(self, init_db, client):
        client = await client
        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 1}]
            }]
        }]
        resp = await client.post('/placements/', headers={'Authorization': 'invalid'}, data=ujson.dumps(body))
        assert resp.status == 401
        assert (await resp.json()) ==  {'message': 'Invalid authorization'}



class TestPlacementsModelGet(object):

    async def test_get_not_found(self, init_db, client, headers_without_content_type):
        client = await client
        resp = await client.get('/placements/?store_id=1', headers=headers_without_content_type)
        assert resp.status == 404

    async def test_get_invalid_with_body(self, init_db, client, headers):
        client = await client
        resp = await client.get('/placements/?store_id=1', headers=headers, data='{}')
        assert resp.status == 400
        assert (await resp.json()) == {'message': 'Request body is not acceptable'}

    async def test_get(self, init_db, client, headers, temp_dir, headers_without_content_type):
        client = await client
        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 1}]
            }]
        }]
        await client.post('/placements/', headers=headers, data=ujson.dumps(body))

        resp = await client.get('/placements/?store_id=1', headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json()) ==  [{
            'ab_testing': False,
            'show_details': True,
            'distribute_items': False,
            'hash': '941e021d7ae6ca23f8969870ffe48b87a315e05c',
            'name': 'Placement Test',
            'small_hash': '941e0',
            'store_id': 1,
            'variations': [{
                'slots': [{
                    'max_items': 10,
                    'name': 'test',
                    'engine': {
                        'configuration': {
                            'aggregators_ids_name': 'filter_test',
                            'item_id_name': 'item_id',
                            'data_importer_path': 'test.test'
                        },
                        'id': 1,
                        'item_type': {
                            'id': 1,
                            'store_items_class': None,
                            'stores': [{
                                'configuration': {'data_path': temp_dir.name},
                                'country': 'test',
                                'id': 1,
                                'name': 'test'
                            }],
                            'schema': {
                                'type': 'object',
                                'id_names': ['item_id'],
                                'properties': {
                                    'filter_test': {'type': 'string'},
                                    'item_id': {'type': 'integer'}
                                }
                            },
                            'available_filters': [{
                                'name': 'filter_test',
                                'schema': {'type': 'string'}
                            },{
                                'name': 'item_id',
                                'schema': {'type': 'integer'}
                            }],
                            'name': 'products'
                        },
                        'item_type_id': 1,
                        'name': 'Visual Similarity',
                        'store': {
                            'country': 'test',
                            'id': 1,
                            'name': 'test',
                            'configuration': {'data_path': temp_dir.name}
                        },
                        'store_id': 1,
                        'core_id': 1,
                        'core': {
                            'id': 1,
                            'name': 'test with vars',
                            'strategy_class': {
                                'module': 'tests.integration.fixtures',
                                'class_name': 'EngineCoreTestWithVars'
                            }
                        },
                        'variables': [{
                            'name': 'item_id', 'schema': {'type': 'integer'}
                        },{
                            'name': 'filter_test', 'schema': {'type': 'string'}
                        }],
                    },
                    'engine_id': 1,
                    'slot_variables': [{
                        'slot_id': 1,
                        'id': 1,
                        'engine_variable_name': 'item_id',
                        'override': False,
                        'override_value': None,
                        'external_variable': {
                            'name': 'test2',
                            'store_id': 1
                        },
                        'external_variable_name': 'test2',
                        'external_variable_store_id': 1
                    }],
                    'slot_filters': [{
                        'is_inclusive': True,
                        'type_id': 'item_property_value',
                        'slot_id': 1,
                        'id': 2,
                        'property_name': 'filter_test',
                        'override': False,
                        'override_value': None,
                        'skip_values': None,
                        'external_variable': {
                            'name': 'test3',
                            'store_id': 1
                        },
                        'external_variable_name': 'test3',
                        'external_variable_store_id': 1
                    },{
                        'is_inclusive': True,
                        'type_id': 'property_value',
                        'slot_id': 1,
                        'id': 1,
                        'property_name': 'filter_test',
                        'override': False,
                        'override_value': None,
                        'skip_values': None,
                        'external_variable': {
                            'name': 'test',
                            'store_id': 1
                        },
                        'external_variable_name': 'test',
                        'external_variable_store_id': 1
                    }],
                    'fallbacks': [],
                    'id': 1,
                    'store_id': 1
                }],
                'id': 1,
                'placement_hash': '941e021d7ae6ca23f8969870ffe48b87a315e05c',
                'weight': None
            }]
        }]


class TestPlacementsModelUriTemplatePatch(object):

    async def test_patch_without_body(self, init_db, client, headers):
        client = await client
        resp = await client.patch('/placements/1/', headers=headers, data='')
        assert resp.status == 400
        assert (await resp.json()) == {'message': 'Request body is missing'}

    async def test_patch_with_invalid_body(self, init_db, client, headers):
        client = await client
        resp = await client.patch('/placements/1/', headers=headers, data='{}')
        assert resp.status == 400
        assert (await resp.json()) == {
            'message': '{} does not have enough properties. '\
                       "Failed validating instance for schema['minProperties']",
            'schema': {
                'additionalProperties': False,
                'minProperties': 1,
                'properties': {
                    'ab_testing': {'type': 'boolean'},
                    'show_details': {'type': 'boolean'},
                    'distribute_items': {'type': 'boolean'},
                    'name': {'type': 'string'},
                    'store_id': {'type': 'integer'},
                    'variations': {'$ref': '#/definitions/PlacementsModel.variations'}
                },
                'type': 'object'
            }
        }

    async def test_patch_valid(self, init_db, client, headers):
        client = await client
        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 1}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        body = {
            'variations': [{
                '_operation': 'update',
                'id': 1,
                'slots': [{'id': 1, '_operation': 'remove'}]
            }]
        }
        resp = await client.patch('/placements/{}/'.format(obj['small_hash']),
            headers=headers, data=ujson.dumps(body))

        assert resp.status == 200
        assert (await resp.json()) ==  {
            'ab_testing': False,
            'show_details': True,
            'distribute_items': False,
            'hash': '941e021d7ae6ca23f8969870ffe48b87a315e05c',
            'name': 'Placement Test',
            'small_hash': '941e0',
            'store_id': 1,
            'variations': [{
                'slots': [],
                'id': 1,
                'placement_hash': '941e021d7ae6ca23f8969870ffe48b87a315e05c',
                'weight': None
            }]
        }


class TestPlacementsModelUriTemplateDelete(object):

    async def test_delete_with_body(self, init_db, client, headers):
        client = await client
        resp = await client.delete('/placements/1/', headers=headers, data='{}')
        assert resp.status == 400
        assert (await resp.json()) == {'message': 'Request body is not acceptable'}

    async def test_delete_valid(self, init_db, client, headers, headers_without_content_type):
        client = await client
        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 1}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200

        resp = await client.delete('/placements/{}/'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 204

        resp = await client.get('/placements/{}/'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 404


class TestPlacementsModelUriTemplateGet(object):

    async def test_get_with_body(self, init_db, client, headers):
        client = await client
        resp = await client.get('/placements/1/', headers=headers, data='{}')
        assert resp.status == 400
        assert (await resp.json()) == {'message': 'Request body is not acceptable'}

    async def test_get_not_found(self, init_db, client, headers_without_content_type):
        client = await client
        resp = await client.get('/placements/1/', headers=headers_without_content_type)
        assert resp.status == 404

    async def test_get_valid(self, init_db, client, headers, temp_dir, headers_without_content_type):
        client = await client
        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 1}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/'.format(obj['small_hash']), headers=headers_without_content_type)

        assert resp.status == 200
        assert (await resp.json()) == {
            'ab_testing': False,
            'show_details': True,
            'distribute_items': False,
            'hash': '941e021d7ae6ca23f8969870ffe48b87a315e05c',
            'name': 'Placement Test',
            'small_hash': '941e0',
            'store_id': 1,
            'variations': [{
                'slots': [{
                    'max_items': 10,
                    'name': 'test',
                    'engine': {
                        'configuration': {
                            'aggregators_ids_name': 'filter_test',
                            'item_id_name': 'item_id',
                            'data_importer_path': 'test.test'
                        },
                        'id': 1,
                        'item_type': {
                            'id': 1,
                            'store_items_class': None,
                            'stores': [{
                                'configuration': {'data_path': temp_dir.name},
                                'country': 'test',
                                'id': 1,
                                'name': 'test'
                            }],
                            'schema': {
                                'type': 'object',
                                'id_names': ['item_id'],
                                'properties': {
                                    'filter_test': {'type': 'string'},
                                    'item_id': {'type': 'integer'}
                                }
                            },
                            'available_filters': [{
                                'name': 'filter_test',
                                'schema': {'type': 'string'}
                            },{
                                'name': 'item_id',
                                'schema': {'type': 'integer'}
                            }],
                            'name': 'products'
                        },
                        'item_type_id': 1,
                        'name': 'Visual Similarity',
                        'store': {
                            'country': 'test',
                            'id': 1,
                            'name': 'test',
                            'configuration': {'data_path': temp_dir.name}
                        },
                        'store_id': 1,
                        'core_id': 1,
                        'core': {
                            'id': 1,
                            'name': 'test with vars',
                            'strategy_class': {
                                'module': 'tests.integration.fixtures',
                                'class_name': 'EngineCoreTestWithVars'
                            }
                        },
                        'variables': [{
                            'name': 'item_id', 'schema': {'type': 'integer'}
                        },{
                            'name': 'filter_test', 'schema': {'type': 'string'}
                        }],
                    },
                    'engine_id': 1,
                    'slot_variables': [{
                        'slot_id': 1,
                        'id': 1,
                        'engine_variable_name': 'item_id',
                        'override': False,
                        'override_value': None,
                        'external_variable': {
                            'name': 'test2',
                            'store_id': 1
                        },
                        'external_variable_name': 'test2',
                        'external_variable_store_id': 1
                    }],
                    'slot_filters': [{
                        'is_inclusive': True,
                        'type_id': 'item_property_value',
                        'slot_id': 1,
                        'id': 2,
                        'property_name': 'filter_test',
                        'override': False,
                        'override_value': None,
                        'skip_values': None,
                        'external_variable': {
                            'name': 'test3',
                            'store_id': 1
                        },
                        'external_variable_name': 'test3',
                        'external_variable_store_id': 1
                    },{
                        'is_inclusive': True,
                        'type_id': 'property_value',
                        'slot_id': 1,
                        'id': 1,
                        'property_name': 'filter_test',
                        'override': False,
                        'override_value': None,
                        'skip_values': None,
                        'external_variable': {
                            'name': 'test',
                            'store_id': 1
                        },
                        'external_variable_name': 'test',
                        'external_variable_store_id': 1
                    }],
                    'fallbacks': [],
                    'id': 1,
                    'store_id': 1
                }],
                'id': 1,
                'placement_hash': '941e021d7ae6ca23f8969870ffe48b87a315e05c',
                'weight': None
            }]
        }


@pytest.fixture
def random_patch(monkeypatch):
    monkeypatch.setattr('swaggerit.models.orm._jobs_meta.random.getrandbits',
        mock.MagicMock(return_value=131940827655846590526331314439483569710))


def CoroMock():
    coro = mock.MagicMock(name="CoroutineResult")
    corofunc = mock.MagicMock(name="CoroutineFunction", side_effect=asyncio.coroutine(coro))
    corofunc.coro = coro
    return corofunc


class TestPlacementsGetRecomendations(object):

    async def test_get_items_not_found(self, init_db, client, headers, headers_without_content_type):
        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        client = await client
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 404

    async def test_get_items_placement_not_found(self, init_db, client, headers_without_content_type):
        client = await client
        resp = await client.get('/placements/123/items', headers=headers_without_content_type)
        assert resp.status == 404

    async def test_get_items_with_external_variable_valid(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        client = await client
        class_loader = mock.MagicMock()
        monkeypatch.setattr('myreco.item_types.model.ModuleObjectLoader', class_loader)
        monkeypatch.setattr('myreco.engines.model.ModuleObjectLoader', class_loader)

        class_loader.load()().get_items = CoroMock()
        class_loader.load()().get_items.coro.return_value = [{'id': 1}, {'id': 2}, {'id': 3}]
        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 1}]
            }]
        }]
        class_loader.load()().get_variables.return_value = \
            [{'name': 'item_id', 'schema': {'type': 'integer'}}]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?test2=1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert class_loader.load()().get_items.call_count == 1
        assert class_loader.load()().get_items.call_args_list[0][1] == {'item_id': 1}

    async def test_get_items_valid(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1'
        },{
            'item_id': 2,
            'sku': 'test2'
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [
            {'sku': 'test1', 'item_id': 1},
            {'sku': 'test3', 'item_id': 3},
            {'sku': 'test2', 'item_id': 2}
        ]

    async def test_get_items_with_fallback(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1'
        },{
            'item_id': 2,
            'sku': 'test2'
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 3}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        EngineCoreTestWithVars.get_items.coro.return_value = []
        resp = await client.get('/placements/{}/items'.format(obj['small_hash']), headers=headers_without_content_type)
        EngineCoreTestWithVars.get_items.coro.reset_mock()

        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [
            {'sku': 'test1', 'item_id': 1},
            {'sku': 'test3', 'item_id': 3},
            {'sku': 'test2', 'item_id': 2}
        ]

    async def test_get_items_with_explict_fallbacks(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1'
        },{
            'item_id': 2,
            'sku': 'test2'
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?explict_fallbacks=true'.format(
                                obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json()) == {
            "name": "Placement Test",
            "slots": [
                {
                    "name": "test2",
                    'item_type': 'new_products',
                    "items": {
                        "main": [
                            {
                                "item_id": 1,
                                "sku": "test1"
                            },
                            {
                                "item_id": 3,
                                "sku": "test3"
                            },
                            {
                                "item_id": 2,
                                "sku": "test2"
                            }
                        ],
                        "fallbacks": []
                    }
                }
            ],
            "small_hash": "941e0"
        }

    async def test_get_items_without_show_details(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_string': 'test'
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_string': 'test'
        },{
            'item_id': 3,
            'sku': 'test3',
            'filter_string': 'test'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)

        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'show_details': False,
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [
            {'sku': 'test1', 'item_id': 1},
            {'sku': 'test3', 'item_id': 3},
            {'sku': 'test2', 'item_id': 2}
        ]

    async def test_get_items_distributed(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1'
        },{
            'item_id': 2,
            'sku': 'test2'
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        resp = await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'distribute_items': True,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 1}, {'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        random.seed(0)
        EngineCoreTestWithVars.get_items.coro.return_value = [{'test': 1}, {'test': 2}]
        resp = await client.get('/placements/{}/items'.format(obj['small_hash']), headers=headers_without_content_type)
        EngineCoreTestWithVars.get_items.coro.reset_mock()

        assert resp.status == 200
        assert (await resp.json())['distributed_items'] == [
            {'sku': 'test1', 'item_id': 1, 'type': 'new_products'},
            {'sku': 'test3', 'item_id': 3, 'type': 'new_products'},
            {'test': 1, 'type': 'products'},
            {'sku': 'test2', 'item_id': 2, 'type': 'new_products'},
            {'test': 2, 'type': 'products'}
        ]


class TestPlacementsGetRecomendationsFilters(object):

    async def test_get_items_by_string_inclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_string': 'test'
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_string': 'test'
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?filter_string_inclusive=test'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [
            {'sku': 'test1', 'item_id': 1, 'filter_string': 'test'},
            {'sku': 'test2', 'item_id': 2, 'filter_string': 'test'}
        ]

    async def test_get_items_by_string_exclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_string': 'test'
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_string': 'test'
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?filter_string_exclusive=test'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [{'sku': 'test3', 'item_id': 3}]

    async def test_get_items_by_integer_inclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_integer': 1
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_integer': 1
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?filter_integer_inclusive=1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [
            {'sku': 'test1', 'item_id': 1, 'filter_integer': 1, 'base_prop': 2},
            {'sku': 'test2', 'item_id': 2, 'filter_integer': 1, 'base_prop': 2}
        ]

    async def test_get_items_by_integer_exclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_integer': 1
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_integer': 1
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?filter_integer_exclusive=1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [{'sku': 'test3', 'item_id': 3}]

    async def test_get_items_by_boolean_inclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_boolean': True
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_boolean': True
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?filter_boolean_inclusive=true'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [
            {'sku': 'test1', 'item_id': 1, 'filter_boolean': True},
            {'sku': 'test2', 'item_id': 2, 'filter_boolean': True}
        ]

    async def test_get_items_by_boolean_exclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_boolean': True
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_boolean': True
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?filter_boolean_exclusive=true'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [{'sku': 'test3', 'item_id': 3}]

    async def test_get_items_by_array_inclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_array': ['t1', 't2']
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_array': ['t2', 't3']
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?filter_array_inclusive=t2,t3'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [
            {'filter_array': ['t1', 't2'], 'item_id': 1, 'sku': 'test1'},
            {'sku': 'test2', 'item_id': 2, 'filter_array': ['t2', 't3']}
        ]

    async def test_get_items_by_array_exclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_array': ['t1', 't2']
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_array': ['t2', 't3']
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?filter_array_exclusive=t2'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [{'sku': 'test3', 'item_id': 3}]

    async def test_get_items_by_object_inclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_object': {'id': 1}
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_object': {'id': 2}
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?filter_object_inclusive=id:1'.format(obj['small_hash']), headers=headers_without_content_type)

        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [
            {'sku': 'test1', 'item_id': 1, 'filter_object': {'id': 1}}
        ]

    async def test_get_items_by_object_exclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_object': {'id': 1}
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_object': {'id': 2}
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?filter_object_exclusive=id:1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [
            {'sku': 'test3', 'item_id': 3},
            {'sku': 'test2', 'item_id': 2, 'filter_object': {'id': 2}}
        ]

    async def test_get_items_by_base_prop_inclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_integer': 1
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_integer': 1
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        data_file = tempfile.NamedTemporaryFile(delete=False)
        data_file2 = tempfile.NamedTemporaryFile(delete=False)
        data_file2.write('\n'.join([ujson.dumps(p) for p in products]).encode())
        data_file2.close()
        data_filez = zipfile.ZipFile(data_file.name, 'w')
        data_filez.write(data_file2.name)
        data_filez.close()
        data_filez = open(data_file.name, 'rb')

        headers_ = {}
        headers_.update(headers)
        headers_['Content-Type'] = 'application/zip'
        resp = await client.post('/item_types/4/import_data_file?store_id=1&upload_file=false', headers=headers_, data=data_filez)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/import_data_file?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        os.remove(data_file.name)

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?filter_base_prop_inclusive=2'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [
            {'sku': 'test1', 'item_id': 1, 'filter_integer': 1, 'base_prop': 2},
            {'sku': 'test2', 'item_id': 2, 'filter_integer': 1, 'base_prop': 2}
        ]

    async def test_get_items_by_base_prop_exclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_integer': 1
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_integer': 1
        },{
            'item_id': 3,
            'sku': 'test3'
        }]

        data_file = tempfile.NamedTemporaryFile(delete=False)
        data_file2 = tempfile.NamedTemporaryFile(delete=False)
        data_file2.write('\n'.join([ujson.dumps(p) for p in products]).encode())
        data_file2.close()
        data_filez = zipfile.ZipFile(data_file.name, 'w')
        data_filez.write(data_file2.name)
        data_filez.close()
        data_filez = open(data_file.name, 'rb')

        headers_ = {}
        headers_.update(headers)
        headers_['Content-Type'] = 'application/zip'
        resp = await client.post('/item_types/4/import_data_file?store_id=1&upload_file=false', headers=headers_, data=data_filez)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/import_data_file?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        os.remove(data_file.name)

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?filter_base_prop_exclusive=2'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [{'sku': 'test3', 'item_id': 3}]



class TestPlacementsGetRecomendationsFiltersOf(object):

    async def test_get_items_of_string_inclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_string': 'test1'
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_string': 'test2'
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?filter_string_inclusive_of=1|test1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [
            {'sku': 'test1', 'item_id': 1, 'filter_string': 'test1'}]

    async def test_get_items_of_string_exclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_string': 'test1'
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_string': 'test2'
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?filter_string_exclusive_of=1|test1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [
            {'sku': 'test3', 'item_id': 3},
            {'sku': 'test2', 'item_id': 2, 'filter_string': 'test2'}]

    async def test_get_items_of_integer_inclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_integer': 1
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_integer': 2
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?filter_integer_inclusive_of=1|test1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [
            {'sku': 'test1', 'item_id': 1, 'filter_integer': 1, 'base_prop': 2}]

    async def test_get_items_of_integer_exclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_integer': 1
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_integer': 2
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?filter_integer_exclusive_of=1|test1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [
            {'sku': 'test3', 'item_id': 3},
            {'sku': 'test2', 'item_id': 2, 'filter_integer': 2, 'base_prop': 3}
        ]

    async def test_get_items_of_boolean_inclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_boolean': True
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_boolean': True
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?filter_boolean_inclusive_of=1|test1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [
            {'sku': 'test1', 'item_id': 1, 'filter_boolean': True},
            {'sku': 'test2', 'item_id': 2, 'filter_boolean': True}
        ]

    async def test_get_items_of_boolean_inclusive_with_false(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_boolean': True
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_boolean': True
        },{
            'item_id': 3,
            'sku': 'test3',
            'filter_boolean': False
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?filter_boolean_inclusive_of=3|test3'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [
            {'sku': 'test1', 'item_id': 1, 'filter_boolean': True},
            {'sku': 'test2', 'item_id': 2, 'filter_boolean': True}
        ]

    async def test_get_items_of_boolean_exclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_boolean': True
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_boolean': True
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?filter_boolean_exclusive_of=1|test1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [
            {'sku': 'test3', 'item_id': 3}]

    async def test_get_items_of_array_inclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_array': ['t1', 't2']
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_array': ['t2', 't3']
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?filter_array_inclusive_of=1|test1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [
            {'filter_array': ['t1', 't2'], 'item_id': 1, 'sku': 'test1'},
            {'sku': 'test2', 'item_id': 2, 'filter_array': ['t2', 't3']}]

    async def test_get_items_of_array_exclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_array': ['t1', 't2']
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_array': ['t2', 't3']
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?filter_array_exclusive_of=1|test1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [
            {'sku': 'test3', 'item_id': 3}]

    async def test_get_items_of_object_inclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_object': {'id': 1}
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_object': {'id': 2}
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?filter_object_inclusive_of=1|test1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [
            {'sku': 'test1', 'item_id': 1, 'filter_object': {'id': 1}}]

    async def test_get_items_of_object_exclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_object': {'id': 1}
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_object': {'id': 2}
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?filter_object_exclusive_of=1|test1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [
            {'sku': 'test3', 'item_id': 3},
            {'sku': 'test2', 'item_id': 2, 'filter_object': {'id': 2}}]

    async def test_get_items_of_index_inclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_object': {'id': 1}
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_object': {'id': 2}
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?index_inclusive_of=1|test1,2|test2'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        obj1 = {'sku': 'test1', 'item_id': 1, 'filter_object': {'id': 1}}
        obj2 = {'sku': 'test2', 'item_id': 2, 'filter_object': {'id': 2}}
        assert (await resp.json())['slots'][0]['items'] == [obj1, obj2] or \
            (await resp.json())['slots'][0]['items'] == [obj2, obj1]

    async def test_get_items_of_index_exclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_object': {'id': 1}
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_object': {'id': 2}
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        await client.post('/item_types/4/items?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/item_types/4/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/item_types/4/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        await client.post('/engines/4/export_objects?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engines/4/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?index_exclusive_of=1|test1,2|test2'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [{'sku': 'test3', 'item_id': 3}]
