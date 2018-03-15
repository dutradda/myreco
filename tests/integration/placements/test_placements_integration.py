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


from tests.integration.fixtures import EngineStrategyTestWithVars, EngineStrategyTest
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
        'configuration': {}
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
                'sku': {'type': 'string'},
                'filter_override': {
                    'type': 'object',
                    'id_names': ['id'],
                    'properties': {
                        'id': {'type': 'integer'}
                    }
                },
            }
        }
    }
    session.loop.run_until_complete(models['item_types'].insert(session, item_type))


    strategy = {
        'name': 'test_with_vars',
        'class_module': 'tests.integration.fixtures',
        'class_name': 'EngineStrategyTestWithVars'
    }
    session.loop.run_until_complete(models['engine_strategies'].insert(session, strategy))

    strategy = {
        'name': 'test',
        'class_module': 'tests.integration.fixtures',
        'class_name': 'EngineStrategyTest'
    }
    session.loop.run_until_complete(models['engine_strategies'].insert(session, strategy))

    engine = {
        'name': 'Engine products with vars',
        'objects': [{
            '_operation': 'insert',
            'name': 'Object with vars',
            'type': 'object_with_vars',
            'configuration': {
                'item_id_name': 'item_id',
                'aggregators_ids_name': 'filter_test',
                'data_importer_path': 'test.test'
            }
        }],
        'store_id': 1,
        'strategy_id': 1,
        'item_type_id': 1
    }
    session.loop.run_until_complete(models['engines'].insert(session, engine))
    engine = {
        'name': 'Engine categories with vars',
        'objects': [{
            '_operation': 'insert',
            'name': 'Object with vars 2',
            'type': 'object_with_vars',
            'configuration': {
                'item_id_name': 'item_id',
                'aggregators_ids_name': 'filter_test',
                'data_importer_path': 'test.test'
            }
        }],
        'store_id': 1,
        'strategy_id': 1,
        'item_type_id': 2
    }
    session.loop.run_until_complete(models['engines'].insert(session, engine))
    engine = {
        'name': 'Invalid Top Seller',
        'objects': [{
            '_operation': 'insert',
            'name': 'Object Top Seller Invalid',
            'type': 'top_seller_array',
            'configuration': {'days_interval': 7}
        }],
        'store_id': 1,
        'strategy_id': 2,
        'item_type_id': 3
    }
    session.loop.run_until_complete(models['engines'].insert(session, engine))
    engine = {
        'name': 'Top Seller',
        'objects': [{
            '_operation': 'insert',
            'name': 'Object Top Seller',
            'type': 'top_seller_array',
            'configuration': {'days_interval': 7}
        }],
        'store_id': 1,
        'item_type_id': 4,
        'strategy_id': 2
    }
    session.loop.run_until_complete(models['engines'].insert(session, engine))
    engine = {
        'name': 'With Fallback',
        'store_id': 1,
        'item_type_id': 4,
        'objects': [{
            '_operation': 'insert',
            'name': 'Object with vars 3',
            'type': 'object_with_vars',
            'configuration': {
                'item_id_name': 'item_id',
                'aggregators_ids_name': 'filter_string',
                'data_importer_path': 'test.test'
            }
        }],
        'strategy_id': 1
    }
    session.loop.run_until_complete(models['engines'].insert(session, engine))

    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'test', 'store_id': 1})) # ID: 1
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'test2', 'store_id': 1})) # ID: 2
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'test3', 'store_id': 1})) # ID: 3
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_string_inclusive', 'store_id': 1})) # ID: 4
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_integer_inclusive', 'store_id': 1})) # ID: 5
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_base_prop_inclusive', 'store_id': 1})) # ID: 6
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_boolean_inclusive', 'store_id': 1})) # ID: 7
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_array_inclusive', 'store_id': 1})) # ID: 8
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_object_inclusive', 'store_id': 1})) # ID: 9
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_string_exclusive', 'store_id': 1})) # ID: 10
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_integer_exclusive', 'store_id': 1})) # ID: 11
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_base_prop_exclusive', 'store_id': 1})) # ID: 12
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_boolean_exclusive', 'store_id': 1})) # ID: 13
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_array_exclusive', 'store_id': 1})) # ID: 14
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_object_exclusive', 'store_id': 1})) # ID: 15
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_string_inclusive_of', 'store_id': 1})) # ID: 16
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_integer_inclusive_of', 'store_id': 1})) # ID: 17
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_boolean_inclusive_of', 'store_id': 1})) # ID: 18
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_array_inclusive_of', 'store_id': 1})) # ID: 19
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_object_inclusive_of', 'store_id': 1})) # ID: 20
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_string_exclusive_of', 'store_id': 1})) # ID: 21
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_integer_exclusive_of', 'store_id': 1})) # ID: 22
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_boolean_exclusive_of', 'store_id': 1})) # ID: 23
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_array_exclusive_of', 'store_id': 1})) # ID: 24
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'filter_object_exclusive_of', 'store_id': 1})) # ID: 25
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'index_inclusive_of', 'store_id': 1})) # ID: 26
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'index_exclusive_of', 'store_id': 1})) # ID: 27
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'override', 'store_id': 1})) # ID: 28

    slot = {
        'max_items': 10,
        'name': 'test',
        'store_id': 1,
        'engine_id': 1,
        'slot_variables': [{
            '_operation': 'insert',
            'external_variable_id': 2,
            'engine_variable_name': 'item_id'
        }],
        'slot_filters': [{
            '_operation': 'insert',
            'external_variable_id': 1,
            'is_inclusive': True,
            'type_id': 'property_value',
            'property_name': 'filter_test'
        },{
            '_operation': 'insert',
            'external_variable_id': 3,
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
            'external_variable_id': 4,
            'is_inclusive': True,
            'type_id': 'property_value',
            'property_name': 'filter_string'
        },{
            '_operation': 'insert',
            'external_variable_id': 5,
            'is_inclusive': True,
            'type_id': 'property_value',
            'property_name': 'filter_integer'
        },{
            '_operation': 'insert',
            'external_variable_id': 6,
            'is_inclusive': True,
            'type_id': 'property_value',
            'property_name': 'base_prop'
        },{
            '_operation': 'insert',
            'external_variable_id': 7,
            'is_inclusive': True,
            'type_id': 'property_value',
            'property_name': 'filter_boolean'
        },{
            '_operation': 'insert',
            'external_variable_id': 8,
            'is_inclusive': True,
            'type_id': 'property_value',
            'property_name': 'filter_array'
        },{
            '_operation': 'insert',
            'external_variable_id': 9,
            'is_inclusive': True,
            'type_id': 'property_value',
            'property_name': 'filter_object'
        },{
            '_operation': 'insert',
            'external_variable_id': 10,
            'is_inclusive': False,
            'type_id': 'property_value',
            'property_name': 'filter_string'
        },{
            '_operation': 'insert',
            'external_variable_id': 11,
            'is_inclusive': False,
            'type_id': 'property_value',
            'property_name': 'filter_integer'
        },{
            '_operation': 'insert',
            'external_variable_id': 12,
            'is_inclusive': False,
            'type_id': 'property_value',
            'property_name': 'base_prop'
        },{
            '_operation': 'insert',
            'external_variable_id': 13,
            'is_inclusive': False,
            'type_id': 'property_value',
            'property_name': 'filter_boolean'
        },{
            '_operation': 'insert',
            'external_variable_id': 14,
            'is_inclusive': False,
            'type_id': 'property_value',
            'property_name': 'filter_array'
        },{
            '_operation': 'insert',
            'external_variable_id': 15,
            'is_inclusive': False,
            'type_id': 'property_value',
            'property_name': 'filter_object'
        },{
            '_operation': 'insert',
            'external_variable_id': 16,
            'is_inclusive': True,
            'type_id': 'item_property_value',
            'property_name': 'filter_string'
        },{
            '_operation': 'insert',
            'external_variable_id': 17,
            'is_inclusive': True,
            'type_id': 'item_property_value',
            'property_name': 'filter_integer'
        },{
            '_operation': 'insert',
            'external_variable_id': 18,
            'is_inclusive': True,
            'type_id': 'item_property_value',
            'property_name': 'filter_boolean'
        },{
            '_operation': 'insert',
            'external_variable_id': 19,
            'is_inclusive': True,
            'type_id': 'item_property_value',
            'property_name': 'filter_array'
        },{
            '_operation': 'insert',
            'external_variable_id': 20,
            'is_inclusive': True,
            'type_id': 'item_property_value',
            'property_name': 'filter_object'
        },{
            '_operation': 'insert',
            'external_variable_id': 21,
            'is_inclusive': False,
            'type_id': 'item_property_value',
            'property_name': 'filter_string'
        },{
            '_operation': 'insert',
            'external_variable_id': 22,
            'is_inclusive': False,
            'type_id': 'item_property_value',
            'property_name': 'filter_integer'
        },{
            '_operation': 'insert',
            'external_variable_id': 23,
            'is_inclusive': False,
            'type_id': 'item_property_value',
            'property_name': 'filter_boolean'
        },{
            '_operation': 'insert',
            'external_variable_id': 24,
            'is_inclusive': False,
            'type_id': 'item_property_value',
            'property_name': 'filter_array'
        },{
            '_operation': 'insert',
            'external_variable_id': 25,
            'is_inclusive': False,
            'type_id': 'item_property_value',
            'property_name': 'filter_object'
        },{
            '_operation': 'insert',
            'external_variable_id': 26,
            'is_inclusive': True,
            'type_id': 'property_value_index',
            'property_name': 'sku'
        },{
            '_operation': 'insert',
            'external_variable_id': 27,
            'is_inclusive': False,
            'type_id': 'property_value_index',
            'property_name': 'sku'
        },{
            '_operation': 'insert',
            'external_variable_id': 28,
            'is_inclusive': False,
            'type_id': 'property_value',
            'property_name': 'filter_override',
            'override': True,
            'override_value': 'id:1'
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

    _all_models.pop('store_items_products_1', None)
    _all_models.pop('store_items_categories_1', None)
    _all_models.pop('store_items_invalid_1', None)
    _all_models.pop('store_items_new_products_1', None)


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
                    'is_redirect': {'type': 'boolean'},
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
                'name': 'Var 1',
                'slots': [{'id': 1}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        assert resp.status == 201

        store = {
            'country': 'test',
            'id': 1,
            'name': 'test',
            'configuration': {}
        }
        item_type = {
            'id': 1,
            'store_items_class': None,
            'stores': [store],
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
        }
        strategy = {
            'id': 1,
            'name': 'test_with_vars',
            'class_module': 'tests.integration.fixtures',
            'class_name': 'EngineStrategyTestWithVars',
            'object_types': ['object_with_vars']
        }

        assert (await resp.json()) ==  [{
            'ab_testing': False,
            'show_details': True,
            'distribute_items': False,
            'is_redirect': False,
            'hash': '941e021d7ae6ca23f8969870ffe48b87a315e05c',
            'name': 'Placement Test',
            'small_hash': '941e0',
            'store_id': 1,
            'variations': [{
                'name': 'Var 1',
                'slots': [{
                    'max_items': 10,
                    'name': 'test',
                    'engine': {
                        'objects': [{
                            'id': 1,
                            'item_type_id': 1,
                            'store_id': 1,
                            'strategy_id': 1,
                            'name': 'Object with vars',
                            'type': 'object_with_vars',
                            'configuration': {
                                'aggregators_ids_name': 'filter_test',
                                'item_id_name': 'item_id',
                                'data_importer_path': 'test.test'
                            },
                            'item_type': item_type,
                            'store': store,
                            'strategy': strategy
                        }],
                        'id': 1,
                        'item_type': item_type,
                        'item_type_id': 1,
                        'name': 'Engine products with vars',
                        'store': store,
                        'store_id': 1,
                        'strategy_id': 1,
                        'strategy': strategy,
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
                            'id': 2,
                            'name': 'test2',
                            'store_id': 1
                        },
                        'external_variable_id': 2
                    }],
                    'slot_filters': [{
                        'is_inclusive': True,
                        'type_id': 'property_value',
                        'type': {
                            'id': 'property_value',
                            'name': 'By Property Value'
                        },
                        'slot_id': 1,
                        'id': 1,
                        'property_name': 'filter_test',
                        'override': False,
                        'override_value': None,
                        'skip_values': None,
                        'external_variable': {
                            'id': 1,
                            'name': 'test',
                            'store_id': 1
                        },
                        'external_variable_id': 1
                    },{
                        'is_inclusive': True,
                        'type_id': 'item_property_value',
                        'type': {
                            'id': 'item_property_value',
                            'name': 'By Item Property Value'
                        },
                        'slot_id': 1,
                        'id': 2,
                        'property_name': 'filter_test',
                        'override': False,
                        'override_value': None,
                        'skip_values': None,
                        'external_variable': {
                            'id': 3,
                            'name': 'test3',
                            'store_id': 1
                        },
                        'external_variable_id': 3
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
                'name': 'Var 1',
                '_operation': 'insert',
                'slots': [{'id': 1}]
            }]
        }]
        await client.post('/placements/', headers=headers, data=ujson.dumps(body))

        resp = await client.get('/placements/?store_id=1', headers=headers_without_content_type)
        assert resp.status == 200
7


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
                    'is_redirect': {'type': 'boolean'},
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
                'name': 'Var 1',
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
            'is_redirect': False,
            'hash': '941e021d7ae6ca23f8969870ffe48b87a315e05c',
            'name': 'Placement Test',
            'small_hash': '941e0',
            'store_id': 1,
            'variations': [{
                'slots': [],
                'id': 1,
                'name': 'Var 1',
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
                'name': 'Var 1',
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
                'name': 'Var 1',
                '_operation': 'insert',
                'slots': [{'id': 1}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/'.format(obj['small_hash']), headers=headers_without_content_type)

        assert resp.status == 200


        store = {
            'country': 'test',
            'id': 1,
            'name': 'test',
            'configuration': {}
        }
        item_type = {
            'id': 1,
            'store_items_class': None,
            'stores': [store],
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
        }
        strategy = {
            'id': 1,
            'name': 'test_with_vars',
            'class_module': 'tests.integration.fixtures',
            'class_name': 'EngineStrategyTestWithVars',
            'object_types': ['object_with_vars']
        }

        assert (await resp.json()) ==  {
            'ab_testing': False,
            'show_details': True,
            'distribute_items': False,
            'is_redirect': False,
            'hash': '941e021d7ae6ca23f8969870ffe48b87a315e05c',
            'name': 'Placement Test',
            'small_hash': '941e0',
            'store_id': 1,
            'variations': [{
                'name': 'Var 1',
                'slots': [{
                    'max_items': 10,
                    'name': 'test',
                    'engine': {
                        'objects': [{
                            'id': 1,
                            'item_type_id': 1,
                            'store_id': 1,
                            'strategy_id': 1,
                            'name': 'Object with vars',
                            'type': 'object_with_vars',
                            'configuration': {
                                'aggregators_ids_name': 'filter_test',
                                'item_id_name': 'item_id',
                                'data_importer_path': 'test.test'
                            },
                            'item_type': item_type,
                            'store': store,
                            'strategy': strategy
                        }],
                        'id': 1,
                        'item_type': item_type,
                        'item_type_id': 1,
                        'name': 'Engine products with vars',
                        'store': store,
                        'store_id': 1,
                        'strategy_id': 1,
                        'strategy': strategy,
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
                            'id': 2,
                            'name': 'test2',
                            'store_id': 1
                        },
                        'external_variable_id': 2
                    }],
                    'slot_filters': [{
                        'is_inclusive': True,
                        'type_id': 'property_value',
                        'type': {
                            'id': 'property_value',
                            'name': 'By Property Value'
                        },
                        'slot_id': 1,
                        'id': 1,
                        'property_name': 'filter_test',
                        'override': False,
                        'override_value': None,
                        'skip_values': None,
                        'external_variable': {
                            'id': 1,
                            'name': 'test',
                            'store_id': 1
                        },
                        'external_variable_id': 1
                    },{
                        'is_inclusive': True,
                        'type_id': 'item_property_value',
                        'type': {
                            'id': 'item_property_value',
                            'name': 'By Item Property Value'
                        },
                        'slot_id': 1,
                        'id': 2,
                        'property_name': 'filter_test',
                        'override': False,
                        'override_value': None,
                        'skip_values': None,
                        'external_variable': {
                            'id': 3,
                            'name': 'test3',
                            'store_id': 1
                        },
                        'external_variable_id': 3
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
                'name': 'Var 1',
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
        monkeypatch.setattr('myreco.engine_strategies.model.ModuleObjectLoader', class_loader)

        class_loader.load()().get_items = CoroMock()
        class_loader.load()().get_items.coro.return_value = [{'id': 1}, {'id': 2}, {'id': 3}]
        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
                'slots': [{'id': 3}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        EngineStrategyTestWithVars.get_items.coro.return_value = []
        resp = await client.get('/placements/{}/items'.format(obj['small_hash']), headers=headers_without_content_type)
        EngineStrategyTestWithVars.get_items.coro.reset_mock()

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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'show_details': False,
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'distribute_items': True,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
                'slots': [{'id': 1}, {'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        random.seed(0)
        EngineStrategyTestWithVars.get_items.coro.return_value = [{'test': 1}, {'test': 2}]
        resp = await client.get('/placements/{}/items'.format(obj['small_hash']), headers=headers_without_content_type)
        EngineStrategyTestWithVars.get_items.coro.reset_mock()

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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?index_exclusive_of=1|test1,2|test2'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [{'sku': 'test3', 'item_id': 3}]

    async def test_get_items_404(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?index_exclusive_of=1|test1,2|test2,3|test3'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 404
        assert (await resp.json()) == None

    async def test_get_items_with_override(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        random_patch(monkeypatch)
        client = await client
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_override': {'id': 1}
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_override': {'id': 2}
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

        await client.post('/engine_objects/4/export?import_data=true', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/engine_objects/4/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
                'slots': [{'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['items'] == [
            {'sku': 'test3', 'item_id': 3},
            {'sku': 'test2', 'item_id': 2, 'filter_override': {'id': 2}}]


class TestPlacementsGetRecomendationsRedirect(object):

    async def test_get_items_invalid_item_idx(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        client = await client
        class_loader = mock.MagicMock()
        monkeypatch.setattr('myreco.item_types.model.ModuleObjectLoader', class_loader)
        monkeypatch.setattr('myreco.engine_strategies.model.ModuleObjectLoader', class_loader)

        class_loader.load()().get_items = CoroMock()
        class_loader.load()().get_items.coro.return_value = [{'id': 1}, {'id': 2}, {'id': 3}]
        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
                'slots': [{'id': 1}]
            }],
            'is_redirect': True
        }]
        class_loader.load()().get_variables.return_value = \
            [{'name': 'item_id', 'schema': {'type': 'integer'}}]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?test2=1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 400
        assert await resp.json() == {'message': "Query argument 'item_idx' is mandatory when 'is_redirect' is true."}

    async def test_get_items_invalid_slot_idx(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        client = await client
        class_loader = mock.MagicMock()
        monkeypatch.setattr('myreco.item_types.model.ModuleObjectLoader', class_loader)
        monkeypatch.setattr('myreco.engine_strategies.model.ModuleObjectLoader', class_loader)

        class_loader.load()().get_items = CoroMock()
        class_loader.load()().get_items.coro.return_value = [{'id': 1}, {'id': 2}, {'id': 3}]
        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
                'slots': [{'id': 1}]
            }],
            'is_redirect': True
        }]
        class_loader.load()().get_variables.return_value = \
            [{'name': 'item_id', 'schema': {'type': 'integer'}}]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get('/placements/{}/items?item_idx=1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 400
        assert await resp.json() == {'message': "Query argument 'slot_idx' is mandatory when 'distribute_items' is false."}

    async def test_get_items_valid(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        client = await client
        class_loader = mock.MagicMock()
        monkeypatch.setattr('myreco.item_types.model.ModuleObjectLoader', class_loader)
        monkeypatch.setattr('myreco.engine_strategies.model.ModuleObjectLoader', class_loader)

        class_loader.load()().get_items = CoroMock()
        class_loader.load()().get_items.coro.return_value = [{'id': 1}, {'id': 2}, {'id': 3}]
        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
                'slots': [{'id': 1}]
            }],
            'is_redirect': True
        }]
        class_loader.load()().get_variables.return_value = \
            [{'name': 'item_id', 'schema': {'type': 'integer'}}]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get(
            '/placements/{}/items?item_idx=0&slot_idx=0'.format(obj['small_hash']),
            headers=headers_without_content_type,
            allow_redirects=False
        )
        assert resp.status == 302
        assert dict(resp.headers)['Location'] == str({'id': 1})
        assert await resp.json() == None

    async def test_get_items_invalid_with_distribute_items(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        client = await client
        class_loader = mock.MagicMock()
        monkeypatch.setattr('myreco.item_types.model.ModuleObjectLoader', class_loader)
        monkeypatch.setattr('myreco.engine_strategies.model.ModuleObjectLoader', class_loader)

        class_loader.load()().get_items = CoroMock()
        class_loader.load()().get_items.coro.return_value = [{'id': 1}, {'id': 2}, {'id': 3}]
        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
                'slots': [{'id': 1}]
            }],
            'is_redirect': True,
            'distribute_items': True
        }]
        class_loader.load()().get_variables.return_value = \
            [{'name': 'item_id', 'schema': {'type': 'integer'}}]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get(
            '/placements/{}/items?item_idx=0&slot_idx=0'.format(obj['small_hash']),
            headers=headers_without_content_type,
            allow_redirects=False
        )
        assert resp.status == 400
        assert await resp.json() == {'message': "Query argument 'slot_idx' can't be setted when 'distribute_items' is true."}

    async def test_get_items_valid_with_distribute_items(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        client = await client
        class_loader = mock.MagicMock()
        monkeypatch.setattr('myreco.item_types.model.ModuleObjectLoader', class_loader)
        monkeypatch.setattr('myreco.engine_strategies.model.ModuleObjectLoader', class_loader)

        class_loader.load()().get_items = CoroMock()
        class_loader.load()().get_items.coro.return_value = [{'id': 1}, {'id': 2}, {'id': 3}]
        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'name': 'Var 1',
                'slots': [{'id': 1}]
            }],
            'is_redirect': True,
            'distribute_items': True
        }]
        class_loader.load()().get_variables.return_value = \
            [{'name': 'item_id', 'schema': {'type': 'integer'}}]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        resp = await client.get(
            '/placements/{}/items?item_idx=0'.format(obj['small_hash']),
            headers=headers_without_content_type,
            allow_redirects=False
        )
        assert resp.status == 302
        assert dict(resp.headers)['Location'] == str({'id': 1, 'type': 'products'})
        assert await resp.json() == None
