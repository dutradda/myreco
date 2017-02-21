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

    engine_core = {
        'name': 'visual_similarity',
        'configuration': {
            'core_module': {
                'path': 'tests.integration.fixtures',
                'object_name': 'EngineCoreTestWithVars'
            }
        }
    }
    session.loop.run_until_complete(models['engines_cores'].insert(session, engine_core))
    engine_core = {
        'name': 'top_seller',
        'configuration': {
            'core_module': {
                'path': 'tests.integration.fixtures',
                'object_name': 'EngineCoreTest'
            }
        }
    }
    session.loop.run_until_complete(models['engines_cores'].insert(session, engine_core))

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
    session.loop.run_until_complete(models['items_types'].insert(session, item_type))
    item_type = {
        'name': 'categories',
        'stores': [{'id': 1}],
        'schema': schema
    }
    session.loop.run_until_complete(models['items_types'].insert(session, item_type))
    item_type = {
        'name': 'invalid',
        'stores': [{'id': 1}],
        'schema': {
            'type': 'object',
            'id_names': ['item_id'],
            'properties': {'item_id': {'type': 'string'}}
        }
    }
    session.loop.run_until_complete(models['items_types'].insert(session, item_type))
    item_type = {
        'name': 'products_new',
        'stores': [{'id': 1}],
        'import_processor': {
            'path': 'tests.integration.fixtures',
            'object_name': 'ProductsImportProcessor'
        },
        'schema': {
            'type': 'object',
            'id_names': ['item_id', 'sku'],
            'properties': {
                'filter_string': {'type': 'string'},
                'filter_integer': {'type': 'integer'},
                'filter_boolean': {'type': 'boolean'},
                'filter_pre_processing': {'type': 'integer'},
                'filter_post_processing': {'type': 'integer'},
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
    session.loop.run_until_complete(models['items_types'].insert(session, item_type))

    engine = {
        'name': 'Visual Similarity',
        'configuration_json': ujson.dumps({
            'item_id_name': 'item_id',
            'aggregators_ids_name': 'filter_test',
            'data_importer_path': 'test.test'
        }),
        'store_id': 1,
        'core_id': 1,
        'item_type_id': 1
    }
    session.loop.run_until_complete(models['engines'].insert(session, engine))
    engine = {
        'name': 'Categories Visual Similarity',
        'configuration_json': ujson.dumps({
            'item_id_name': 'item_id',
            'aggregators_ids_name': 'filter_test'
        }),
        'store_id': 1,
        'core_id': 1,
        'item_type_id': 2
    }
    session.loop.run_until_complete(models['engines'].insert(session, engine))
    engine = {
        'name': 'Invalid Top Seller',
        'configuration_json': ujson.dumps({
            'days_interval': 7
        }),
        'store_id': 1,
        'core_id': 2,
        'item_type_id': 3
    }
    session.loop.run_until_complete(models['engines'].insert(session, engine))
    engine = {
        'name': 'Top Seller',
        'configuration_json': ujson.dumps({
            'days_interval': 7
        }),
        'store_id': 1,
        'core_id': 2,
        'item_type_id': 4
    }
    session.loop.run_until_complete(models['engines'].insert(session, engine))
    engine = {
        'name': 'With Fallback',
        'store_id': 1,
        'core_id': 1,
        'item_type_id': 4,
        'configuration': {
            'item_id_name': 'item_id',
            'aggregators_ids_name': 'filter_string',
            'data_importer_path': 'test.test'
        }
    }
    session.loop.run_until_complete(models['engines'].insert(session, engine))

    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'test', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'test2', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'test3', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'filter_string_inclusive', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'filter_integer_inclusive', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'filter_post_processing_inclusive', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'filter_boolean_inclusive', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'filter_array_inclusive', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'filter_object_inclusive', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'filter_string_exclusive', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'filter_integer_exclusive', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'filter_post_processing_exclusive', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'filter_boolean_exclusive', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'filter_array_exclusive', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'filter_object_exclusive', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'filter_string_inclusive_of', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'filter_integer_inclusive_of', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'filter_boolean_inclusive_of', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'filter_array_inclusive_of', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'filter_object_inclusive_of', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'filter_string_exclusive_of', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'filter_integer_exclusive_of', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'filter_boolean_exclusive_of', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'filter_array_exclusive_of', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'filter_object_exclusive_of', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'index_inclusive_of', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'index_exclusive_of', 'store_id': 1}))

    slot = {
        'max_recos': 10,
        'name': 'test',
        'store_id': 1,
        'engine_id': 1,
        'slot_variables': [{
            '_operation': 'insert',
            'variable_name': 'test',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter_test'
        },{
            '_operation': 'insert',
            'variable_name': 'test3',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'Property Of',
            'inside_engine_name': 'filter_test'
        },{
            '_operation': 'insert',
            'variable_name': 'test2',
            'variable_store_id': 1,
            'inside_engine_name': 'item_id'
        }]
    }
    session.loop.run_until_complete(models['slots'].insert(session, slot))
    slot = {
        'max_recos': 10,
        'name': 'test2',
        'store_id': 1,
        'engine_id': 4,
        'slot_variables': [{
            '_operation': 'insert',
            'variable_name': 'filter_string_inclusive',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter_string'
        },{
            '_operation': 'insert',
            'variable_name': 'filter_integer_inclusive',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter_integer'
        },{
            '_operation': 'insert',
            'variable_name': 'filter_post_processing_inclusive',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter_post_processing'
        },{
            '_operation': 'insert',
            'variable_name': 'filter_boolean_inclusive',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter_boolean'
        },{
            '_operation': 'insert',
            'variable_name': 'filter_array_inclusive',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter_array'
        },{
            '_operation': 'insert',
            'variable_name': 'filter_object_inclusive',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter_object'
        },{
            '_operation': 'insert',
            'variable_name': 'filter_string_exclusive',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': False,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter_string'
        },{
            '_operation': 'insert',
            'variable_name': 'filter_integer_exclusive',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': False,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter_integer'
        },{
            '_operation': 'insert',
            'variable_name': 'filter_post_processing_exclusive',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': False,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter_post_processing'
        },{
            '_operation': 'insert',
            'variable_name': 'filter_boolean_exclusive',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': False,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter_boolean'
        },{
            '_operation': 'insert',
            'variable_name': 'filter_array_exclusive',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': False,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter_array'
        },{
            '_operation': 'insert',
            'variable_name': 'filter_object_exclusive',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': False,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter_object'
        },{
            '_operation': 'insert',
            'variable_name': 'filter_string_inclusive_of',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'Property Of',
            'inside_engine_name': 'filter_string'
        },{
            '_operation': 'insert',
            'variable_name': 'filter_integer_inclusive_of',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'Property Of',
            'inside_engine_name': 'filter_integer'
        },{
            '_operation': 'insert',
            'variable_name': 'filter_boolean_inclusive_of',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'Property Of',
            'inside_engine_name': 'filter_boolean'
        },{
            '_operation': 'insert',
            'variable_name': 'filter_array_inclusive_of',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'Property Of',
            'inside_engine_name': 'filter_array'
        },{
            '_operation': 'insert',
            'variable_name': 'filter_object_inclusive_of',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'Property Of',
            'inside_engine_name': 'filter_object'
        },{
            '_operation': 'insert',
            'variable_name': 'filter_string_exclusive_of',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': False,
            'filter_type': 'Property Of',
            'inside_engine_name': 'filter_string'
        },{
            '_operation': 'insert',
            'variable_name': 'filter_integer_exclusive_of',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': False,
            'filter_type': 'Property Of',
            'inside_engine_name': 'filter_integer'
        },{
            '_operation': 'insert',
            'variable_name': 'filter_boolean_exclusive_of',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': False,
            'filter_type': 'Property Of',
            'inside_engine_name': 'filter_boolean'
        },{
            '_operation': 'insert',
            'variable_name': 'filter_array_exclusive_of',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': False,
            'filter_type': 'Property Of',
            'inside_engine_name': 'filter_array'
        },{
            '_operation': 'insert',
            'variable_name': 'filter_object_exclusive_of',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': False,
            'filter_type': 'Property Of',
            'inside_engine_name': 'filter_object'
        },{
            '_operation': 'insert',
            'variable_name': 'index_inclusive_of',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'Index Of',
            'inside_engine_name': 'sku'
        },{
            '_operation': 'insert',
            'variable_name': 'index_exclusive_of',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': False,
            'filter_type': 'Index Of',
            'inside_engine_name': 'sku'
        }]
    }
    session.loop.run_until_complete(models['slots'].insert(session, slot))
    slot = {
        'max_recos': 10,
        'name': 'test',
        'store_id': 1,
        'engine_id': 5,
        'fallbacks': [{'id': 2}]
    }
    session.loop.run_until_complete(models['slots'].insert(session, slot))

    yield None

    _all_models.pop('products_1')
    api.remove_swagger_paths(_all_models.pop('products_collection'))
    _all_models.pop('categories_1')
    api.remove_swagger_paths(_all_models.pop('categories_collection'))
    _all_models.pop('invalid_1')
    api.remove_swagger_paths(_all_models.pop('invalid_collection'))
    _all_models.pop('products_new_1')
    api.remove_swagger_paths(_all_models.pop('products_new_collection'))


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
                    'distribute_recos': {'type': 'boolean'},
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
            'distribute_recos': False,
            'hash': '941e021d7ae6ca23f8969870ffe48b87a315e05c',
            'name': 'Placement Test',
            'small_hash': '941e0',
            'store_id': 1,
            'variations': [{
                'slots': [{
                    'max_recos': 10,
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
                            'import_processor': None,
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
                        'core': {
                            'id': 1,
                            'name': 'visual_similarity',
                            'configuration': {
                                'core_module': {
                                    'path': 'tests.integration.fixtures',
                                    'object_name': 'EngineCoreTestWithVars'
                                }
                            }
                        },
                        'core_id': 1,
                        'variables': [{
                            'name': 'item_id', 'schema': {'type': 'integer'}
                        },{
                            'name': 'filter_test', 'schema': {'type': 'string'}
                        }],
                    },
                    'engine_id': 1,
                    'slot_variables': [{
                        'is_filter': True,
                        'is_inclusive_filter': True,
                        'filter_type': 'By Property',
                        'slot_id': 1,
                        'id': 1,
                        'inside_engine_name': 'filter_test',
                        'override': False,
                        'override_value': None,
                        'skip_values': None,
                        'variable': {
                            'name': 'test',
                            'store_id': 1
                        },
                        'variable_name': 'test',
                        'variable_store_id': 1
                    },{
                        'is_filter': True,
                        'is_inclusive_filter': True,
                        'filter_type': 'Property Of',
                        'slot_id': 1,
                        'id': 2,
                        'inside_engine_name': 'filter_test',
                        'override': False,
                        'override_value': None,
                        'skip_values': None,
                        'variable': {
                            'name': 'test3',
                            'store_id': 1
                        },
                        'variable_name': 'test3',
                        'variable_store_id': 1
                    },{
                        'is_filter': False,
                        'is_inclusive_filter': None,
                        'filter_type': None,
                        'slot_id': 1,
                        'id': 3,
                        'inside_engine_name': 'item_id',
                        'override': False,
                        'override_value': None,
                        'skip_values': None,
                        'variable': {
                            'name': 'test2',
                            'store_id': 1
                        },
                        'variable_name': 'test2',
                        'variable_store_id': 1
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
            'distribute_recos': False,
            'hash': '941e021d7ae6ca23f8969870ffe48b87a315e05c',
            'name': 'Placement Test',
            'small_hash': '941e0',
            'store_id': 1,
            'variations': [{
                'slots': [{
                    'max_recos': 10,
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
                            'import_processor': None,
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
                        'core': {
                            'id': 1,
                            'name': 'visual_similarity',
                            'configuration': {
                                'core_module': {
                                    'path': 'tests.integration.fixtures',
                                    'object_name': 'EngineCoreTestWithVars'
                                }
                            }
                        },
                        'core_id': 1,
                        'variables': [{
                            'name': 'item_id', 'schema': {'type': 'integer'}
                        },{
                            'name': 'filter_test', 'schema': {'type': 'string'}
                        }],
                    },
                    'engine_id': 1,
                    'slot_variables': [{
                        'is_filter': True,
                        'is_inclusive_filter': True,
                        'filter_type': 'By Property',
                        'slot_id': 1,
                        'id': 1,
                        'inside_engine_name': 'filter_test',
                        'override': False,
                        'override_value': None,
                        'skip_values': None,
                        'variable': {
                            'name': 'test',
                            'store_id': 1
                        },
                        'variable_name': 'test',
                        'variable_store_id': 1
                    },{
                        'is_filter': True,
                        'is_inclusive_filter': True,
                        'filter_type': 'Property Of',
                        'slot_id': 1,
                        'id': 2,
                        'inside_engine_name': 'filter_test',
                        'override': False,
                        'override_value': None,
                        'skip_values': None,
                        'variable': {
                            'name': 'test3',
                            'store_id': 1
                        },
                        'variable_name': 'test3',
                        'variable_store_id': 1
                    },{
                        'is_filter': False,
                        'is_inclusive_filter': None,
                        'filter_type': None,
                        'slot_id': 1,
                        'id': 3,
                        'inside_engine_name': 'item_id',
                        'override': False,
                        'override_value': None,
                        'skip_values': None,
                        'variable': {
                            'name': 'test2',
                            'store_id': 1
                        },
                        'variable_name': 'test2',
                        'variable_store_id': 1
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
                    'distribute_recos': {'type': 'boolean'},
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
            'distribute_recos': False,
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
            'distribute_recos': False,
            'hash': '941e021d7ae6ca23f8969870ffe48b87a315e05c',
            'name': 'Placement Test',
            'small_hash': '941e0',
            'store_id': 1,
            'variations': [{
                'slots': [{
                    'max_recos': 10,
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
                            'import_processor': None,
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
                        'core': {
                            'id': 1,
                            'name': 'visual_similarity',
                            'configuration': {
                                'core_module': {
                                    'path': 'tests.integration.fixtures',
                                    'object_name': 'EngineCoreTestWithVars'
                                }
                            }
                        },
                        'core_id': 1,
                        'variables': [{
                            'name': 'item_id', 'schema': {'type': 'integer'}
                        },{
                            'name': 'filter_test', 'schema': {'type': 'string'}
                        }],
                    },
                    'engine_id': 1,
                    'slot_variables': [{
                        'is_filter': True,
                        'is_inclusive_filter': True,
                        'filter_type': 'By Property',
                        'slot_id': 1,
                        'id': 1,
                        'inside_engine_name': 'filter_test',
                        'override': False,
                        'override_value': None,
                        'skip_values': None,
                        'variable': {
                            'name': 'test',
                            'store_id': 1
                        },
                        'variable_name': 'test',
                        'variable_store_id': 1
                    },{
                        'is_filter': True,
                        'is_inclusive_filter': True,
                        'filter_type': 'Property Of',
                        'slot_id': 1,
                        'id': 2,
                        'inside_engine_name': 'filter_test',
                        'override': False,
                        'override_value': None,
                        'skip_values': None,
                        'variable': {
                            'name': 'test3',
                            'store_id': 1
                        },
                        'variable_name': 'test3',
                        'variable_store_id': 1
                    },{
                        'is_filter': False,
                        'is_inclusive_filter': None,
                        'filter_type': None,
                        'slot_id': 1,
                        'id': 3,
                        'inside_engine_name': 'item_id',
                        'override': False,
                        'override_value': None,
                        'skip_values': None,
                        'variable': {
                            'name': 'test2',
                            'store_id': 1
                        },
                        'variable_name': 'test2',
                        'variable_store_id': 1
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

    async def test_get_recommendations_not_found(self, init_db, client, headers, headers_without_content_type):
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

        resp = await client.get('/placements/{}/recommendations'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 404

    async def test_get_recommendations_placement_not_found(self, init_db, client, headers_without_content_type):
        client = await client
        resp = await client.get('/placements/123/recommendations', headers=headers_without_content_type)
        assert resp.status == 404

    async def test_get_recommendations_with_variable_valid(self, init_db, client, headers, monkeypatch, headers_without_content_type):
        client = await client
        class_loader = mock.MagicMock()
        monkeypatch.setattr('myreco.placements.models.ModuleObjectLoader', class_loader)

        class_loader.load()().get_recommendations = CoroMock()
        class_loader.load()().get_recommendations.coro.return_value = [{'id': 1}, {'id': 2}, {'id': 3}]
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

        resp = await client.get('/placements/{}/recommendations?test2=1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert class_loader.load()().get_recommendations.call_count == 1
        assert class_loader.load()().get_recommendations.call_args_list[0][1] == {'item_id': 1}

    async def test_get_recommendations_valid(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [
            {'sku': 'test1', 'item_id': 1},
            {'sku': 'test3', 'item_id': 3},
            {'sku': 'test2', 'item_id': 2}
        ]

    async def test_get_recommendations_with_fallback(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        EngineCoreTestWithVars.get_recommendations.coro.return_value = []
        resp = await client.get('/placements/{}/recommendations'.format(obj['small_hash']), headers=headers_without_content_type)
        EngineCoreTestWithVars.get_recommendations.coro.reset_mock()

        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [
            {'sku': 'test1', 'item_id': 1},
            {'sku': 'test3', 'item_id': 3},
            {'sku': 'test2', 'item_id': 2}
        ]

    async def test_get_recommendations_with_explict_fallbacks(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations?explict_fallbacks=true'.format(
                                obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json()) == {
            "name": "Placement Test",
            "slots": [
                {
                    "name": "test2",
                    'item_type': 'products_new',
                    "recommendations": {
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

    async def test_get_recommendations_without_show_details(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)

        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [
            {'sku': 'test1', 'item_id': 1},
            {'sku': 'test3', 'item_id': 3},
            {'sku': 'test2', 'item_id': 2}
        ]

    async def test_get_recommendations_distributed(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        resp = await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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
            'distribute_recos': True,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 1}, {'id': 2}]
            }]
        }]
        resp = await client.post('/placements/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        random.seed(0)
        EngineCoreTestWithVars.get_recommendations.coro.return_value = [{'test': 1}, {'test': 2}]
        resp = await client.get('/placements/{}/recommendations'.format(obj['small_hash']), headers=headers_without_content_type)
        EngineCoreTestWithVars.get_recommendations.coro.reset_mock()

        assert resp.status == 200
        assert (await resp.json())['distributed_recommendations'] == [
            {'sku': 'test1', 'item_id': 1, 'type': 'products_new'},
            {'sku': 'test3', 'item_id': 3, 'type': 'products_new'},
            {'test': 1, 'type': 'products'},
            {'sku': 'test2', 'item_id': 2, 'type': 'products_new'},
            {'test': 2, 'type': 'products'}
        ]


class TestPlacementsGetRecomendationsFilters(object):

    async def test_get_recommendations_by_string_inclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations?filter_string_inclusive=test'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [
            {'sku': 'test1', 'item_id': 1, 'filter_string': 'test'},
            {'sku': 'test2', 'item_id': 2, 'filter_string': 'test'}
        ]

    async def test_get_recommendations_by_string_exclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations?filter_string_exclusive=test'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [{'sku': 'test3', 'item_id': 3}]

    async def test_get_recommendations_by_integer_inclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations?filter_integer_inclusive=1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [
            {'sku': 'test1', 'item_id': 1, 'filter_integer': 1},
            {'sku': 'test2', 'item_id': 2, 'filter_integer': 1}
        ]

    async def test_get_recommendations_by_integer_exclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations?filter_integer_exclusive=1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [{'sku': 'test3', 'item_id': 3}]

    async def test_get_recommendations_by_boolean_inclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations?filter_boolean_inclusive=true'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [
            {'sku': 'test1', 'item_id': 1, 'filter_boolean': True},
            {'sku': 'test2', 'item_id': 2, 'filter_boolean': True}
        ]

    async def test_get_recommendations_by_boolean_exclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations?filter_boolean_exclusive=true'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [{'sku': 'test3', 'item_id': 3}]

    async def test_get_recommendations_by_array_inclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations?filter_array_inclusive=t2,t3'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [
            {'filter_array': ['t1', 't2'], 'item_id': 1, 'sku': 'test1'},
            {'sku': 'test2', 'item_id': 2, 'filter_array': ['t2', 't3']}
        ]

    async def test_get_recommendations_by_array_exclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations?filter_array_exclusive=t2'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [{'sku': 'test3', 'item_id': 3}]

    async def test_get_recommendations_by_object_inclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations?filter_object_inclusive=id:1'.format(obj['small_hash']), headers=headers_without_content_type)

        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [
            {'sku': 'test1', 'item_id': 1, 'filter_object': {'id': 1}}
        ]

    async def test_get_recommendations_by_object_exclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations?filter_object_exclusive=id:1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [
            {'sku': 'test3', 'item_id': 3},
            {'sku': 'test2', 'item_id': 2, 'filter_object': {'id': 2}}
        ]

    async def test_get_recommendations_by_post_processing_inclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        resp = await client.post('/products_new/import_data_file?store_id=1&upload_file=false', headers=headers_, data=data_filez)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/import_data_file?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        os.remove(data_file.name)

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations?filter_post_processing_inclusive=1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [
            {'sku': 'test1', 'item_id': 1, 'filter_integer': 1, 'filter_post_processing': 1, 'filter_pre_processing': 2},
            {'sku': 'test2', 'item_id': 2, 'filter_integer': 1, 'filter_post_processing': 1, 'filter_pre_processing': 2}
        ]

    async def test_get_recommendations_by_post_processing_exclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        resp = await client.post('/products_new/import_data_file?store_id=1&upload_file=false', headers=headers_, data=data_filez)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/import_data_file?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        os.remove(data_file.name)

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations?filter_post_processing_exclusive=1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [{'sku': 'test3', 'item_id': 3}]



class TestPlacementsGetRecomendationsFiltersOf(object):

    async def test_get_recommendations_of_string_inclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations?filter_string_inclusive_of=1|test1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [
            {'sku': 'test1', 'item_id': 1, 'filter_string': 'test1'}]

    async def test_get_recommendations_of_string_exclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations?filter_string_exclusive_of=1|test1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [
            {'sku': 'test3', 'item_id': 3},
            {'sku': 'test2', 'item_id': 2, 'filter_string': 'test2'}]

    async def test_get_recommendations_of_integer_inclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations?filter_integer_inclusive_of=1|test1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [
            {'sku': 'test1', 'item_id': 1, 'filter_integer': 1}]

    async def test_get_recommendations_of_integer_exclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations?filter_integer_exclusive_of=1|test1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [
            {'sku': 'test3', 'item_id': 3},
            {'sku': 'test2', 'item_id': 2, 'filter_integer': 2}
        ]

    async def test_get_recommendations_of_boolean_inclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations?filter_boolean_inclusive_of=1|test1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [
            {'sku': 'test1', 'item_id': 1, 'filter_boolean': True},
            {'sku': 'test2', 'item_id': 2, 'filter_boolean': True}
        ]

    async def test_get_recommendations_of_boolean_inclusive_with_false(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations?filter_boolean_inclusive_of=3|test3'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [
            {'sku': 'test1', 'item_id': 1, 'filter_boolean': True},
            {'sku': 'test2', 'item_id': 2, 'filter_boolean': True}
        ]

    async def test_get_recommendations_of_boolean_exclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations?filter_boolean_exclusive_of=1|test1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [
            {'sku': 'test3', 'item_id': 3}]

    async def test_get_recommendations_of_array_inclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations?filter_array_inclusive_of=1|test1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [
            {'filter_array': ['t1', 't2'], 'item_id': 1, 'sku': 'test1'},
            {'sku': 'test2', 'item_id': 2, 'filter_array': ['t2', 't3']}]

    async def test_get_recommendations_of_array_exclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations?filter_array_exclusive_of=1|test1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [
            {'sku': 'test3', 'item_id': 3}]

    async def test_get_recommendations_of_object_inclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations?filter_object_inclusive_of=1|test1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [
            {'sku': 'test1', 'item_id': 1, 'filter_object': {'id': 1}}]

    async def test_get_recommendations_of_object_exclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations?filter_object_exclusive_of=1|test1'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [
            {'sku': 'test3', 'item_id': 3},
            {'sku': 'test2', 'item_id': 2, 'filter_object': {'id': 2}}]

    async def test_get_recommendations_of_index_inclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations?index_inclusive_of=1|test1,2|test2'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        obj1 = {'sku': 'test1', 'item_id': 1, 'filter_object': {'id': 1}}
        obj2 = {'sku': 'test2', 'item_id': 2, 'filter_object': {'id': 2}}
        assert (await resp.json())['slots'][0]['recommendations'] == [obj1, obj2] or \
            (await resp.json())['slots'][0]['recommendations'] == [obj2, obj1]

    async def test_get_recommendations_of_index_exclusive(self, init_db, client, headers, monkeypatch, headers_without_content_type):
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
        await client.post('/products_new/?store_id=1', headers=headers, data=ujson.dumps(products))

        await client.post('/products_new/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)
        while True:
            resp = await client.get(
                '/products_new/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

        resp = await client.get('/placements/{}/recommendations?index_exclusive_of=1|test1,2|test2'.format(obj['small_hash']), headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json())['slots'][0]['recommendations'] == [{'sku': 'test3', 'item_id': 3}]
