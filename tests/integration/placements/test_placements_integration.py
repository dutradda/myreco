
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


from tests.integration.fixtures_models import (
    SQLAlchemyRedisModelBase, SlotsModel, PlacementsModel,
    UsersModel, StoresModel, VariablesModel, ItemsTypesModel,
    EnginesModel, EnginesCoresModel, DataImporter, TestEngine)
from pytest_falcon.plugin import Client
from myreco.factory import ModelsFactory
from falconswagger.http_api import HttpAPI
from base64 import b64encode
from fakeredis import FakeStrictRedis
from unittest import mock
from tempfile import TemporaryDirectory
import pytest
import json
import numpy as np
import os.path
import random


@pytest.fixture
def model_base():
    return SQLAlchemyRedisModelBase


@pytest.fixture
def redis():
    return FakeStrictRedis()


@pytest.fixture
def temp_dir():
    return TemporaryDirectory()

@pytest.fixture
def app(redis, session, temp_dir):
    UsersModel.__api__ = None
    user = {
        'name': 'test',
        'email': 'test',
        'password': 'test',
        'admin': True
    }
    UsersModel.insert(session, user)

    StoresModel.__api__ = None
    store = {
        'name': 'test',
        'country': 'test',
        'configuration': {'data_path': temp_dir.name}
    }
    StoresModel.insert(session, store)

    EnginesCoresModel.__api__ = None
    engine_core = {
        'name': 'visual_similarity',
        'configuration': {
            'core_module': {
                'path': 'tests.integration.fixtures_models',
                'class_name': 'TestEngine'
            }
        }
    }
    EnginesCoresModel.insert(session, engine_core)
    engine_core = {
        'name': 'top_seller',
        'configuration': {
            'core_module': {
                'path': 'myreco.engines.cores.top_seller.engine',
                'class_name': 'TopSellerEngine'
            },
            'data_importer_module': {
                'path': 'tests.integration.fixtures_models',
                'class_name': 'TestDataImporter'
            }
        }
    }
    EnginesCoresModel.insert(session, engine_core)

    schema = {
        'type': 'object',
        'id_names': ['item_id'],
        'properties': {
            'filter_test': {'type': 'string'},
            'item_id': {'type': 'integer'}
        }
    }

    ItemsTypesModel.__api__ = None
    item_type = {
        'name': 'products',
        'stores': [{'id': 1}],
        'schema': schema
    }
    ItemsTypesModel.insert(session, item_type)
    item_type = {
        'name': 'categories',
        'stores': [{'id': 1}],
        'schema': schema
    }
    ItemsTypesModel.insert(session, item_type)
    item_type = {
        'name': 'invalid',
        'stores': [{'id': 1}],
        'schema': {
            'type': 'object',
            'id_names': ['item_id'],
            'properties': {'item_id': {'type': 'string'}}
        }
    }
    ItemsTypesModel.insert(session, item_type)
    item_type = {
        'name': 'products_new',
        'stores': [{'id': 1}],
        'schema': {
            'type': 'object',
            'id_names': ['item_id', 'sku'],
            'properties': {
                'filter_string': {'type': 'string'},
                'filter_integer': {'type': 'integer'},
                'filter_boolean': {'type': 'boolean'},
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
    ItemsTypesModel.insert(session, item_type)

    EnginesModel.__api__ = None
    engine = {
        'name': 'Visual Similarity',
        'configuration_json': json.dumps({
            'item_id_name': 'item_id',
            'aggregators_ids_name': 'filter_test',
            'data_importer_path': 'test.test'
        }),
        'store_id': 1,
        'core_id': 1,
        'item_type_id': 1
    }
    EnginesModel.insert(session, engine)
    engine = {
        'name': 'Categories Visual Similarity',
        'configuration_json': json.dumps({
            'item_id_name': 'item_id',
            'aggregators_ids_name': 'filter_test'
        }),
        'store_id': 1,
        'core_id': 1,
        'item_type_id': 2
    }
    EnginesModel.insert(session, engine)
    engine = {
        'name': 'Invalid Top Seller',
        'configuration_json': json.dumps({
            'days_interval': 7
        }),
        'store_id': 1,
        'core_id': 2,
        'item_type_id': 3
    }
    EnginesModel.insert(session, engine)
    engine = {
        'name': 'Top Seller',
        'configuration_json': json.dumps({
            'days_interval': 7
        }),
        'store_id': 1,
        'core_id': 2,
        'item_type_id': 4
    }
    EnginesModel.insert(session, engine)
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
    EnginesModel.insert(session, engine)

    VariablesModel.__api__ = None
    VariablesModel.insert(session, {'name': 'test', 'store_id': 1})
    VariablesModel.insert(session, {'name': 'test2', 'store_id': 1})
    VariablesModel.insert(session, {'name': 'test3', 'store_id': 1})
    VariablesModel.insert(session, {'name': 'filter_string_inclusive', 'store_id': 1})
    VariablesModel.insert(session, {'name': 'filter_integer_inclusive', 'store_id': 1})
    VariablesModel.insert(session, {'name': 'filter_boolean_inclusive', 'store_id': 1})
    VariablesModel.insert(session, {'name': 'filter_array_inclusive', 'store_id': 1})
    VariablesModel.insert(session, {'name': 'filter_object_inclusive', 'store_id': 1})
    VariablesModel.insert(session, {'name': 'filter_string_exclusive', 'store_id': 1})
    VariablesModel.insert(session, {'name': 'filter_integer_exclusive', 'store_id': 1})
    VariablesModel.insert(session, {'name': 'filter_boolean_exclusive', 'store_id': 1})
    VariablesModel.insert(session, {'name': 'filter_array_exclusive', 'store_id': 1})
    VariablesModel.insert(session, {'name': 'filter_object_exclusive', 'store_id': 1})
    VariablesModel.insert(session, {'name': 'filter_string_inclusive_of', 'store_id': 1})
    VariablesModel.insert(session, {'name': 'filter_integer_inclusive_of', 'store_id': 1})
    VariablesModel.insert(session, {'name': 'filter_boolean_inclusive_of', 'store_id': 1})
    VariablesModel.insert(session, {'name': 'filter_array_inclusive_of', 'store_id': 1})
    VariablesModel.insert(session, {'name': 'filter_object_inclusive_of', 'store_id': 1})
    VariablesModel.insert(session, {'name': 'filter_string_exclusive_of', 'store_id': 1})
    VariablesModel.insert(session, {'name': 'filter_integer_exclusive_of', 'store_id': 1})
    VariablesModel.insert(session, {'name': 'filter_boolean_exclusive_of', 'store_id': 1})
    VariablesModel.insert(session, {'name': 'filter_array_exclusive_of', 'store_id': 1})
    VariablesModel.insert(session, {'name': 'filter_object_exclusive_of', 'store_id': 1})

    SlotsModel.__api__ = None
    slot = {
        'max_recos': 10,
        'name': 'test',
        'store_id': 1,
        'engine_id': 1,
        'engine_variables': [{
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
    SlotsModel.insert(session, slot)
    slot = {
        'max_recos': 10,
        'name': 'test2',
        'store_id': 1,
        'engine_id': 4,
        'engine_variables': [{
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
        }]
    }
    SlotsModel.insert(session, slot)
    slot = {
        'max_recos': 10,
        'name': 'test',
        'store_id': 1,
        'engine_id': 5,
        'fallbacks': [{'id': 2}]
    }
    SlotsModel.insert(session, slot)

    PlacementsModel.__api__ = None
    api = HttpAPI([PlacementsModel, ItemsTypesModel], session.bind, session.redis_bind)
    ItemsTypesModel.associate_all_items(session)
    return api


@pytest.fixture
def headers():
    return {
        'Authorization': b64encode('test:test'.encode()).decode()
    }


class TestPlacementsModelPost(object):

    def test_post_without_body(self, client, headers):
        resp = client.post('/placements/', headers=headers)
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is missing'}

    def test_post_with_invalid_body(self, client, headers):
        resp = client.post('/placements/', headers=headers, body='[{}]')
        assert resp.status_code == 400
        assert json.loads(resp.body) ==  {
            'error': {
                'input': {},
                'message': "'name' is a required property",
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
                        'variations': {'$ref': '#/definitions/variations'}
                    }
                }
            }
        }

    def test_post(self, client, headers, temp_dir):
        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 1}]
            }]
        }]
        resp = client.post('/placements/', headers=headers, body=json.dumps(body))
        assert resp.status_code == 201
        assert json.loads(resp.body) ==  [{
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
                                    'path': 'tests.integration.fixtures_models',
                                    'class_name': 'TestEngine'
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
                    'engine_variables': [{
                        'is_filter': True,
                        'is_inclusive_filter': True,
                        'filter_type': 'By Property',
                        'slot_id': 1,
                        'id': 1,
                        'inside_engine_name': 'filter_test',
                        'override': False,
                        'override_value_json': None,
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
                        'override_value_json': None,
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
                        'override_value_json': None,
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

    def test_post_with_invalid_grant(self, client):
        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 1}]
            }]
        }]
        resp = client.post('/placements/', headers={'Authorization': 'invalid'}, body=json.dumps(body))
        assert resp.status_code == 401
        assert json.loads(resp.body) ==  {'error': 'Invalid authorization'}



class TestPlacementsModelGet(object):

    def test_get_not_found(self, client, headers):
        resp = client.get('/placements/?store_id=1', headers=headers)
        assert resp.status_code == 404

    def test_get_invalid_with_body(self, client, headers):
        resp = client.get('/placements/?store_id=1', headers=headers, body='{}')
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is not acceptable'}

    def test_get(self, client, headers, temp_dir):
        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 1}]
            }]
        }]
        client.post('/placements/', headers=headers, body=json.dumps(body))

        resp = client.get('/placements/?store_id=1', headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) ==  [{
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
                                    'path': 'tests.integration.fixtures_models',
                                    'class_name': 'TestEngine'
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
                    'engine_variables': [{
                        'is_filter': True,
                        'is_inclusive_filter': True,
                        'filter_type': 'By Property',
                        'slot_id': 1,
                        'id': 1,
                        'inside_engine_name': 'filter_test',
                        'override': False,
                        'override_value_json': None,
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
                        'override_value_json': None,
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
                        'override_value_json': None,
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

    def test_patch_without_body(self, client, headers):
        resp = client.patch('/placements/1/', headers=headers, body='')
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is missing'}

    def test_patch_with_invalid_body(self, client, headers):
        resp = client.patch('/placements/1/', headers=headers, body='{}')
        assert resp.status_code == 400
        assert json.loads(resp.body) == {
            'error': {
                'input': {},
                'message': '{} does not have enough properties',
                'schema': {
                    'additionalProperties': False,
                    'minProperties': 1,
                    'properties': {
                        'ab_testing': {'type': 'boolean'},
                        'show_details': {'type': 'boolean'},
                        'distribute_recos': {'type': 'boolean'},
                        'name': {'type': 'string'},
                        'store_id': {'type': 'integer'},
                        'variations': {'$ref': '#/definitions/variations'}
                    },
                    'type': 'object'
                }
            }
        }

    def test_patch_not_found(self, client, headers):
        body = {
            'name': 'test'
        }
        resp = client.patch('/engines/1/', headers=headers, body=json.dumps(body))
        assert resp.status_code == 404

    def test_patch_valid(self, client, headers):
        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 1}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]

        body = {
            'variations': [{
                '_operation': 'update',
                'id': 1,
                'slots': [{'id': 1, '_operation': 'remove'}]
            }]
        }
        resp = client.patch('/placements/{}/'.format(obj['small_hash']),
            headers=headers, body=json.dumps(body))

        assert resp.status_code == 200
        assert json.loads(resp.body) ==  {
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

    def test_delete_with_body(self, client, headers):
        resp = client.delete('/placements/1/', headers=headers, body='{}')
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is not acceptable'}

    def test_delete_valid(self, client, headers):
        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 1}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200

        resp = client.delete('/placements/{}/'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 204

        resp = client.get('/placements/{}/'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 404


class TestPlacementsModelUriTemplateGet(object):

    def test_get_with_body(self, client, headers):
        resp = client.get('/placements/1/', headers=headers, body='{}')
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is not acceptable'}

    def test_get_not_found(self, client, headers):
        resp = client.get('/placements/1/', headers=headers)
        assert resp.status_code == 404

    def test_get_valid(self, client, headers, temp_dir):
        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 1}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]

        resp = client.get('/placements/{}/'.format(obj['small_hash']), headers=headers)

        assert resp.status_code == 200
        assert json.loads(resp.body) == {
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
                                    'path': 'tests.integration.fixtures_models',
                                    'class_name': 'TestEngine'
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
                    'engine_variables': [{
                        'is_filter': True,
                        'is_inclusive_filter': True,
                        'filter_type': 'By Property',
                        'slot_id': 1,
                        'id': 1,
                        'inside_engine_name': 'filter_test',
                        'override': False,
                        'override_value_json': None,
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
                        'override_value_json': None,
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
                        'override_value_json': None,
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
def filters_updater_app(session):
    table_args = {'mysql_engine':'innodb'}
    factory = ModelsFactory('myreco', commons_models_attributes={'__table_args__': table_args},
                            commons_tables_attributes=table_args)
    models = factory.make_all_models('exporter')
    api = HttpAPI([models['items_types'], models['engines']], session.bind, FakeStrictRedis())
    models['items_types'].associate_all_items(session)

    return api


@pytest.fixture
def filters_updater_client(filters_updater_app):
    return Client(filters_updater_app)


@mock.patch('falconswagger.models.base.random.getrandbits',
    new=mock.MagicMock(return_value=131940827655846590526331314439483569710))
class TestPlacementsGetRecomendations(object):

    def test_get_recommendations_not_found(self, client, headers):
        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 404

    def test_get_recommendations_placement_not_found(self, client, headers):
        resp = client.get('/placements/123/recommendations', headers=headers)
        assert resp.status_code == 404

    @mock.patch('myreco.placements.models.ModuleClassLoader')
    def test_get_recommendations_with_variable_valid(self, class_loader, client, headers):
        class_loader.load()().get_recommendations.return_value = [{'id': 1}, {'id': 2}, {'id': 3}]
        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 1}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?test2=1'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert class_loader.load()().get_recommendations.call_count == 1
        assert class_loader.load()().get_recommendations.call_args_list[0][1] == {'item_id': 1}

    def test_get_recommendations_valid(self, client, app, headers, filters_updater_client):
        items_model = app.models['products'].__models__[1]
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
        client.post('/products_new/?store_id=1', headers=headers, body=json.dumps(products))

        filters_updater_client.post('/products_new/update_filters?store_id=1', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/products_new/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        filters_updater_client.post('/engines/4/export_objects?import_data=true', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body)['recommendations'] == [{'sku': 'test1', 'item_id': 1, 'type': 'products_new'},
            {'sku': 'test3', 'item_id': 3, 'type': 'products_new'}, {'sku': 'test2', 'item_id': 2, 'type': 'products_new'}]

    def test_get_recommendations_with_fallback(self, client, app, headers, filters_updater_client):
        items_model = app.models['products'].__models__[1]
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
        client.post('/products_new/?store_id=1', headers=headers, body=json.dumps(products))

        filters_updater_client.post('/products_new/update_filters?store_id=1', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/products_new/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        filters_updater_client.post('/engines/4/export_objects?import_data=true', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 3}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]

        TestEngine.get_recommendations.return_value = []
        resp = client.get('/placements/{}/recommendations'.format(obj['small_hash']), headers=headers)
        TestEngine.get_recommendations.reset_mock()

        assert resp.status_code == 200
        assert json.loads(resp.body)['recommendations'] == [{'sku': 'test1', 'item_id': 1, 'type': 'products_new'},
            {'sku': 'test3', 'item_id': 3, 'type': 'products_new'}, {'sku': 'test2', 'item_id': 2, 'type': 'products_new'}]

    def test_get_recommendations_by_slots(self, client, app, headers, filters_updater_client):
        items_model = app.models['products'].__models__[1]
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
        client.post('/products_new/?store_id=1', headers=headers, body=json.dumps(products))

        filters_updater_client.post('/products_new/update_filters?store_id=1', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/products_new/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        filters_updater_client.post('/engines/4/export_objects?import_data=true', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/slots'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) == {
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

    def test_get_recommendations_without_show_details(self, client, app, headers, filters_updater_client):
        items_model = app.models['products'].__models__[1]
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
        client.post('/products_new/?store_id=1', headers=headers, body=json.dumps(products))

        filters_updater_client.post('/products_new/update_filters?store_id=1', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/products_new/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        filters_updater_client.post('/engines/4/export_objects?import_data=true', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
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
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body)['recommendations'] == [
            {'sku': 'test1', 'item_id': 1, 'type': 'products_new'},
            {'sku': 'test3', 'item_id': 3, 'type': 'products_new'},
            {'sku': 'test2', 'item_id': 2, 'type': 'products_new'}
        ]

    def test_get_recommendations_distributed(self, client, app, headers, filters_updater_client):
        random.seed(0)
        items_model = app.models['products'].__models__[1]
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
        client.post('/products_new/?store_id=1', headers=headers, body=json.dumps(products))

        filters_updater_client.post('/products_new/update_filters?store_id=1', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/products_new/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        filters_updater_client.post('/engines/4/export_objects?import_data=true', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
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
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]

        TestEngine.get_recommendations.return_value = [{'test': 1}, {'test': 2}]
        resp = client.get('/placements/{}/recommendations'.format(obj['small_hash']), headers=headers)
        TestEngine.get_recommendations.reset_mock()

        assert resp.status_code == 200
        assert json.loads(resp.body)['recommendations'] == [
            {'sku': 'test1', 'item_id': 1, 'type': 'products_new'},
            {'sku': 'test3', 'item_id': 3, 'type': 'products_new'},
            {'test': 1, 'type': 'products'},
            {'sku': 'test2', 'item_id': 2, 'type': 'products_new'},
            {'test': 2, 'type': 'products'}
        ]


@mock.patch('falconswagger.models.base.random.getrandbits',
    new=mock.MagicMock(return_value=131940827655846590526331314439483569710))
class TestPlacementsGetRecomendationsFilters(object):

    def test_get_recommendations_by_string_inclusive(self, client, app, headers, filters_updater_client, redis):
        items_model = app.models['products'].__models__[1]
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_string': 'test'
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_string': 'test2'
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        client.post('/products_new/?store_id=1', headers=headers, body=json.dumps(products))

        filters_updater_client.post('/products_new/update_filters?store_id=1', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/products_new/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        filters_updater_client.post('/engines/4/export_objects?import_data=true', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_string_inclusive=test,test2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body)['recommendations'] == [{'sku': 'test1', 'item_id': 1, 'filter_string': 'test', 'type': 'products_new'},
            {'sku': 'test2', 'item_id': 2, 'filter_string': 'test2', 'type': 'products_new'}]

    def test_get_recommendations_by_string_exclusive(self, client, app, headers, filters_updater_client):
        items_model = app.models['products'].__models__[1]
        products = [{
            'item_id': 1,
            'sku': 'test1',
            'filter_string': 'test'
        },{
            'item_id': 2,
            'sku': 'test2',
            'filter_string': 'test2'
        },{
            'item_id': 3,
            'sku': 'test3'
        }]
        client.post('/products_new/?store_id=1', headers=headers, body=json.dumps(products))

        filters_updater_client.post('/products_new/update_filters?store_id=1', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/products_new/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        filters_updater_client.post('/engines/4/export_objects?import_data=true', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_string_exclusive=test,test2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body)['recommendations'] == [{'sku': 'test3', 'item_id': 3, 'type': 'products_new'}]

    def test_get_recommendations_by_integer_inclusive(self, client, app, headers, filters_updater_client):
        items_model = app.models['products'].__models__[1]
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
        client.post('/products_new/?store_id=1', headers=headers, body=json.dumps(products))

        filters_updater_client.post('/products_new/update_filters?store_id=1', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/products_new/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        filters_updater_client.post('/engines/4/export_objects?import_data=true', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_integer_inclusive=1,2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body)['recommendations'] == [{'sku': 'test1', 'item_id': 1, 'filter_integer': 1, 'type': 'products_new'},
            {'sku': 'test2', 'item_id': 2, 'filter_integer': 2, 'type': 'products_new'}]

    def test_get_recommendations_by_integer_exclusive(self, client, app, headers, filters_updater_client):
        items_model = app.models['products'].__models__[1]
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
        client.post('/products_new/?store_id=1', headers=headers, body=json.dumps(products))

        filters_updater_client.post('/products_new/update_filters?store_id=1', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/products_new/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        filters_updater_client.post('/engines/4/export_objects?import_data=true', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_integer_exclusive=1,2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body)['recommendations'] == [{'sku': 'test3', 'item_id': 3, 'type': 'products_new'}]

    def test_get_recommendations_by_boolean_inclusive(self, client, app, headers, filters_updater_client):
        items_model = app.models['products'].__models__[1]
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
        client.post('/products_new/?store_id=1', headers=headers, body=json.dumps(products))

        filters_updater_client.post('/products_new/update_filters?store_id=1', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/products_new/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        filters_updater_client.post('/engines/4/export_objects?import_data=true', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_boolean_inclusive=false,true'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body)['recommendations'] == [{'sku': 'test1', 'item_id': 1, 'filter_boolean': True, 'type': 'products_new'},
            {'sku': 'test2', 'item_id': 2, 'filter_boolean': True, 'type': 'products_new'}]

    def test_get_recommendations_by_boolean_exclusive(self, client, app, headers, filters_updater_client):
        items_model = app.models['products'].__models__[1]
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
        client.post('/products_new/?store_id=1', headers=headers, body=json.dumps(products))

        filters_updater_client.post('/products_new/update_filters?store_id=1', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/products_new/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        filters_updater_client.post('/engines/4/export_objects?import_data=true', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_boolean_exclusive=true'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body)['recommendations'] == [{'sku': 'test3', 'item_id': 3, 'type': 'products_new'}]

    def test_get_recommendations_by_array_inclusive(self, client, app, headers, filters_updater_client):
        items_model = app.models['products'].__models__[1]
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
        client.post('/products_new/?store_id=1', headers=headers, body=json.dumps(products))

        filters_updater_client.post('/products_new/update_filters?store_id=1', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/products_new/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        filters_updater_client.post('/engines/4/export_objects?import_data=true', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_array_inclusive=t2,t3'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body)['recommendations'] == [{'sku': 'test1', 'item_id': 1, 'filter_array': ['t1', 't2'], 'type': 'products_new'},
            {'sku': 'test2', 'item_id': 2, 'filter_array': ['t2', 't3'], 'type': 'products_new'}]

    def test_get_recommendations_by_array_exclusive(self, client, app, headers, filters_updater_client):
        items_model = app.models['products'].__models__[1]
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
        client.post('/products_new/?store_id=1', headers=headers, body=json.dumps(products))

        filters_updater_client.post('/products_new/update_filters?store_id=1', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/products_new/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        filters_updater_client.post('/engines/4/export_objects?import_data=true', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_array_exclusive=t2,t3'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body)['recommendations'] == [{'sku': 'test3', 'item_id': 3, 'type': 'products_new'}]

    def test_get_recommendations_by_object_inclusive(self, client, app, headers, filters_updater_client):
        items_model = app.models['products'].__models__[1]
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
        client.post('/products_new/?store_id=1', headers=headers, body=json.dumps(products))

        filters_updater_client.post('/products_new/update_filters?store_id=1', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/products_new/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        filters_updater_client.post('/engines/4/export_objects?import_data=true', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_object_inclusive=id:1,id:2'.format(obj['small_hash']), headers=headers)

        assert resp.status_code == 200
        assert json.loads(resp.body)['recommendations'] == [{'sku': 'test1', 'item_id': 1, 'filter_object': {'id': 1}, 'type': 'products_new'},
            {'sku': 'test2', 'item_id': 2, 'filter_object': {'id': 2}, 'type': 'products_new'}]

    def test_get_recommendations_by_object_exclusive(self, client, app, headers, filters_updater_client):
        items_model = app.models['products'].__models__[1]
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
        client.post('/products_new/?store_id=1', headers=headers, body=json.dumps(products))

        filters_updater_client.post('/products_new/update_filters?store_id=1', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/products_new/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        filters_updater_client.post('/engines/4/export_objects?import_data=true', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_object_exclusive=id:1,id:2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body)['recommendations'] == [{'sku': 'test3', 'item_id': 3, 'type': 'products_new'}]


@mock.patch('falconswagger.models.base.random.getrandbits',
    new=mock.MagicMock(return_value=131940827655846590526331314439483569710))
class TestPlacementsGetRecomendationsFiltersOf(object):

    def test_get_recommendations_of_string_inclusive(self, client, app, headers, filters_updater_client, redis):
        items_model = app.models['products'].__models__[1]
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
        client.post('/products_new/?store_id=1', headers=headers, body=json.dumps(products))

        filters_updater_client.post('/products_new/update_filters?store_id=1', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/products_new/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        filters_updater_client.post('/engines/4/export_objects?import_data=true', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_string_inclusive_of=item_id:1|sku:test1,item_id:2|sku:test2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body)['recommendations'] == [{'sku': 'test1', 'item_id': 1, 'filter_string': 'test1', 'type': 'products_new'},
            {'sku': 'test2', 'item_id': 2, 'filter_string': 'test2', 'type': 'products_new'}]

    def test_get_recommendations_of_string_exclusive(self, client, app, headers, filters_updater_client, redis):
        items_model = app.models['products'].__models__[1]
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
        client.post('/products_new/?store_id=1', headers=headers, body=json.dumps(products))

        filters_updater_client.post('/products_new/update_filters?store_id=1', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/products_new/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        filters_updater_client.post('/engines/4/export_objects?import_data=true', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_string_exclusive_of=item_id:1|sku:test1,item_id:2|sku:test2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body)['recommendations'] == [{'sku': 'test3', 'item_id': 3, 'type': 'products_new'}]

    def test_get_recommendations_of_integer_inclusive(self, client, app, headers, filters_updater_client, redis):
        items_model = app.models['products'].__models__[1]
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
        client.post('/products_new/?store_id=1', headers=headers, body=json.dumps(products))

        filters_updater_client.post('/products_new/update_filters?store_id=1', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/products_new/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        filters_updater_client.post('/engines/4/export_objects?import_data=true', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_integer_inclusive_of=item_id:1|sku:test1,item_id:2|sku:test2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body)['recommendations'] == [{'sku': 'test1', 'item_id': 1, 'filter_integer': 1, 'type': 'products_new'},
            {'sku': 'test2', 'item_id': 2, 'filter_integer': 2, 'type': 'products_new'}]

    def test_get_recommendations_of_integer_exclusive(self, client, app, headers, filters_updater_client, redis):
        items_model = app.models['products'].__models__[1]
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
        client.post('/products_new/?store_id=1', headers=headers, body=json.dumps(products))

        filters_updater_client.post('/products_new/update_filters?store_id=1', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/products_new/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        filters_updater_client.post('/engines/4/export_objects?import_data=true', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_integer_exclusive_of=item_id:1|sku:test1,item_id:2|sku:test2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body)['recommendations'] == [{'sku': 'test3', 'item_id': 3, 'type': 'products_new'}]

    def test_get_recommendations_of_boolean_inclusive(self, client, app, headers, filters_updater_client, redis):
        items_model = app.models['products'].__models__[1]
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
        client.post('/products_new/?store_id=1', headers=headers, body=json.dumps(products))

        filters_updater_client.post('/products_new/update_filters?store_id=1', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/products_new/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        filters_updater_client.post('/engines/4/export_objects?import_data=true', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_boolean_inclusive_of=item_id:1|sku:test1,item_id:2|sku:test2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body)['recommendations'] == [{'sku': 'test1', 'item_id': 1, 'filter_boolean': True, 'type': 'products_new'},
            {'sku': 'test2', 'item_id': 2, 'filter_boolean': True, 'type': 'products_new'}]

    def test_get_recommendations_of_boolean_inclusive_with_false(self, client, app, headers, filters_updater_client, redis):
        items_model = app.models['products'].__models__[1]
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
        client.post('/products_new/?store_id=1', headers=headers, body=json.dumps(products))

        filters_updater_client.post('/products_new/update_filters?store_id=1', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/products_new/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        filters_updater_client.post('/engines/4/export_objects?import_data=true', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_boolean_inclusive_of=item_id:1|sku:test1,item_id:2|sku:test2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body)['recommendations'] == [{'sku': 'test1', 'item_id': 1, 'filter_boolean': True, 'type': 'products_new'},
            {'sku': 'test2', 'item_id': 2, 'filter_boolean': True, 'type': 'products_new'}]

    def test_get_recommendations_of_boolean_exclusive(self, client, app, headers, filters_updater_client, redis):
        items_model = app.models['products'].__models__[1]
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
        client.post('/products_new/?store_id=1', headers=headers, body=json.dumps(products))

        filters_updater_client.post('/products_new/update_filters?store_id=1', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/products_new/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        filters_updater_client.post('/engines/4/export_objects?import_data=true', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_boolean_exclusive_of=item_id:1|sku:test1,item_id:2|sku:test2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body)['recommendations'] == [{'sku': 'test3', 'item_id': 3, 'type': 'products_new'}]

    def test_get_recommendations_of_array_inclusive(self, client, app, headers, filters_updater_client, redis):
        items_model = app.models['products'].__models__[1]
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
        client.post('/products_new/?store_id=1', headers=headers, body=json.dumps(products))

        filters_updater_client.post('/products_new/update_filters?store_id=1', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/products_new/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        filters_updater_client.post('/engines/4/export_objects?import_data=true', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_array_inclusive_of=item_id:1|sku:test1,item_id:2|sku:test2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body)['recommendations'] == [{'sku': 'test1', 'item_id': 1, 'filter_array': ['t1', 't2'], 'type': 'products_new'},
            {'sku': 'test2', 'item_id': 2, 'filter_array': ['t2', 't3'], 'type': 'products_new'}]

    def test_get_recommendations_of_array_exclusive(self, client, app, headers, filters_updater_client, redis):
        items_model = app.models['products'].__models__[1]
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
        client.post('/products_new/?store_id=1', headers=headers, body=json.dumps(products))

        filters_updater_client.post('/products_new/update_filters?store_id=1', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/products_new/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        filters_updater_client.post('/engines/4/export_objects?import_data=true', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_array_exclusive_of=item_id:1|sku:test1,item_id:2|sku:test2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body)['recommendations'] == [{'sku': 'test3', 'item_id': 3, 'type': 'products_new'}]

    def test_get_recommendations_of_object_inclusive(self, client, app, headers, filters_updater_client, redis):
        items_model = app.models['products'].__models__[1]
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
        client.post('/products_new/?store_id=1', headers=headers, body=json.dumps(products))

        filters_updater_client.post('/products_new/update_filters?store_id=1', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/products_new/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        filters_updater_client.post('/engines/4/export_objects?import_data=true', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_object_inclusive_of=item_id:1|sku:test1,item_id:2|sku:test2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body)['recommendations'] == [{'sku': 'test1', 'item_id': 1, 'filter_object': {'id': 1}, 'type': 'products_new'},
            {'sku': 'test2', 'item_id': 2, 'filter_object': {'id': 2}, 'type': 'products_new'}]

    def test_get_recommendations_of_object_exclusive(self, client, app, headers, filters_updater_client, redis):
        items_model = app.models['products'].__models__[1]
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
        client.post('/products_new/?store_id=1', headers=headers, body=json.dumps(products))

        filters_updater_client.post('/products_new/update_filters?store_id=1', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/products_new/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        filters_updater_client.post('/engines/4/export_objects?import_data=true', headers=headers)
        while True:
            resp = filters_updater_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'slots': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_object_exclusive_of=item_id:1|sku:test1,item_id:2|sku:test2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body)['recommendations'] == [{'sku': 'test3', 'item_id': 3, 'type': 'products_new'}]
