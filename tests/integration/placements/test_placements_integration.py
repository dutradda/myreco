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
    SQLAlchemyRedisModelBase, EnginesManagersModel, PlacementsModel,
    UsersModel, StoresModel, VariablesModel, ItemsTypesModel,
    EnginesModel, EnginesTypesNamesModel, DataImporter)
from pytest_falcon.plugin import Client
from myreco.engines.types.base import EngineType
from myreco.engines.types.items_indices_map import ItemsIndicesMap
from myreco.engines.types.utils import build_data_path
from myreco.engines.types.top_seller.engine import build_data_pattern as build_top_seller_data_pattern
from myreco.factory import ModelsFactory
from falconswagger.http_api import HttpAPI
from base64 import b64encode
from fakeredis import FakeStrictRedis
from unittest import mock
from tempfile import TemporaryDirectory
from csv import DictWriter
from os import makedirs
import pytest
import json
import numpy as np
import gzip
import os.path


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

    EnginesTypesNamesModel.__api__ = None
    EnginesTypesNamesModel.insert(session, {'name': 'visual_similarity'})
    EnginesTypesNamesModel.insert(session, {'name': 'top_seller'})

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
        'type_name_id': 1,
        'item_type_id': 1
    }
    EnginesModel.insert(session, engine)
    engine = {
        'name': 'Categories Visual Similarity',
        'configuration_json': json.dumps({
            'item_id_name': 'item_id',
            'aggregators_ids_name': 'filter_test',
            'data_importer_path': 'test.test'
        }),
        'store_id': 1,
        'type_name_id': 1,
        'item_type_id': 2
    }
    EnginesModel.insert(session, engine)
    engine = {
        'name': 'Invalid Top Seller',
        'configuration_json': json.dumps({
            'days_interval': 7,
            'data_importer_path': 'test.test'
        }),
        'store_id': 1,
        'type_name_id': 2,
        'item_type_id': 3
    }
    EnginesModel.insert(session, engine)
    engine = {
        'name': 'Top Seller',
        'configuration_json': json.dumps({
            'days_interval': 7,
            'data_importer_path': 'tests.integration.fixtures_models.DataImporter'
        }),
        'store_id': 1,
        'type_name_id': 2,
        'item_type_id': 4
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

    EnginesManagersModel.__api__ = None
    engine_manager = {
        'max_recos': 10,
        'store_id': 1,
        'engine_id': 1,
        'engine_variables': [{
            '_operation': 'insert',
            'variable_id': 1,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter_test'
        },{
            '_operation': 'insert',
            'variable_id': 3,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'Property Of',
            'inside_engine_name': 'filter_test'
        },{
            '_operation': 'insert',
            'variable_id': 2,
            'inside_engine_name': 'item_id'
        }]
    }
    EnginesManagersModel.insert(session, engine_manager)
    engine_manager = {
        'max_recos': 10,
        'store_id': 1,
        'engine_id': 4,
        'engine_variables': [{
            '_operation': 'insert',
            'variable_id': 4,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter_string'
        },{
            '_operation': 'insert',
            'variable_id': 5,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter_integer'
        },{
            '_operation': 'insert',
            'variable_id': 6,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter_boolean'
        },{
            '_operation': 'insert',
            'variable_id': 7,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter_array'
        },{
            '_operation': 'insert',
            'variable_id': 8,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter_object'
        },{
            '_operation': 'insert',
            'variable_id': 9,
            'is_filter': True,
            'is_inclusive_filter': False,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter_string'
        },{
            '_operation': 'insert',
            'variable_id': 10,
            'is_filter': True,
            'is_inclusive_filter': False,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter_integer'
        },{
            '_operation': 'insert',
            'variable_id': 11,
            'is_filter': True,
            'is_inclusive_filter': False,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter_boolean'
        },{
            '_operation': 'insert',
            'variable_id': 12,
            'is_filter': True,
            'is_inclusive_filter': False,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter_array'
        },{
            '_operation': 'insert',
            'variable_id': 13,
            'is_filter': True,
            'is_inclusive_filter': False,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter_object'
        },{
            '_operation': 'insert',
            'variable_id': 14,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'Property Of',
            'inside_engine_name': 'filter_string'
        },{
            '_operation': 'insert',
            'variable_id': 15,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'Property Of',
            'inside_engine_name': 'filter_integer'
        },{
            '_operation': 'insert',
            'variable_id': 16,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'Property Of',
            'inside_engine_name': 'filter_boolean'
        },{
            '_operation': 'insert',
            'variable_id': 17,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'Property Of',
            'inside_engine_name': 'filter_array'
        },{
            '_operation': 'insert',
            'variable_id': 18,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'Property Of',
            'inside_engine_name': 'filter_object'
        },{
            '_operation': 'insert',
            'variable_id': 19,
            'is_filter': True,
            'is_inclusive_filter': False,
            'filter_type': 'Property Of',
            'inside_engine_name': 'filter_string'
        },{
            '_operation': 'insert',
            'variable_id': 20,
            'is_filter': True,
            'is_inclusive_filter': False,
            'filter_type': 'Property Of',
            'inside_engine_name': 'filter_integer'
        },{
            '_operation': 'insert',
            'variable_id': 21,
            'is_filter': True,
            'is_inclusive_filter': False,
            'filter_type': 'Property Of',
            'inside_engine_name': 'filter_boolean'
        },{
            '_operation': 'insert',
            'variable_id': 22,
            'is_filter': True,
            'is_inclusive_filter': False,
            'filter_type': 'Property Of',
            'inside_engine_name': 'filter_array'
        },{
            '_operation': 'insert',
            'variable_id': 23,
            'is_filter': True,
            'is_inclusive_filter': False,
            'filter_type': 'Property Of',
            'inside_engine_name': 'filter_object'
        }]
    }
    EnginesManagersModel.insert(session, engine_manager)

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
                'engines_managers': [{'id': 1}]
            }]
        }]
        resp = client.post('/placements/', headers=headers, body=json.dumps(body))
        assert resp.status_code == 201
        assert json.loads(resp.body) ==  [{
            'ab_testing': False,
            'hash': '603de7791bc268d86c705e417448d3c6efbb3439',
            'name': 'Placement Test',
            'small_hash': '603de',
            'store_id': 1,
            'variations': [{
                'engines_managers': [{
                    'max_recos': 10,
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
                        'type_name': {
                            'id': 1,
                            'name': 'visual_similarity'
                        },
                        'type_name_id': 1,
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
                        'engine_manager_id': 1,
                        'id': 1,
                        'inside_engine_name': 'filter_test',
                        'override': False,
                        'override_value_json': None,
                        'variable': {
                            'id': 1,
                            'name': 'test',
                            'store_id': 1
                        },
                        'variable_id': 1
                    },{
                        'is_filter': True,
                        'is_inclusive_filter': True,
                        'filter_type': 'Property Of',
                        'engine_manager_id': 1,
                        'id': 2,
                        'inside_engine_name': 'filter_test',
                        'override': False,
                        'override_value_json': None,
                        'variable': {
                            'id': 3,
                            'name': 'test3',
                            'store_id': 1
                        },
                        'variable_id': 3
                    },{
                        'is_filter': False,
                        'is_inclusive_filter': None,
                        'filter_type': None,
                        'engine_manager_id': 1,
                        'id': 3,
                        'inside_engine_name': 'item_id',
                        'override': False,
                        'override_value_json': None,
                        'variable': {
                            'id': 2,
                            'name': 'test2',
                            'store_id': 1
                        },
                        'variable_id': 2
                    }],
                    'fallbacks': [],
                    'id': 1,
                    'store_id': 1
                }],
                'id': 1,
                'placement_hash': '603de7791bc268d86c705e417448d3c6efbb3439',
                'weight': None
            }]
        }]

    def test_post_with_invalid_grant(self, client):
        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'engines_managers': [{'id': 1}]
            }]
        }]
        resp = client.post('/placements/', headers={'Authorization': 'invalid'}, body=json.dumps(body))
        assert resp.status_code == 401
        assert json.loads(resp.body) ==  {'error': 'Invalid authorization'}



class TestPlacementsModelGet(object):

    def test_get_not_found(self, client, headers):
        resp = client.get('/placements/', headers=headers)
        assert resp.status_code == 404

    def test_get_invalid_with_body(self, client, headers):
        resp = client.get('/placements/', headers=headers, body='{}')
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is not acceptable'}

    def test_get(self, client, headers, temp_dir):
        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'engines_managers': [{'id': 1}]
            }]
        }]
        client.post('/placements/', headers=headers, body=json.dumps(body))

        resp = client.get('/placements/', headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) ==  [{
            'ab_testing': False,
            'hash': '603de7791bc268d86c705e417448d3c6efbb3439',
            'name': 'Placement Test',
            'small_hash': '603de',
            'store_id': 1,
            'variations': [{
                'engines_managers': [{
                    'max_recos': 10,
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
                        'type_name': {
                            'id': 1,
                            'name': 'visual_similarity'
                        },
                        'type_name_id': 1,
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
                        'engine_manager_id': 1,
                        'id': 1,
                        'inside_engine_name': 'filter_test',
                        'override': False,
                        'override_value_json': None,
                        'variable': {
                            'id': 1,
                            'name': 'test',
                            'store_id': 1
                        },
                        'variable_id': 1
                    },{
                        'is_filter': True,
                        'is_inclusive_filter': True,
                        'filter_type': 'Property Of',
                        'engine_manager_id': 1,
                        'id': 2,
                        'inside_engine_name': 'filter_test',
                        'override': False,
                        'override_value_json': None,
                        'variable': {
                            'id': 3,
                            'name': 'test3',
                            'store_id': 1
                        },
                        'variable_id': 3
                    },{
                        'is_filter': False,
                        'is_inclusive_filter': None,
                        'filter_type': None,
                        'engine_manager_id': 1,
                        'id': 3,
                        'inside_engine_name': 'item_id',
                        'override': False,
                        'override_value_json': None,
                        'variable': {
                            'id': 2,
                            'name': 'test2',
                            'store_id': 1
                        },
                        'variable_id': 2
                    }],
                    'fallbacks': [],
                    'id': 1,
                    'store_id': 1
                }],
                'id': 1,
                'placement_hash': '603de7791bc268d86c705e417448d3c6efbb3439',
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
                'engines_managers': [{'id': 1}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]

        body = {
            'variations': [{
                '_operation': 'update',
                'id': 1,
                'engines_managers': [{'id': 1, '_operation': 'remove'}]
            }]
        }
        resp = client.patch('/placements/{}/'.format(obj['small_hash']),
            headers=headers, body=json.dumps(body))

        assert resp.status_code == 200
        assert json.loads(resp.body) ==  {
            'ab_testing': False,
            'hash': '603de7791bc268d86c705e417448d3c6efbb3439',
            'name': 'Placement Test',
            'small_hash': '603de',
            'store_id': 1,
            'variations': [{
                'engines_managers': [],
                'id': 1,
                'placement_hash': '603de7791bc268d86c705e417448d3c6efbb3439',
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
                'engines_managers': [{'id': 1}]
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
                'engines_managers': [{'id': 1}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]

        resp = client.get('/placements/{}/'.format(obj['small_hash']), headers=headers)

        assert resp.status_code == 200
        assert json.loads(resp.body) == {
            'ab_testing': False,
            'hash': '603de7791bc268d86c705e417448d3c6efbb3439',
            'name': 'Placement Test',
            'small_hash': '603de',
            'store_id': 1,
            'variations': [{
                'engines_managers': [{
                    'max_recos': 10,
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
                        'type_name': {
                            'id': 1,
                            'name': 'visual_similarity'
                        },
                        'type_name_id': 1,
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
                        'engine_manager_id': 1,
                        'id': 1,
                        'inside_engine_name': 'filter_test',
                        'override': False,
                        'override_value_json': None,
                        'variable': {
                            'id': 1,
                            'name': 'test',
                            'store_id': 1
                        },
                        'variable_id': 1
                    },{
                        'is_filter': True,
                        'is_inclusive_filter': True,
                        'filter_type': 'Property Of',
                        'engine_manager_id': 1,
                        'id': 2,
                        'inside_engine_name': 'filter_test',
                        'override': False,
                        'override_value_json': None,
                        'variable': {
                            'id': 3,
                            'name': 'test3',
                            'store_id': 1
                        },
                        'variable_id': 3
                    },{
                        'is_filter': False,
                        'is_inclusive_filter': None,
                        'filter_type': None,
                        'engine_manager_id': 1,
                        'id': 3,
                        'inside_engine_name': 'item_id',
                        'override': False,
                        'override_value_json': None,
                        'variable': {
                            'id': 2,
                            'name': 'test2',
                            'store_id': 1
                        },
                        'variable_id': 2
                    }],
                    'fallbacks': [],
                    'id': 1,
                    'store_id': 1
                }],
                'id': 1,
                'placement_hash': '603de7791bc268d86c705e417448d3c6efbb3439',
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


@pytest.fixture
def top_seller_data_importer(request):
    def get_data(engine, items_indices_map, session):
        data_path = build_data_path(engine)
        if not os.path.isdir(data_path):
            makedirs(data_path)

        filename_prefix = build_top_seller_data_pattern(engine['configuration'])
        data = [{'item_id': 2, 'sku': 'test2', 'value': 1},
                {'item_id': 1, 'sku': 'test1', 'value': 3},
                {'item_id': 3, 'sku': 'test3', 'value': 2}]
        file_ = gzip.open(os.path.join(data_path, filename_prefix) + '-000000001.gz', 'wt')
        writer = DictWriter(file_, ['sku', 'value', 'item_id'])
        writer.writeheader()
        writer.writerows(data)
        file_.close()
        return {'lines_count': len(data)}

    def finalizer():
        DataImporter.get_data = mock.MagicMock()

    DataImporter.get_data = get_data
    request.addfinalizer(finalizer)
    return DataImporter


@mock.patch('falconswagger.models.base.random.getrandbits',
    new=mock.MagicMock(return_value=131940827655846590526331314439483569710))
class TestPlacementsGetRecomendations(object):

    def test_get_recommendations_not_found(self, client, headers):
        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'engines_managers': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 404

    def test_get_recommendations_placement_not_found(self, client, headers):
        resp = client.get('/placements/123/recommendations', headers=headers)
        assert resp.status_code == 404

    @mock.patch('myreco.placements.models.EngineTypeChooser')
    def test_get_recommendations_with_variable_valid(self, engine_chooser, client, headers):
        engine_chooser()().get_recommendations.return_value = [1, 2, 3]
        body = [{
            'store_id': 1,
            'name': 'Placement Test',
            'variations': [{
                '_operation': 'insert',
                'engines_managers': [{'id': 1}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?test2=1'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert engine_chooser()().get_recommendations.call_count == 1
        assert engine_chooser()().get_recommendations.call_args_list[0][1] == {'item_id': 1}

    def test_get_recommendations_valid(self, client, app, headers, top_seller_data_importer, filters_updater_client):
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
                'engines_managers': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) == [{'sku': 'test1', 'item_id': 1},
            {'sku': 'test3', 'item_id': 3}, {'sku': 'test2', 'item_id': 2}]


@mock.patch('falconswagger.models.base.random.getrandbits',
    new=mock.MagicMock(return_value=131940827655846590526331314439483569710))
class TestPlacementsGetRecomendationsFilters(object):

    def test_get_recommendations_by_string_inclusive(self, client, app, headers, top_seller_data_importer, filters_updater_client, redis):
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
                'engines_managers': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_string_inclusive=test,test2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) == [{'sku': 'test1', 'item_id': 1, 'filter_string': 'test'},
            {'sku': 'test2', 'item_id': 2, 'filter_string': 'test2'}]

    def test_get_recommendations_by_string_exclusive(self, client, app, headers, top_seller_data_importer, filters_updater_client):
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
                'engines_managers': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_string_exclusive=test,test2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert resp.body == json.dumps([{'sku': 'test3', 'item_id': 3}])

    def test_get_recommendations_by_integer_inclusive(self, client, app, headers, top_seller_data_importer, filters_updater_client):
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
                'engines_managers': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_integer_inclusive=1,2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) == [{'sku': 'test1', 'item_id': 1, 'filter_integer': 1},
            {'sku': 'test2', 'item_id': 2, 'filter_integer': 2}]

    def test_get_recommendations_by_integer_exclusive(self, client, app, headers, top_seller_data_importer, filters_updater_client):
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
                'engines_managers': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_integer_exclusive=1,2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) == [{'sku': 'test3', 'item_id': 3}]

    def test_get_recommendations_by_boolean_inclusive(self, client, app, headers, top_seller_data_importer, filters_updater_client):
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
                'engines_managers': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_boolean_inclusive=null,1'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) == [{'sku': 'test1', 'item_id': 1, 'filter_boolean': True},
            {'sku': 'test2', 'item_id': 2, 'filter_boolean': True}]

    def test_get_recommendations_by_boolean_exclusive(self, client, app, headers, top_seller_data_importer, filters_updater_client):
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
                'engines_managers': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_boolean_exclusive=null:1'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) == [{'sku': 'test3', 'item_id': 3}]

    def test_get_recommendations_by_array_inclusive(self, client, app, headers, top_seller_data_importer, filters_updater_client):
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
                'engines_managers': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_array_inclusive=t2,t3'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) == [{'sku': 'test1', 'item_id': 1, 'filter_array': ['t1', 't2']},
            {'sku': 'test2', 'item_id': 2, 'filter_array': ['t2', 't3']}]

    def test_get_recommendations_by_array_exclusive(self, client, app, headers, top_seller_data_importer, filters_updater_client):
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
                'engines_managers': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_array_exclusive=t2,t3'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) == [{'sku': 'test3', 'item_id': 3}]

    def test_get_recommendations_by_object_inclusive(self, client, app, headers, top_seller_data_importer, filters_updater_client):
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
                'engines_managers': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_object_inclusive=id:1,id:2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) == [{'sku': 'test1', 'item_id': 1, 'filter_object': {'id': 1}},
            {'sku': 'test2', 'item_id': 2, 'filter_object': {'id': 2}}]

    def test_get_recommendations_by_object_exclusive(self, client, app, headers, top_seller_data_importer, filters_updater_client):
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
                'engines_managers': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_object_exclusive=id:1,id:2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) == [{'sku': 'test3', 'item_id': 3}]


@mock.patch('falconswagger.models.base.random.getrandbits',
    new=mock.MagicMock(return_value=131940827655846590526331314439483569710))
class TestPlacementsGetRecomendationsFiltersOf(object):

    def test_get_recommendations_of_string_inclusive(self, client, app, headers, top_seller_data_importer, filters_updater_client, redis):
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
                'engines_managers': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_string_inclusive_of=item_id:1|sku:test1,item_id:2|sku:test2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) == [{'sku': 'test1', 'item_id': 1, 'filter_string': 'test1'},
            {'sku': 'test2', 'item_id': 2, 'filter_string': 'test2'}]

    def test_get_recommendations_of_string_exclusive(self, client, app, headers, top_seller_data_importer, filters_updater_client, redis):
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
                'engines_managers': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_string_exclusive_of=item_id:1|sku:test1,item_id:2|sku:test2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) == [{'sku': 'test3', 'item_id': 3}]

    def test_get_recommendations_of_integer_inclusive(self, client, app, headers, top_seller_data_importer, filters_updater_client, redis):
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
                'engines_managers': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_integer_inclusive_of=item_id:1|sku:test1,item_id:2|sku:test2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) == [{'sku': 'test1', 'item_id': 1, 'filter_integer': 1},
            {'sku': 'test2', 'item_id': 2, 'filter_integer': 2}]

    def test_get_recommendations_of_integer_exclusive(self, client, app, headers, top_seller_data_importer, filters_updater_client, redis):
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
                'engines_managers': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_integer_exclusive_of=item_id:1|sku:test1,item_id:2|sku:test2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) == [{'sku': 'test3', 'item_id': 3}]

    def test_get_recommendations_of_boolean_inclusive(self, client, app, headers, top_seller_data_importer, filters_updater_client, redis):
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
                'engines_managers': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_boolean_inclusive_of=item_id:1|sku:test1,item_id:2|sku:test2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) == [{'sku': 'test1', 'item_id': 1, 'filter_boolean': True},
            {'sku': 'test2', 'item_id': 2, 'filter_boolean': True}]

    def test_get_recommendations_of_boolean_inclusive_with_false(self, client, app, headers, top_seller_data_importer, filters_updater_client, redis):
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
                'engines_managers': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_boolean_inclusive_of=item_id:1|sku:test1,item_id:2|sku:test2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) == [{'sku': 'test1', 'item_id': 1, 'filter_boolean': True},
            {'sku': 'test2', 'item_id': 2, 'filter_boolean': True}]

    def test_get_recommendations_of_boolean_exclusive(self, client, app, headers, top_seller_data_importer, filters_updater_client, redis):
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
                'engines_managers': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_boolean_exclusive_of=item_id:1|sku:test1,item_id:2|sku:test2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) == [{'sku': 'test3', 'item_id': 3}]

    def test_get_recommendations_of_array_inclusive(self, client, app, headers, top_seller_data_importer, filters_updater_client, redis):
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
                'engines_managers': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_array_inclusive_of=item_id:1|sku:test1,item_id:2|sku:test2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) == [{'sku': 'test1', 'item_id': 1, 'filter_array': ['t1', 't2']},
            {'sku': 'test2', 'item_id': 2, 'filter_array': ['t2', 't3']}]

    def test_get_recommendations_of_array_exclusive(self, client, app, headers, top_seller_data_importer, filters_updater_client, redis):
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
                'engines_managers': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_array_exclusive_of=item_id:1|sku:test1,item_id:2|sku:test2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) == [{'sku': 'test3', 'item_id': 3}]

    def test_get_recommendations_of_object_inclusive(self, client, app, headers, top_seller_data_importer, filters_updater_client, redis):
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
                'engines_managers': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_object_inclusive_of=item_id:1|sku:test1,item_id:2|sku:test2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) == [{'sku': 'test1', 'item_id': 1, 'filter_object': {'id': 1}},
            {'sku': 'test2', 'item_id': 2, 'filter_object': {'id': 2}}]

    def test_get_recommendations_of_object_exclusive(self, client, app, headers, top_seller_data_importer, filters_updater_client, redis):
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
                'engines_managers': [{'id': 2}]
            }]
        }]
        obj = json.loads(client.post('/placements/', headers=headers, body=json.dumps(body)).body)[0]
        resp = client.get('/placements/{}/recommendations?filter_object_exclusive_of=item_id:1|sku:test1,item_id:2|sku:test2'.format(obj['small_hash']), headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) == [{'sku': 'test3', 'item_id': 3}]
