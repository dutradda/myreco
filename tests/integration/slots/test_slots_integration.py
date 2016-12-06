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
    SQLAlchemyRedisModelBase, SlotsModel,
    StoresModel, UsersModel, VariablesModel, ItemsTypesModel,
    EnginesModel, EnginesCoresModel)
from falconswagger.http_api import HttpAPI
from base64 import b64encode
from fakeredis import FakeStrictRedis
import pytest
import json


@pytest.fixture
def model_base():
    return SQLAlchemyRedisModelBase


@pytest.fixture
def app(session):
    user = {
        'name': 'test',
        'email': 'test',
        'password': 'test',
        'admin': True
    }
    UsersModel.insert(session, user)

    store = {
        'name': 'test',
        'country': 'test',
        'configuration': {'data_path': '/test'}
    }
    StoresModel.insert(session, store)

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
                'class_name': 'DataImporter'
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
            'aggregators_ids_name': 'filter_test',
            'data_importer_path': 'test.test'
        }),
        'store_id': 1,
        'core_id': 1,
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
        'core_id': 2,
        'item_type_id': 3
    }
    EnginesModel.insert(session, engine)

    VariablesModel.insert(session, {'name': 'test', 'store_id': 1})
    VariablesModel.insert(session, {'name': 'test2', 'store_id': 1})

    return HttpAPI([SlotsModel], session.bind, FakeStrictRedis())


@pytest.fixture
def headers():
    return {
        'Authorization': b64encode('test:test'.encode()).decode()
    }



class TestSlotsModelPost(object):

    def test_post_without_body(self, client, headers):
        resp = client.post('/slots/', headers=headers)
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is missing'}

    def test_post_with_invalid_body(self, client, headers):
        resp = client.post('/slots/', headers=headers, body='[{}]')
        assert resp.status_code == 400
        assert json.loads(resp.body) ==  {
            'error': {
                'input': {},
                'message': "'engine_id' is a required property",
                'schema': {
                    'type': 'object',
                    'additionalProperties': False,
                    'required': ['engine_id', 'store_id', 'slot_variables', 'max_recos', 'name'],
                    'properties': {
                        'max_recos': {'type': 'integer'},
                        'name': {'type': 'string'},
                        'store_id': {'type': 'integer'},
                        'engine_id': {'type': 'integer'},
                        'fallbacks': {'$ref': '#/definitions/fallbacks'},
                        'slot_variables': {'$ref': '#/definitions/slot_variables'}
                    }
                }
            }
        }

    def test_post_with_invalid_variable_engine(self, client, headers):
        body = [{
            'max_recos': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_variables': [{
                '_operation': 'insert',
                'variable_name': 'test',
                'variable_store_id': 1,
                'inside_engine_name': 'test'
            }]
        }]
        resp = client.post('/slots/', headers=headers, body=json.dumps(body))
        assert resp.status_code == 400
        assert json.loads(resp.body) ==  {
            'error': {
                'message': "Invalid engine variable with 'inside_engine_name' value 'test'",
                'input': [{
                    'max_recos': 10,
                    'name': 'test',
                    'engine_id': 1,
                    'store_id': 1,
                    'slot_variables': [{
                        '_operation': 'insert',
                        'inside_engine_name': 'test',
                        'variable_name': 'test',
                        'variable_store_id': 1
                    }]
                }],
                'schema': {
                    'available_variables': [{
                        'name': 'filter_test',
                        'schema': {"type": "string"}
                    },{
                        'name': 'item_id',
                        'schema': {"type": "integer"}
                    }]
                }
            }
        }

    def test_post_with_invalid_filter(self, client, headers):
        body = [{
            'max_recos': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_variables': [{
                '_operation': 'insert',
                'variable_name': 'test',
                'variable_store_id': 1,
                'inside_engine_name': 'test',
                'is_filter': True,
                'filter_type': 'By Property',
                'is_inclusive_filter': True
            }]
        }]
        resp = client.post('/slots/', headers=headers, body=json.dumps(body))
        assert resp.status_code == 400
        assert json.loads(resp.body) ==  {
            'error': {
                'message': "Invalid filter with 'inside_engine_name' value 'test'",
                'input': [{
                    'max_recos': 10,
                    'name': 'test',
                    'engine_id': 1,
                    'store_id': 1,
                    'slot_variables': [{
                        '_operation': 'insert',
                        'inside_engine_name': 'test',
                        'is_filter': True,
                        'is_inclusive_filter': True,
                        'filter_type': 'By Property',
                        'variable_name': 'test',
                        'variable_store_id': 1
                    }]
                }],
                'schema': {
                    'available_filters': [{
                        'name': 'filter_test',
                        'schema': {"type": "string"}
                    },{
                        'name': 'item_id',
                        'schema': {"type": "integer"}
                    }]
                }
            }
        }

    def test_post_with_invalid_filter_missing_properties(self, client, headers):
        body = [{
            'max_recos': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_variables': [{
                '_operation': 'insert',
                'variable_name': 'test',
                'variable_store_id': 1,
                'inside_engine_name': 'filter_test',
                'is_filter': True,
                'filter_type': 'By Property'
            }]
        }]
        resp = client.post('/slots/', headers=headers, body=json.dumps(body))
        assert resp.status_code == 400
        assert json.loads(resp.body) ==  {
            'error': {
                'message': "When 'is_filter' is 'true' the properties "\
                "'is_inclusive_filter' and 'filter_type' must be setted",
                'input': [{
                    'max_recos': 10,
                    'name': 'test',
                    'engine_id': 1,
                    'store_id': 1,
                    'slot_variables': [{
                        '_operation': 'insert',
                        'filter_type': 'By Property',
                        'inside_engine_name': 'filter_test',
                        'is_filter': True,
                        'variable_name': 'test',
                        'variable_store_id': 1
                    }]
                }]
            }
        }

    def test_post_with_insert_engine_variable_engine_var(self, client, headers):
        body = [{
            'max_recos': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_variables': [{
                '_operation': 'insert',
                'variable_name': 'test',
                'variable_store_id': 1,
                'inside_engine_name': 'filter_test'
            }]
        }]
        resp = client.post('/slots/', headers=headers, body=json.dumps(body))

        assert resp.status_code == 201
        assert json.loads(resp.body) == [{
            'max_recos': 10,
            'name': 'test',
            'fallbacks': [],
            'id': 1,
            'slot_variables': [
                {
                    'variable': {
                        'name': 'test',
                        'store_id': 1
                    },
                    'id': 1,
                    'inside_engine_name': 'filter_test',
                    'slot_id': 1,
                    'override': False,
                    'override_value': None,
                    'skip_values': None,
                    'variable_name': 'test',
                    'variable_store_id': 1,
                    'is_filter': False,
                    'is_inclusive_filter': None,
                    'filter_type': None
                }
            ],
            'engine': {
                'item_type': {
                    'id': 1,
                    'stores': [{
                        'configuration': {'data_path': '/test'},
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
                'store_id': 1,
                'name': 'Visual Similarity',
                'item_type_id': 1,
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
                'id': 1,
                'variables': [{
                    'name': 'item_id', 'schema': {'type': 'integer'}
                },{
                    'name': 'filter_test', 'schema': {'type': 'string'}
                }],
                'core_id': 1,
                'configuration': {
                    'aggregators_ids_name': 'filter_test',
                    'item_id_name': 'item_id',
                    'data_importer_path': 'test.test'
                },
                'store': {
                    'id': 1,
                    'country': 'test',
                    'name': 'test',
                    'configuration': {'data_path': '/test'}
                }
            },
            'store_id': 1,
            'engine_id': 1
        }]

    def test_post_with_insert_engine_variable_engine_filter(self, client, headers):
        body = [{
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
            }]
        }]
        resp = client.post('/slots/', headers=headers, body=json.dumps(body))

        assert resp.status_code == 201
        assert json.loads(resp.body) == [{
            'fallbacks': [],
            'id': 1,
            'max_recos': 10,
            'name': 'test',
            'slot_variables': [
                {
                    'variable': {
                        'name': 'test',
                        'store_id': 1
                    },
                    'id': 1,
                    'inside_engine_name': 'filter_test',
                    'slot_id': 1,
                    'override': False,
                    'override_value': None,
                    'skip_values': None,
                    'variable_name': 'test',
                    'variable_store_id': 1,
                    'is_filter': True,
                    'is_inclusive_filter': True,
                    'filter_type': 'By Property'
                }
            ],
            'engine': {
                'item_type': {
                    'id': 1,
                    'stores': [{
                        'configuration': {'data_path': '/test'},
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
                'store_id': 1,
                'name': 'Visual Similarity',
                'item_type_id': 1,
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
                'id': 1,
                'variables': [{
                    'name': 'item_id', 'schema': {'type': 'integer'}
                },{
                    'name': 'filter_test', 'schema': {'type': 'string'}
                }],
                'core_id': 1,
                'configuration': {
                    'aggregators_ids_name': 'filter_test',
                    'item_id_name': 'item_id',
                    'data_importer_path': 'test.test'
                },
                'store': {
                    'id': 1,
                    'country': 'test',
                    'name': 'test',
                    'configuration': {'data_path': '/test'}
                }
            },
            'store_id': 1,
            'engine_id': 1
        }]

    def test_post_with_fallback(self, client, headers):
        body = [{
            'max_recos': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_variables': [{
                '_operation': 'insert',
                'variable_name': 'test',
                'variable_store_id': 1,
                'inside_engine_name': 'item_id'
            }]
        }]
        client.post('/slots/', headers=headers, body=json.dumps(body))

        body = [{
            'max_recos': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_variables': [{
                '_operation': 'insert',
                'variable_name': 'test',
                'variable_store_id': 1,
                'inside_engine_name': 'item_id'
            }],
            'fallbacks': [{'id': 1}]
        }]
        resp = client.post('/slots/', headers=headers, body=json.dumps(body))

        assert resp.status_code == 201
        assert json.loads(resp.body) == [{
            'fallbacks': [{
                'max_recos': 10,
                'name': 'test',
                'id': 1,
                'slot_variables': [
                    {
                        'variable': {
                            'name': 'test',
                            'store_id': 1
                        },
                        'id': 1,
                        'inside_engine_name': 'item_id',
                        'slot_id': 1,
                        'override': False,
                        'override_value': None,
                        'skip_values': None,
                        'variable_name': 'test',
                        'variable_store_id': 1,
                        'is_filter': False,
                        'is_inclusive_filter': None,
                        'filter_type': None
                    }
                ],
                'engine': {
                    'item_type': {
                        'id': 1,
                        'stores': [{
                            'configuration': {'data_path': '/test'},
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
                    'store_id': 1,
                    'name': 'Visual Similarity',
                    'item_type_id': 1,
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
                    'id': 1,
                    'variables': [{
                        'name': 'item_id', 'schema': {'type': 'integer'}
                    },{
                        'name': 'filter_test', 'schema': {'type': 'string'}
                    }],
                    'core_id': 1,
                    'configuration': {
                        'aggregators_ids_name': 'filter_test',
                        'item_id_name': 'item_id',
                        'data_importer_path': 'test.test'
                    },
                    'store': {
                        'id': 1,
                        'country': 'test',
                        'name': 'test',
                        'configuration': {'data_path': '/test'}
                    }
                },
                'store_id': 1,
                'engine_id': 1
            }],
            'id': 2,
            'max_recos': 10,
            'name': 'test',
            'slot_variables': [
                {
                    'variable': {
                        'name': 'test',
                        'store_id': 1
                    },
                    'id': 2,
                    'inside_engine_name': 'item_id',
                    'slot_id': 2,
                    'override': False,
                    'override_value': None,
                    'skip_values': None,
                    'variable_name': 'test',
                    'variable_store_id': 1,
                    'is_filter': False,
                    'is_inclusive_filter': None,
                    'filter_type': None
                }
            ],
            'engine': {
                'item_type': {
                    'id': 1,
                    'stores': [{
                        'configuration': {'data_path': '/test'},
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
                'store_id': 1,
                'name': 'Visual Similarity',
                'item_type_id': 1,
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
                'id': 1,
                'variables': [{
                    'name': 'item_id', 'schema': {'type': 'integer'}
                },{
                    'name': 'filter_test', 'schema': {'type': 'string'}
                }],
                'core_id': 1,
                'configuration': {
                    'aggregators_ids_name': 'filter_test',
                    'item_id_name': 'item_id',
                    'data_importer_path': 'test.test'
                },
                'store': {
                    'id': 1,
                    'country': 'test',
                    'name': 'test',
                    'configuration': {'data_path': '/test'}
                }
            },
            'store_id': 1,
            'engine_id': 1
        }]

    def test_post_with_invalid_grant(self, client):
        body = [{
            'max_recos': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_variables': [{
                '_operation': 'insert',
                'variable_name': 'test',
                'variable_store_id': 1,
                'inside_engine_name': 'item_id'
            }]
        }]
        resp = client.post('/slots/', headers={'Authorization': 'invalid'}, body=json.dumps(body))
        assert resp.status_code == 401
        assert json.loads(resp.body) ==  {'error': 'Invalid authorization'}


class TestSlotsModelGet(object):

    def test_get_not_found(self, client, headers):
        resp = client.get('/slots/?store_id=1', headers=headers)
        assert resp.status_code == 404

    def test_get_invalid_with_body(self, client, headers):
        resp = client.get('/slots/?store_id=1', headers=headers, body='{}')
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is not acceptable'}

    def test_get(self, client, headers):
        body = [{
            'max_recos': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_variables': [{
                '_operation': 'insert',
                'variable_name': 'test',
                'variable_store_id': 1,
                'inside_engine_name': 'filter_test'
            }]
        }]
        client.post('/slots/', headers=headers, body=json.dumps(body))

        resp = client.get('/slots/?store_id=1', headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) ==  [{
            'fallbacks': [],
            'id': 1,
            'max_recos': 10,
            'name': 'test',
            'slot_variables': [
                {
                    'variable': {
                        'name': 'test',
                        'store_id': 1
                    },
                    'id': 1,
                    'inside_engine_name': 'filter_test',
                    'slot_id': 1,
                    'override': False,
                    'override_value': None,
                    'skip_values': None,
                    'variable_name': 'test',
                    'variable_store_id': 1,
                    'is_filter': False,
                    'is_inclusive_filter': None,
                    'filter_type': None
                }
            ],
            'engine': {
                'item_type': {
                    'id': 1,
                    'stores': [{
                        'configuration': {'data_path': '/test'},
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
                'store_id': 1,
                'name': 'Visual Similarity',
                'item_type_id': 1,
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
                'id': 1,
                'variables': [{
                    'name': 'item_id', 'schema': {'type': 'integer'}
                },{
                    'name': 'filter_test', 'schema': {'type': 'string'}
                }],
                'core_id': 1,
                'configuration': {
                    'aggregators_ids_name': 'filter_test',
                    'item_id_name': 'item_id',
                    'data_importer_path': 'test.test'
                },
                'store': {
                    'id': 1,
                    'country': 'test',
                    'name': 'test',
                    'configuration': {'data_path': '/test'}
                }
            },
            'store_id': 1,
            'engine_id': 1
        }]


class TestSlotsModelUriTemplatePatch(object):

    def test_patch_without_body(self, client, headers):
        resp = client.patch('/slots/1/', headers=headers, body='')
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is missing'}

    def test_patch_with_invalid_body(self, client, headers):
        resp = client.patch('/slots/1/', headers=headers, body='{}')
        assert resp.status_code == 400
        assert json.loads(resp.body) ==  {
            'error': {
                'input': {},
                'schema': {
                    'additionalProperties': False,
                    'minProperties': 1,
                    'properties': {
                        'max_recos': {
                            'type': 'integer'
                        },
                        'name': {
                            'type': 'string'
                        },
                        'engine_id': {
                            'type': 'integer'
                        },
                        'store_id': {
                            'type': 'integer'
                        },
                        'fallbacks': {
                            '$ref': '#/definitions/fallbacks'
                        },
                        'slot_variables': {
                            '$ref': '#/definitions/slot_variables'
                        }
                    },
                    'type': 'object'
                },
                'message': '{} does not have enough properties'
            }
        }


    def test_patch_with_invalid_engine_variable(self, client, headers):
        body = [{
            'max_recos': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_variables': [{
                '_operation': 'insert',
                'variable_name': 'test',
                'variable_store_id': 1,
                'inside_engine_name': 'filter_test'
            }]
        }]
        resp = client.post('/slots/', headers=headers, body=json.dumps(body))

        body = {
            'slot_variables': [{
                '_operation': 'update',
                'id': 1,
                'inside_engine_name': 'invalid'
            }]
        }
        resp = client.patch('/slots/1/', headers=headers, body=json.dumps(body))
        assert resp.status_code == 400
        assert json.loads(resp.body) ==  {
            'error': {
                'message': "Invalid engine variable with 'inside_engine_name' value 'invalid'",
                'input': {
                    'id': 1,
                    'slot_variables': [{
                        '_operation': 'update',
                        'id': 1,
                        'inside_engine_name': 'invalid'
                    }],
                },
                'schema': {
                    'available_variables': [{
                        'name': 'filter_test',
                        'schema': {"type": "string"}
                    },{
                        'name': 'item_id',
                        'schema': {"type": "integer"}
                    }]
                }
            }
        }

    def test_patch_with_invalid_fallback_id(self, client, headers):
        body = [{
            'max_recos': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_variables': [{
                '_operation': 'insert',
                'variable_name': 'test',
                'variable_store_id': 1,
                'inside_engine_name': 'filter_test'
            }]
        }]
        resp = client.post('/slots/', headers=headers, body=json.dumps(body))

        body = {
            'fallbacks': [{'id': 1}]
        }
        resp = client.patch('/slots/1/', headers=headers, body=json.dumps(body))
        assert resp.status_code == 400
        assert json.loads(resp.body) == {
            'error': {
                'input': {'fallbacks': [{'id': 1}], 'id': 1},
                'message': "a Engine Manager can't fallback itself"
            }
        }

    def test_patch_with_invalid_fallback_item_type(self, client, headers):
        body = [{
            'max_recos': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_variables': [{
                '_operation': 'insert',
                'variable_name': 'test',
                'variable_store_id': 1,
                'inside_engine_name': 'filter_test'
            }]
        },{
            'max_recos': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 2,
            'slot_variables': [{
                '_operation': 'insert',
                'variable_name': 'test',
                'variable_store_id': 1,
                'inside_engine_name': 'item_id'
            }]
        }]
        resp = client.post('/slots/', headers=headers, body=json.dumps(body))

        body = {
            'fallbacks': [{'id': 2}]
        }
        resp = client.patch('/slots/1/', headers=headers, body=json.dumps(body))
        assert resp.status_code == 400
        assert json.loads(resp.body) == {
            'error': {
                'input': {'fallbacks': [{'id': 2}], 'id': 1},
                'message': "Cannot set a fallback with different items types"
            }
        }

    def test_patch_not_found(self, client, headers):
        body = {
            'name': 'test',
            'store_id': 1
        }
        resp = client.patch('/engines/1/', headers=headers, body=json.dumps(body))
        assert resp.status_code == 404

    def test_patch(self, client, headers):
        body = [{
            'max_recos': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_variables': [{
                '_operation': 'insert',
                'variable_name': 'test',
                'variable_store_id': 1,
                'inside_engine_name': 'item_id'
            }]
        }]
        obj = json.loads(client.post('/slots/', headers=headers, body=json.dumps(body)).body)[0]

        body = {
            'slot_variables': [{
                '_operation': 'update',
                'id': 1,
                'inside_engine_name': 'filter_test'
            }]
        }
        resp = client.patch('/slots/1/', headers=headers, body=json.dumps(body))

        assert resp.status_code == 200
        assert json.loads(resp.body) ==  {
            'fallbacks': [],
            'id': 1,
            'max_recos': 10,
            'name': 'test',
            'slot_variables': [
                {
                    'variable': {
                        'name': 'test',
                        'store_id': 1
                    },
                    'id': 1,
                    'inside_engine_name': 'filter_test',
                    'slot_id': 1,
                    'override': False,
                    'override_value': None,
                    'skip_values': None,
                    'variable_name': 'test',
                    'variable_store_id': 1,
                    'is_filter': False,
                    'is_inclusive_filter': None,
                    'filter_type': None
                }
            ],
            'engine': {
                'item_type': {
                    'id': 1,
                    'stores': [{
                        'configuration': {'data_path': '/test'},
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
                'store_id': 1,
                'name': 'Visual Similarity',
                'item_type_id': 1,
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
                'id': 1,
                'variables': [{
                    'name': 'item_id', 'schema': {'type': 'integer'}
                },{
                    'name': 'filter_test', 'schema': {'type': 'string'}
                }],
                'core_id': 1,
                'configuration': {
                    'aggregators_ids_name': 'filter_test',
                    'item_id_name': 'item_id',
                    'data_importer_path': 'test.test'
                },
                'store': {
                    'id': 1,
                    'country': 'test',
                    'name': 'test',
                    'configuration': {'data_path': '/test'}
                }
            },
            'store_id': 1,
            'engine_id': 1
        }


class TestSlotsModelUriTemplateDelete(object):

    def test_delete_with_body(self, client, headers):
        resp = client.delete('/slots/1/', headers=headers, body='{}')
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is not acceptable'}

    def test_delete(self, client, headers):
        body = [{
            'max_recos': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_variables': [{
                '_operation': 'insert',
                'variable_name': 'test',
                'variable_store_id': 1,
                'inside_engine_name': 'filter_test'
            }]
        }]
        client.post('/slots/', headers=headers, body=json.dumps(body))
        resp = client.get('/slots/1/', headers=headers)
        assert resp.status_code == 200

        resp = client.delete('/slots/1/', headers=headers)
        assert resp.status_code == 204

        resp = client.get('/slots/1/', headers=headers)
        assert resp.status_code == 404


class TestSlotsModelUriTemplateGet(object):

    def test_get_with_body(self, client, headers):
        resp = client.get('/slots/1/', headers=headers, body='{}')
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is not acceptable'}

    def test_get_not_found(self, client, headers):
        resp = client.get('/slots/1/', headers=headers)
        assert resp.status_code == 404

    def test_get(self, client, headers):
        body = [{
            'max_recos': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_variables': [{
                '_operation': 'insert',
                'variable_name': 'test',
                'variable_store_id': 1,
                'inside_engine_name': 'filter_test'
            }]
        }]
        client.post('/slots/', headers=headers, body=json.dumps(body))

        resp = client.get('/slots/1/', headers=headers)

        assert resp.status_code == 200
        assert json.loads(resp.body) == {
            'fallbacks': [],
            'id': 1,
            'max_recos': 10,
            'name': 'test',
            'slot_variables': [
                {
                    'variable': {
                        'name': 'test',
                        'store_id': 1
                    },
                    'id': 1,
                    'inside_engine_name': 'filter_test',
                    'slot_id': 1,
                    'override': False,
                    'override_value': None,
                    'skip_values': None,
                    'variable_name': 'test',
                    'variable_store_id': 1,
                    'is_filter': False,
                    'is_inclusive_filter': None,
                    'filter_type': None
                }
            ],
            'engine': {
                'item_type': {
                    'id': 1,
                    'stores': [{
                        'configuration': {'data_path': '/test'},
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
                'store_id': 1,
                'name': 'Visual Similarity',
                'item_type_id': 1,
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
                'id': 1,
                'variables': [{
                    'name': 'item_id', 'schema': {'type': 'integer'}
                },{
                    'name': 'filter_test', 'schema': {'type': 'string'}
                }],
                'core_id': 1,
                'configuration': {
                    'aggregators_ids_name': 'filter_test',
                    'item_id_name': 'item_id',
                    'data_importer_path': 'test.test'
                },
                'store': {
                    'id': 1,
                    'country': 'test',
                    'name': 'test',
                    'configuration': {'data_path': '/test'}
                }
            },
            'store_id': 1,
            'engine_id': 1
        }
