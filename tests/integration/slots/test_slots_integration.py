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


from unittest import mock
from time import sleep
from datetime import datetime
from tests.integration.fixtures import EngineCoreTest
from swaggerit.models._base import _all_models
import asyncio
import tempfile
import pytest
import ujson


@pytest.fixture
def init_db(models, session, api, monkeypatch):
    monkeypatch.setattr('myreco.engines.cores.base.makedirs', mock.MagicMock())

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
        'configuration': {'data_path': '/test'}
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
            'aggregators_ids_name': 'filter_test',
            'data_importer_path': 'test.test'
        }),
        'store_id': 1,
        'core_id': 1,
        'item_type_id': 2
    }
    session.loop.run_until_complete(models['engines'].insert(session, engine))
    engine = {
        'name': 'Invalid Top Seller',
        'configuration_json': ujson.dumps({
            'days_interval': 7,
            'data_importer_path': 'test.test'
        }),
        'store_id': 1,
        'core_id': 2,
        'item_type_id': 3
    }
    session.loop.run_until_complete(models['engines'].insert(session, engine))

    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'test', 'store_id': 1}))
    session.loop.run_until_complete(models['variables'].insert(session, {'name': 'test2', 'store_id': 1}))

    yield None

    _all_models.pop('products_1')
    api.remove_swagger_paths(_all_models.pop('products_collection'))
    _all_models.pop('categories_1')
    api.remove_swagger_paths(_all_models.pop('categories_collection'))
    _all_models.pop('invalid_1')
    api.remove_swagger_paths(_all_models.pop('invalid_collection'))


class TestSlotsModelPost(object):

   async def test_post_without_body(self, init_db, client, headers, headers_without_content_type):
        client = await client
        resp = await client.post('/slots/', headers=headers)
        assert resp.status == 400
        assert await resp.json() == {'message': 'Request body is missing'}

   async def test_post_with_invalid_body(self, init_db, client, headers, headers_without_content_type):
        client = await client
        resp = await client.post('/slots/', headers=headers, data='[{}]')
        assert resp.status == 400
        assert await resp.json() ==  {
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
                    'fallbacks': {'$ref': '#/definitions/SlotsModel.fallbacks'},
                    'slot_variables': {'$ref': '#/definitions/SlotsModel.slot_variables'}
                }
            }
        }

   async def test_post_with_invalid_variable_engine(self, init_db, client, headers, headers_without_content_type):
        client = await client
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
        resp = await client.post('/slots/', headers=headers, data=ujson.dumps(body))
        assert resp.status == 400
        assert await resp.json() ==  {
            'message': "Invalid engine variable with 'inside_engine_name' value 'test'",
            'instance': [{
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

   async def test_post_with_invalid_filter(self, init_db, client, headers, headers_without_content_type):
        client = await client
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
        resp = await client.post('/slots/', headers=headers, data=ujson.dumps(body))
        assert resp.status == 400
        assert await resp.json() ==  {
            'message': "Invalid filter with 'inside_engine_name' value 'test'",
            'instance': [{
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

   async def test_post_with_invalid_filter_missing_properties(self, init_db, client, headers, headers_without_content_type):
        client = await client
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
        resp = await client.post('/slots/', headers=headers, data=ujson.dumps(body))
        assert resp.status == 400
        assert await resp.json() ==  {
            'message': "When 'is_filter' is 'true' the properties "\
            "'is_inclusive_filter' and 'filter_type' must be setted",
            'instance': [{
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

   async def test_post_with_insert_engine_variable_engine_var(self, init_db, client, headers, headers_without_content_type):
        client = await client
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
        resp = await client.post('/slots/', headers=headers, data=ujson.dumps(body))

        assert resp.status == 201
        assert await resp.json() == [{
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
                    'post_processing_import': None,
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
                            'path': 'tests.integration.fixtures',
                            'object_name': 'EngineCoreTestWithVars'
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

   async def test_post_with_insert_engine_variable_engine_filter(self, init_db, client, headers, headers_without_content_type):
        client = await client
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
        resp = await client.post('/slots/', headers=headers, data=ujson.dumps(body))

        assert resp.status == 201
        assert await resp.json() == [{
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
                    'post_processing_import': None,
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
                            'path': 'tests.integration.fixtures',
                            'object_name': 'EngineCoreTestWithVars'
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

   async def test_post_with_fallback(self, init_db, client, headers, headers_without_content_type):
        client = await client
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
        await client.post('/slots/', headers=headers, data=ujson.dumps(body))

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
        resp = await client.post('/slots/', headers=headers, data=ujson.dumps(body))

        assert resp.status == 201
        assert await resp.json() == [{
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
                        'post_processing_import': None,
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
                                'path': 'tests.integration.fixtures',
                                'object_name': 'EngineCoreTestWithVars'
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
                    'post_processing_import': None,
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
                            'path': 'tests.integration.fixtures',
                            'object_name': 'EngineCoreTestWithVars'
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

   async def test_post_with_invalid_grant(self, client):
        client = await client
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
        resp = await client.post('/slots/', headers={'Authorization': 'invalid'}, data=ujson.dumps(body))
        assert resp.status == 401
        assert await resp.json() ==  {'message': 'Invalid authorization'}


class TestSlotsModelGet(object):

   async def test_get_not_found(self, init_db, client, headers, headers_without_content_type):
        client = await client
        resp = await client.get('/slots/?store_id=1', headers=headers_without_content_type)
        assert resp.status == 404

   async def test_get_invalid_with_body(self, init_db, client, headers, headers_without_content_type):
        client = await client
        resp = await client.get('/slots/?store_id=1', headers=headers, data='{}')
        assert resp.status == 400
        assert await resp.json() == {'message': 'Request body is not acceptable'}

   async def test_get(self, init_db, client, headers, headers_without_content_type):
        client = await client
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
        await client.post('/slots/', headers=headers, data=ujson.dumps(body))

        resp = await client.get('/slots/?store_id=1', headers=headers_without_content_type)
        assert resp.status == 200
        assert await resp.json() ==  [{
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
                    'post_processing_import': None,
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
                            'path': 'tests.integration.fixtures',
                            'object_name': 'EngineCoreTestWithVars'
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

   async def test_patch_without_body(self, init_db, client, headers, headers_without_content_type):
        client = await client
        resp = await client.patch('/slots/1/', headers=headers, data='')
        assert resp.status == 400
        assert await resp.json() == {'message': 'Request body is missing'}

   async def test_patch_with_invalid_body(self, init_db, client, headers, headers_without_content_type):
        client = await client
        resp = await client.patch('/slots/1/', headers=headers, data='{}')
        assert resp.status == 400
        assert await resp.json() ==  {
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
                        '$ref': '#/definitions/SlotsModel.fallbacks'
                    },
                    'slot_variables': {
                        '$ref': '#/definitions/SlotsModel.slot_variables'
                    }
                },
                'type': 'object'
            },
            'message': '{} does not have enough properties'
        }

   async def test_patch_with_invalid_engine_variable(self, init_db, client, headers, headers_without_content_type):
        client = await client
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
        resp = await client.post('/slots/', headers=headers, data=ujson.dumps(body))

        body = {
            'slot_variables': [{
                '_operation': 'update',
                'id': 1,
                'inside_engine_name': 'invalid'
            }]
        }
        resp = await client.patch('/slots/1/', headers=headers, data=ujson.dumps(body))
        assert resp.status == 400
        assert await resp.json() ==  {
            'message': "Invalid engine variable with 'inside_engine_name' value 'invalid'",
            'instance': [{
                'slot_variables': [{
                    '_operation': 'update',
                    'id': 1,
                    'inside_engine_name': 'invalid'
                }],
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

   async def test_patch_with_invalid_fallback_id(self, init_db, client, headers, headers_without_content_type):
        client = await client
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
        resp = await client.post('/slots/', headers=headers, data=ujson.dumps(body))

        body = {
            'fallbacks': [{'id': 1}]
        }
        resp = await client.patch('/slots/1/', headers=headers, data=ujson.dumps(body))
        assert resp.status == 400
        assert await resp.json() == {
            'instance': [{'fallbacks': [{'id': 1}]}],
            'message': "a Engine Manager can't fallback itself"
        }

   async def test_patch_with_invalid_fallback_item_type(self, init_db, client, headers, headers_without_content_type):
        client = await client
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
        resp = await client.post('/slots/', headers=headers, data=ujson.dumps(body))

        body = {
            'fallbacks': [{'id': 2}]
        }
        resp = await client.patch('/slots/1/', headers=headers, data=ujson.dumps(body))
        assert resp.status == 400
        assert await resp.json() == {
            'instance': [{'fallbacks': [{'id': 2}]}],
            'message': "Cannot set a fallback with different items types"
        }

   async def test_patch_not_found(self, init_db, client, headers, headers_without_content_type):
        client = await client
        body = {
            'name': 'test',
            'store_id': 1
        }
        resp = await client.patch('/slots/1/', headers=headers, data=ujson.dumps(body))
        assert resp.status == 404

   async def test_patch_valid(self, init_db, client, headers, headers_without_content_type):
        client = await client
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
        resp = await client.post('/slots/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        body = {
            'slot_variables': [{
                '_operation': 'update',
                'id': 1,
                'inside_engine_name': 'filter_test'
            }]
        }
        resp = await client.patch('/slots/1/', headers=headers, data=ujson.dumps(body))

        assert resp.status == 200
        assert await resp.json() ==  {
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
                    'post_processing_import': None,
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
                            'path': 'tests.integration.fixtures',
                            'object_name': 'EngineCoreTestWithVars'
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

   async def test_delete_with_body(self, init_db, client, headers, headers_without_content_type):
        client = await client
        resp = await client.delete('/slots/1/', headers=headers, data='{}')
        assert resp.status == 400
        assert await resp.json() == {'message': 'Request body is not acceptable'}

   async def test_delete(self, init_db, client, headers, headers_without_content_type):
        client = await client
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
        await client.post('/slots/', headers=headers, data=ujson.dumps(body))
        resp = await client.get('/slots/1/', headers=headers_without_content_type)
        assert resp.status == 200

        resp = await client.delete('/slots/1/', headers=headers_without_content_type)
        assert resp.status == 204

        resp = await client.get('/slots/1/', headers=headers_without_content_type)
        assert resp.status == 404


class TestSlotsModelUriTemplateGet(object):

   async def test_get_with_body(self, init_db, client, headers, headers_without_content_type):
        client = await client
        resp = await client.get('/slots/1/', headers=headers, data='{}')
        assert resp.status == 400
        assert await resp.json() == {'message': 'Request body is not acceptable'}

   async def test_get_not_found(self, init_db, client, headers, headers_without_content_type):
        client = await client
        resp = await client.get('/slots/1/', headers=headers_without_content_type)
        assert resp.status == 404

   async def test_get(self, init_db, client, headers, headers_without_content_type):
        client = await client
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
        await client.post('/slots/', headers=headers, data=ujson.dumps(body))

        resp = await client.get('/slots/1/', headers=headers_without_content_type)

        assert resp.status == 200
        assert await resp.json() == {
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
                    'post_processing_import': None,
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
                            'path': 'tests.integration.fixtures',
                            'object_name': 'EngineCoreTestWithVars'
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
