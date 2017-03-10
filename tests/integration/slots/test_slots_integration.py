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
from tests.integration.fixtures import EngineStrategyTest
from swaggerit.models._base import _all_models
import asyncio
import tempfile
import pytest
import ujson


@pytest.fixture
def init_db(models, session, api, monkeypatch):
    monkeypatch.setattr('myreco.engines.strategies.base.makedirs', mock.MagicMock())

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
        'strategy_class': {
            'module': 'tests.integration.fixtures',
            'class_name': 'EngineStrategyTestWithVars'
        },
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
        'strategy_class': {
            'module': 'tests.integration.fixtures',
            'class_name': 'EngineStrategyTestWithVars'
        },
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
        'strategy_class': {
            'module': 'tests.integration.fixtures',
            'class_name': 'EngineStrategyTest'
        },
        'item_type_id': 3
    }
    session.loop.run_until_complete(models['engines'].insert(session, engine))

    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'test', 'store_id': 1}))
    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'test2', 'store_id': 1}))

    yield None

    _all_models.pop('store_items_products_1')
    _all_models.pop('store_items_categories_1')
    _all_models.pop('store_items_invalid_1')


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
            'message': "'engine_id' is a required property. "\
                       "Failed validating instance['0'] for schema['items']['required']",
            'schema': {
                'type': 'object',
                'additionalProperties': False,
                'required': ['engine_id', 'store_id', 'max_items', 'name'],
                'properties': {
                    'max_items': {'type': 'integer'},
                    'name': {'type': 'string'},
                    'store_id': {'type': 'integer'},
                    'engine_id': {'type': 'integer'},
                    'fallbacks': {'$ref': '#/definitions/SlotsModel.fallbacks'},
                    'slot_variables': {'$ref': '#/definitions/SlotsModel.slot_variables'},
                    'slot_filters': {'$ref': '#/definitions/SlotsModel.slot_filters'}
                }
            }
        }

   async def test_post_with_invalid_external_variable_engine(self, init_db, client, headers, headers_without_content_type):
        client = await client
        body = [{
            'max_items': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_variables': [{
                '_operation': 'insert',
                'external_variable_name': 'test',
                'external_variable_store_id': 1,
                'engine_variable_name': 'test'
            }]
        }]
        resp = await client.post('/slots/', headers=headers, data=ujson.dumps(body))
        assert resp.status == 400
        assert await resp.json() ==  {
            'message': "Invalid slot variable with 'engine_variable_name' attribute value 'test'",
            'instance': [{
                'max_items': 10,
                'name': 'test',
                'engine_id': 1,
                'store_id': 1,
                'slot_variables': [{
                    '_operation': 'insert',
                    'engine_variable_name': 'test',
                    'external_variable_name': 'test',
                    'external_variable_store_id': 1
                }]
            }],
            'schema': {
                'available_variables': [{
                    'name': 'item_id',
                    'schema': {"type": "integer"}
                },{
                    'name': 'filter_test',
                    'schema': {"type": "string"}
                }]
            }
        }

   async def test_post_with_invalid_filter(self, init_db, client, headers, headers_without_content_type):
        client = await client
        body = [{
            'max_items': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_filters': [{
                '_operation': 'insert',
                'external_variable_name': 'test',
                'external_variable_store_id': 1,
                'property_name': 'test',
                'type_id': 'property_value',
                'is_inclusive': True
            }]
        }]
        resp = await client.post('/slots/', headers=headers, data=ujson.dumps(body))
        assert resp.status == 400
        assert await resp.json() ==  {
            'message': "Invalid slot filter with 'property_name' attribute value 'test'",
            'instance': [{
                'max_items': 10,
                'name': 'test',
                'engine_id': 1,
                'store_id': 1,
                'slot_filters': [{
                    '_operation': 'insert',
                    'property_name': 'test',
                    'is_inclusive': True,
                    'type_id': 'property_value',
                    'external_variable_name': 'test',
                    'external_variable_store_id': 1
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

   async def test_post_with_insert_engine_external_variable_engine_var(self, init_db, client, headers, headers_without_content_type):
        client = await client
        body = [{
            'max_items': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_variables': [{
                '_operation': 'insert',
                'external_variable_name': 'test',
                'external_variable_store_id': 1,
                'engine_variable_name': 'filter_test'
            }]
        }]
        resp = await client.post('/slots/', headers=headers, data=ujson.dumps(body))

        assert resp.status == 201
        assert await resp.json() == [{
            'max_items': 10,
            'name': 'test',
            'fallbacks': [],
            'id': 1,
            'slot_filters':[],
            'slot_variables': [
                {
                    'external_variable': {
                        'name': 'test',
                        'store_id': 1
                    },
                    'id': 1,
                    'engine_variable_name': 'filter_test',
                    'slot_id': 1,
                    'override': False,
                    'override_value': None,
                    'external_variable_name': 'test',
                    'external_variable_store_id': 1
                }
            ],
            'engine': {
                'item_type': {
                    'id': 1,
                    'store_items_class': None,
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
                'strategy_class': {
                    'class_name': 'EngineStrategyTestWithVars',
                    'module': 'tests.integration.fixtures'
                },
                'id': 1,
                'variables': [{
                    'name': 'item_id', 'schema': {'type': 'integer'}
                },{
                    'name': 'filter_test', 'schema': {'type': 'string'}
                }],
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

   async def test_post_with_insert_engine_external_variable_engine_filter(self, init_db, client, headers, headers_without_content_type):
        client = await client
        body = [{
            'max_items': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_filters': [{
                '_operation': 'insert',
                'external_variable_name': 'test',
                'external_variable_store_id': 1,
                'is_inclusive': True,
                'type_id': 'property_value',
                'property_name': 'filter_test'
            }]
        }]
        resp = await client.post('/slots/', headers=headers, data=ujson.dumps(body))

        assert resp.status == 201
        assert await resp.json() == [{
            'fallbacks': [],
            'id': 1,
            'max_items': 10,
            'name': 'test',
            'slot_variables': [],
            'slot_filters': [
                {
                    'external_variable': {
                        'name': 'test',
                        'store_id': 1
                    },
                    'id': 1,
                    'property_name': 'filter_test',
                    'slot_id': 1,
                    'override': False,
                    'override_value': None,
                    'skip_values': None,
                    'external_variable_name': 'test',
                    'external_variable_store_id': 1,
                    'is_inclusive': True,
                    'type_id': 'property_value'
                }
            ],
            'engine': {
                'item_type': {
                    'id': 1,
                    'store_items_class': None,
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
                'strategy_class': {
                    'class_name': 'EngineStrategyTestWithVars',
                    'module': 'tests.integration.fixtures'
                },
                'id': 1,
                'variables': [{
                    'name': 'item_id', 'schema': {'type': 'integer'}
                },{
                    'name': 'filter_test', 'schema': {'type': 'string'}
                }],
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
            'max_items': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_variables': [{
                '_operation': 'insert',
                'external_variable_name': 'test',
                'external_variable_store_id': 1,
                'engine_variable_name': 'item_id'
            }]
        }]
        await client.post('/slots/', headers=headers, data=ujson.dumps(body))

        body = [{
            'max_items': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_variables': [{
                '_operation': 'insert',
                'external_variable_name': 'test',
                'external_variable_store_id': 1,
                'engine_variable_name': 'item_id'
            }],
            'fallbacks': [{'id': 1}]
        }]
        resp = await client.post('/slots/', headers=headers, data=ujson.dumps(body))

        assert resp.status == 201
        assert await resp.json() == [{
            'fallbacks': [{
                'max_items': 10,
                'name': 'test',
                'id': 1,
                'slot_filters': [],
                'slot_variables': [
                    {
                        'external_variable': {
                            'name': 'test',
                            'store_id': 1
                        },
                        'id': 1,
                        'engine_variable_name': 'item_id',
                        'slot_id': 1,
                        'override': False,
                        'override_value': None,
                        'external_variable_name': 'test',
                        'external_variable_store_id': 1
                    }
                ],
                'engine': {
                    'item_type': {
                        'id': 1,
                        'store_items_class': None,
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
                    'strategy_class': {
                        'class_name': 'EngineStrategyTestWithVars',
                        'module': 'tests.integration.fixtures'
                    },
                    'id': 1,
                    'variables': [{
                        'name': 'item_id', 'schema': {'type': 'integer'}
                    },{
                        'name': 'filter_test', 'schema': {'type': 'string'}
                    }],
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
            'max_items': 10,
            'name': 'test',
            'slot_filters': [],
            'slot_variables': [
                {
                    'external_variable': {
                        'name': 'test',
                        'store_id': 1
                    },
                    'id': 2,
                    'engine_variable_name': 'item_id',
                    'slot_id': 2,
                    'override': False,
                    'override_value': None,
                    'external_variable_name': 'test',
                    'external_variable_store_id': 1
                }
            ],
            'engine': {
                'item_type': {
                    'id': 1,
                    'store_items_class': None,
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
                'strategy_class': {
                    'class_name': 'EngineStrategyTestWithVars',
                    'module': 'tests.integration.fixtures'
                },
                'id': 1,
                'variables': [{
                    'name': 'item_id', 'schema': {'type': 'integer'}
                },{
                    'name': 'filter_test', 'schema': {'type': 'string'}
                }],
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
            'max_items': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_variables': [{
                '_operation': 'insert',
                'external_variable_name': 'test',
                'external_variable_store_id': 1,
                'engine_variable_name': 'item_id'
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
            'max_items': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_variables': [{
                '_operation': 'insert',
                'external_variable_name': 'test',
                'external_variable_store_id': 1,
                'engine_variable_name': 'filter_test'
            }]
        }]
        await client.post('/slots/', headers=headers, data=ujson.dumps(body))

        resp = await client.get('/slots/?store_id=1', headers=headers_without_content_type)
        assert resp.status == 200
        assert await resp.json() ==  [{
            'fallbacks': [],
            'id': 1,
            'max_items': 10,
            'name': 'test',
            'slot_filters': [],
            'slot_variables': [
                {
                    'external_variable': {
                        'name': 'test',
                        'store_id': 1
                    },
                    'id': 1,
                    'engine_variable_name': 'filter_test',
                    'slot_id': 1,
                    'override': False,
                    'override_value': None,
                    'external_variable_name': 'test',
                    'external_variable_store_id': 1
                }
            ],
            'engine': {
                'item_type': {
                    'id': 1,
                    'store_items_class': None,
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
                'strategy_class': {
                    'class_name': 'EngineStrategyTestWithVars',
                    'module': 'tests.integration.fixtures'
                },
                'id': 1,
                'variables': [{
                    'name': 'item_id', 'schema': {'type': 'integer'}
                },{
                    'name': 'filter_test', 'schema': {'type': 'string'}
                }],
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
                    'max_items': {
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
                    },
                    'slot_filters': {
                        '$ref': '#/definitions/SlotsModel.slot_filters'
                    }
                },
                'type': 'object'
            },
            'message': '{} does not have enough properties. '\
                       "Failed validating instance for schema['minProperties']"
        }

   async def test_patch_with_invalid_engine_external_variable(self, init_db, client, headers, headers_without_content_type):
        client = await client
        body = [{
            'max_items': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_variables': [{
                '_operation': 'insert',
                'external_variable_name': 'test',
                'external_variable_store_id': 1,
                'engine_variable_name': 'filter_test'
            }]
        }]
        resp = await client.post('/slots/', headers=headers, data=ujson.dumps(body))
        assert resp.status == 201

        body = {
            'slot_variables': [{
                '_operation': 'update',
                'id': 1,
                'engine_variable_name': 'invalid'
            }]
        }
        resp = await client.patch('/slots/1/', headers=headers, data=ujson.dumps(body))
        assert resp.status == 400
        assert await resp.json() ==  {
            'message': "Invalid slot variable with 'engine_variable_name' attribute value 'invalid'",
            'instance': [{
                'slot_variables': [{
                    '_operation': 'update',
                    'id': 1,
                    'engine_variable_name': 'invalid'
                }],
            }],
            'schema': {
                'available_variables': [{
                    'name': 'item_id',
                    'schema': {"type": "integer"}
                },{
                    'name': 'filter_test',
                    'schema': {"type": "string"}
                }]
            }
        }

   async def test_patch_with_invalid_fallback_id(self, init_db, client, headers, headers_without_content_type):
        client = await client
        body = [{
            'max_items': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_variables': [{
                '_operation': 'insert',
                'external_variable_name': 'test',
                'external_variable_store_id': 1,
                'engine_variable_name': 'filter_test'
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
            'max_items': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_variables': [{
                '_operation': 'insert',
                'external_variable_name': 'test',
                'external_variable_store_id': 1,
                'engine_variable_name': 'filter_test'
            }]
        },{
            'max_items': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 2,
            'slot_variables': [{
                '_operation': 'insert',
                'external_variable_name': 'test',
                'external_variable_store_id': 1,
                'engine_variable_name': 'item_id'
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
            'max_items': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_variables': [{
                '_operation': 'insert',
                'external_variable_name': 'test',
                'external_variable_store_id': 1,
                'engine_variable_name': 'item_id'
            }]
        }]
        resp = await client.post('/slots/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        body = {
            'slot_variables': [{
                '_operation': 'update',
                'id': 1,
                'engine_variable_name': 'filter_test'
            }]
        }
        resp = await client.patch('/slots/1/', headers=headers, data=ujson.dumps(body))

        assert resp.status == 200
        assert await resp.json() ==  {
            'fallbacks': [],
            'id': 1,
            'max_items': 10,
            'name': 'test',
            'slot_filters': [],
            'slot_variables': [
                {
                    'external_variable': {
                        'name': 'test',
                        'store_id': 1
                    },
                    'id': 1,
                    'engine_variable_name': 'filter_test',
                    'slot_id': 1,
                    'override': False,
                    'override_value': None,
                    'external_variable_name': 'test',
                    'external_variable_store_id': 1
                }
            ],
            'engine': {
                'item_type': {
                    'id': 1,
                    'store_items_class': None,
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
                'strategy_class': {
                    'class_name': 'EngineStrategyTestWithVars',
                    'module': 'tests.integration.fixtures'
                },
                'id': 1,
                'variables': [{
                    'name': 'item_id', 'schema': {'type': 'integer'}
                },{
                    'name': 'filter_test', 'schema': {'type': 'string'}
                }],
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
            'max_items': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_variables': [{
                '_operation': 'insert',
                'external_variable_name': 'test',
                'external_variable_store_id': 1,
                'engine_variable_name': 'filter_test'
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
            'max_items': 10,
            'name': 'test',
            'store_id': 1,
            'engine_id': 1,
            'slot_variables': [{
                '_operation': 'insert',
                'external_variable_name': 'test',
                'external_variable_store_id': 1,
                'engine_variable_name': 'filter_test'
            }]
        }]
        await client.post('/slots/', headers=headers, data=ujson.dumps(body))

        resp = await client.get('/slots/1/', headers=headers_without_content_type)

        assert resp.status == 200
        assert await resp.json() == {
            'fallbacks': [],
            'id': 1,
            'max_items': 10,
            'name': 'test',
            'slot_filters': [],
            'slot_variables': [
                {
                    'external_variable': {
                        'name': 'test',
                        'store_id': 1
                    },
                    'id': 1,
                    'engine_variable_name': 'filter_test',
                    'slot_id': 1,
                    'override': False,
                    'override_value': None,
                    'external_variable_name': 'test',
                    'external_variable_store_id': 1
                }
            ],
            'engine': {
                'item_type': {
                    'id': 1,
                    'store_items_class': None,
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
                'strategy_class': {
                    'class_name': 'EngineStrategyTestWithVars',
                    'module': 'tests.integration.fixtures'
                },
                'id': 1,
                'variables': [{
                    'name': 'item_id', 'schema': {'type': 'integer'}
                },{
                    'name': 'filter_test', 'schema': {'type': 'string'}
                }],
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
