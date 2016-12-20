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


from tests.integration.fixtures_models import UsersModel, StoresModel, SQLAlchemyRedisModelBase
from tests.integration.fixtures_models import ItemsTypesModel, SlotsModel
from falconswagger.swagger_api import SwaggerAPI
from myreco.factory import ModelsFactory
from myreco.engines.cores.items_indices_map import ItemsIndicesMap
from base64 import b64encode
from unittest import mock
from pytest_falcon.plugin import Client
from datetime import datetime
from time import sleep
import pytest
import json
import numpy as np


@pytest.fixture
def model_base():
    return SQLAlchemyRedisModelBase


@pytest.fixture
def app(redis, session):
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

    return SwaggerAPI([ItemsTypesModel], session.bind, redis, title='Myreco API')


@pytest.fixture
def headers():
    return {
        'Authorization': b64encode('test:test'.encode()).decode()
    }


class TestItemsTypesModelPost(object):

    def test_post_without_body(self, client, headers):
        resp = client.post('/items_types/', headers=headers)
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is missing'}

    def test_post_with_invalid_body(self, client, headers):
        resp = client.post('/items_types/', headers=headers, body='[{}]')
        assert resp.status_code == 400
        assert json.loads(resp.body) ==  {
            'error': {
                'input': {},
                'message': "'name' is a required property",
                'schema': {
                    'type': 'object',
                    'additionalProperties': False,
                    'required': ['name', 'schema', 'stores'],
                    'properties': {
                        'name': {'type': 'string'},
                        'stores': {'$ref': '#/definitions/stores'},
                        'schema': {'$ref': '#/definitions/items'}
                    }
                }
            }
        }

    def test_post_valid(self, client, headers):
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {
                'properties': {'id': {'type': 'integer'}},
                'type': 'object',
                'id_names': ['id']
            }
        }]
        resp = client.post('/items_types/', headers=headers, body=json.dumps(body))
        body[0]['id'] = 1
        body[0]['available_filters'] = [{'name': 'id', 'schema': {'type': 'integer'}}]
        body[0]['stores'] = [{
            'configuration': {'data_path': '/test'},
            'country': 'test',
            'id': 1,
            'name': 'test'
        }]

        assert resp.status_code == 201
        assert json.loads(resp.body) ==  body

    def test_post_with_invalid_grant(self, client):
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {'properties': {'id': {'type': 'integer'}}, 'type': 'object', 'id_names': ['id']}
        }]
        resp = client.post('/items_types/', headers={'Authorization': 'invalid'}, body=json.dumps(body))
        assert resp.status_code == 401
        assert json.loads(resp.body) ==  {'error': 'Invalid authorization'}

    def test_post_with_invalid_schema_type(self, client, headers):
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {
                'properties': {
                    'id': {'type': 'string'}
                }
            }
        }]
        resp = client.post('/items_types/', headers=headers, body=json.dumps(body))
        assert resp.status_code == 400
        assert json.loads(resp.body) == {
            'error': {
                'input': {
                    'properties': {'id': {'type': 'string'}}
                },
                'message': "'type' is a required property",
                'schema': {
                    'type': 'object',
                    'required': ['type', 'id_names', 'properties'],
                    'properties': {
                        'type': {'enum': ['object']},
                        'id_names': {
                            'items': {'minLength': 1, 'type': 'string'},
                            'minItems': 1,
                            'type': 'array'
                        },
                        'properties': {'$ref': 'items_schema.json#/definitions/baseObject/properties/properties'}
                    }
                }
            }
        }

    def test_post_with_invalid_id_name(self, client, headers):
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {
                'type': 'object',
                'id_names': ['test'],
                'properties': {
                    'id': {'type': 'string'}
                }
            }
        }]
        resp = client.post('/items_types/', headers=headers, body=json.dumps(body))
        assert resp.status_code == 400
        assert json.loads(resp.body) == {
            'error': {
                'input': ['test'],
                'message': "id_name 'test' was not found in schema properties",
                'schema': {
                    'type': 'object',
                    'id_names': ['test'],
                    'properties': {
                        'id': {'type': 'string'}
                    }
                }
            }
        }


class TestItemsTypesModelGet(object):

    def test_get_not_found(self, client, headers):
        resp = client.get('/items_types/?stores=id:1', headers=headers)
        assert resp.status_code == 404

    def test_get_invalid_with_body(self, client, headers):
        resp = client.get('/items_types/?stores=id:1', headers=headers, body='{}')
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is not acceptable'}

    def test_get(self, client, headers):
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {'properties': {'test': {'type': 'string'}}, 'type': 'object', 'id_names': ['test']}
        }]
        client.post('/items_types/', headers=headers, body=json.dumps(body))
        body[0]['id'] = 1
        body[0]['available_filters'] = [{'name': 'test', 'schema': {'type': 'string'}}]
        body[0]['stores'] = [{
            'configuration': {'data_path': '/test'},
            'country': 'test',
            'id': 1,
            'name': 'test'
        }]

        resp = client.get('/items_types/?stores=id:1', headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) ==  body


class TestItemsTypesModelUriTemplatePatch(object):

    def test_patch_without_body(self, client, headers):
        resp = client.patch('/items_types/1/', headers=headers, body='')
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is missing'}

    def test_patch_with_invalid_body(self, client, headers):
        resp = client.patch('/items_types/1/', headers=headers, body='{}')
        assert resp.status_code == 400
        assert json.loads(resp.body) ==  {
            'error': {
                'input': {},
                'message': '{} does not have enough properties',
                'schema': {
                    'type': 'object',
                    'additionalProperties': False,
                    'minProperties': 1,
                    'properties': {
                        'name': {'type': 'string'},
                        'stores': {'$ref': '#/definitions/stores'},
                        'schema': {'$ref': '#/definitions/items'}
                    }
                }
            }
        }

    def test_patch_not_found(self, client, headers):
        body = {
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {'properties': {'test': {'type': 'string'}}, 'type': 'object', 'id_names': ['test']}
        }
        resp = client.patch('/items_types/1/', headers=headers, body=json.dumps(body))
        assert resp.status_code == 404

    def test_patch(self, client, headers):
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {'properties': {'test': {'type': 'string'}}, 'type': 'object', 'id_names': ['test']}
        }]
        obj = json.loads(client.post('/items_types/', headers=headers, body=json.dumps(body)).body)[0]

        body = {
            'name': 'test2'
        }
        resp = client.patch('/items_types/1/', headers=headers, body=json.dumps(body))
        obj['name'] = 'test2'
        obj['available_filters'] = [{'name': 'test', 'schema': {'type': 'string'}}]

        assert resp.status_code == 200
        assert json.loads(resp.body) ==  obj


class TestItemsTypesModelUriTemplateDelete(object):

    def test_delete_with_body(self, client, headers):
        resp = client.delete('/items_types/1/', headers=headers, body='{}')
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is not acceptable'}

    def test_delete(self, client, headers):
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {'properties': {'test': {'type': 'string'}}, 'type': 'object', 'id_names': ['test']}
        }]
        resp = client.post('/items_types/', headers=headers, body=json.dumps(body))

        resp = client.get('/items_types/1/', headers=headers)
        assert resp.status_code == 200

        resp = client.delete('/items_types/1/', headers=headers)
        assert resp.status_code == 204

        resp = client.get('/items_types/1/', headers=headers)
        assert resp.status_code == 404

    def test_if_delete_disassociate_model_correctly(self, client, headers):
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {'properties': {'test': {'type': 'string'}}, 'type': 'object', 'id_names': ['test']}
        }]
        resp = client.post('/items_types/', headers=headers, body=json.dumps(body))

        resp = client.options('/test/', headers=headers)
        assert resp.status_code == 204

        client.delete('/items_types/1/', headers=headers)

        resp = client.options('/test/', headers=headers)
        assert resp.status_code == 404


class TestItemsTypesModelUriTemplateGet(object):

    def test_get_with_body(self, client, headers):
        resp = client.get('/items_types/1/', headers=headers, body='{}')
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is not acceptable'}

    def test_get_not_found(self, client, headers):
        resp = client.get('/items_types/1/', headers=headers)
        assert resp.status_code == 404

    def test_get(self, client, headers):
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {'properties': {'test': {'type': 'string'}}, 'type': 'object', 'id_names': ['test']}
        }]
        client.post('/items_types/', headers=headers, body=json.dumps(body))
        resp = client.get('/items_types/1/', headers=headers)
        body[0]['id'] = 1
        body[0]['available_filters'] = [{'name': 'test', 'schema': {'type': 'string'}}]
        body[0]['stores'] = [{
            'configuration': {'data_path': '/test'},
            'country': 'test',
            'id': 1,
            'name': 'test'
        }]

        assert resp.status_code == 200
        assert json.loads(resp.body) == body[0]


class TestItemsModelSchema(object):

    def test_if_build_item_model_schema_correctly(self, client, headers):
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {'properties': {'id': {'type': 'string'}}, 'type': 'object', 'id_names': ['id']}
        }]
        resp = client.post('/items_types/', headers=headers, body=json.dumps(body))

        resp = client.get('/swagger.json', headers=headers)
        assert resp.status_code == 200
        paths = {
            '/test': json.loads(resp.body)['paths'].get('/test'),
            '/test/{id}': json.loads(resp.body)['paths'].get('/test/{id}')
        }
        assert paths == {
            '/test': {
                'parameters': [{
                    'name': 'Authorization',
                    'in': 'header',
                    'required': True,
                    'type': 'string'
                },{
                    'in': 'query',
                    'name': 'store_id',
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
                            'items': {
                                'type': 'object',
                                'id_names': ['id'],
                                'properties': {'id': {'type': 'string'}}
                            }
                        }
                    }],
                    'operationId': 'TestModel.post_by_body',
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
                            'items': {
                                'type': 'object',
                                'id_names': ['id'],
                                'properties': {
                                    'id': {'type': 'string'},
                                    '_operation': {'enum': ['delete', 'update']}
                                }
                            }
                        }
                    }],
                    'operationId': 'TestModel.patch_by_body',
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
                    },{
                        'name': 'id',
                        'in': 'query',
                        'type': 'string'
                    }],
                    'operationId': 'TestModel.get_by_body',
                    'responses': {'200': {'description': 'Got'}}
                },
            },
            '/test/{id}': {
                'parameters': [{
                    'name': 'Authorization',
                    'in': 'header',
                    'required': True,
                    'type': 'string'
                },{
                    'in': 'query',
                    'name': 'store_id',
                    'required': True,
                    'type': 'integer'
                },{
                    'name': 'id',
                    'in': 'path',
                    'type': 'string',
                    'required': True
                }],
                'get': {
                    'operationId': 'TestModel.get_by_uri_template',
                    'responses': {'200': {'description': 'Got'}}
                }
            }
        }

    def test_if_build_item_model_schema_correctly_with_two_id_names(
            self, client, headers):
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {
                'type': 'object',
                'id_names': ['id', 'id2'],
                'properties': {
                    'id': {'type': 'string'},
                    'id2': {'type': 'integer'}
                }
            }
        }]
        client.post('/items_types/', headers=headers, body=json.dumps(body))

        resp = client.get('/swagger.json', headers=headers)
        assert resp.status_code == 200
        paths = {
            '/test': json.loads(resp.body)['paths'].get('/test'),
            '/test/{id}/{id2}': json.loads(resp.body)['paths'].get('/test/{id}/{id2}')
        }
        assert paths == {
            '/test': {
                'parameters': [{
                    'name': 'Authorization',
                    'in': 'header',
                    'required': True,
                    'type': 'string'
                },{
                    'in': 'query',
                    'name': 'store_id',
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
                            'items': {
                                'type': 'object',
                                'id_names': ['id', 'id2'],
                                'properties': {
                                    'id': {'type': 'string'},
                                    'id2': {'type': 'integer'}
                                }
                            }
                        }
                    }],
                    'operationId': 'TestModel.post_by_body',
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
                            'items': {
                                'type': 'object',
                                'id_names': ['id', 'id2'],
                                'properties': {
                                    'id': {'type': 'string'},
                                    'id2': {'type': 'integer'},
                                    '_operation': {'enum': ['delete', 'update']}
                                }
                            }
                        }
                    }],
                    'operationId': 'TestModel.patch_by_body',
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
                    },{
                        'name': 'id',
                        'in': 'query',
                        'type': 'string'
                    },{
                        'name': 'id2',
                        'in': 'query',
                        'type': 'integer'
                    }],
                    'operationId': 'TestModel.get_by_body',
                    'responses': {'200': {'description': 'Got'}}
                },
            },
            '/test/{id}/{id2}': {
                'parameters': [{
                    'name': 'Authorization',
                    'in': 'header',
                    'required': True,
                    'type': 'string'
                },{
                    'in': 'query',
                    'name': 'store_id',
                    'required': True,
                    'type': 'integer'
                },{
                    'name': 'id',
                    'in': 'path',
                    'type': 'string',
                    'required': True
                },{
                    'name': 'id2',
                    'in': 'path',
                    'type': 'integer',
                    'required': True
                }],
                'get': {
                    'operationId': 'TestModel.get_by_uri_template',
                    'responses': {'200': {'description': 'Got'}}
                }
            }
        }


class TestItemsModelPost(object):

    def test_items_post_valid(self, client, headers):
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {'properties': {'id': {'type': 'string'}}, 'type': 'object', 'id_names': ['id']}
        }]
        client.post('/items_types/', headers=headers, body=json.dumps(body))

        body = [{'id': 'test'}]
        resp = client.post('/test?store_id=1', headers=headers, body=json.dumps(body))
        assert resp.status_code == 201
        assert json.loads(resp.body) == body

    def test_items_post_invalid(self, client, headers):
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {'properties': {'id': {'type': 'string'}}, 'type': 'object', 'id_names': ['id']}
        }]
        client.post('/items_types/', headers=headers, body=json.dumps(body))

        body = [{'id': 1}]
        resp = client.post('/test/?store_id=1', headers=headers, body=json.dumps(body))
        assert resp.status_code == 400
        assert json.loads(resp.body) == {
            'error': {
                'input': 1,
                'message': "1 is not of type 'string'",
                'schema': {'type': 'string'}
            }
        }


class TestItemsModelPatch(object):

    def test_items_patch_valid(self, client, headers):
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {
                'properties': {
                    'id': {'type': 'string'},
                    't1': {'type': 'integer'}
                },
                'type': 'object',
                'id_names': ['id']
            }
        }]
        client.post('/items_types/', headers=headers, body=json.dumps(body))

        body = [{'id': 'test', 't1': 1}]
        resp = client.post('/test?store_id=1', headers=headers, body=json.dumps(body))

        body = [{'id': 'test', 't1': 2}]
        resp = client.patch('/test?store_id=1', headers=headers, body=json.dumps(body))
        assert resp.status_code == 200
        assert json.loads(resp.body) == body

        resp = client.get('/test/test?store_id=1', headers=headers)
        assert json.loads(resp.body) == body[0]

    def test_items_patch_with_delete(self, client, headers):
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {
                'properties': {
                    'id': {'type': 'string'},
                    't1': {'type': 'integer'}
                },
                'type': 'object',
                'id_names': ['id']
            }
        }]
        client.post('/items_types/', headers=headers, body=json.dumps(body))

        body = [{'id': 'test', 't1': 1}, {'id': 'test2', 't1': 2}]
        resp = client.post('/test/?store_id=1', headers=headers, body=json.dumps(body))

        body = [{'id': 'test', '_operation': 'delete'}]
        resp = client.patch('/test?store_id=1', headers=headers, body=json.dumps(body))
        assert resp.status_code == 200
        assert json.loads(resp.body) == [{'id': 'test', '_operation': 'delete'}]

        resp = client.get('/test/test?store_id=1', headers=headers)
        assert resp.status_code == 404

    def test_items_patch_invalid(self, client, headers):
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {
                'properties': {
                    'id': {'type': 'string'}
                },
                'type': 'object',
                'id_names': ['id']
            }
        }]
        client.post('/items_types/', headers=headers, body=json.dumps(body))

        body = [{'id': 1}]
        resp = client.post('/test/?store_id=1', headers=headers, body=json.dumps(body))
        assert resp.status_code == 400
        assert json.loads(resp.body) == {
            'error': {
                'input': 1,
                'message': "1 is not of type 'string'",
                'schema': {'type': 'string'}
            }
        }

    def test_if_items_patch_updates_stock_filter(self, client, headers, redis, app, session):
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {'properties': {'id': {'type': 'string'}}, 'type': 'object', 'id_names': ['id']}
        }]
        client.post('/items_types/', headers=headers, body=json.dumps(body))

        body = [{'id': 'test'}]
        client.post('/test?store_id=1', headers=headers, body=json.dumps(body))

        test_model = app.models['test'].__models__[1]
        ItemsIndicesMap(test_model).update(session)

        body = [{'id': 'test', '_operation': 'delete'}]
        resp = client.patch('/test?store_id=1', headers=headers, body=json.dumps(body))
        stock_filter = np.fromstring(redis.get('test_1_stock_filter'), dtype=np.bool).tolist()
        assert stock_filter == [False]


@pytest.fixture
def filters_updater_app(redis, session):
    table_args = {'mysql_engine':'innodb'}
    factory = ModelsFactory('myreco', commons_models_attributes={'__table_args__': table_args},
                            commons_tables_attributes=table_args)
    models = factory.make_all_models('exporter')
    user = {
        'name': 'test',
        'email': 'test',
        'password': 'test',
        'admin': True
    }
    models['users'].insert(session, user)

    store = {
        'name': 'test',
        'country': 'test',
        'configuration': {'data_path': '/test'}
    }
    models['stores'].insert(session, store)


    item_type = {
        'name': 'products',
        'stores': [{'id': 1}],
        'schema': {
            'type': 'object',
            'id_names': ['sku'],
            'properties': {
                'sku': {'type': 'string'},
                'filter1': {'type': 'integer'},
                'filter2': {'type': 'boolean'},
                'filter3': {'type': 'string'},
                'filter4': {
                    'type': 'object',
                    'id_names': ['id'],
                    'properties': {'id': {'type': 'integer'}}
                },
                'filter5': {
                    'type': 'array',
                    'items': {'type': 'integer'}
                }
            }
        }
    }
    models['items_types'].insert(session, item_type)

    engine_core = {
        'name': 'top_seller',
        'configuration': {
            'core_module': {
                'path': 'myreco.engines.cores.top_seller.core',
                'class_name': 'TopSellerEngineCore'
            }
        }
    }
    models['engines_cores'].insert(session, engine_core)

    engine = {
        'name': 'Top Seller',
        'configuration_json': json.dumps({
            'days_interval': 7,
            'data_importer_path': 'myreco.engines.cores.base.AbstractDataImporter'
        }),
        'store_id': 1,
        'core_id': 1,
        'item_type_id': 1
    }
    models['engines'].insert(session, engine)

    models['variables'].insert(session, {'name': 'test', 'store_id': 1})

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
            'inside_engine_name': 'filter1'
        },{
            '_operation': 'insert',
            'variable_name': 'test',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter2'
        },{
            '_operation': 'insert',
            'variable_name': 'test',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter3'
        },{
            '_operation': 'insert',
            'variable_name': 'test',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter4'
        },{
            '_operation': 'insert',
            'variable_name': 'test',
            'variable_store_id': 1,
            'is_filter': True,
            'is_inclusive_filter': True,
            'filter_type': 'By Property',
            'inside_engine_name': 'filter5'
        }]
    }
    models['slots'].insert(session, slot)

    api = SwaggerAPI([models['items_types']], session.bind, redis,
                      title='Myreco API')
    models['items_types'].associate_all_items(session)

    return api


@pytest.fixture
def filters_updater_client(filters_updater_app):
    return Client(filters_updater_app)


def datetime_mock():
    mock_ = mock.MagicMock()
    mock_.now.return_value = datetime(1900, 1, 1)
    return mock_


@mock.patch('falconswagger.models.http.random.getrandbits',
    new=mock.MagicMock(return_value=131940827655846590526331314439483569710))
@mock.patch('falconswagger.models.orm.http.datetime', new=datetime_mock())
class TestItemsTypesModelFiltersUpdater(object):

    def test_filters_updater_post(self, filters_updater_client, headers):
        products = [{
            'sku': 'test', 'filter1': 1, 'filter2': True,
            'filter3': 'test', 'filter4': {'id': 1}, 'filter5': [1]}]
        filters_updater_client.post('/products?store_id=1',
                                    body=json.dumps(products), headers=headers)

        resp = filters_updater_client.post('/products/update_filters?store_id=1', headers=headers)
        assert json.loads(resp.body) == {'hash': '6342e10bd7dca3240c698aa79c98362e'}

    def test_filters_updater_get_done(self, filters_updater_client, headers):
        products = [{
            'sku': 'test', 'filter1': 1, 'filter2': True,
            'filter3': 'test', 'filter4': {'id': 1}, 'filter5': [1]}]
        filters_updater_client.post('/products?store_id=1',
                                    body=json.dumps(products), headers=headers)
        resp = filters_updater_client.post('/products/update_filters?store_id=1', headers=headers)
        sleep(0.05)

        while True:
            resp = filters_updater_client.get(
                '/products/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        resp = filters_updater_client.get(
            '/products/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
            headers=headers)
        assert json.loads(resp.body) == {
            'status': 'done',
            'result': {
                'items_indices_map': {'maximum_index': 0, 'total_items': 1},
                'filters': {
                    'filter1': {'filters_quantity': 1},
                    'filter2': {'true_values': 1},
                    'filter3': {'filters_quantity': 1},
                    'filter4': {'filters_quantity': 1},
                    'filter5': {'filters_quantity': 1}
                }
            },
            'elapsed_time': '0:00'
        }

    def test_if_update_filters_builds_stock_filter(self, filters_updater_client, headers, redis):
        products = {
            'test': {
                'sku': 'test',
                'filter1': 1,
                'filter2': True,
                'filter3': 'test',
                'filter4': {'id': 1},
                'filter5': [1]
            },
            'test2': {
                'sku': 'test2',
                'filter1': 1,
                'filter2': True,
                'filter3': 'test',
                'filter4': {'id': 1},
                'filter5': [1,2]
            },
            'test3': {
                'sku': 'test3',
                'filter1': 2,
                'filter2': False,
                'filter3': 'test2',
                'filter4': {'id': 2},
                'filter5': [2,3]
            }
        }
        filters_updater_client.post('/products?store_id=1',
                                    body=json.dumps(list(products.values())), headers=headers)
        filters_updater_client.post('/products/update_filters?store_id=1', headers=headers)
        sleep(0.05)

        while True:
            resp = filters_updater_client.get(
                '/products/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        stock_filter = np.fromstring(redis.get('products_1_stock_filter'), dtype=np.bool).tolist()
        assert stock_filter == [True, True, True]

    def test_if_update_filters_builds_boolean_filter(self, filters_updater_client, headers, redis, session):
        products = {
            'test': {
                'sku': 'test',
                'filter1': 1,
                'filter2': True,
                'filter3': 'test',
                'filter4': {'id': 1},
                'filter5': [1]
            },
            'test2': {
                'sku': 'test2',
                'filter1': 1,
                'filter2': True,
                'filter3': 'test',
                'filter4': {'id': 1},
                'filter5': [1,2]
            },
            'test3': {
                'sku': 'test3',
                'filter1': 2,
                'filter2': False,
                'filter3': 'test2',
                'filter4': {'id': 2},
                'filter5': [2,3]
            }
        }
        filters_updater_client.post('/products?store_id=1',
                                    body=json.dumps(list(products.values())), headers=headers)
        filters_updater_client.post('/products/update_filters?store_id=1', headers=headers)
        sleep(0.05)

        while True:
            resp = filters_updater_client.get(
                '/products/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        products_model = filters_updater_client.app.models['products'].__models__[1]
        indices_items_map = ItemsIndicesMap(products_model).get_indices_items_map(session)

        expected = [None, None, None]
        for k, v in indices_items_map.items():
            expected[k] = True if v == 'test' or v == 'test2' else False

        filter_ = redis.get('products_1_filter2_filter')
        filter_ = np.fromstring(filter_, dtype=np.bool).tolist()
        assert filter_ == expected

    def test_if_update_filters_builds_integer_filter(self, filters_updater_client, headers, redis, session):
        products = {
            'test': {
                'sku': 'test',
                'filter1': 1,
                'filter2': True,
                'filter3': 'test',
                'filter4': {'id': 1},
                'filter5': [1]
            },
            'test2': {
                'sku': 'test2',
                'filter1': 1,
                'filter2': True,
                'filter3': 'test',
                'filter4': {'id': 1},
                'filter5': [1,2]
            },
            'test3': {
                'sku': 'test3',
                'filter1': 2,
                'filter2': False,
                'filter3': 'test2',
                'filter4': {'id': 2},
                'filter5': [2,3]
            }
        }
        filters_updater_client.post('/products?store_id=1',
                                    body=json.dumps(list(products.values())), headers=headers)
        filters_updater_client.post('/products/update_filters?store_id=1', headers=headers)
        sleep(0.05)

        while True:
            resp = filters_updater_client.get(
                '/products/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        products_model = filters_updater_client.app.models['products'].__models__[1]
        indices_items_map = ItemsIndicesMap(products_model).get_indices_items_map(session)

        expected1 = [None, None, None]
        for k, v in indices_items_map.items():
            expected1[k] = True if v == 'test' or v == 'test2' else False

        expected2 = [None, None, None]
        for k, v in indices_items_map.items():
            expected2[k] = False if v == 'test' or v == 'test2' else True

        filter_ = redis.hgetall('products_1_filter1_filter')
        key1 = '1'.encode()
        key2 = '2'.encode()
        filter_[key1] = np.fromstring(filter_[key1], dtype=np.bool).tolist()
        filter_[key2] = np.fromstring(filter_[key2], dtype=np.bool).tolist()

        assert filter_ == {key1: expected1, key2: expected2}

    def test_if_update_filters_builds_string_filter(self, filters_updater_client, headers, redis, session):
        products = {
            'test': {
                'sku': 'test',
                'filter1': 1,
                'filter2': True,
                'filter3': 'test',
                'filter4': {'id': 1},
                'filter5': [1]
            },
            'test2': {
                'sku': 'test2',
                'filter1': 1,
                'filter2': True,
                'filter3': 'test',
                'filter4': {'id': 1},
                'filter5': [1,2]
            },
            'test3': {
                'sku': 'test3',
                'filter1': 2,
                'filter2': False,
                'filter3': 'test2',
                'filter4': {'id': 2},
                'filter5': [2,3]
            }
        }
        filters_updater_client.post('/products?store_id=1',
                                    body=json.dumps(list(products.values())), headers=headers)
        filters_updater_client.post('/products/update_filters?store_id=1', headers=headers)
        sleep(0.05)

        while True:
            resp = filters_updater_client.get(
                '/products/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        products_model = filters_updater_client.app.models['products'].__models__[1]
        indices_items_map = ItemsIndicesMap(products_model).get_indices_items_map(session)

        expected1 = [None, None, None]
        for k, v in indices_items_map.items():
            expected1[k] = True if v == 'test' or v == 'test2' else False

        expected2 = [None, None, None]
        for k, v in indices_items_map.items():
            expected2[k] = False if v == 'test' or v == 'test2' else True

        filter_ = redis.hgetall('products_1_filter3_filter')
        key1 = 'test'.encode()
        key2 = 'test2'.encode()
        filter_[key1] = np.fromstring(filter_[key1], dtype=np.bool).tolist()
        filter_[key2] = np.fromstring(filter_[key2], dtype=np.bool).tolist()

        assert filter_ == {key1: expected1, key2: expected2}

    def test_if_update_filters_builds_object_filter(self, filters_updater_client, headers, redis, session):
        products = {
            'test': {
                'sku': 'test',
                'filter1': 1,
                'filter2': True,
                'filter3': 'test',
                'filter4': {'id': 1},
                'filter5': [1]
            },
            'test2': {
                'sku': 'test2',
                'filter1': 1,
                'filter2': True,
                'filter3': 'test',
                'filter4': {'id': 1},
                'filter5': [1,2]
            },
            'test3': {
                'sku': 'test3',
                'filter1': 2,
                'filter2': False,
                'filter3': 'test2',
                'filter4': {'id': 2},
                'filter5': [2,3]
            }
        }
        filters_updater_client.post('/products?store_id=1',
                                    body=json.dumps(list(products.values())), headers=headers)
        filters_updater_client.post('/products/update_filters?store_id=1', headers=headers)
        sleep(0.05)

        while True:
            resp = filters_updater_client.get(
                '/products/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        products_model = filters_updater_client.app.models['products'].__models__[1]
        indices_items_map = ItemsIndicesMap(products_model).get_indices_items_map(session)

        expected1 = [None, None, None]
        for k, v in indices_items_map.items():
            expected1[k] = True if v == 'test' or v == 'test2' else False

        expected2 = [None, None, None]
        for k, v in indices_items_map.items():
            expected2[k] = False if v == 'test' or v == 'test2' else True

        filter_ = redis.hgetall('products_1_filter4_filter')
        key1 = '(1,)'.encode()
        key2 = '(2,)'.encode()
        filter_[key1] = np.fromstring(filter_[key1], dtype=np.bool).tolist()
        filter_[key2] = np.fromstring(filter_[key2], dtype=np.bool).tolist()

        assert filter_ == {key1: expected1, key2: expected2}

    def test_if_update_filters_builds_array_filter(self, filters_updater_client, headers, redis, session):
        products = {
            'test': {
                'sku': 'test',
                'filter1': 1,
                'filter2': True,
                'filter3': 'test',
                'filter4': {'id': 1},
                'filter5': [1]
            },
            'test2': {
                'sku': 'test2',
                'filter1': 1,
                'filter2': True,
                'filter3': 'test',
                'filter4': {'id': 1},
                'filter5': [1,2]
            },
            'test3': {
                'sku': 'test3',
                'filter1': 2,
                'filter2': False,
                'filter3': 'test2',
                'filter4': {'id': 2},
                'filter5': [2,3]
            }
        }
        filters_updater_client.post('/products?store_id=1',
                                    body=json.dumps(list(products.values())), headers=headers)
        filters_updater_client.post('/products/update_filters?store_id=1', headers=headers)
        sleep(0.05)

        while True:
            resp = filters_updater_client.get(
                '/products/update_filters?store_id=1&hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        products_model = filters_updater_client.app.models['products'].__models__[1]
        indices_items_map = ItemsIndicesMap(products_model).get_indices_items_map(session)

        expected1 = [None, None, None]
        for k, v in indices_items_map.items():
            expected1[k] = True if v == 'test' or v == 'test2' else False

        expected2 = [None, None, None]
        for k, v in indices_items_map.items():
            expected2[k] = True if v == 'test2' or v == 'test3' else False

        expected3 = [None, None, None]
        for k, v in indices_items_map.items():
            expected3[k] = True if v == 'test3' else False

        filter_ = redis.hgetall('products_1_filter5_filter')
        key1 = '1'.encode()
        key2 = '2'.encode()
        key3 = '3'.encode()
        filter_[key1] = np.fromstring(filter_[key1], dtype=np.bool).tolist()
        filter_[key2] = np.fromstring(filter_[key2], dtype=np.bool).tolist()
        filter_[key3] = np.fromstring(filter_[key3], dtype=np.bool).tolist()

        assert filter_ == {key1: expected1, key2: expected2, key3: expected3}
