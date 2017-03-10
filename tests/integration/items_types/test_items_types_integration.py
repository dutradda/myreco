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


from swaggerit.models._base import _all_models
from myreco.engines.cores.items_indices_map import ItemsIndicesMap
from myreco.authorizer import MyrecoAuthorizer
from base64 import b64encode
from unittest import mock
from datetime import datetime
from time import sleep
import pytest
import ujson
import numpy as np


@pytest.fixture
def init_db(models, session, api):
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

    yield None

    _all_models.pop('test_1', None)
    if 'test_collection' in _all_models:
        api.remove_swagger_paths(_all_models.pop('test_collection'))


class TestItemsTypesModelPost(object):

    async def test_post_without_body(self, init_db, headers, client):
        client = await client
        resp = await client.post('/items_types/', headers=headers)
        assert resp.status == 400
        assert await resp.json() == {'message': 'Request body is missing'}

    async def test_post_with_invalid_body(self, init_db, headers, client):
        client = await client
        resp = await client.post('/items_types/', headers=headers, data='[{}]')
        assert resp.status == 400
        assert await resp.json() ==  {
            'message': "'name' is a required property. "\
                       "Failed validating instance['0'] for schema['items']['required']",
            'schema': {
                'type': 'object',
                'additionalProperties': False,
                'required': ['name', 'schema', 'stores'],
                'properties': {
                    'name': {'type': 'string'},
                    'stores': {'$ref': '#/definitions/ItemsTypesModel.stores'},
                    'schema': {'$ref': '#/definitions/ItemsTypesModel.items'},
                    'store_items_base_class': {'$ref': '#/definitions/ItemsTypesModel.store_items_base_class'}
                }
            }
        }

    async def test_post_valid(self, init_db, headers, client):
        client = await client
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {
                'properties': {'id': {'type': 'integer'}},
                'type': 'object',
                'id_names': ['id']
            }
        }]
        resp = await client.post('/items_types/', headers=headers, data=ujson.dumps(body))
        body[0]['id'] = 1
        body[0]['available_filters'] = [{'name': 'id', 'schema': {'type': 'integer'}}]
        body[0]['stores'] = [{
            'configuration': {'data_path': '/test'},
            'country': 'test',
            'id': 1,
            'name': 'test'
        }]
        body[0]['store_items_base_class'] = None

        assert resp.status == 201
        assert await resp.json() ==  body

    async def test_post_with_invalid_grant(self, init_db, client):
        client = await client
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {'properties': {'id': {'type': 'integer'}}, 'type': 'object', 'id_names': ['id']}
        }]
        resp = await client.post('/items_types/', headers={'Authorization': 'invalid'}, data=ujson.dumps(body))
        assert resp.status == 401
        assert await resp.json() ==  {'message': 'Invalid authorization'}

    async def test_post_with_invalid_schema_type(self, init_db, headers, client):
        client = await client
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {
                'properties': {
                    'id': {'type': 'string'}
                }
            }
        }]
        resp = await client.post('/items_types/', headers=headers, data=ujson.dumps(body))
        assert resp.status == 400
        assert await resp.json() == {
            'instance': {
                'properties': {'id': {'type': 'string'}}
            },
            'message': "'type' is a required property. "\
                       "Failed validating instance['0']['schema'] "\
                       "for schema['items']['properties']['schema']['required']",
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
                    'properties': {'$ref': 'store_items_metaschema.json#/definitions/baseObject/properties/properties'}
                }
            }
        }

    async def test_post_with_invalid_id_name(self, init_db, headers, client):
        client = await client
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
        resp = await client.post('/items_types/', headers=headers, data=ujson.dumps(body))
        assert resp.status == 400
        assert await resp.json() == {
            'instance': ['test'],
            'message': "id_name 'test' was not found in schema properties",
            'schema': {
                'type': 'object',
                'id_names': ['test'],
                'properties': {
                    'id': {'type': 'string'}
                }
            }
        }


class TestItemsTypesModelGet(object):

    async def test_get_not_found(self, init_db, headers_without_content_type, client):
        client = await client
        resp = await client.get('/items_types/?stores=id:1', headers=headers_without_content_type)
        assert resp.status == 404

    async def test_get_invalid_with_body(self, init_db, headers, client):
        client = await client
        resp = await client.get('/items_types/?stores=id:1', headers=headers, data='{}')
        assert resp.status == 400
        assert await resp.json() == {'message': 'Request body is not acceptable'}

    async def test_get(self, init_db, headers, headers_without_content_type, client):
        client = await client
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {'properties': {'test': {'type': 'string'}}, 'type': 'object', 'id_names': ['test']}
        }]
        await client.post('/items_types/', headers=headers, data=ujson.dumps(body))
        body[0]['id'] = 1
        body[0]['available_filters'] = [{'name': 'test', 'schema': {'type': 'string'}}]
        body[0]['stores'] = [{
            'configuration': {'data_path': '/test'},
            'country': 'test',
            'id': 1,
            'name': 'test'
        }]
        body[0]['store_items_base_class'] = None

        resp = await client.get('/items_types/?stores=id:1', headers=headers_without_content_type)
        assert resp.status == 200
        assert await resp.json() ==  body


class TestItemsTypesModelUriTemplatePatch(object):

    async def test_patch_without_body(self, init_db, headers, client):
        client = await client
        resp = await client.patch('/items_types/1/', headers=headers, data='')
        assert resp.status == 400
        assert await resp.json() == {'message': 'Request body is missing'}

    async def test_patch_with_invalid_body(self, init_db, headers, client):
        client = await client
        resp = await client.patch('/items_types/1/', headers=headers, data='{}')
        assert resp.status == 400
        assert await resp.json() ==  {
            'message': '{} does not have enough properties. '\
                       "Failed validating instance for schema['minProperties']",
            'schema': {
                'type': 'object',
                'additionalProperties': False,
                'minProperties': 1,
                'properties': {
                    'name': {'type': 'string'},
                    'stores': {'$ref': '#/definitions/ItemsTypesModel.stores'},
                    'schema': {'$ref': '#/definitions/ItemsTypesModel.items'},
                    'store_items_base_class': {'$ref': '#/definitions/ItemsTypesModel.store_items_base_class'}
                }
            }
        }

    async def test_patch_not_found(self, init_db, headers, client):
        client = await client
        body = {
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {'properties': {'test': {'type': 'string'}}, 'type': 'object', 'id_names': ['test']}
        }
        resp = await client.patch('/items_types/1/', headers=headers, data=ujson.dumps(body))
        assert resp.status == 404

    async def test_patch_valid(self, init_db, headers, client):
        client = await client
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {'properties': {'test': {'type': 'string'}}, 'type': 'object', 'id_names': ['test']}
        }]
        resp = await client.post('/items_types/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        body = {
            'name': 'test2'
        }
        resp = await client.patch('/items_types/1/', headers=headers, data=ujson.dumps(body))
        obj['name'] = 'test2'
        obj['available_filters'] = [{'name': 'test', 'schema': {'type': 'string'}}]

        assert resp.status == 200
        assert await resp.json() ==  obj


class TestItemsTypesModelUriTemplateDelete(object):

    async def test_delete_with_body(self, init_db, headers, client):
        client = await client
        resp = await client.delete('/items_types/1/', headers=headers, data='{}')
        assert resp.status == 400
        assert await resp.json() == {'message': 'Request body is not acceptable'}

    async def test_delete(self, init_db, headers, headers_without_content_type, client):
        client = await client
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {'properties': {'test': {'type': 'string'}}, 'type': 'object', 'id_names': ['test']}
        }]
        resp = await client.post('/items_types/', headers=headers, data=ujson.dumps(body))

        resp = await client.get('/items_types/1/', headers=headers_without_content_type)
        assert resp.status == 200

        resp = await client.delete('/items_types/1/', headers=headers_without_content_type)
        assert resp.status == 204

        resp = await client.get('/items_types/1/', headers=headers_without_content_type)
        assert resp.status == 404


class TestItemsTypesModelUriTemplateGet(object):

    async def test_get_with_body(self, init_db, headers, client):
        client = await client
        resp = await client.get('/items_types/1/', headers=headers, data='{}')
        assert resp.status == 400
        assert await resp.json() == {'message': 'Request body is not acceptable'}

    async def test_get_not_found(self, init_db, headers_without_content_type, client):
        client = await client
        resp = await client.get('/items_types/1/', headers=headers_without_content_type)
        assert resp.status == 404

    async def test_get(self, init_db, headers, headers_without_content_type, client):
        client = await client
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {'properties': {'test': {'type': 'string'}}, 'type': 'object', 'id_names': ['test']}
        }]
        await client.post('/items_types/', headers=headers, data=ujson.dumps(body))
        resp = await client.get('/items_types/1/', headers=headers_without_content_type)
        body[0]['id'] = 1
        body[0]['available_filters'] = [{'name': 'test', 'schema': {'type': 'string'}}]
        body[0]['stores'] = [{
            'configuration': {'data_path': '/test'},
            'country': 'test',
            'id': 1,
            'name': 'test'
        }]
        body[0]['store_items_base_class'] = None

        assert resp.status == 200
        assert await resp.json() == body[0]


class TestItemsModelPost(object):

    async def test_items_post_valid(self, init_db, headers, client):
        client = await client
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {'properties': {'id': {'type': 'string'}}, 'type': 'object', 'id_names': ['id']}
        }]
        await client.post('/items_types/', headers=headers, data=ujson.dumps(body))

        body = [{'id': 'test'}]
        resp = await client.post('/items_types/1/items?store_id=1', headers=headers, data=ujson.dumps(body))
        assert resp.status == 201
        assert await resp.json() == body

    async def test_items_post_invalid(self, init_db, headers, client):
        client = await client
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {'properties': {'id': {'type': 'string'}}, 'type': 'object', 'id_names': ['id']}
        }]
        await client.post('/items_types/', headers=headers, data=ujson.dumps(body))

        body = [{'id': 1}]
        resp = await client.post('/items_types/1/items?store_id=1', headers=headers, data=ujson.dumps(body))
        assert resp.status == 400
        assert await resp.json() == {
            'instance': 1,
            'message': "1 is not of type 'string'. "\
                       "Failed validating instance['0']['id'] "\
                       "for schema['items']['properties']['id']['type']",
            'schema': {'type': 'string'}
        }


class TestItemsModelPatch(object):

    async def test_items_patch_valid(self, init_db, headers, client, headers_without_content_type):
        client = await client
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
        await client.post('/items_types/', headers=headers, data=ujson.dumps(body))

        body = [{'id': 'test', 't1': 1}]
        resp = await client.post('/items_types/1/items?store_id=1', headers=headers, data=ujson.dumps(body))

        body = [{'id': 'test', 't1': 2}]
        resp = await client.patch('/items_types/1/items?store_id=1', headers=headers, data=ujson.dumps(body))
        assert resp.status == 200
        assert await resp.json() == body

        resp = await client.get('/items_types/1/items?store_id=1', headers=headers_without_content_type)
        assert resp.status == 200
        assert await resp.json() == body

    async def test_items_patch_with_delete(self, init_db, headers, client, headers_without_content_type):
        client = await client
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
        await client.post('/items_types/', headers=headers, data=ujson.dumps(body))

        body = [{'id': 'test', 't1': 1}, {'id': 'test2', 't1': 2}]
        resp = await client.post('/items_types/1/items?store_id=1', headers=headers, data=ujson.dumps(body))

        body = [{'id': 'test', '_operation': 'delete'}]
        resp = await client.patch('/items_types/1/items?store_id=1', headers=headers, data=ujson.dumps(body))
        assert resp.status == 200
        assert await resp.json() == [{'id': 'test', '_operation': 'delete'}]

        resp = await client.get('/test/items_types/1/items?store_id=1', headers=headers_without_content_type)
        assert resp.status == 404

    async def test_items_patch_invalid(self, init_db, headers, client):
        client = await client
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
        await client.post('/items_types/', headers=headers, data=ujson.dumps(body))

        body = [{'id': 1}]
        resp = await client.post('/items_types/1/items?store_id=1', headers=headers, data=ujson.dumps(body))
        assert resp.status == 400
        assert await resp.json() == {
            'instance': 1,
            'message': "1 is not of type 'string'. "\
                       "Failed validating instance['0']['id'] "\
                       "for schema['items']['properties']['id']['type']",
            'schema': {'type': 'string'}
        }

    async def test_if_items_patch_updates_stock_filter(self, init_db, headers, redis, session, client, api):
        body = [{
            'name': 'test',
            'stores': [{'id': 1}],
            'schema': {'properties': {'id': {'type': 'string'}}, 'type': 'object', 'id_names': ['id']}
        }]
        client = await client
        await client.post('/items_types/', headers=headers, data=ujson.dumps(body))

        body = [{'id': 'test'}]
        resp = await client.post('/items_types/1/items?store_id=1', headers=headers, data=ujson.dumps(body))
        assert resp.status == 201


        test_model = _all_models['store_items_test_1']
        await ItemsIndicesMap(test_model).update(session)

        body = [{'id': 'test', '_operation': 'delete'}]
        resp = await client.patch('/items_types/1/items?store_id=1', headers=headers, data=ujson.dumps(body))
        stock_filter = np.fromstring(await redis.get('store_items_test_1_stock_filter'), dtype=np.bool).tolist()
        assert stock_filter == [False]


@pytest.fixture
def update_filters_init_db(models, session, api, monkeypatch):
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
    types = session.loop.run_until_complete(models['items_types'].insert(session, item_type))

    engine_core = {
        'name': 'top_seller',
        'configuration': {
            'core_module': {
                'path': 'myreco.engines.cores.top_seller.core',
                'object_name': 'TopSellerEngineCore'
            }
        }
    }
    session.loop.run_until_complete(models['engines_cores'].insert(session, engine_core))

    engine = {
        'name': 'Top Seller',
        'configuration_json': ujson.dumps({
            'days_interval': 7,
            'data_importer_path': 'myreco.engines.cores.base.AbstractDataImporter'
        }),
        'store_id': 1,
        'core_id': 1,
        'item_type_id': 1
    }
    session.loop.run_until_complete(models['engines'].insert(session, engine))

    session.loop.run_until_complete(models['external_variables'].insert(session, {'name': 'test', 'store_id': 1}))

    slot = {
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
            'property_name': 'filter1'
        },{
            '_operation': 'insert',
            'external_variable_name': 'test',
            'external_variable_store_id': 1,
            'is_inclusive': True,
            'type_id': 'property_value',
            'property_name': 'filter2'
        },{
            '_operation': 'insert',
            'external_variable_name': 'test',
            'external_variable_store_id': 1,
            'is_inclusive': True,
            'type_id': 'property_value',
            'property_name': 'filter3'
        },{
            '_operation': 'insert',
            'external_variable_name': 'test',
            'external_variable_store_id': 1,
            'is_inclusive': True,
            'type_id': 'property_value',
            'property_name': 'filter4'
        },{
            '_operation': 'insert',
            'external_variable_name': 'test',
            'external_variable_store_id': 1,
            'is_inclusive': True,
            'type_id': 'property_value',
            'property_name': 'filter5'
        }]
    }
    session.loop.run_until_complete(models['slots'].insert(session, slot))

    yield None

    _all_models.pop('store_items_products_1')


def datetime_mock():
    mock_ = mock.MagicMock()
    mock_.now.return_value = datetime(1900, 1, 1)
    return mock_



def set_patches(monkeypatch):
    monkeypatch.setattr('swaggerit.models.orm._jobs_meta.random.getrandbits',
        mock.MagicMock(return_value=131940827655846590526331314439483569710))
    monkeypatch.setattr('swaggerit.models.orm._jobs_meta.datetime', datetime_mock())


class TestItemsTypesModelFiltersUpdater(object):

    async def test_filters_updater_post(self, update_filters_init_db, headers, client, monkeypatch, headers_without_content_type):
        set_patches(monkeypatch)
        client = await client
        products = [{
            'sku': 'test', 'filter1': 1, 'filter2': True,
            'filter3': 'test', 'filter4': {'id': 1}, 'filter5': [1]}]
        resp = await client.post('/items_types/1/items?store_id=1',
                          data=ujson.dumps(products), headers=headers)
        assert resp.status == 201

        resp = await client.post('/items_types/1/update_filters?store_id=1', headers=headers_without_content_type)
        assert await resp.json() == {'job_hash': '6342e10bd7dca3240c698aa79c98362e'}

    async def test_filters_updater_get_done(self, update_filters_init_db, headers, client, monkeypatch, headers_without_content_type):
        set_patches(monkeypatch)
        client = await client
        products = [{
            'sku': 'test', 'filter1': 1, 'filter2': True,
            'filter3': 'test', 'filter4': {'id': 1}, 'filter5': [1]}]
        await client.post('/items_types/1/items?store_id=1',
                                    data=ujson.dumps(products), headers=headers)
        resp = await client.post('/items_types/1/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)

        while True:
            resp = await client.get(
                '/items_types/1/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        resp = await client.get(
            '/items_types/1/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
            headers=headers_without_content_type)
        assert await resp.json() == {
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
            'time_info': {
                'elapsed': '0:00',
                'end': '1900-01-01 00:00',
                'start': '1900-01-01 00:00'
            }
        }

    async def test_if_update_filters_builds_stock_filter(self, update_filters_init_db, headers, redis, monkeypatch, headers_without_content_type, client):
        set_patches(monkeypatch)
        client = await client
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
        await client.post('/items_types/1/items?store_id=1',
                                    data=ujson.dumps(list(products.values())), headers=headers)
        await client.post('/items_types/1/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)

        while True:
            resp = await client.get(
                '/items_types/1/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        stock_filter = np.fromstring(await redis.get('store_items_products_1_stock_filter'), dtype=np.bool).tolist()
        assert stock_filter == [True, True, True]

    async def test_if_update_filters_builds_boolean_filter(self, update_filters_init_db, headers, redis, session, monkeypatch, headers_without_content_type, client):
        set_patches(monkeypatch)
        client = await client
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
        await client.post('/items_types/1/items?store_id=1',
                                    data=ujson.dumps(list(products.values())), headers=headers)
        await client.post('/items_types/1/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)

        while True:
            resp = await client.get(
                '/items_types/1/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        products_model = _all_models['store_items_products_1']
        indices_items_map = await ItemsIndicesMap(products_model).get_indices_items_map(session)

        expected = [None, None, None]
        for k, v in indices_items_map.items():
            expected[k] = True if v == 'test' or v == 'test2' else False

        filter_ = await redis.get('store_items_products_1_filter2_filter')
        filter_ = np.fromstring(filter_, dtype=np.bool).tolist()
        assert filter_ == expected

    async def test_if_update_filters_builds_integer_filter(self, update_filters_init_db, headers, redis, session, monkeypatch, headers_without_content_type, client):
        set_patches(monkeypatch)
        client = await client
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
        await client.post('/items_types/1/items?store_id=1',
                                    data=ujson.dumps(list(products.values())), headers=headers)
        await client.post('/items_types/1/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)

        while True:
            resp = await client.get(
                '/items_types/1/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        products_model = _all_models['store_items_products_1']
        indices_items_map = await ItemsIndicesMap(products_model).get_indices_items_map(session)

        expected1 = [None, None, None]
        for k, v in indices_items_map.items():
            expected1[k] = True if v == 'test' or v == 'test2' else False

        expected2 = [None, None, None]
        for k, v in indices_items_map.items():
            expected2[k] = False if v == 'test' or v == 'test2' else True

        filter_ = await redis.hgetall('store_items_products_1_filter1_filter')
        key1 = '1'.encode()
        key2 = '2'.encode()
        filter_[key1] = np.fromstring(filter_[key1], dtype=np.bool).tolist()
        filter_[key2] = np.fromstring(filter_[key2], dtype=np.bool).tolist()

        assert filter_ == {key1: expected1, key2: expected2}

    async def test_if_update_filters_builds_string_filter(self, update_filters_init_db, headers, redis, session, monkeypatch, headers_without_content_type, client):
        set_patches(monkeypatch)
        client = await client
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
        await client.post('/items_types/1/items?store_id=1',
                                    data=ujson.dumps(list(products.values())), headers=headers)
        await client.post('/items_types/1/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)

        while True:
            resp = await client.get(
                '/items_types/1/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        products_model = _all_models['store_items_products_1']
        indices_items_map = await ItemsIndicesMap(products_model).get_indices_items_map(session)

        expected1 = [None, None, None]
        for k, v in indices_items_map.items():
            expected1[k] = True if v == 'test' or v == 'test2' else False

        expected2 = [None, None, None]
        for k, v in indices_items_map.items():
            expected2[k] = False if v == 'test' or v == 'test2' else True

        filter_ = await redis.hgetall('store_items_products_1_filter3_filter')
        key1 = 'test'.encode()
        key2 = 'test2'.encode()
        filter_[key1] = np.fromstring(filter_[key1], dtype=np.bool).tolist()
        filter_[key2] = np.fromstring(filter_[key2], dtype=np.bool).tolist()

        assert filter_ == {key1: expected1, key2: expected2}

    async def test_if_update_filters_builds_object_filter(self, update_filters_init_db, headers, redis, session, monkeypatch, headers_without_content_type, client):
        set_patches(monkeypatch)
        client = await client
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
        await client.post('/items_types/1/items?store_id=1',
                                    data=ujson.dumps(list(products.values())), headers=headers)
        await client.post('/items_types/1/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)

        while True:
            resp = await client.get(
                '/items_types/1/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        products_model = _all_models['store_items_products_1']
        indices_items_map = await ItemsIndicesMap(products_model).get_indices_items_map(session)

        expected1 = [None, None, None]
        for k, v in indices_items_map.items():
            expected1[k] = True if v == 'test' or v == 'test2' else False

        expected2 = [None, None, None]
        for k, v in indices_items_map.items():
            expected2[k] = False if v == 'test' or v == 'test2' else True

        filter_ = await redis.hgetall('store_items_products_1_filter4_filter')
        key1 = '(1,)'.encode()
        key2 = '(2,)'.encode()
        filter_[key1] = np.fromstring(filter_[key1], dtype=np.bool).tolist()
        filter_[key2] = np.fromstring(filter_[key2], dtype=np.bool).tolist()

        assert filter_ == {key1: expected1, key2: expected2}

    async def test_if_update_filters_builds_array_filter(self, update_filters_init_db, headers, redis, session, monkeypatch, headers_without_content_type, client):
        set_patches(monkeypatch)
        client = await client
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
        await client.post('/items_types/1/items?store_id=1',
                                    data=ujson.dumps(list(products.values())), headers=headers)
        await client.post('/items_types/1/update_filters?store_id=1', headers=headers_without_content_type)
        sleep(0.05)

        while True:
            resp = await client.get(
                '/items_types/1/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers_without_content_type)
            if (await resp.json())['status'] != 'running':
                break

        products_model = _all_models['store_items_products_1']
        indices_items_map = await ItemsIndicesMap(products_model).get_indices_items_map(session)

        expected1 = [None, None, None]
        for k, v in indices_items_map.items():
            expected1[k] = True if v == 'test' or v == 'test2' else False

        expected2 = [None, None, None]
        for k, v in indices_items_map.items():
            expected2[k] = True if v == 'test2' or v == 'test3' else False

        expected3 = [None, None, None]
        for k, v in indices_items_map.items():
            expected3[k] = True if v == 'test3' else False

        filter_ = await redis.hgetall('store_items_products_1_filter5_filter')
        key1 = '1'.encode()
        key2 = '2'.encode()
        key3 = '3'.encode()
        filter_[key1] = np.fromstring(filter_[key1], dtype=np.bool).tolist()
        filter_[key2] = np.fromstring(filter_[key2], dtype=np.bool).tolist()
        filter_[key3] = np.fromstring(filter_[key3], dtype=np.bool).tolist()

        assert filter_ == {key1: expected1, key2: expected2, key3: expected3}
