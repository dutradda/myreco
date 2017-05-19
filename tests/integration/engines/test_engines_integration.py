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
def init_db(models, session, api):
    user = {
        'name': 'test',
        'email': 'test',
        'password': 'test',
        'admin': True
    }
    session.loop.run_until_complete(models['users'].insert(session, user))

    tmp = tempfile.TemporaryDirectory()
    store = {
        'name': 'test',
        'country': 'test',
        'configuration': {}
    }
    session.loop.run_until_complete(models['stores'].insert(session, store))

    item_type = {
        'name': 'products',
        'schema': {
            'type': 'object',
            'id_names': ['sku'],
            'properties': {'sku': {'type': 'string'}}
        },
        'stores': [{'id': 1}]
    }
    session.loop.run_until_complete(models['item_types'].insert(session, item_type))

    strategy = {
        'name': 'test',
        'class_module': 'tests.integration.fixtures',
        'class_name': 'EngineStrategyTest'
    }
    session.loop.run_until_complete(models['engine_strategies'].insert(session, strategy))

    engine = {
        'name': 'Seven Days Top Seller',
        'objects': [{
            '_operation': 'insert',
            'name': 'Top Seller Object',
            'type': 'top_seller_array',
            'configuration': {'days_interval': 7}
        }],
        'store_id': 1,
        'item_type_id': 1,
        'strategy_id': 1
    }
    session.loop.run_until_complete(models['engines'].insert(session, engine))


    tmp.cleanup()
    _all_models.pop('store_items_products_1', None)


class TestEnginesModelPost(object):

    async def test_post_without_body(self, init_db, headers, client):
        client = await client
        resp = await client.post('/engines/', headers=headers)
        assert resp.status == 400
        assert await resp.json() == {'message': 'Request body is missing'}

    async def test_post_with_invalid_body(self, init_db, headers, client):
        client = await client
        resp = await client.post('/engines/', headers=headers, data='[{}]')
        assert resp.status == 400
        assert await resp.json() ==  {
            'message': "'store_id' is a required property. Failed validating "\
                       "instance['0'] for schema['items']['required']",
            'schema': {
                'type': 'object',
                'additionalProperties': False,
                'required': ['store_id', 'item_type_id', 'strategy_id'],
                'properties': {
                    'name': {'type': 'string'},
                    'store_id': {'type': 'integer'},
                    'item_type_id': {'type': 'integer'},
                    'strategy_id': {'type': 'integer'},
                    'objects': {
                        'oneOf': [{
                            '$ref': '#/definitions/EnginesModel.insert_objects_schema'
                        },{
                            '$ref': '#/definitions/EnginesModel.update_objects_schema'
                        },{
                            '$ref': '#/definitions/EnginesModel.get_objects_schema'
                        }]
                    }
                }
            }
        }

    async def test_post_with_invalid_grant(self, init_db, client):
        client = await client
        resp = await client.post('/engines/', headers={'Authorization': 'invalid'})
        assert resp.status == 401
        assert await resp.json() ==  {'message': 'Invalid authorization'}

    async def test_post_valid_with_insert_object(self, init_db, headers, client):
        body = [{
            'name': 'Seven Days Top Seller 2',
            'objects': [{
                '_operation': 'insert',
                'name': 'Top Seller Object 2',
                'type': 'top_seller_array',
                'configuration': {'days_interval': 7}
            }],
            'store_id': 1,
            'item_type_id': 1,
            'strategy_id': 1
        }]
        client = await client
        resp = await client.post('/engines/', headers=headers, data=ujson.dumps(body))
        assert resp.status == 201

        body[0]['id'] = 2
        body[0]['store'] = {'id': 1, 'name': 'test', 'country': 'test', 'configuration': {}}
        body[0]['strategy_id'] = 1
        body[0]['item_type'] = {
            'id': 1,
            'store_items_class': None,
            'stores': [body[0]['store']],
            'name': 'products',
            'schema': {
                'type': 'object',
                'id_names': ['sku'],
                'properties': {'sku': {'type': 'string'}}
            },
            'available_filters': [{'name': 'sku', 'schema': {'type': 'string'}}]
        }
        body[0]['strategy'] = {
            'id': 1,
            'name': 'test',
            'class_module': 'tests.integration.fixtures',
            'class_name': 'EngineStrategyTest',
            'object_types': ['top_seller_array']
        }
        body[0]['objects'] = [{
            'id': 2,
            'store_id': 1,
            'item_type_id': 1,
            'strategy_id': 1,
            'name': 'Top Seller Object 2',
            'type': 'top_seller_array',
            'configuration': {'days_interval': 7},
            'item_type': body[0]['item_type'],
            'store': body[0]['store'],
            'strategy': body[0]['strategy']
        }]
        body[0]['variables'] = []

        assert await resp.json() ==  body

    async def test_post_valid_with_update_object(self, init_db, headers, client):
        body = [{
            'name': 'Seven Days Top Seller 2',
            'objects': [{
                '_operation': 'update',
                'id': 1,
                'name': 'Top Seller Object 2'
            }],
            'store_id': 1,
            'item_type_id': 1,
            'strategy_id': 1
        }]
        client = await client
        resp = await client.post('/engines/', headers=headers, data=ujson.dumps(body))
        assert resp.status == 201

        body[0]['id'] = 2
        body[0]['store'] = {'id': 1, 'name': 'test', 'country': 'test', 'configuration': {}}
        body[0]['strategy_id'] = 1
        body[0]['item_type'] = {
            'id': 1,
            'store_items_class': None,
            'stores': [body[0]['store']],
            'name': 'products',
            'schema': {
                'type': 'object',
                'id_names': ['sku'],
                'properties': {'sku': {'type': 'string'}}
            },
            'available_filters': [{'name': 'sku', 'schema': {'type': 'string'}}]
        }
        body[0]['strategy'] = {
            'id': 1,
            'name': 'test',
            'class_module': 'tests.integration.fixtures',
            'class_name': 'EngineStrategyTest',
            'object_types': ['top_seller_array']
        }
        body[0]['objects'] = [{
            'id': 1,
            'store_id': 1,
            'item_type_id': 1,
            'strategy_id': 1,
            'name': 'Top Seller Object 2',
            'type': 'top_seller_array',
            'configuration': {'days_interval': 7},
            'item_type': body[0]['item_type'],
            'store': body[0]['store'],
            'strategy': body[0]['strategy']
        }]
        body[0]['variables'] = []

        assert await resp.json() ==  body

    async def test_post_valid_with_get_object(self, init_db, headers, client):
        body = [{
            'name': 'Seven Days Top Seller 2',
            'objects': [{
                '_operation': 'get',
                'id': 1
            }],
            'store_id': 1,
            'item_type_id': 1,
            'strategy_id': 1
        }]
        client = await client
        resp = await client.post('/engines/', headers=headers, data=ujson.dumps(body))
        assert resp.status == 201

        body[0]['id'] = 2
        body[0]['store'] = {'id': 1, 'name': 'test', 'country': 'test', 'configuration': {}}
        body[0]['strategy_id'] = 1
        body[0]['item_type'] = {
            'id': 1,
            'store_items_class': None,
            'stores': [body[0]['store']],
            'name': 'products',
            'schema': {
                'type': 'object',
                'id_names': ['sku'],
                'properties': {'sku': {'type': 'string'}}
            },
            'available_filters': [{'name': 'sku', 'schema': {'type': 'string'}}]
        }
        body[0]['strategy'] = {
            'id': 1,
            'name': 'test',
            'class_module': 'tests.integration.fixtures',
            'class_name': 'EngineStrategyTest',
            'object_types': ['top_seller_array']
        }
        body[0]['objects'] = [{
            'id': 1,
            'store_id': 1,
            'item_type_id': 1,
            'strategy_id': 1,
            'name': 'Top Seller Object',
            'type': 'top_seller_array',
            'configuration': {'days_interval': 7},
            'item_type': body[0]['item_type'],
            'store': body[0]['store'],
            'strategy': body[0]['strategy']
        }]
        body[0]['variables'] = []

        assert await resp.json() ==  body


class TestEnginesModelGet(object):

    async def test_get_not_found(self, init_db, headers_without_content_type, client):
        client = await client
        resp = await client.get('/engines/?store_id=2', headers=headers_without_content_type)
        assert resp.status == 404

    async def test_get_invalid_with_body(self, init_db, headers, client):
        client = await client
        resp = await client.get('/engines/?store_id=1', headers=headers, data='{}')
        assert resp.status == 400
        assert await resp.json() == {'message': 'Request body is not acceptable'}

    async def test_get_valid(self, init_db, headers, headers_without_content_type, client):
        body = [{
            'name': 'Seven Days Top Seller',
            'store_id': 1,
            'item_type_id': 1,
            'strategy_id': 1
        }]
        body[0]['id'] = 1
        body[0]['store'] = {'id': 1, 'name': 'test', 'country': 'test', 'configuration': {}}
        body[0]['strategy_id'] = 1
        body[0]['item_type'] = {
            'id': 1,
            'store_items_class': None,
            'stores': [body[0]['store']],
            'name': 'products',
            'schema': {
                'type': 'object',
                'id_names': ['sku'],
                'properties': {'sku': {'type': 'string'}}
            },
            'available_filters': [{'name': 'sku', 'schema': {'type': 'string'}}]
        }
        body[0]['strategy'] = {
            'id': 1,
            'name': 'test',
            'class_module': 'tests.integration.fixtures',
            'class_name': 'EngineStrategyTest',
            'object_types': ['top_seller_array']
        }
        body[0]['objects'] = [{
            'id': 1,
            'store_id': 1,
            'item_type_id': 1,
            'strategy_id': 1,
            'name': 'Top Seller Object',
            'type': 'top_seller_array',
            'configuration': {'days_interval': 7},
            'item_type': body[0]['item_type'],
            'store': body[0]['store'],
            'strategy': body[0]['strategy']
        }]
        body[0]['variables'] = []

        client = await client
        resp = await client.get('/engines/?store_id=1', headers=headers_without_content_type)
        assert resp.status == 200
        assert await resp.json() ==  body


class TestEnginesModelUriTemplatePatch(object):

    async def test_patch_without_body(self, init_db, headers, client):
        client = await client
        resp = await client.patch('/engines/1/', headers=headers, data='')
        assert resp.status == 400
        assert await resp.json() == {'message': 'Request body is missing'}

    async def test_patch_with_invalid_body(self, init_db, headers, client):
        client = await client
        resp = await client.patch('/engines/1/', headers=headers, data='{}')
        assert resp.status == 400
        assert await resp.json() ==  {
            'message': "{} does not have enough properties. "\
                       "Failed validating instance for schema['minProperties']",
            'schema': {
                'type': 'object',
                'additionalProperties': False,
                'minProperties': 1,
                'properties': {
                    'name': {'type': 'string'},
                    'store_id': {'type': 'integer'},
                    'item_type_id': {'type': 'integer'},
                    'strategy_id': {'type': 'integer'},
                    'objects': {
                        'oneOf': [{
                            '$ref': '#/definitions/EnginesModel.insert_objects_schema'
                        },{
                            '$ref': '#/definitions/EnginesModel.update_objects_schema'
                        },{
                            '$ref': '#/definitions/EnginesModel.get_objects_schema'
                        },{
                            '$ref': '#/definitions/EnginesModel.delete_remove_objects_schema'
                        }]
                    }
                }
            }
        }

    async def test_patch_not_found(self, init_db, headers, client):
        body = {
            'name': 'test',
            'store_id': 1
        }
        client = await client
        resp = await client.patch('/engines/2/', headers=headers, data=ujson.dumps(body))
        assert resp.status == 404

    async def test_patch(self, init_db, headers, headers_without_content_type, client):
        client = await client
        resp = await client.get('/engines/1', headers=headers_without_content_type)
        obj = await resp.json()

        body = {
            'name': 'test2'
        }
        resp = await client.patch('/engines/1/', headers=headers, data=ujson.dumps(body))
        obj['name'] = 'test2'

        assert resp.status == 200
        assert await resp.json() ==  obj

    async def test_patch_with_invalid_update_object_configuration(self, init_db, headers, client):
        body = {
            'objects': [{'id': 1, '_operation': 'update', 'configuration': {}}]
        }
        client = await client
        resp = await client.patch('/engines/1/', headers=headers, data=ujson.dumps(body))
        assert resp.status == 400
        assert await resp.json() ==  {
            'message': "'days_interval' is a required property. "\
                       "Failed validating instance['top_seller_array'] for "\
                       "schema['properties']['top_seller_array']['required']",
            'schema': {
                'required': ['days_interval'],
                'additionalProperties': False,
                'properties': {
                    'days_interval': {'type': 'integer'}
                }
            }
        }


class TestEnginesModelUriTemplateDelete(object):

    async def test_delete_with_body(self, init_db, headers, client):
        client = await client
        resp = await client.delete('/engines/1/', headers=headers, data='{}')
        assert resp.status == 400
        assert await resp.json() == {'message': 'Request body is not acceptable'}

    async def test_delete(self, init_db, headers, headers_without_content_type, client):
        client = await client
        resp = await client.get('/engines/1/', headers=headers_without_content_type)
        assert resp.status == 200

        resp = await client.delete('/engines/1/', headers=headers_without_content_type)
        assert resp.status == 204

        resp = await client.get('/engines/1/', headers=headers_without_content_type)
        assert resp.status == 404


class TestEnginesModelUriTemplateGet(object):

    async def test_get_with_body(self, init_db, headers, client):
        client = await client
        resp = await client.get('/engines/1/', headers=headers, data='{}')
        assert resp.status == 400
        assert await resp.json() == {'message': 'Request body is not acceptable'}

    async def test_get_not_found(self, init_db, headers_without_content_type, client):
        client = await client
        resp = await client.get('/engines/2/', headers=headers_without_content_type)
        assert resp.status == 404

    async def test_get(self, init_db, headers, headers_without_content_type, client):
        client = await client
        resp = await client.get('/engines/1/', headers=headers_without_content_type)
        body = [{
            'name': 'Seven Days Top Seller',
            'store_id': 1,
            'item_type_id': 1,
            'strategy_id': 1
        }]
        body[0]['id'] = 1
        body[0]['store'] = {'id': 1, 'name': 'test', 'country': 'test', 'configuration': {}}
        body[0]['strategy_id'] = 1
        body[0]['item_type'] = {
            'id': 1,
            'store_items_class': None,
            'stores': [body[0]['store']],
            'name': 'products',
            'schema': {
                'type': 'object',
                'id_names': ['sku'],
                'properties': {'sku': {'type': 'string'}}
            },
            'available_filters': [{'name': 'sku', 'schema': {'type': 'string'}}]
        }
        body[0]['strategy'] = {
            'id': 1,
            'name': 'test',
            'class_module': 'tests.integration.fixtures',
            'class_name': 'EngineStrategyTest',
            'object_types': ['top_seller_array']
        }
        body[0]['objects'] = [{
            'id': 1,
            'store_id': 1,
            'item_type_id': 1,
            'strategy_id': 1,
            'name': 'Top Seller Object',
            'type': 'top_seller_array',
            'configuration': {'days_interval': 7},
            'item_type': body[0]['item_type'],
            'store': body[0]['store'],
            'strategy': body[0]['strategy']
        }]
        body[0]['variables'] = []

        assert resp.status == 200
        assert await resp.json() == body[0]
