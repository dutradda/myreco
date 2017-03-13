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


import pytest
import ujson
import asyncio


@pytest.fixture
def init_db(models, session):
    user = {
        'name': 'test',
        'email': 'test',
        'password': 'test',
        'admin': True
    }
    asyncio.run_coroutine_threadsafe(models['users'].insert(session, user), session.loop)


class TestEngineCoresModelPost(object):

   async def test_post_without_body(self, init_db, client, headers):
        client = await client
        resp = await client.post('/engine_cores/', headers=headers)
        assert resp.status == 400
        assert (await resp.json()) == {'message': 'Request body is missing'}

   async def test_post_with_invalid_body(self, init_db, client, headers):
        client = await client
        resp = await client.post('/engine_cores/', headers=headers, data='[{}]')
        assert resp.status == 400
        assert (await resp.json()) ==  {
            'message': "'name' is a required property. "\
                       "Failed validating instance['0'] for schema['items']['required']",
            'schema': {
                'type': 'object',
                'additionalProperties': False,
                'required': ['name', 'strategy_class'],
                'properties': {
                    'name': {'type': 'string'},
                    'strategy_class': {'$ref': '#/definitions/EngineCoresModel.strategy_class'}
                }
            }
        }

   async def test_post_with_invalid_grant(self, init_db, client, headers):
        client = await client
        body = [{
            'name': 'test',
            'strategy_class': {
                'module': 'tests.integration.fixtures',
                'class_name': 'EngineCoreTest'
            }
        }]
        resp = await client.post('/engine_cores/', headers={'Authorization': 'invalid'}, data=ujson.dumps(body))
        assert resp.status == 401
        assert (await resp.json()) ==  {'message': 'Invalid authorization'}

   async def test_post_with_invalid_module(self, init_db, client, headers):
        client = await client
        body = [{
            'name': 'test',
            'strategy_class': {
                'module': 'invalid',
                'class_name': 'EngineCoreTest'
            }
        }]
        resp = await client.post('/engine_cores/', headers=headers, data=ujson.dumps(body))

        assert resp.status == 400
        assert (await resp.json()) ==  {
            'instance': {},
            'message': "Error loading module 'invalid.EngineCoreTest'."\
                       "\nError Class: ImportError. Error Message: No module named 'invalid'"
        }

   async def test_post_with_invalid_class(self, init_db, client, headers):
        client = await client
        body = [{
            'name': 'test',
            'strategy_class': {
                'module': 'tests.integration.fixtures',
                'class_name': 'invalid'
            }
        }]
        resp = await client.post('/engine_cores/', headers=headers, data=ujson.dumps(body))

        assert resp.status == 400
        assert (await resp.json()) ==  {
            'instance': {},
            'message': "Error loading module 'tests.integration.fixtures.invalid'."\
                       "\nError Class: AttributeError. "\
                       "Error Message: module 'tests.integration.fixtures' has no attribute 'invalid'"
        }

   async def test_post(self, init_db, client, headers):
        client = await client
        body = [{
            'name': 'test',
            'strategy_class': {
                'module': 'tests.integration.fixtures',
                'class_name': 'EngineCoreTest'
            }
        }]
        resp = await client.post('/engine_cores/', headers=headers, data=ujson.dumps(body))
        body[0]['id'] = 1

        assert resp.status == 201
        assert (await resp.json()) ==  body


class TestEngineCoresModelGet(object):

   async def test_get_not_found(self, init_db, client, headers_without_content_type):
        client = await client
        resp = await client.get('/engine_cores/', headers=headers_without_content_type)
        assert resp.status == 404

   async def test_get_invalid_with_body(self, init_db, client, headers):
        client = await client
        resp = await client.get('/engine_cores/', headers=headers, data='{}')
        assert resp.status == 400
        assert (await resp.json()) == {'message': 'Request body is not acceptable'}

   async def test_get(self, init_db, client, headers, headers_without_content_type):
        client = await client
        body = [{
            'name': 'test',
            'strategy_class': {
                'module': 'tests.integration.fixtures',
                'class_name': 'EngineCoreTest'
            }
        }]
        await client.post('/engine_cores/', headers=headers, data=ujson.dumps(body))
        body[0]['id'] = 1

        resp = await client.get('/engine_cores/', headers=headers_without_content_type)
        assert resp.status == 200
        assert (await resp.json()) ==  body


class TestEngineCoresModelUriTemplatePatch(object):

   async def test_patch_without_body(self, init_db, client, headers):
        client = await client
        resp = await client.patch('/engine_cores/1/', headers=headers, data='')
        assert resp.status == 400
        assert (await resp.json()) == {'message': 'Request body is missing'}

   async def test_patch_with_invalid_body(self, init_db, client, headers):
        client = await client
        resp = await client.patch('/engine_cores/1/', headers=headers, data='{}')
        assert resp.status == 400
        assert (await resp.json()) ==  {
            'message': '{} does not have enough properties. '\
                       "Failed validating instance for schema['minProperties']",
            'schema': {
                'type': 'object',
                'additionalProperties': False,
                'minProperties': 1,
                'properties': {
                    'name': {'type': 'string'},
                    'strategy_class': {'$ref': '#/definitions/EngineCoresModel.strategy_class'}
                }
            }
        }

   async def test_patch_not_found(self, init_db, client, headers):
        client = await client
        body = {
            'name': 'test',
            'strategy_class': {
                'module': 'tests.integration.fixtures',
                'class_name': 'EngineCoreTest'
            }
        }
        resp = await client.patch('/engine_cores/1/', headers=headers, data=ujson.dumps(body))
        assert resp.status == 404

   async def test_patch(self, init_db, client, headers):
        client = await client
        body = [{
            'name': 'test',
            'strategy_class': {
                'module': 'tests.integration.fixtures',
                'class_name': 'EngineCoreTest'
            }
        }]
        resp = await client.post('/engine_cores/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        body = {
            'name': 'test2'
        }
        resp = await client.patch('/engine_cores/1/', headers=headers, data=ujson.dumps(body))
        obj['name'] = 'test2'

        assert resp.status == 200
        assert (await resp.json()) ==  obj


class TestEngineCoresModelUriTemplateDelete(object):

   async def test_delete_with_body(self, init_db, client, headers):
        client = await client
        resp = await client.delete('/engine_cores/1/', headers=headers, data='{}')
        assert resp.status == 400
        assert (await resp.json()) == {'message': 'Request body is not acceptable'}

   async def test_delete_valid(self, init_db, client, headers, headers_without_content_type):
        client = await client
        body = [{
            'name': 'test',
            'strategy_class': {
                'module': 'tests.integration.fixtures',
                'class_name': 'EngineCoreTest'
            }
        }]
        resp = await client.post('/engine_cores/', headers=headers, data=ujson.dumps(body))

        resp = await client.get('/engine_cores/1/', headers=headers_without_content_type)
        assert resp.status == 200

        resp = await client.delete('/engine_cores/1/', headers=headers_without_content_type)
        assert resp.status == 204

        resp = await client.get('/engine_cores/1/', headers=headers_without_content_type)
        assert resp.status == 404
