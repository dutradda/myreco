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


@pytest.fixture
def init_db(models, session):
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


class TestExternalVariablesModelPost(object):

   async def test_post_without_body(self, init_db, client, headers):
        client = await client
        resp = await client.post('/external_variables/', headers=headers)
        assert resp.status == 400
        assert (await resp.json()) == {'message': 'Request body is missing'}

   async def test_post_with_invalid_body(self, init_db, client, headers):
        client = await client
        resp = await client.post('/external_variables/', headers=headers, data='[{}]')
        assert resp.status == 400
        assert (await resp.json()) ==  {
            'message': "'name' is a required property. "\
                       "Failed validating instance['0'] for schema['items']['required']",
            'schema': {
                'type': 'object',
                'additionalProperties': False,
                'required': ['name', 'store_id'],
                'properties': {
                    'name': {'type': 'string'},
                    'store_id': {'type': 'integer'}
                }
            }
        }

   async def test_post_with_invalid_grant(self, init_db, client, headers):
        client = await client
        body = [{
            'name': 'test',
            'store_id': 1
        }]
        resp = await client.post('/external_variables/', headers={'Authorization': 'invalid'}, data=ujson.dumps(body))
        assert resp.status == 401
        assert (await resp.json()) ==  {'message': 'Invalid authorization'}

   async def test_post(self, init_db, client, headers):
        client = await client
        body = [{
            'name': 'test',
            'store_id': 1
        }]
        resp = await client.post('/external_variables/', headers=headers, data=ujson.dumps(body))

        assert resp.status == 201

        body[0]['id'] = 1
        assert (await resp.json()) ==  body


class TestExternalVariablesModelGet(object):

   async def test_get_not_found(self, init_db, client, headers_without_content_type):
        client = await client
        resp = await client.get('/external_variables/?store_id=1', headers=headers_without_content_type)
        assert resp.status == 404

   async def test_get_invalid_with_body(self, init_db, client, headers):
        client = await client
        resp = await client.get('/external_variables/?store_id=1', headers=headers, data='{}')
        assert resp.status == 400
        assert (await resp.json()) == {'message': 'Request body is not acceptable'}

   async def test_get(self, init_db, client, headers, headers_without_content_type):
        client = await client
        body = [{
            'name': 'test',
            'store_id': 1
        }]
        await client.post('/external_variables/', headers=headers, data=ujson.dumps(body))

        resp = await client.get('/external_variables/?store_id=1', headers=headers_without_content_type)
        assert resp.status == 200

        body[0]['id'] = 1
        assert (await resp.json()) ==  body


class TestExternalVariablesModelUriTemplatePatch(object):

   async def test_patch_without_body(self, init_db, client, headers):
        client = await client
        resp = await client.patch('/external_variables/1/', headers=headers, data='')
        assert resp.status == 400
        assert (await resp.json()) == {'message': 'Request body is missing'}

   async def test_patch_with_invalid_body(self, init_db, client, headers):
        client = await client
        resp = await client.patch('/external_variables/1/', headers=headers, data='{}')
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
                    'store_id': {'type': 'integer'}
                }
            }
        }

   async def test_patch_not_found(self, init_db, client, headers):
        client = await client
        body = {
            'name': 'test',
            'store_id': 1
        }
        resp = await client.patch('/external_variables/1/', headers=headers, data=ujson.dumps(body))
        assert resp.status == 404

   async def test_patch(self, init_db, client, headers):
        client = await client
        body = [{
            'name': 'test',
            'store_id': 1
        }]
        resp = await client.post('/external_variables/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        body = {
            'name': 'test2'
        }
        resp = await client.patch('/external_variables/1/', headers=headers, data=ujson.dumps(body))
        obj['name'] = 'test2'
        obj['id'] = 1

        assert resp.status == 200
        assert (await resp.json()) ==  obj


class TestExternalVariablesModelUriTemplateDelete(object):

   async def test_delete_with_body(self, init_db, client, headers):
        client = await client
        resp = await client.delete('/external_variables/1/', headers=headers, data='{}')
        assert resp.status == 400
        assert (await resp.json()) == {'message': 'Request body is not acceptable'}

   async def test_delete_valid(self, init_db, client, headers, headers_without_content_type):
        client = await client
        body = [{
            'name': 'test',
            'store_id': 1
        }]
        resp = await client.post('/external_variables/', headers=headers, data=ujson.dumps(body))

        resp = await client.get('/external_variables/1/', headers=headers_without_content_type)
        assert resp.status == 200

        resp = await client.delete('/external_variables/1/', headers=headers_without_content_type)
        assert resp.status == 204

        resp = await client.get('/external_variables/1/', headers=headers_without_content_type)
        assert resp.status == 404


class TestExternalVariablesModelUriTemplateGet(object):

   async def test_get_with_body(self, init_db, client, headers):
        client = await client
        resp = await client.get('/external_variables/1/', headers=headers, data='{}')
        assert resp.status == 400
        assert (await resp.json()) == {'message': 'Request body is not acceptable'}

   async def test_get_not_found(self, init_db, client, headers_without_content_type):
        client = await client
        resp = await client.get('/external_variables/1/', headers=headers_without_content_type)
        assert resp.status == 404

   async def test_get(self, init_db, client, headers, headers_without_content_type):
        client = await client
        body = [{
            'name': 'test',
            'store_id': 1
        }]
        await client.post('/external_variables/', headers=headers, data=ujson.dumps(body))

        resp = await client.get('/external_variables/1/', headers=headers_without_content_type)
        assert resp.status == 200

        body[0]['id'] = 1
        assert (await resp.json()) == body[0]
