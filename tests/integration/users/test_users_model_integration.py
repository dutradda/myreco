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


from swaggerit.utils import get_swagger_json
from unittest import mock
import pytest
import ujson
import os.path
import asyncio


@pytest.fixture
def init_db(models, session):
    uris = [{'uri': '/test2'}, {'uri': '/test3'}, {'uri': '/users/test'}]
    asyncio.run_coroutine_threadsafe(models['uris'].insert(session, uris[0]), session.loop)
    asyncio.run_coroutine_threadsafe(models['uris'].insert(session, uris[1]), session.loop)
    asyncio.run_coroutine_threadsafe(models['uris'].insert(session, uris[2]), session.loop)

    user = {
        'name': 'test',
        'email': 'test',
        'password': 'test',
        'admin': True
    }
    asyncio.run_coroutine_threadsafe(models['users'].insert(session, user), session.loop)

    grants = [{
        'uri': {'uri': '/test', '_operation': 'insert'},
        'method': {'method': 'post', '_operation': 'insert'}
    }]
    asyncio.run_coroutine_threadsafe(models['grants'].insert(session, grants), session.loop)

    methods = [{'method': 'put'}]
    asyncio.run_coroutine_threadsafe(models['methods'].insert(session, methods), session.loop)

    grants = [{
        'uri_id': 3,
        'method_id': 3
    }]
    asyncio.run_coroutine_threadsafe(models['grants'].insert(session, grants), session.loop)


class TestUsersModelPost(object):
   async def test_post_valid_grants_update(self, init_db, models, headers, session, client):
        client = await client
        await models['uris'].insert(session, {'uri': '/users/test2'})
        user = [{
            'name': 'test2',
            'email': 'test2',
            'password': 'test',
            'grants': [{
                'uri_id': 2,
                'method_id': 3,
                '_operation': 'insert'
            }]
        }]
        resp = await client.post('/users', data=ujson.dumps(user), headers=headers)

        assert resp.status == 201
        assert (await resp.json()) == [{
            'id': 'test2:test',
            'name': 'test2',
            'email': 'test2',
            'password': 'test',
            'grants': [{
                'id': 5,
                'method_id': 3,
                'uri_id': 2,
                'method': {'id': 3, 'method': 'post'},
                'uri': {'id': 2, 'uri': '/test3'}
            },{
                'id': 6,
                'method_id': 1,
                'uri_id': 5,
                'method': {'id': 1, 'method': 'patch'},
                'uri': {'id': 5, 'uri': '/users/test2'}
            },{
                'id': 7,
                'method_id': 2,
                'uri_id': 5,
                'method': {'id': 2, 'method': 'get'},
                'uri': {'id': 5, 'uri': '/users/test2'}
            }],
            'stores': [],
            'admin': False
        }]

   async def test_post_valid_with_grants_insert_and_uri_and_method_update(
            self, init_db, models, headers, session, client):
        client = await client
        models['grants'].insert(session, {'uri': {'uri': '/users/test2', '_operation': 'insert'}, 'method_id': 1})
        user = [{
            'name': 'test2',
            'email': 'test2',
            'password': 'test',
            'grants': [{
                'uri': {'id': 2},
                'method': {'id': 1},
                '_operation': 'insert'
            }]
        }]
        resp = await client.post('/users', data=ujson.dumps(user), headers=headers)

        assert resp.status == 201
        assert (await resp.json()) == [{
            'id': 'test2:test',
            'name': 'test2',
            'email': 'test2',
            'password': 'test',
            'grants': [{
                'id': 5,
                'method_id': 1,
                'uri_id': 2,
                'method': {'id': 1, 'method': 'patch'},
                'uri': {'id': 2, 'uri': '/test3'}
            },{
                'id': 6,
                'uri_id': 5,
                'method': {'id': 1, 'method': 'patch'},
                'method_id': 1,
                'uri': {'id': 5, 'uri': '/users/test2'}
            },{
                'id': 7,
                'uri_id': 5,
                'method': {'id': 2, 'method': 'get'},
                'method_id': 2,
                'uri': {'id': 5, 'uri': '/users/test2'}
            }],
            'stores': [],
            'admin': False
        }]

   async def test_post_valid_with_grants_uri_and_method_insert(
            self, init_db, headers, client):
        client = await client
        user = [{
            'name': 'test2',
            'email': 'test2',
            'password': 'test',
            'grants': [{
                'uri': {'uri': '/test4', '_operation': 'insert'},
                'method': {'method': 'delete','_operation': 'insert'},
                '_operation': 'insert'
            }]
        }]
        resp = await client.post('/users', data=ujson.dumps(user), headers=headers)

        assert resp.status == 201
        assert (await resp.json()) == [{
            'id': 'test2:test',
            'name': 'test2',
            'email': 'test2',
            'password': 'test',
            'grants': [{
                'id': 6,
                'uri_id': 5,
                'method': {
                    'id': 1,
                    'method': 'patch'
                },
                'method_id': 1,
                'uri': {
                    'id': 5,
                    'uri': '/users/test2'
                }
            },{
                'id': 7,
                'uri_id': 5,
                'method': {
                    'id': 2,
                    'method': 'get'
                },
                'method_id': 2,
                'uri': {
                    'id': 5,
                    'uri': '/users/test2'
                }
            },{
                'id': 5,
                'method_id': 5,
                'uri_id': 6,
                'method': {'id': 5, 'method': 'delete'},
                'uri': {'id': 6, 'uri': '/test4'}
            }],
            'stores': [],
            'admin': False
        }]

   async def test_post_invalid_json(self, init_db, headers, root_path, client):
        client = await client
        user = [{
            'name': 'test2',
            'email': 'test2',
            'password': 'test',
            'grants': [{
                'test': 1,
                'method_id': 1
            }]
        }]
        resp = await client.post('/users', data=ujson.dumps(user), headers=headers)

        assert resp.status == 400
        result = (await resp.json())
        message = result.pop('message')
        expected_schema = ujson.dumps(get_swagger_json(root_path + '/../myreco/users/models.py'),
                                      escape_forward_slashes=False)
        expected_schema = \
            expected_schema.replace('#/definitions/grants', '#/definitions/UsersModel.grants')\
            .replace('#/definitions/method', '#/definitions/UsersModel.method')\
            .replace('#/definitions/uri', '#/definitions/UsersModel.uri')
        expected_schema = ujson.loads(expected_schema)

        fail_msg = "Failed validating instance['0']['grants']['0'] "\
                   "for schema['items']['allOf']['1']['properties']['grants']['items']['oneOf']"
        assert message == \
                "{'method_id': 1, 'test': 1} is not valid under any of the given schemas. " + fail_msg \
            or message == \
                "{'test': 1, 'method_id': 1} is not valid under any of the given schemas. " + fail_msg
        assert result == {
            'instance': {'method_id': 1, 'test': 1},
            'schema': expected_schema['definitions']['grants']
        }

   async def test_post_with_invalid_grant(self, client):
        client = await client
        body = [{
            'name': 'test2',
            'email': 'test2',
            'password': 'test',
            'grants': [{
                'uri_id': 2,
                'method_id': 2,
                '_operation': 'insert'
            }]
        }]
        resp = await client.post('/users', headers={'Authorization': 'invalid'}, data=ujson.dumps(body))
        assert resp.status == 401
        assert (await resp.json()) ==  {'message': 'Invalid authorization'}


class TestUsersModelPatchOne(object):
   async def test_patch_one_property(self, init_db, client, headers):
        client = await client
        user = [{
            'name': 'test2',
            'email': 'test2',
            'password': 'test'
        }]
        resp = await client.post('/users', data=ujson.dumps(user), headers=headers)
        assert resp.status == 201

        users = {'name': 'test2_updated'}
        resp = await client.patch('/users/test2', data=ujson.dumps(users), headers=headers)

        assert resp.status == 200
        assert (await resp.json()) == {
            'id': 'test2:test',
            'name': 'test2_updated',
            'email': 'test2',
            'password': 'test',
            'stores': [],
            'admin': False,
            'grants': [{
                'id': 5,
                'uri_id': 5,
                'method': {
                    'id': 1,
                    'method': 'patch'
                },
                'method_id': 1,
                'uri': {
                    'id': 5,
                    'uri': '/users/test2'
                }
            },{
                'id': 6,
                'uri_id': 5,
                'method': {
                    'id': 2,
                    'method': 'get'
                },
                'method_id': 2,
                'uri': {
                    'id': 5,
                    'uri': '/users/test2'
                }
            }]
        }

   async def test_patch_two_properties(self, init_db, client, headers):
        client = await client
        user = [{
            'name': 'test2',
            'email': 'test2',
            'password': 'test'
        }]
        resp = await client.post('/users', data=ujson.dumps(user), headers=headers)
        assert resp.status == 201

        users = {
            'email': 'test22',
            'password': 'test2'
        }
        resp = await client.patch('/users/test2', data=ujson.dumps(users), headers=headers)

        assert resp.status == 200
        assert (await resp.json()) == {
            'id': 'test22:test2',
            'name': 'test2',
            'email': 'test22',
            'password': 'test2',
            'stores': [],
            'admin': False,
            'grants': [{
                'id': 5,
                'uri_id': 5,
                'method': {
                    'id': 1,
                    'method': 'patch'
                },
                'method_id': 1,
                'uri': {
                    'id': 5,
                    'uri': '/users/test2'
                }
            },{
                'id': 6,
                'uri_id': 5,
                'method': {
                    'id': 2,
                    'method': 'get'
                },
                'method_id': 2,
                'uri': {
                    'id': 5,
                    'uri': '/users/test2'
                }
            }]
        }


class TestUsersModelPatchMany(object):
   async def test_patch_one_property(self, init_db, client, headers):
        client = await client
        user = [{
            'name': 'test2',
            'email': 'test2',
            'password': 'test'
        }]
        resp = await client.post('/users', data=ujson.dumps(user), headers=headers)
        assert resp.status == 201

        users = [{'email': 'test2', 'name': 'test2_updated'}]
        resp = await client.patch('/users', data=ujson.dumps(users), headers=headers)

        assert resp.status == 200
        assert (await resp.json()) == [{
            'id': 'test2:test',
            'name': 'test2_updated',
            'email': 'test2',
            'password': 'test',
            'stores': [],
            'admin': False,
            'grants': [{
                'id': 5,
                'uri_id': 5,
                'method': {
                    'id': 1,
                    'method': 'patch'
                },
                'method_id': 1,
                'uri': {
                    'id': 5,
                    'uri': '/users/test2'
                }
            },{
                'id': 6,
                'uri_id': 5,
                'method': {
                    'id': 2,
                    'method': 'get'
                },
                'method_id': 2,
                'uri': {
                    'id': 5,
                    'uri': '/users/test2'
                }
            }]
        }]

   async def test_patch_two_properties(self, init_db, client, headers):
        client = await client
        user = [{
            'name': 'test2',
            'email': 'test2',
            'password': 'test'
        }]
        resp = await client.post('/users', data=ujson.dumps(user), headers=headers)
        assert resp.status == 201

        users = {
            'email': 'test2',
            'name': 'test22',
            'password': 'test2'
        }
        resp = await client.patch('/users/test2', data=ujson.dumps(users), headers=headers)

        assert resp.status == 200
        assert (await resp.json()) == {
            'id': 'test2:test2',
            'name': 'test22',
            'email': 'test2',
            'password': 'test2',
            'stores': [],
            'admin': False,
            'grants': [{
                'id': 5,
                'uri_id': 5,
                'method': {
                    'id': 1,
                    'method': 'patch'
                },
                'method_id': 1,
                'uri': {
                    'id': 5,
                    'uri': '/users/test2'
                }
            },{
                'id': 6,
                'uri_id': 5,
                'method': {
                    'id': 2,
                    'method': 'get'
                },
                'method_id': 2,
                'uri': {
                    'id': 5,
                    'uri': '/users/test2'
                }
            }]
        }


class TestUsersModelDeleteGet(object):
   async def test_delete_one(self, init_db, client, headers, headers_without_content_type):
        client = await client
        user = [{
            'name': 'test2',
            'email': 'test2',
            'password': 'test'
        }]
        resp = await client.post('/users', data=ujson.dumps(user), headers=headers)
        assert resp.status == 201

        resp = await client.get('/users/test2', headers=headers_without_content_type)
        assert resp.status == 200

        resp = await client.delete('/users/test2', headers=headers_without_content_type)
        assert resp.status == 204

        resp = await client.get('/users/test2', headers=headers_without_content_type)
        assert resp.status == 404

   async def test_delete_many(self, init_db, client, headers, headers_without_content_type):
        client = await client
        store = [{
            'name': 'test',
            'country': 'test',
            'configuration': {'data_path': '/test'}
        }]
        resp = await client.post('/stores', data=ujson.dumps(store), headers=headers)
        assert resp.status == 201

        user = [{
            'name': 'test2',
            'email': 'test2',
            'password': 'test',
            'stores': [{'id':1}]
        }]
        resp = await client.post('/users', data=ujson.dumps(user), headers=headers)
        assert resp.status == 201

        resp = await client.get('/users?stores=id:1', data=ujson.dumps([{'email': 'test2'}]), headers=headers)
        assert resp.status == 200

        resp = await client.delete('/users', data=ujson.dumps([{'email': 'test2'}]), headers=headers)
        assert resp.status == 204

        resp = await client.get('/users?stores=id:1', data=ujson.dumps([{'email': 'test2'}]), headers=headers)
        assert resp.status == 404
