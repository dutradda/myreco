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
    SQLAlchemyRedisModelBase, StoresModel, UsersModel,
    GrantsModel, URIsModel, MethodsModel)
from falconswagger.swagger_api import SwaggerAPI
from falconswagger.utils import get_model_schema
from fakeredis import FakeStrictRedis
from unittest import mock
from base64 import b64encode
from pytest_falcon.plugin import Client

import pytest
import json
import os.path


@pytest.fixture
def model_base():
    return SQLAlchemyRedisModelBase


@pytest.fixture
def init_(session):
    uris = [{'uri': '/test2'}, {'uri': '/test3'}, {'uri': '/users/test'}]
    URIsModel.insert(session, uris[0])
    URIsModel.insert(session, uris[1])
    URIsModel.insert(session, uris[2])

    user = {
        'name': 'test',
        'email': 'test',
        'password': 'test',
        'admin': True
    }
    UsersModel.insert(session, user)

    grants = [{
        'uri': {'uri': '/test', '_operation': 'insert'},
        'method': {'method': 'post', '_operation': 'insert'}
    }]
    GrantsModel.insert(session, grants)

    methods = [{'method': 'put'}]
    MethodsModel.insert(session, methods)

    grants = [{
        'uri_id': 3,
        'method_id': 3
    }]
    GrantsModel.insert(session, grants)


@pytest.fixture
def app(session, init_):
    return SwaggerAPI([UsersModel, StoresModel], session.bind, FakeStrictRedis(),
                      title='Myreco API')


@pytest.fixture
def headers():
    return {
        'Authorization': b64encode('test:test'.encode()).decode()
    }


class TestUsersResourcePost(object):
    def test_post_valid_grants_update(self, client, headers, session):
        URIsModel.insert(session, {'uri': '/users/test2'})
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
        resp = client.post('/users', data=json.dumps(user), headers=headers)

        assert resp.status_code == 201
        assert json.loads(resp.body) == [{
            'id': 'test2:test',
            'name': 'test2',
            'email': 'test2',
            'password': 'test',
            'grants': [{
                'method_id': 3,
                'uri_id': 2,
                'method': {'id': 3, 'method': 'post'},
                'uri': {'id': 2, 'uri': '/test3'}
            },{
                'method_id': 1,
                'uri_id': 5,
                'method': {'id': 1, 'method': 'patch'},
                'uri': {'id': 5, 'uri': '/users/test2'}
            },{
                'method_id': 2,
                'uri_id': 5,
                'method': {'id': 2, 'method': 'get'},
                'uri': {'id': 5, 'uri': '/users/test2'}
            }],
            'stores': [],
            'admin': False
        }]

    def test_post_valid_with_grants_insert_and_uri_and_method_update(
            self, client, headers, session):
        GrantsModel.insert(session, {'uri': {'uri': '/users/test2', '_operation': 'insert'}, 'method_id': 1})
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
        resp = client.post('/users', data=json.dumps(user), headers=headers)

        assert resp.status_code == 201
        assert json.loads(resp.body) == [{
            'id': 'test2:test',
            'name': 'test2',
            'email': 'test2',
            'password': 'test',
            'grants': [{
                'method_id': 1,
                'uri_id': 2,
                'method': {'id': 1, 'method': 'patch'},
                'uri': {'id': 2, 'uri': '/test3'}
            },{
                'uri_id': 5,
                'method': {'id': 1, 'method': 'patch'},
                'method_id': 1,
                'uri': {'id': 5, 'uri': '/users/test2'}
            },{
                'uri_id': 5,
                'method': {'id': 2, 'method': 'get'},
                'method_id': 2,
                'uri': {'id': 5, 'uri': '/users/test2'}
            }],
            'stores': [],
            'admin': False
        }]

    def test_post_valid_with_grants_uri_and_method_insert(
            self, client, headers):
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
        resp = client.post('/users', data=json.dumps(user), headers=headers)

        assert resp.status_code == 201
        assert json.loads(resp.body) == [{
            'id': 'test2:test',
            'name': 'test2',
            'email': 'test2',
            'password': 'test',
            'grants': [{
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
                'method_id': 5,
                'uri_id': 6,
                'method': {'id': 5, 'method': 'delete'},
                'uri': {'id': 6, 'uri': '/test4'}
            }],
            'stores': [],
            'admin': False
        }]

    def test_post_invalid_json(self, client, headers, root_path):
        user = [{
            'name': 'test2',
            'email': 'test2',
            'password': 'test',
            'grants': [{
                'test': 1,
                'method_id': 1
            }]
        }]
        resp = client.post('/users', data=json.dumps(user), headers=headers)

        assert resp.status_code == 400
        result = json.loads(resp.body)
        message = result['error'].pop('message')
        expected_schema = get_model_schema(root_path + '/../myreco/users/models.py')

        assert message == \
                "{'method_id': 1, 'test': 1} is not valid under any of the given schemas" \
            or message == \
                "{'test': 1, 'method_id': 1} is not valid under any of the given schemas"
        assert result == {
            'error': {
                'input': {'method_id': 1, 'test': 1},
                'schema': expected_schema['definitions']['grants']
            }
        }

    def test_post_with_invalid_grant(self, client):
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
        resp = client.post('/users', headers={'Authorization': 'invalid'}, body=json.dumps(body))
        assert resp.status_code == 401
        assert json.loads(resp.body) ==  {'error': 'Invalid authorization'}


class TestUsersResourcePutInsert(object):
    def test_put_with_ambiguous_ids(self, client, headers, root_path):
        user = {
            'name': 'test2',
            'email': 'test22',
            'password': 'test',
            'grants': [{
                'uri_id': 1,
                'method_id': 1,
                '_operation': 'update'
            }]
        }
        resp = client.put('/users/test2', body=json.dumps(user), headers=headers)
        expected_schema = get_model_schema(root_path + '/../myreco/users/models.py')
        expected_schema = {
            'definitions': expected_schema['definitions'],
            '$ref': '#/definitions/schema_with_grants'
        }

        assert resp.status_code == 400
        assert json.loads(resp.body) == {
            'error': {
                'schema': expected_schema,
                'input': {
                    'body': {
                        'grants': [{
                            'method_id': 1,
                            'uri_id': 1,
                            '_operation': 'update'
                        }],
                    'name': 'test2',
                    'email': 'test22',
                    'password': 'test'
                },
                'uri': {'email': 'test2'}
            },
            'message': "Ambiguous value for 'email'"}
        }

    def test_put_with_insert_and_grants_update(self, client, headers):
        user = {
            'name': 'test2',
            'email': 'test2',
            'password': 'test',
            'grants': [{
                'uri_id': 1,
                'method_id': 1,
                '_operation': 'insert'
            }]
        }
        resp = client.put('/users/test2', body=json.dumps(user), headers=headers)

        assert resp.status_code == 201
        assert json.loads(resp.body) == {
            'id': 'test2:test',
            'name': 'test2',
            'email': 'test2',
            'password': 'test',
            'stores': [],
            'admin': False,
            'grants': [{
                'method_id': 1,
                'uri_id': 1,
                'method': {'id': 1, 'method': 'patch'},
                'uri': {'id': 1, 'uri': '/test2'}
            },{
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

    def test_put_with_insert_and_grants_insert(self, client, headers):
        user = {
            'name': 'test2',
            'email': 'test2',
            'password': 'test',
            'grants': [{
                'uri': {'uri': '/test4', '_operation': 'insert'},
                'method': {'method': 'delete', '_operation': 'insert'},
                '_operation': 'insert'
            }]
        }
        resp = client.put('/users/test2', body=json.dumps(user), headers=headers)

        assert resp.status_code == 201
        assert json.loads(resp.body) == {
            'id': 'test2:test',
            'name': 'test2',
            'email': 'test2',
            'password': 'test',
            'stores': [],
            'admin': False,
            'grants': [{
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
                'method_id': 5,
                'uri_id': 6,
                'method': {'id': 5, 'method': 'delete'},
                'uri': {'id': 6, 'uri': '/test4'}
            }]
        }


class TestUsersResourcePutUpdateOne(object):
    def test_put_update_and_grants_update(self, client, headers):
        user = {
            'name': 'test2',
            'password': 'test',
            'email': 'test2',
            'grants': [{
                'uri_id': 1,
                'method_id': 1,
                '_operation': 'insert'
            }]
        }
        resp = client.put('/users/test2', body=json.dumps(user), headers=headers)
        assert resp.status_code == 201

        user = {
            'name': 'test2_updated',
            'email': 'test2',
            'password': 'test_updated',
            'grants': [{
                'uri_id': 3,
                'method_id': 4,
                '_operation': 'insert'
            }]
        }
        resp = client.put('/users/test2', body=json.dumps(user), headers=headers)

        assert resp.status_code == 200
        assert json.loads(resp.body) == {
            'id': 'test2:test_updated',
            'name': 'test2_updated',
            'email': 'test2',
            'password': 'test_updated',
            'stores': [],
            'admin': False,
            'grants': [{
                'method_id': 1,
                'uri_id': 1,
                'method': {'id': 1, 'method': 'patch'},
                'uri': {'id': 1, 'uri': '/test2'}
            },{
                'method_id': 4,
                'uri_id': 3,
                'method': {'id': 4, 'method': 'put'},
                'uri': {'id': 3, 'uri': '/users/test'}
            },{
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

    def test_put_update_and_grants_remove(self, client, headers):
        user = {
            'name': 'test2',
            'email': 'test2',
            'password': 'test',
            'grants': [{
                'uri_id': 1,
                'method_id': 1,
                '_operation': 'insert'
            }]
        }
        resp = client.put('/users/test2', body=json.dumps(user), headers=headers)
        assert resp.status_code == 201

        user = {
            'name': 'test2_updated',
            'email': 'test2',
            'password': 'test_updated',
            'grants': [{
                'uri_id': 1,
                'method_id': 1,
                '_operation': 'remove'
            }]
        }
        resp = client.put('/users/test2', body=json.dumps(user), headers=headers)

        assert resp.status_code == 200
        assert json.loads(resp.body) == {
            'id': 'test2:test_updated',
            'name': 'test2_updated',
            'email': 'test2',
            'password': 'test_updated',
            'stores': [],
            'grants': [{
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
            }],
            'admin': False
        }

    def test_put_update_and_grants_update_and_grants_remove(self, client, headers):
        user = {
            'name': 'test2',
            'email': 'test2',
            'password': 'test',
            'grants': [{
                'uri_id': 1,
                'method_id': 1,
                '_operation': 'insert'
            }]
        }
        resp = client.put('/users/test2', body=json.dumps(user), headers=headers)
        assert resp.status_code == 201

        user = {
            'name': 'test2_updated',
            'email': 'test2',
            'password': 'test_updated',
            'grants': [{
                'uri_id': 3,
                'method_id': 3,
                '_operation': 'update'
            },{
                'uri_id': 1,
                'method_id': 1,
                '_operation': 'remove'
            }]
        }
        resp = client.put('/users/test2', body=json.dumps(user), headers=headers)

        assert resp.status_code == 200
        assert json.loads(resp.body) == {
            'id': 'test2:test_updated',
            'name': 'test2_updated',
            'email': 'test2',
            'password': 'test_updated',
            'stores': [],
            'admin': False,
            'grants': [{
                'method_id': 3,
                'uri_id': 3,
                'method': {'id': 3, 'method': 'post'},
                'uri': {'id': 3, 'uri': '/users/test'}
            },{
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

    def test_put_update_primary_key_with_redis(self, session, init_, headers):
        redis = mock.MagicMock()
        redis.hmget.return_value = [None]
        client = Client(SwaggerAPI([UsersModel], session.bind, redis, title='Myreco API'))
        user = {
            'name': 'test2',
            'password': 'test',
            'email': 'test2',
            'grants': [{
                'uri_id': 1,
                'method_id': 1,
                '_operation': 'insert'
            }]
        }
        resp = client.put('/users/test2', body=json.dumps(user), headers=headers)
        assert resp.status_code == 201

        user = {
            'name': 'test2_updated',
            'email': 'test2',
            'password': 'test_updated',
            'grants': [{
                'uri_id': 1,
                'method_id': 1,
                '_operation': 'update'
            }]
        }
        session.redis_bind = mock.MagicMock()
        redis.smembers.return_value = {'users'.encode()}
        resp = client.put('/users/test2', body=json.dumps(user), headers=headers)

        assert resp.status_code == 200
        assert json.loads(resp.body) == {
            'id': 'test2:test_updated',
            'name': 'test2_updated',
            'email': 'test2',
            'password': 'test_updated',
            'stores': [],
            'admin': False,
            'grants': [{
                'method_id': 1,
                'uri_id': 1,
                'method': {'id': 1, 'method': 'patch'},
                'uri': {'id': 1, 'uri': '/test2'}
            },{
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
        assert redis.hdel.call_args_list == [mock.call('users', b'test2:test')]


class TestUsersResourcePutUpdateMany(object):
    def test_put_update_and_grants_update(self, client, headers):
        user = {
            'name': 'test2',
            'email': 'test2',
            'password': 'test',
            'grants': [{
                'uri_id': 1,
                'method_id': 1,
                '_operation': 'insert'
            }]
        }
        resp = client.put('/users/test2', body=json.dumps(user), headers=headers)
        assert resp.status_code == 201

        users = [{
            'id': 'test2:test',
            'name': 'test2_updated',
            'email': 'test2_updated',
            'password': 'test_updated',
            'grants': [{
                'uri_id': 3,
                'method_id': 3,
                '_operation': 'update'
            }]
        }]
        resp = client.put('/users', body=json.dumps(users), headers=headers)

        assert resp.status_code == 200
        assert json.loads(resp.body) == [{
            'id': 'test2_updated:test_updated',
            'name': 'test2_updated',
            'email': 'test2_updated',
            'password': 'test_updated',
            'stores': [],
            'admin': False,
            'grants': [{
                'method_id': 1,
                'uri_id': 1,
                'method': {'id': 1, 'method': 'patch'},
                'uri': {'id': 1, 'uri': '/test2'}
            },{
                'method_id': 3,
                'uri_id': 3,
                'method': {'id': 3, 'method': 'post'},
                'uri': {'id': 3, 'uri': '/users/test'}
            },{
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

    def test_put_update_and_grants_remove(self, client, headers):
        user = {
            'name': 'test2',
            'email': 'test2',
            'password': 'test',
            'grants': [{
                'uri_id': 1,
                'method_id': 1,
                '_operation': 'insert'
            }]
        }
        resp = client.put('/users/test2', body=json.dumps(user), headers=headers)
        assert resp.status_code == 201

        users = [{
            'id': 'test2:test',
            'name': 'test2_updated',
            'email': 'test2_updated',
            'password': 'test_updated',
            'grants': [{
                'uri_id': 1,
                'method_id': 1,
                '_operation': 'remove'
            }]
        }]
        resp = client.put('/users', body=json.dumps(users), headers=headers)

        assert resp.status_code == 200
        assert json.loads(resp.body) == [{
            'id': 'test2_updated:test_updated',
            'name': 'test2_updated',
            'email': 'test2_updated',
            'password': 'test_updated',
            'stores': [],
            'grants': [{
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
            }],
            'admin': False
        }]

    def test_put_update_and_grants_update_and_grants_remove(self, client, headers):
        user = {
            'name': 'test2',
            'email': 'test2',
            'password': 'test',
            'grants': [{
                'uri_id': 1,
                'method_id': 1,
                '_operation': 'insert'
            }]
        }
        resp = client.put('/users/test2', body=json.dumps(user), headers=headers)
        assert resp.status_code == 201

        users = [{
            'id': 'test2:test',
            'name': 'test2_updated',
            'email': 'test2_updated',
            'password': 'test_updated',
            'grants': [{
                'uri_id': 3,
                'method_id': 3,
                '_operation': 'update'
            },{
                'uri_id': 1,
                'method_id': 1,
                '_operation': 'remove'
            }]
        }]
        resp = client.put('/users', body=json.dumps(users), headers=headers)

        assert resp.status_code == 200
        assert json.loads(resp.body) == [{
            'id': 'test2_updated:test_updated',
            'name': 'test2_updated',
            'email': 'test2_updated',
            'password': 'test_updated',
            'stores': [],
            'admin': False,
            'grants': [{
                'method_id': 3,
                'uri_id': 3,
                'method': {'id': 3, 'method': 'post'},
                'uri': {'id': 3, 'uri': '/users/test'}
            },{
                'uri_id': 5,
                'method': {'id': 1, 'method': 'patch'},
                'method_id': 1,
                'uri': {'id': 5, 'uri': '/users/test2'}
            },{
                'uri_id': 5,
                'method': {'id': 2, 'method': 'get'},
                'method_id': 2,
                'uri': {'id': 5, 'uri': '/users/test2'}
            }]
        }]


class TestUsersResourcePatchOne(object):
    def test_patch_one_property(self, client, headers):
        user = [{
            'name': 'test2',
            'email': 'test2',
            'password': 'test'
        }]
        resp = client.post('/users', body=json.dumps(user), headers=headers)
        assert resp.status_code == 201

        users = {'name': 'test2_updated'}
        resp = client.patch('/users/test2', body=json.dumps(users), headers=headers)

        assert resp.status_code == 200
        assert json.loads(resp.body) == {
            'id': 'test2:test',
            'name': 'test2_updated',
            'email': 'test2',
            'password': 'test',
            'stores': [],
            'admin': False,
            'grants': [{
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

    def test_patch_two_properties(self, client, headers):
        user = [{
            'name': 'test2',
            'email': 'test2',
            'password': 'test'
        }]
        resp = client.post('/users', body=json.dumps(user), headers=headers)
        assert resp.status_code == 201

        users = {
            'email': 'test22',
            'password': 'test2'
        }
        resp = client.patch('/users/test2', body=json.dumps(users), headers=headers)

        assert resp.status_code == 200
        assert json.loads(resp.body) == {
            'id': 'test22:test2',
            'name': 'test2',
            'email': 'test22',
            'password': 'test2',
            'stores': [],
            'admin': False,
            'grants': [{
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


class TestUsersResourcePatchMany(object):
    def test_patch_one_property(self, client, headers):
        user = [{
            'name': 'test2',
            'email': 'test2',
            'password': 'test'
        }]
        resp = client.post('/users', body=json.dumps(user), headers=headers)
        assert resp.status_code == 201

        users = [{'email': 'test2', 'name': 'test2_updated'}]
        resp = client.patch('/users', body=json.dumps(users), headers=headers)

        assert resp.status_code == 200
        assert json.loads(resp.body) == [{
            'id': 'test2:test',
            'name': 'test2_updated',
            'email': 'test2',
            'password': 'test',
            'stores': [],
            'admin': False,
            'grants': [{
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

    def test_patch_two_properties(self, client, headers):
        user = [{
            'name': 'test2',
            'email': 'test2',
            'password': 'test'
        }]
        resp = client.post('/users', body=json.dumps(user), headers=headers)
        assert resp.status_code == 201

        users = {
            'email': 'test2',
            'name': 'test22',
            'password': 'test2'
        }
        resp = client.patch('/users/test2', body=json.dumps(users), headers=headers)

        assert resp.status_code == 200
        assert json.loads(resp.body) == {
            'id': 'test2:test2',
            'name': 'test22',
            'email': 'test2',
            'password': 'test2',
            'stores': [],
            'admin': False,
            'grants': [{
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


class TestUsersResourceDeleteGet(object):
    def test_delete_one(self, client, headers):
        user = [{
            'name': 'test2',
            'email': 'test2',
            'password': 'test'
        }]
        resp = client.post('/users', body=json.dumps(user), headers=headers)
        assert resp.status_code == 201

        resp = client.get('/users/test2', body='', headers=headers)
        assert resp.status_code == 200

        resp = client.delete('/users/test2', headers=headers)
        assert resp.status_code == 204

        resp = client.get('/users/test2', headers=headers)
        assert resp.status_code == 404

    def test_delete_many(self, client, headers):
        store = [{
            'name': 'test',
            'country': 'test',
            'configuration': {'data_path': '/test'}
        }]
        resp = client.post('/stores', body=json.dumps(store), headers=headers)
        assert resp.status_code == 201

        user = [{
            'name': 'test2',
            'email': 'test2',
            'password': 'test',
            'stores': [{'id':1}]
        }]
        resp = client.post('/users', body=json.dumps(user), headers=headers)
        assert resp.status_code == 201

        resp = client.get('/users?stores=id:1', body=json.dumps([{'email': 'test2'}]), headers=headers)
        assert resp.status_code == 200

        resp = client.delete('/users', body=json.dumps([{'email': 'test2'}]), headers=headers)
        assert resp.status_code == 204

        resp = client.get('/users?stores=id:1', body=json.dumps([{'email': 'test2'}]), headers=headers)
        assert resp.status_code == 404
