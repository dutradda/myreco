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
from falconswagger.http_api import HttpAPI
from tests.integration.fixtures_models import ItemsTypesModel
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

    return HttpAPI([ItemsTypesModel], session.bind, FakeStrictRedis())


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
                    'required': ['name', 'id_names', 'schema', 'store_id'],
                    'properties': {
                        'name': {'type': 'string'},
                        'store_id': {'type': 'integer'},
                        'id_names': {
                            'type': 'array',
                            'minItems': 1,
                            'items': {'type': 'string'}
                        },
                        'schema': {'$ref': 'http://json-schema.org/draft-04/schema#'}
                    }
                }
            }
        }

    def test_post(self, client, headers):
        body = [{
            'name': 'test',
            'store_id': 1,
            'id_names': ['id'],
            'schema': {'properties': {'id': {'type': 'integer'}}}
        }]
        resp = client.post('/items_types/', headers=headers, body=json.dumps(body))
        body[0]['id'] = 1
        body[0]['available_filters'] = [{'name': 'id', 'schema': {'type': 'integer'}}]

        assert resp.status_code == 201
        assert json.loads(resp.body) ==  body

    def test_post_with_invalid_grant(self, client):
        body = [{
            'name': 'test',
            'store_id': 1,
            'id_names': ['test'],
            'schema': {'properties': {'id': {'type': 'integer'}}}
        }]
        resp = client.post('/items_types/', headers={'Authorization': 'invalid'}, body=json.dumps(body))
        assert resp.status_code == 401
        assert json.loads(resp.body) ==  {'error': 'Invalid authorization'}

    def test_post_with_invalid_id_name(self, client, headers):
        body = [{
            'name': 'test',
            'store_id': 1,
            'id_names': ['id', 'id2'],
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
                'input': ['id', 'id2'],
                'message': "Invalid id_name 'id2'",
                'schema': {
                    'properties': {
                        'id': {'type': 'string'}
                    }
                }
            }
        }


class TestItemsTypesModelGet(object):

    def test_get_not_found(self, client, headers):
        resp = client.get('/items_types/', headers=headers)
        assert resp.status_code == 404

    def test_get_invalid_with_body(self, client, headers):
        resp = client.get('/items_types/', headers=headers, body='{}')
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is not acceptable'}

    def test_get(self, client, headers):
        body = [{
            'name': 'test',
            'store_id': 1,
            'id_names': ['test'],
            'schema': {'properties': {'test': {'type': 'string'}}}
        }]
        client.post('/items_types/', headers=headers, body=json.dumps(body))
        body[0]['id'] = 1
        body[0]['available_filters'] = [{'name': 'test', 'schema': {'type': 'string'}}]

        resp = client.get('/items_types/', headers=headers)
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
                        'store_id': {'type': 'integer'},
                        'id_names': {
                            'type': 'array',
                            'minItems': 1,
                            'items': {'type': 'string'}
                        },
                        'schema': {'$ref': 'http://json-schema.org/draft-04/schema#'}
                    }
                }
            }
        }

    def test_patch_not_found(self, client, headers):
        body = {
            'name': 'test',
            'store_id': 1,
            'id_names': ['test'],
            'schema': {'properties': {'test': {'type': 'string'}}}
        }
        resp = client.patch('/items_types/1/', headers=headers, body=json.dumps(body))
        assert resp.status_code == 404

    def test_patch(self, client, headers):
        body = [{
            'name': 'test',
            'store_id': 1,
            'id_names': ['test'],
            'schema': {'properties': {'test': {'type': 'string'}}}
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

    def test_patch_with_invalid_id_name(self, client, headers):
        body = [{
            'name': 'test',
            'store_id': 1,
            'id_names': ['id'],
            'schema': {
                'properties': {
                    'id': {'type': 'string'}
                }
            }
        }]
        client.post('/items_types/', headers=headers, body=json.dumps(body))

        body = {'id_names': ['id', 'id2']}
        resp = client.patch('/items_types/1/', headers=headers, body=json.dumps(body))
        assert resp.status_code == 400
        assert json.loads(resp.body) == {
            'error': {
                'input': ['id', 'id2'],
                'message': "Invalid id_name 'id2'",
                'schema': {
                    'properties': {
                        'id': {'type': 'string'}
                    }
                }
            }
        }


class TestItemsTypesModelUriTemplateDelete(object):

    def test_delete_with_body(self, client, headers):
        resp = client.delete('/items_types/1/', headers=headers, body='{}')
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is not acceptable'}

    def test_delete(self, client, headers):
        body = [{
            'name': 'test',
            'store_id': 1,
            'id_names': ['test'],
            'schema': {'properties': {'test': {'type': 'string'}}}
        }]
        client.post('/items_types/', headers=headers, body=json.dumps(body))

        resp = client.get('/items_types/1/', headers=headers)
        assert resp.status_code == 200

        resp = client.delete('/items_types/1/', headers=headers)
        assert resp.status_code == 204

        resp = client.get('/items_types/1/', headers=headers)
        assert resp.status_code == 404

    def test_if_delete_disassociate_model_correctly(self, client, headers):
        body = [{
            'name': 'test',
            'store_id': 1,
            'id_names': ['test'],
            'schema': {'properties': {'test': {'type': 'string'}}}
        }]
        client.post('/items_types/', headers=headers, body=json.dumps(body))

        resp = client.get('/test/_schema', headers=headers)
        assert resp.status_code == 200

        client.delete('/items_types/1/', headers=headers)

        resp = client.get('/test/_schema', headers=headers)
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
            'store_id': 1,
            'id_names': ['test'],
            'schema': {'properties': {'test': {'type': 'string'}}}
        }]
        client.post('/items_types/', headers=headers, body=json.dumps(body))
        resp = client.get('/items_types/1/', headers=headers)
        body[0]['id'] = 1
        body[0]['available_filters'] = [{'name': 'test', 'schema': {'type': 'string'}}]

        assert resp.status_code == 200
        assert json.loads(resp.body) == body[0]


class TestItemsModelSchema(object):

    def test_if_build_item_model_schema_correctly(self, client, headers):
        body = [{
            'name': 'test',
            'store_id': 1,
            'id_names': ['id'],
            'schema': {'properties': {'id': {'type': 'string'}}}
        }]
        client.post('/items_types/', headers=headers, body=json.dumps(body))

        resp = client.get('/test/_schema', headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) == {
            '/test': {
                'parameters': [{
                    'name': 'Authorization',
                    'in': 'header',
                    'required': True,
                    'type': 'string'
                }],
                'post': {
                    'parameters': [{
                        'name': 'body',
                        'in': 'body',
                        'required': True,
                        'schema': {
                            'type': 'array',
                            'minItems': 1,
                            'items': {'properties': {'id': {'type': 'string'}}}
                        }
                    }],
                    'operationId': 'post_by_body',
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
                                'properties': {
                                    'id': {'type': 'string'},
                                    '_operation': {'enum': ['delete', 'update']}
                                }
                            }
                        }
                    }],
                    'operationId': 'patch_by_body',
                    'responses': {'200': {'description': 'Updated'}}
                },
                'get': {
                    'parameters': [{
                        'name': 'page',
                        'in': 'query',
                        'type': 'integer'
                    },{
                        'name': 'items_per_page',
                        'in': 'query',
                        'type': 'integer'
                    },{
                        'name': 'id',
                        'in': 'query',
                        'type': 'string'
                    }],
                    'operationId': 'get_by_body',
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
                    'name': 'id',
                    'in': 'path',
                    'type': 'string',
                    'required': True
                }],
                'get': {
                    'operationId': 'get_by_uri_template',
                    'responses': {'200': {'description': 'Got'}}
                }
            }
        }

    def test_if_build_item_model_schema_correctly_with_two_id_names(
            self, client, headers):
        body = [{
            'name': 'test',
            'store_id': 1,
            'id_names': ['id', 'id2'],
            'schema': {
                'properties': {
                    'id': {'type': 'string'},
                    'id2': {'type': 'integer'}
                }
            }
        }]
        client.post('/items_types/', headers=headers, body=json.dumps(body))

        resp = client.get('/test/_schema', headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) == {
            '/test': {
                'parameters': [{
                    'name': 'Authorization',
                    'in': 'header',
                    'required': True,
                    'type': 'string'
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
                                'properties': {
                                    'id': {'type': 'string'},
                                    'id2': {'type': 'integer'}
                                }
                            }
                        }
                    }],
                    'operationId': 'post_by_body',
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
                                'properties': {
                                    'id': {'type': 'string'},
                                    'id2': {'type': 'integer'},
                                    '_operation': {'enum': ['delete', 'update']}
                                }
                            }
                        }
                    }],
                    'operationId': 'patch_by_body',
                    'responses': {'200': {'description': 'Updated'}}
                },
                'get': {
                    'parameters': [{
                        'name': 'page',
                        'in': 'query',
                        'type': 'integer'
                    },{
                        'name': 'items_per_page',
                        'in': 'query',
                        'type': 'integer'
                    },{
                        'name': 'id',
                        'in': 'query',
                        'type': 'string'
                    },{
                        'name': 'id2',
                        'in': 'query',
                        'type': 'integer'
                    }],
                    'operationId': 'get_by_body',
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
                    'operationId': 'get_by_uri_template',
                    'responses': {'200': {'description': 'Got'}}
                }
            }
        }


class TestItemsModelPost(object):

    def test_items_post_valid(self, client, headers):
        body = [{
            'name': 'test',
            'store_id': 1,
            'id_names': ['id'],
            'schema': {
                'properties': {
                    'id': {'type': 'string'}
                }
            }
        }]
        client.post('/items_types/', headers=headers, body=json.dumps(body))

        body = [{'id': 'test'}]
        resp = client.post('/test/', headers=headers, body=json.dumps(body))
        assert resp.status_code == 201
        assert json.loads(resp.body) == body

    def test_items_post_invalid(self, client, headers):
        body = [{
            'name': 'test',
            'store_id': 1,
            'id_names': ['id'],
            'schema': {
                'properties': {
                    'id': {'type': 'string'}
                }
            }
        }]
        client.post('/items_types/', headers=headers, body=json.dumps(body))

        body = [{'id': 1}]
        resp = client.post('/test/', headers=headers, body=json.dumps(body))
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
            'store_id': 1,
            'id_names': ['id'],
            'schema': {
                'properties': {
                    'id': {'type': 'string'},
                    't1': {'type': 'integer'}
                }
            }
        }]
        client.post('/items_types/', headers=headers, body=json.dumps(body))

        body = [{'id': 'test', 't1': 1}]
        client.post('/test/', headers=headers, body=json.dumps(body))

        body = [{'id': 'test', 't1': 2}]
        resp = client.patch('/test', headers=headers, body=json.dumps(body))
        assert resp.status_code == 200
        assert json.loads(resp.body) == body

        resp = client.get('/test/test', headers=headers)
        assert json.loads(resp.body) == body[0]

    def test_items_patch_with_delete(self, client, headers):
        body = [{
            'name': 'test',
            'store_id': 1,
            'id_names': ['id'],
            'schema': {
                'properties': {
                    'id': {'type': 'string'},
                    't1': {'type': 'integer'}
                }
            }
        }]
        client.post('/items_types/', headers=headers, body=json.dumps(body))

        body = [{'id': 'test', 't1': 1}, {'id': 'test2', 't1': 2}]
        resp = client.post('/test/', headers=headers, body=json.dumps(body))

        body = [{'id': 'test', '_operation': 'delete'}]
        resp = client.patch('/test', headers=headers, body=json.dumps(body))
        assert resp.status_code == 200
        assert json.loads(resp.body) == [{'id': 'test', '_operation': 'delete'}]

        resp = client.get('/test/test', headers=headers)
        assert resp.status_code == 404

    def test_items_patch_invalid(self, client, headers):
        body = [{
            'name': 'test',
            'store_id': 1,
            'id_names': ['id'],
            'schema': {
                'properties': {
                    'id': {'type': 'string'}
                }
            }
        }]
        client.post('/items_types/', headers=headers, body=json.dumps(body))

        body = [{'id': 1}]
        resp = client.post('/test/', headers=headers, body=json.dumps(body))
        assert resp.status_code == 400
        assert json.loads(resp.body) == {
            'error': {
                'input': 1,
                'message': "1 is not of type 'string'",
                'schema': {'type': 'string'}
            }
        }
