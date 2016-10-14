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


from tests.integration.fixtures_models import SQLAlchemyRedisModelBase, StoresModel, UsersModel
from falconswagger.http_api import HttpAPI
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

    return HttpAPI([StoresModel], session.bind, FakeStrictRedis())


@pytest.fixture
def headers():
    return {
        'Authorization': b64encode('test:test'.encode()).decode()
    }



class TestStoresModelPost(object):

    def test_post_without_body(self, client, headers):
        resp = client.post('/stores/', headers=headers)
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is missing'}

    def test_post_with_invalid_body(self, client, headers):
        resp = client.post('/stores/', headers=headers, body='[{}]')
        assert resp.status_code == 400
        assert json.loads(resp.body) ==  {
            'error': {
                'input': {},
                'message': "'name' is a required property",
                'schema': {
                    'type': 'object',
                    'additionalProperties': False,
                    'required': ['name', 'country', 'configuration'],
                    'properties': {
                        'configuration': {"$ref": "#/definitions/configuration"},
                        'name': {'type': 'string'},
                        'country': {'type': 'string'}
                    }
                }
            }
        }

    def test_post(self, client, headers):
        body = [{
            'name': 'test',
            'country': 'test',
            'configuration': {'data_path': '/test'}
        }]
        resp = client.post('/stores/', headers=headers, body=json.dumps(body))
        body[0]['id'] = 1

        assert resp.status_code == 201
        assert json.loads(resp.body) ==  body

    def test_post_with_invalid_grant(self, client):
        body = [{
            'name': 'test',
            'country': 'test'
        }]
        resp = client.post('/stores/', headers={'Authorization': 'invalid'}, body=json.dumps(body))
        assert resp.status_code == 401
        assert json.loads(resp.body) ==  {'error': 'Invalid authorization'}


class TestStoresModelGet(object):

    def test_get_not_found(self, client, headers):
        resp = client.get('/stores/', headers=headers)
        assert resp.status_code == 404

    def test_get_invalid_with_body(self, client, headers):
        resp = client.get('/stores/', headers=headers, body='{}')
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is not acceptable'}

    def test_get(self, client, headers):
        body = [{
            'name': 'test',
            'country': 'test',
            'configuration': {'data_path': '/test'}
        }]
        client.post('/stores/', headers=headers, body=json.dumps(body))
        body[0]['id'] = 1

        resp = client.get('/stores/', headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) ==  body


class TestStoresModelUriTemplatePatch(object):

    def test_patch_without_body(self, client, headers):
        resp = client.patch('/stores/1/', headers=headers, body='')
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is missing'}

    def test_patch_with_invalid_body(self, client, headers):
        resp = client.patch('/stores/1/', headers=headers, body='{}')
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
                        'configuration': {"$ref": "#/definitions/configuration"},
                        'name': {'type': 'string'},
                        'country': {'type': 'string'}
                    }
                }
            }
        }

    def test_patch_not_found(self, client, headers):
        body = {
            'name': 'test',
            'country': 'test',
            'configuration': {'data_path': '/test'}
        }
        resp = client.patch('/stores/1/', headers=headers, body=json.dumps(body))
        assert resp.status_code == 404

    def test_patch(self, client, headers):
        body = [{
            'name': 'test',
            'country': 'test',
            'configuration': {'data_path': '/test'}
        }]
        obj = json.loads(client.post('/stores/', headers=headers, body=json.dumps(body)).body)[0]

        body = {
            'name': 'test2'
        }
        resp = client.patch('/stores/1/', headers=headers, body=json.dumps(body))
        obj['name'] = 'test2'

        assert resp.status_code == 200
        assert json.loads(resp.body) ==  obj


class TestStoresModelUriTemplateDelete(object):

    def test_delete_with_body(self, client, headers):
        resp = client.delete('/stores/1/', headers=headers, body='{}')
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is not acceptable'}

    def test_delete(self, client, headers):
        body = [{
            'name': 'test',
            'country': 'test',
            'configuration': {'data_path': '/test'}
        }]
        client.post('/stores/', headers=headers, body=json.dumps(body))

        resp = client.get('/stores/1/', headers=headers)
        assert resp.status_code == 200

        resp = client.delete('/stores/1/', headers=headers)
        assert resp.status_code == 204

        resp = client.get('/stores/1/', headers=headers)
        assert resp.status_code == 404


class TestStoresModelUriTemplateGet(object):

    def test_get_with_body(self, client, headers):
        resp = client.get('/stores/1/', headers=headers, body='{}')
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is not acceptable'}

    def test_get_not_found(self, client, headers):
        resp = client.get('/stores/1/', headers=headers)
        assert resp.status_code == 404

    def test_get(self, client, headers):
        body = [{
            'name': 'test',
            'country': 'test',
            'configuration': {'data_path': '/test'}
        }]
        client.post('/stores/', headers=headers, body=json.dumps(body))

        resp = client.get('/stores/1/', headers=headers)
        body[0]['id'] = 1
        body[0]['configuration'] = {'data_path': '/test'}

        assert resp.status_code == 200
        assert json.loads(resp.body) == body[0]
