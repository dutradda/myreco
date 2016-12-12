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


from tests.integration.fixtures_models import UsersModel, SQLAlchemyRedisModelBase
from falconswagger.swagger_api import SwaggerAPI
from base64 import b64encode
from unittest import mock
import pytest
import sqlalchemy as sa
import pymysql


@pytest.fixture
def model_base():
    return SQLAlchemyRedisModelBase


class model(SQLAlchemyRedisModelBase):
    __tablename__ = 'test'
    id = sa.Column(sa.Integer, primary_key=True)

    @classmethod
    def do_nothing(cls, req, resp, **kwargs):
        pass

    __schema__ = {
        '/test': {
            'parameters': [{
                'name': 'Authorization',
                'in': 'header',
                'required': True,
                'type': 'string'
            }],
            'post': {
                'operationId': 'do_nothing',
                'responses': {'201': {'description': 'Created'}}
            }
        },
        '/test/{id}': {
            'parameters': [{
                'name': 'id',
                'in': 'path',
                'required': True,
                'type': 'integer'
            },{
                'name': 'Authorization',
                'in': 'header',
                'required': True,
                'type': 'string'
            }],
            'post': {
                'operationId': 'do_nothing',
                'responses': {'201': {'description': 'Created'}}
            }
        }
    }


@pytest.fixture
def app(session):
    return SwaggerAPI([model], session.bind, title='Myreco API')


class TestUsersModelIntegrationWithauthorization_hook(object):
    def test_user_authorized_without_uri_and_methods(self, client, session):
        user = {
            'name': 'test',
            'email': 'test@test',
            'password': '123',
            'admin': True
        }
        UsersModel.insert(session, user)

        authorization = \
            b64encode('{}:{}'.format(user['email'], user['password']).encode()).decode()
        headers = {
            'Authorization': authorization
        }

        resp = client.post('/test/', headers=headers)
        assert resp.status_code == 200
        assert resp.body == ''

    def test_user_authorized_with_uri_and_methods(self, client, session):
        user = {
            'name': 'test',
            'email': 'test@test',
            'password': '123',
            'grants': [{
                'uri': {'uri': '/test/{id}', '_operation': 'insert'},
                'method': {'method': 'POST', '_operation': 'insert'},
                '_operation': 'insert'
            }]
        }
        UsersModel.insert(session, user)

        authorization = \
            b64encode('{}:{}'.format(user['email'], user['password']).encode()).decode()
        headers = {
            'Authorization': authorization
        }

        resp = client.post('/test/1/', headers=headers)
        assert resp.status_code == 200
        assert resp.body == ''

    def test_user_not_authorized_with_wrong_uri(self, session, client):
        user = {
            'name': 'test',
            'email': 'test@test',
            'password': '123',
            'grants': [{
                'uri': {'uri': '/test/{id}', '_operation': 'insert'},
                'method': {'method': 'POST', '_operation': 'insert'},
                '_operation': 'insert'
            }]
        }
        UsersModel.insert(session, user)

        authorization = \
            b64encode('{}:{}'.format(user['email'], user['password']).encode()).decode()
        headers = {
            'Authorization': authorization
        }

        resp = client.post('/test/', headers=headers)
        assert resp.status_code == 401
        assert resp.body == '{"error": "Invalid authorization"}'

    def test_user_not_authorized_without_user(self, session, client):
        authorization = b64encode('test:test'.encode()).decode()
        headers = {
            'Authorization': authorization
        }

        resp = client.post('/test/1/', headers=headers)
        assert resp.status_code == 401
        assert resp.body == '{"error": "Invalid authorization"}'

    def test_user_not_authorized_with_authorization_without_colon(self, session, client):
        authorization = b64encode('test'.encode()).decode()
        headers = {
            'Authorization': authorization
        }

        resp = client.post('/test/1/', headers=headers)
        assert resp.status_code == 401
        assert resp.body == '{"error": "Invalid authorization"}'


class TestUsersModel(object):
    def test_user_authorized_without_uri_and_methods(self, session, redis):
        user = {
            'name': 'test',
            'email': 'test@test',
            'password': '123',
            'admin': True
        }
        UsersModel.insert(session, user)
        authorization = b64encode('{}:{}'.format(user['email'], user['password']).encode())
        redis.hmget.return_value = [None]

        assert UsersModel.authorize(session, authorization, None, None, None) is True

    def test_user_authorized_with_uri_and_methods(self, session, redis):
        user = {
            'name': 'test',
            'email': 'test@test',
            'password': '123',
            'grants': [{
                'uri': {'uri': '/test', '_operation': 'insert'},
                'method': {'method': 'POST', '_operation': 'insert'},
                '_operation': 'insert'
            }]
        }
        UsersModel.insert(session, user)
        authorization = b64encode('{}:{}'.format(user['email'], user['password']).encode())
        redis.hmget.return_value = [None]

        assert UsersModel.authorize(session, authorization, '', '/test', 'POST') is True

    def test_user_not_authorized_with_wrong_uri(self, session, redis):
        user = {
            'name': 'test',
            'email': 'test@test',
            'password': '123',
            'grants': [{
                'uri': {'uri': '/test/{id}', '_operation': 'insert'},
                'method': {'method': 'POST', '_operation': 'insert'},
                '_operation': 'insert'
            }]
        }
        UsersModel.insert(session, user)
        authorization = b64encode('{}:{}'.format(user['email'], user['password']).encode())
        redis.hmget.return_value = [None]

        assert UsersModel.authorize(session, authorization, '', '/tes', 'POST') is None

    def test_user_not_authorized_without_user(self, session, redis):
        authorization = b64encode('test:test'.encode())
        redis.hmget.return_value = [None]

        assert UsersModel.authorize(session, authorization, '', '/tes', 'POST') is None

    def test_user_not_authorized_with_authorization_without_colon(self, session, redis):
        authorization = b64encode('test'.encode())
        redis.hmget.return_value = [None]

        assert UsersModel.authorize(session, authorization, '', '/tes', 'POST') is None
