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


from base64 import b64encode


class TestUsersModelIntegrationWithauthorization_hook(object):
   async def test_user_authorized_without_uri_and_methods(self, client, models, session, api):
        client = await client
        user = {
            'name': 'test',
            'email': 'test@test',
            'password': '123',
            'admin': True
        }
        await models['users'].insert(session, user)

        authorization = \
            b64encode('{}:{}'.format(user['email'], user['password']).encode()).decode()
        headers = {
            'Authorization': 'Basic ' + authorization
        }

        resp = await client.post('/testing/', headers=headers)
        assert resp.status == 200
        assert await resp.text() == ''

   async def test_user_authorized_with_uri_and_methods(self, client, models, session):
        client = await client
        user = {
            'name': 'test',
            'email': 'test@test',
            'password': '123',
            'grants': [{
                'uri': {'uri': r'/testing/\d+', '_operation': 'insert'},
                'method': {'method': 'POST', '_operation': 'insert'},
                '_operation': 'insert'
            }]
        }
        await models['users'].insert(session, user)

        authorization = \
            b64encode('{}:{}'.format(user['email'], user['password']).encode()).decode()
        headers = {
            'Authorization': 'Basic ' + authorization
        }

        resp = await client.post('/testing/1/', headers=headers)
        assert resp.status == 200
        assert await resp.text() == ''

   async def test_user_not_authorized_with_wrong_uri(self, client, models, session):
        client = await client
        user = {
            'name': 'test',
            'email': 'test@test',
            'password': '123',
            'grants': [{
                'uri': {'uri': r'/testing/\d+', '_operation': 'insert'},
                'method': {'method': 'POST', '_operation': 'insert'},
                '_operation': 'insert'
            }]
        }
        await models['users'].insert(session, user)

        authorization = \
            b64encode('{}:{}'.format(user['email'], user['password']).encode()).decode()
        headers = {
            'Authorization': 'Basic ' + authorization
        }

        resp = await client.post('/testing/', headers=headers)
        assert resp.status == 403
        assert await resp.text() == '{"message":"Access denied"}'

   async def test_user_not_authorized_without_user(self, client, models, session):
        client = await client
        authorization = b64encode('test:test'.encode()).decode()
        headers = {
            'Authorization': 'Basic ' + authorization
        }

        resp = await client.post('/testing/1/', headers=headers)
        assert resp.status == 401
        assert await resp.text() == '{"message":"Invalid authorization"}'

   async def test_user_not_authorized_with_authorization_without_colon(self, client, models, session):
        client = await client
        authorization = b64encode('test'.encode()).decode()
        headers = {
            'Authorization': 'Basic ' + authorization
        }

        resp = await client.post('/testing/1/', headers=headers)
        assert resp.status == 401
        assert await resp.text() == '{"message":"Invalid authorization"}'


class TestUsersModel(object):
   async def test_user_authorized_without_uri_and_methods(self, models, session):
        user = {
            'name': 'test',
            'email': 'test@test',
            'password': '123',
            'admin': True
        }
        await models['users'].insert(session, user)
        authorization = b64encode('{}:{}'.format(user['email'], user['password']).encode())

        assert await models['users'].authorize(session, authorization, None, None) is True

   async def test_user_authorized_with_uri_and_methods(self, models, session):
        user = {
            'name': 'test',
            'email': 'test@test',
            'password': '123',
            'grants': [{
                'uri': {'uri': '/testing', '_operation': 'insert'},
                'method': {'method': 'POST', '_operation': 'insert'},
                '_operation': 'insert'
            }]
        }
        await models['users'].insert(session, user)
        authorization = b64encode('{}:{}'.format(user['email'], user['password']).encode())

        assert await models['users'].authorize(session, authorization, '/testing', 'POST') is True

   async def test_user_not_authorized_with_wrong_uri(self, models, session):
        user = {
            'name': 'test',
            'email': 'test@test',
            'password': '123',
            'grants': [{
                'uri': {'uri': '/testing/{id}', '_operation': 'insert'},
                'method': {'method': 'POST', '_operation': 'insert'},
                '_operation': 'insert'
            }]
        }
        await models['users'].insert(session, user)
        authorization = b64encode('{}:{}'.format(user['email'], user['password']).encode())

        assert await models['users'].authorize(session, authorization, '/tes', 'POST') is False

   async def test_user_not_authorized_without_user(self, models, session):
        authorization = b64encode('test:test'.encode())

        assert await models['users'].authorize(session, authorization, '/tes', 'POST') is None

   async def test_user_not_authorized_with_authorization_without_colon(self, models, session):
        authorization = b64encode('test'.encode())

        assert await models['users'].authorize(session, authorization, '/tes', 'POST') is None
