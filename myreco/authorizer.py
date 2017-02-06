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


from swaggerit.response import SwaggerResponse
import ujson


class MyrecoAuthorizer(object):

    def __init__(self, users_model, realm='myreco'):
        self._users_model = users_model
        self._realm = realm

    async def __call__(self, req, session):
        headers = {'www-authenticate': 'basic realm="{}"'.format(self._realm)}
        response401 = SwaggerResponse(
            401,
            body=ujson.dumps({'message': 'Invalid authorization'}),
            headers=headers
        )
        response403 = SwaggerResponse(
            403,
            body=ujson.dumps({'message': 'Access denied'}),
            headers=headers
        )
        authorization = req.headers.get('authorization', '')

        basic_str = 'Basic '
        if not authorization.startswith(basic_str):
            return response401

        authorization = authorization.replace(basic_str, '')
        authorize = await self._users_model.authorize(session, authorization, req.url, req.method)

        if authorize is None:
            return response401
        elif authorize is False:
            return response403
        else:
            return None
