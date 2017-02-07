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
from tests.integration.fixtures import EngineCoreTest
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
        'configuration': {'data_path': tmp.name}
    }
    session.loop.run_until_complete(models['stores'].insert(session, store))

    core = {
        'name': 'top_seller',
        'configuration': {
            'core_module': {
                'path': 'tests.integration.fixtures',
                'object_name': 'EngineCoreTest'
            }
        }
    }
    session.loop.run_until_complete(models['engines_cores'].insert(session, core))

    item_type = {
        'name': 'products',
        'schema': {
            'type': 'object',
            'id_names': ['sku'],
            'properties': {'sku': {'type': 'string'}}
        },
        'stores': [{'id': 1}]
    }
    session.loop.run_until_complete(models['items_types'].insert(session, item_type))

    engine = {
        'name': 'Seven Days Top Seller',
        'configuration': {'days_interval': 7},
        'store_id': 1,
        'core_id': 1,
        'item_type_id': 1
    }
    session.loop.run_until_complete(models['engines'].insert(session, engine))

    yield tmp.name

    tmp.cleanup()
    _all_models.pop('products_1')
    api.remove_swagger_paths(_all_models.pop('products_collection'))


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
            'message': "'configuration' is a required property",
            'schema': {
                'type': 'object',
                'additionalProperties': False,
                'required': ['configuration', 'store_id', 'core_id', 'item_type_id'],
                'properties': {
                    'name': {'type': 'string'},
                    'configuration': {},
                    'store_id': {'type': 'integer'},
                    'core_id': {'type': 'integer'},
                    'item_type_id': {'type': 'integer'}
                }
            }
        }

    async def test_post_with_invalid_grant(self, init_db, client):
        client = await client
        resp = await client.post('/engines/', headers={'Authorization': 'invalid'})
        assert resp.status == 401
        assert await resp.json() ==  {'message': 'Invalid authorization'}

    async def test_post_valid(self, init_db, headers, client):
        body = [{
            'name': 'Seven Days Top Seller 2',
            'configuration': {"days_interval": 7, 'data_importer_path': 'test.test'},
            'store_id': 1,
            'core_id': 1,
            'item_type_id': 1
        }]
        client = await client
        resp = await client.post('/engines/', headers=headers, data=ujson.dumps(body))
        body[0]['id'] = 2
        body[0]['variables'] = []
        body[0]['store'] = \
            {'id': 1, 'name': 'test', 'country': 'test', 'configuration': {'data_path': init_db}}
        body[0]['core'] = {
            'id': 1,
            'name': 'top_seller',
            'configuration': {
                'core_module': {
                    'path': 'tests.integration.fixtures',
                    'object_name': 'EngineCoreTest'
                }
            }
        }
        body[0]['item_type'] = {
            'id': 1,
            'post_processing_import': None,
            'stores': [{
                'configuration': {'data_path': init_db},
                'country': 'test',
                'id': 1,
                'name': 'test'
            }],
            'name': 'products',
            'schema': {
                'type': 'object',
                'id_names': ['sku'],
                'properties': {'sku': {'type': 'string'}}
            },
            'available_filters': [{'name': 'sku', 'schema': {'type': 'string'}}]
        }

        assert resp.status == 201
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
            'configuration': {"days_interval": 7},
            'store_id': 1,
            'core_id': 1,
            'item_type_id': 1
        }]
        body[0]['id'] = 1
        body[0]['variables'] = []
        body[0]['store'] = \
            {'id': 1, 'name': 'test', 'country': 'test', 'configuration': {'data_path': init_db}}
        body[0]['core'] = {
            'id': 1,
            'name': 'top_seller',
            'configuration': {
                'core_module': {
                    'path': 'tests.integration.fixtures',
                    'object_name': 'EngineCoreTest'
                }
            }
        }
        body[0]['item_type'] = {
            'id': 1,
            'post_processing_import': None,
            'stores': [{
                'configuration': {'data_path': init_db},
                'country': 'test',
                'id': 1,
                'name': 'test'
            }],
            'name': 'products',
            'schema': {
                'type': 'object',
                'id_names': ['sku'],
                'properties': {'sku': {'type': 'string'}}
            },
            'available_filters': [{'name': 'sku', 'schema': {'type': 'string'}}]
        }

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
            'message': '{} does not have enough properties',
            'schema': {
                'type': 'object',
                'additionalProperties': False,
                'minProperties': 1,
                'properties': {
                    'name': {'type': 'string'},
                    'configuration': {},
                    'store_id': {'type': 'integer'},
                    'core_id': {'type': 'integer'},
                    'item_type_id': {'type': 'integer'}
                }
            }
        }

    async def test_patch_with_invalid_configuration(self, init_db, headers, client):
        body = {
            'configuration': {}
        }
        client = await client
        resp = await client.patch('/engines/1/', headers=headers, data=ujson.dumps(body))
        assert resp.status == 400
        assert await resp.json() ==  {
            'message': "'days_interval' is a required property",
            'schema': {
                'type': 'object',
                'required': ['days_interval'],
                'properties': {
                    'days_interval': {'type': 'integer'}
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
            'configuration': {"days_interval": 7},
            'store_id': 1,
            'core_id': 1,
            'item_type_id': 1
        }]
        body[0]['id'] = 1
        body[0]['variables'] = []
        body[0]['store'] = \
            {'id': 1, 'name': 'test', 'country': 'test', 'configuration': {'data_path': init_db}}
        body[0]['core'] = {
            'id': 1,
            'name': 'top_seller',
            'configuration': {
                'core_module': {
                    'path': 'tests.integration.fixtures',
                    'object_name': 'EngineCoreTest'
                }
            }
        }
        body[0]['item_type'] = {
            'id': 1,
            'post_processing_import': None,
            'stores': [{
                'configuration': {'data_path': init_db},
                'country': 'test',
                'id': 1,
                'name': 'test'
            }],
            'name': 'products',
            'schema': {
                'type': 'object',
                'id_names': ['sku'],
                'properties': {'sku': {'type': 'string'}}
            },
            'available_filters': [{'name': 'sku', 'schema': {'type': 'string'}}]
        }

        assert resp.status == 200
        assert await resp.json() == body[0]


def datetime_mock():
    mock_ = mock.MagicMock()
    mock_.now.return_value = datetime(1900, 1, 1)
    return mock_


async def _wait_job_finish(client, headers_without_content_type, job_name='export_objects'):
    sleep(0.05)
    while True:
        resp = await client.get(
            '/engines/1/{}?job_hash=6342e10bd7dca3240c698aa79c98362e'.format(job_name),
            headers=headers_without_content_type)
        if (await resp.json())['status'] != 'running':
            break

    return resp


def set_patches(monkeypatch):
    monkeypatch.setattr('swaggerit.models.orm._jobs_meta.random.getrandbits',
        mock.MagicMock(return_value=131940827655846590526331314439483569710))
    monkeypatch.setattr('swaggerit.models.orm._jobs_meta.datetime', datetime_mock())


class TestEnginesModelsDataImporter(object):

    async def test_importer_post(self, init_db, headers, headers_without_content_type, client, monkeypatch):
        set_patches(monkeypatch)
        client = await client
        resp = await client.post('/engines/1/import_data', headers=headers_without_content_type)

        assert await resp.json() == {'job_hash': '6342e10bd7dca3240c698aa79c98362e'}
        await _wait_job_finish(client, headers_without_content_type, 'import_data')

    async def test_importer_get_running(self, init_db, headers_without_content_type, client, monkeypatch):
        set_patches(monkeypatch)
        client = await client
        await client.post('/engines/1/import_data', headers=headers_without_content_type)
        resp = await client.get('/engines/1/import_data?job_hash=6342e10bd7dca3240c698aa79c98362e',
            headers=headers_without_content_type)

        assert await resp.json() == {'status': 'running'}
        await _wait_job_finish(client, headers_without_content_type, 'import_data')


    async def test_importer_get_done(self, init_db, headers_without_content_type, client, monkeypatch):
        set_patches(monkeypatch)
        client = await client
        await client.post('/engines/1/import_data', headers=headers_without_content_type)

        resp = await _wait_job_finish(client, headers_without_content_type, 'import_data')

        assert await resp.json() == {
            'status': 'done',
            'result': {'lines_count': 3},
            'time_info': {
                'elapsed': '0:00',
                'start': '1900-01-01 00:00',
                'end': '1900-01-01 00:00'
            }
        }

    async def test_importer_get_with_error(self, init_db, headers_without_content_type, client, monkeypatch):
        set_patches(monkeypatch)
        monkeypatch.setattr('tests.integration.fixtures.EngineCoreTest.get_data',
                            mock.MagicMock(side_effect=Exception('testing')))
        client = await client
        await client.post('/engines/1/import_data', headers=headers_without_content_type)

        resp = await _wait_job_finish(client, headers_without_content_type, 'import_data')

        assert await resp.json() == {
            'status': 'error',
            'result': {'message': 'testing', 'name': 'Exception'},
            'time_info': {
                'elapsed': '0:00',
                'start': '1900-01-01 00:00',
                'end': '1900-01-01 00:00'
            }
        }


async def _post_products(client, headers, headers_without_content_type, products=[{'sku': 'test'}]):
    resp = await client.post('/products?store_id=1',
                        data=ujson.dumps(products), headers=headers)
    resp = await client.post('/products/update_filters?store_id=1',
                        headers=headers_without_content_type)

    sleep(0.05)
    while True:
        resp = await client.get(
            '/products/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
            headers=headers_without_content_type)
        if (await resp.json())['status'] != 'running':
            break

    return resp


def set_readers_builders_patch(monkeypatch, values=None):
    if values is None:
        values = ujson.dumps({'value': 1, 'item_key': 'test'}).encode()

    s = asyncio.StreamReader()
    s.feed_data(values)
    s.feed_eof()
    readers_builder = [s]
    mock_ = CoroMock()
    mock_.coro.return_value = readers_builder

    monkeypatch.setattr('myreco.engines.cores.objects_exporter.'
                        'EngineCoreObjectsExporter._build_csv_readers', mock_)


class TestEnginesModelsObjectsExporter(object):

    async def test_exporter_post(self, init_db, headers_without_content_type, headers, client, monkeypatch):
        set_patches(monkeypatch)
        set_readers_builders_patch(monkeypatch)
        
        client = await client
        await _post_products(client, headers, headers_without_content_type)
        resp = await client.post('/engines/1/export_objects', headers=headers_without_content_type)

        assert await resp.json() == {'job_hash': '6342e10bd7dca3240c698aa79c98362e'}
        await _wait_job_finish(client, headers_without_content_type)

    async def test_exporter_get_running(self, init_db, headers_without_content_type, headers, client, monkeypatch, loop):
        set_patches(monkeypatch)

        prods = [ujson.dumps({'value': i, 'item_key': 'test{}'.format(i)}).encode() for i in range(100)]
        set_readers_builders_patch(monkeypatch,  b'\n'.join(prods))

        client = await client
        products = [{'sku': 'test{}'.format(i)} for i in range(10)]

        await _post_products(client, headers, headers_without_content_type, products)
        await client.post('/engines/1/export_objects', headers=headers_without_content_type)

        resp = await client.get(
            '/engines/1/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e', headers=headers_without_content_type)

        assert await resp.json() == {'status': 'running'}
        await _wait_job_finish(client, headers_without_content_type)

    async def test_exporter_get_done(self, init_db, headers_without_content_type, headers, client, monkeypatch):
        set_patches(monkeypatch)
        client = await client
        await _post_products(client, headers, headers_without_content_type)

        set_readers_builders_patch(monkeypatch)

        await client.post('/engines/1/export_objects', headers=headers_without_content_type)

        resp = await _wait_job_finish(client, headers_without_content_type)

        assert await resp.json() == {
            'status': 'done',
            'result': {'length': 1, 'max_sells': 1, 'min_sells': 1},
            'time_info': {
                'elapsed': '0:00',
                'start': '1900-01-01 00:00',
                'end': '1900-01-01 00:00'
            }
        }

    async def test_exporter_get_with_error(
            self, init_db, headers_without_content_type, headers, client, monkeypatch):
        set_patches(monkeypatch)
        client = await client
        await _post_products(client, headers, headers_without_content_type)

        set_readers_builders_patch(monkeypatch, b'')
        await client.post('/engines/1/export_objects', headers=headers_without_content_type)

        resp = await _wait_job_finish(client, headers_without_content_type)

        assert await resp.json() == {
            'status': 'error',
            'result': {
                'message': "No data found for engine 'Seven Days Top Seller'",
                'name': 'EngineError'
            },
            'time_info': {
                'elapsed': '0:00',
                'start': '1900-01-01 00:00',
                'end': '1900-01-01 00:00'
            }
        }


def CoroMock():
    coro = mock.MagicMock(name="CoroutineResult")
    corofunc = mock.MagicMock(name="CoroutineFunction", side_effect=asyncio.coroutine(coro))
    corofunc.coro = coro
    return corofunc


def set_data_importer_patch(monkeypatch, mock_=None):
    if mock_ is None:
        mock_ = mock.MagicMock()

    monkeypatch.setattr('tests.integration.fixtures.EngineCoreTest.get_data', mock_)
    return mock_


class TestEnginesModelsObjectsExporterWithImport(object):

    async def test_exporter_post_with_import(self, init_db, headers, headers_without_content_type, client, monkeypatch):
        set_patches(monkeypatch)
        client = await client
        await _post_products(client, headers, headers_without_content_type)

        set_readers_builders_patch(monkeypatch)
        get_data_patch = set_data_importer_patch(monkeypatch)
        get_data_patch.return_value = {}

        resp = await client.post('/engines/1/export_objects?import_data=true',
                                      headers=headers_without_content_type)
        hash_ = await resp.json()

        await _wait_job_finish(client, headers_without_content_type)

        called = bool(EngineCoreTest.get_data.called)
        EngineCoreTest.get_data.reset_mock()

        assert hash_ == {'job_hash': '6342e10bd7dca3240c698aa79c98362e'}
        assert called

    async def test_exporter_get_running_with_import(self, init_db, headers, headers_without_content_type, client, monkeypatch):
        set_patches(monkeypatch)
        client = await client
        def func(x, y, z):
            sleep(1)
            return {}

        await _post_products(client, headers, headers_without_content_type)

        set_readers_builders_patch(monkeypatch)
        set_data_importer_patch(monkeypatch, func)
        await client.post('/engines/1/export_objects?import_data=true',
                            headers=headers_without_content_type)

        resp = await client.get(
            '/engines/1/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
            headers=headers_without_content_type)

        assert await resp.json() == {'status': 'running'}
        await _wait_job_finish(client, headers_without_content_type)

    async def test_exporter_get_done_with_import(self, init_db, headers, headers_without_content_type, client, monkeypatch):
        set_patches(monkeypatch)
        client = await client
        await _post_products(client, headers, headers_without_content_type)

        set_readers_builders_patch(monkeypatch)
        await client.post('/engines/1/export_objects?import_data=true',
                            headers=headers_without_content_type)

        await _wait_job_finish(client, headers_without_content_type)

        resp = await client.get(
            '/engines/1/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e',
            headers=headers_without_content_type)

        assert await resp.json() == {
            'status': 'done',
            'result': {
                'importer': {'lines_count': 3},
                'exporter': {
                    'length': 1,
                    'max_sells': 1,
                    'min_sells': 1
                }
            },
            'time_info': {
                'elapsed': '0:00',
                'start': '1900-01-01 00:00',
                'end': '1900-01-01 00:00'
            }
        }

    async def test_exporter_get_with_error_in_import_with_import(
            self, init_db, headers, headers_without_content_type, client, monkeypatch):
        set_patches(monkeypatch)
        client = await client
        await _post_products(client, headers, headers_without_content_type)

        get_data_patch = set_data_importer_patch(monkeypatch)
        get_data_patch.side_effect = Exception('testing')
        await client.post('/engines/1/export_objects?import_data=true', headers=headers_without_content_type)

        await _wait_job_finish(client, headers_without_content_type)

        resp = await client.get(
            '/engines/1/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e', headers=headers_without_content_type)

        assert await resp.json() == {
            'status': 'error',
            'result': {'message': 'testing', 'name': 'Exception'},
            'time_info': {
                'elapsed': '0:00',
                'start': '1900-01-01 00:00',
                'end': '1900-01-01 00:00'
            }
        }

    async def test_exporter_get_with_error_in_export_with_import(
            self, init_db, headers, headers_without_content_type, client, monkeypatch):
        set_patches(monkeypatch)
        client = await client
        await _post_products(client, headers, headers_without_content_type)

        set_readers_builders_patch(monkeypatch, b'')
        await client.post('/engines/1/export_objects?import_data=true', headers=headers_without_content_type)

        await _wait_job_finish(client, headers_without_content_type)

        resp = await client.get(
            '/engines/1/export_objects?job_hash=6342e10bd7dca3240c698aa79c98362e', headers=headers_without_content_type)

        assert await resp.json() == {
            'status': 'error',
            'result': {
                'message': "No data found for engine 'Seven Days Top Seller'",
                'name': 'EngineError'
            },
            'time_info': {
                'elapsed': '0:00',
                'start': '1900-01-01 00:00',
                'end': '1900-01-01 00:00'
            }
        }


class TestEnginesCoresModelPost(object):

    async def test_post_without_body(self, init_db, headers, client):
        client = await client
        resp = await client.post('/engines_cores/', headers=headers)
        assert resp.status == 400
        assert await resp.json() == {'message': 'Request body is missing'}

    async def test_post_with_invalid_body(self, init_db, headers, client):
        client = await client
        resp = await client.post('/engines_cores/', headers=headers, data='[{}]')
        assert resp.status == 400
        assert await resp.json() ==  {
            'message': "'configuration' is a required property",
            'schema': {
                'type': 'object',
                'additionalProperties': False,
                'required': ['configuration'],
                'properties': {
                    'name': {'type': 'string'},
                    'configuration': {'$ref': '#/definitions/EnginesCoresModel.configuration'}
                }
            }
        }

    async def test_post_with_invalid_grant(self, init_db, client):
        body = [{
            'name': 'top_seller2',
            'configuration': {
                'core_module': {
                    'path': 'tests.integration.fixtures',
                    'object_name': 'EngineCoreTest'
                }
            }
        }]
        client = await client
        resp = await client.post('/engines_cores/', headers={'Authorization': 'invalid'},data=ujson.dumps(body))
        assert resp.status == 401
        assert await resp.json() ==  {'message': 'Invalid authorization'}

    async def test_post(self, init_db, headers, client):
        body = [{
            'name': 'top_seller2',
            'configuration': {
                'core_module': {
                    'path': 'tests.integration.fixtures',
                    'object_name': 'EngineCoreTest'
                }
            }
        }]
        client = await client
        resp = await client.post('/engines_cores/', headers=headers, data=ujson.dumps(body))
        body[0]['id'] = 2

        assert resp.status == 201
        assert await resp.json() ==  body


class TestEnginesCoresModelGet(object):

    async def test_get_not_found(self, init_db, headers_without_content_type, client):
        client = await client
        await client.delete('/engines/1/', headers=headers_without_content_type)
        await client.delete('/engines_cores/1/', headers=headers_without_content_type)
        resp = await client.get('/engines_cores/', headers=headers_without_content_type)
        assert resp.status == 404

    async def test_get_invalid_with_body(self, init_db, headers, client):
        client = await client
        resp = await client.get('/engines_cores/', headers=headers, data='{}')
        assert resp.status == 400
        assert await resp.json() == {'message': 'Request body is not acceptable'}

    async def test_get(self, init_db, headers, headers_without_content_type, client):
        body = [{
            'name': 'top_seller2',
            'configuration': {
                'core_module': {
                    'path': 'tests.integration.fixtures',
                    'object_name': 'EngineCoreTest'
                }
            }
        }]
        client = await client
        await client.delete('/engines/1/', headers=headers_without_content_type)
        await client.delete('/engines_cores/1/', headers=headers_without_content_type)
        await client.post('/engines_cores/', headers=headers, data=ujson.dumps(body))
        body[0]['id'] = 2
        resp = await client.get('/engines_cores/', headers=headers_without_content_type)
        assert resp.status == 200
        assert await resp.json() ==  body


class TestEnginesCoresModelUriTemplatePatch(object):

    async def test_patch_without_body(self, init_db, headers, client):
        client = await client
        resp = await client.patch('/engines_cores/1/', headers=headers, data='')
        assert resp.status == 400
        assert await resp.json() == {'message': 'Request body is missing'}

    async def test_patch_with_invalid_body(self, init_db, headers, client):
        client = await client
        resp = await client.patch('/engines_cores/1/', headers=headers, data='{}')
        assert resp.status == 400
        assert await resp.json() ==  {
            'message': '{} does not have enough properties',
            'schema': {
                'type': 'object',
                'additionalProperties': False,
                'minProperties': 1,
                'properties': {
                    'name': {'type': 'string'},
                    'configuration': {'$ref': '#/definitions/EnginesCoresModel.configuration'}
                }
            }
        }

    async def test_patch_with_invalid_configuration(self, init_db, headers, client):
        body = [{
            'name': 'top_seller2',
            'configuration': {
            }
        }]
        client = await client
        await client.post('/engines_cores/', headers=headers, data=ujson.dumps(body))

        body = {
            'configuration': {}
        }
        resp = await client.patch('/engines_cores/2/', headers=headers, data=ujson.dumps(body))
        assert resp.status == 400
        assert await resp.json() ==  {
            'message': "'core_module' is a required property",
            'schema': {
                'type': 'object',
                'required': ['core_module'],
                'properties': {
                    'core_module': {'$ref': '#/definitions/EnginesCoresModel.module'}
                }
            }
        }

    async def test_patch_not_found(self, init_db, headers, client):
        body = {
            'name': 'test'
        }
        client = await client
        resp = await client.patch('/engines_cores/2/', headers=headers, data=ujson.dumps(body))
        assert resp.status == 404

    async def test_patch(self, init_db, headers, client):
        body = [{
            'name': 'top_seller2',
            'configuration': {
                'core_module': {
                    'path': 'tests.integration.fixtures',
                    'object_name': 'EngineCoreTest'
                }
            }
        }]
        client = await client
        resp = await client.post('/engines_cores/', headers=headers, data=ujson.dumps(body))
        obj = (await resp.json())[0]

        body = {
            'name': 'test2'
        }
        resp = await client.patch('/engines_cores/2/', headers=headers, data=ujson.dumps(body))
        obj['name'] = 'test2'

        assert resp.status == 200
        assert await resp.json() ==  obj


class TestEnginesCoresModelUriTemplateDelete(object):

    async def test_delete_with_body(self, init_db, headers, client):
        client = await client
        resp = await client.delete('/engines_cores/1/', headers=headers, data='{}')
        assert resp.status == 400
        assert await resp.json() == {'message': 'Request body is not acceptable'}

    async def test_delete(self, init_db, headers, headers_without_content_type, client):
        body = [{
            'name': 'top_seller2',
            'configuration': {
                'core_module': {
                    'path': 'tests.integration.fixtures',
                    'object_name': 'EngineCoreTest'
                }
            }
        }]
        client = await client
        await client.post('/engines_cores/', headers=headers, data=ujson.dumps(body))
        resp = await client.get('/engines_cores/2/', headers=headers_without_content_type)
        assert resp.status == 200

        resp = await client.delete('/engines_cores/2/', headers=headers_without_content_type)
        assert resp.status == 204

        resp = await client.get('/engines_cores/2/', headers=headers_without_content_type)
        assert resp.status == 404


class TestEnginesCoresModelUriTemplateGet(object):

    async def test_get_with_body(self, init_db, headers, client):
        client = await client
        resp = await client.get('/engines_cores/2/', headers=headers, data='{}')
        assert resp.status == 400
        assert await resp.json() == {'message': 'Request body is not acceptable'}

    async def test_get_not_found(self, init_db, headers_without_content_type, client):
        client = await client
        resp = await client.get('/engines_cores/2/', headers=headers_without_content_type)
        assert resp.status == 404

    async def test_get(self, init_db, headers, headers_without_content_type, client):
        body = [{
            'name': 'top_seller2',
            'configuration': {
                'core_module': {
                    'path': 'tests.integration.fixtures',
                    'object_name': 'EngineCoreTest'
                }
            }
        }]
        client = await client
        await client.post('/engines_cores/', headers=headers, data=ujson.dumps(body))
        resp = await client.get('/engines_cores/2/', headers=headers_without_content_type)
        body[0]['id'] = 2
        assert resp.status == 200
        assert await resp.json() == body[0]