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
from tests.integration.fixtures import TopSellerArrayTest
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
        'configuration': {}
    }
    session.loop.run_until_complete(models['stores'].insert(session, store))

    item_type = {
        'name': 'products',
        'schema': {
            'type': 'object',
            'id_names': ['sku'],
            'properties': {'sku': {'type': 'string'}}
        },
        'stores': [{'id': 1}]
    }
    session.loop.run_until_complete(models['item_types'].insert(session, item_type))

    strategy = {
        'name': 'test',
        'class_module': 'tests.integration.fixtures',
        'class_name': 'EngineStrategyTest'
    }
    session.loop.run_until_complete(models['engine_strategies'].insert(session, strategy))

    engine_object = {
        'name': 'Top Seller Object',
        'type': 'top_seller_array',
        'configuration': {'days_interval': 7},
        'store_id': 1,
        'item_type_id': 1,
        'strategy_id': 1
    }
    session.loop.run_until_complete(models['engine_objects'].insert(session, engine_object))


    yield tmp.name

    tmp.cleanup()
    _all_models.pop('store_items_products_1', None)


class TestEngineObjectsModelGet(object):

    async def test_get_not_found(self, init_db, headers_without_content_type, client):
        client = await client
        resp = await client.get(
            '/engine_objects/?store_id=2&item_type_id=1&strategy_id=1',
            headers=headers_without_content_type
        )
        assert resp.status == 404

    async def test_get_invalid_with_body(self, init_db, headers, client):
        client = await client
        resp = await client.get(
            '/engine_objects/?store_id=1&item_type_id=1&strategy_id=1',
            headers=headers,
            data='{}'
        )
        assert resp.status == 400
        assert await resp.json() == {'message': 'Request body is not acceptable'}

    async def test_get_valid(self, init_db, headers, headers_without_content_type, client):
        body = [{
            'name': 'Top Seller Object',
            'type': 'top_seller_array',
            'configuration': {"days_interval": 7},
            'store_id': 1,
            'item_type_id': 1,
            'strategy_id': 1,
            'id': 1,
            'store': {
                'id': 1,
                'name': 'test',
                'country': 'test',
                'configuration': {}
            },
            'item_type': {
                'id': 1,
                'store_items_class': None,
                'stores': [{
                    'configuration': {},
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
            },
            'strategy': {
                'id': 1,
                'name': 'test',
                'class_module': 'tests.integration.fixtures',
                'class_name': 'EngineStrategyTest',
                'object_types': ['top_seller_array']
            }
        }]

        client = await client
        resp = await client.get(
            '/engine_objects/?store_id=1&item_type_id=1&strategy_id=1',
            headers=headers_without_content_type
        )
        assert resp.status == 200
        assert await resp.json() ==  body


class TestEngineObjectsModelUriTemplateGet(object):

    async def test_get_with_body(self, init_db, headers, client):
        client = await client
        resp = await client.get('/engine_objects/1/', headers=headers, data='{}')
        assert resp.status == 400
        assert await resp.json() == {'message': 'Request body is not acceptable'}

    async def test_get_not_found(self, init_db, headers_without_content_type, client):
        client = await client
        resp = await client.get('/engine_objects/2/', headers=headers_without_content_type)
        assert resp.status == 404

    async def test_get(self, init_db, headers, headers_without_content_type, client):
        client = await client
        resp = await client.get('/engine_objects/1/', headers=headers_without_content_type)
        body = {
            'name': 'Top Seller Object',
            'type': 'top_seller_array',
            'configuration': {"days_interval": 7},
            'store_id': 1,
            'item_type_id': 1,
            'strategy_id': 1,
            'id': 1,
            'store': {
                'id': 1,
                'name': 'test',
                'country': 'test',
                'configuration': {}
            },
            'item_type': {
                'id': 1,
                'store_items_class': None,
                'stores': [{
                    'configuration': {},
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
            },
            'strategy': {
                'id': 1,
                'name': 'test',
                'class_module': 'tests.integration.fixtures',
                'class_name': 'EngineStrategyTest',
                'object_types': ['top_seller_array']
            }
        }

        assert resp.status == 200
        assert await resp.json() == body


class TestEngineObjectsModelUriTemplateDelete(object):

   async def test_delete_with_body(self, init_db, client, headers):
        client = await client

        resp = await client.delete('/engine_objects/1/', headers=headers, data='{}')
        assert resp.status == 400
        assert (await resp.json()) == {'message': 'Request body is not acceptable'}

   async def test_delete_valid(self, init_db, client, headers, headers_without_content_type):
        client = await client

        resp = await client.get('/engine_objects/1/', headers=headers_without_content_type)
        assert resp.status == 200

        resp = await client.delete('/engine_objects/1/', headers=headers_without_content_type)
        assert resp.status == 204

        resp = await client.get('/engine_objects/1/', headers=headers_without_content_type)
        assert resp.status == 404


def datetime_mock():
    mock_ = mock.MagicMock()
    mock_.now.return_value = datetime(1900, 1, 1)
    return mock_


async def _wait_job_finish(client, headers_without_content_type, job_name='export'):
    sleep(0.05)
    while True:
        resp = await client.get(
            '/engine_objects/1/{}?job_hash=6342e10bd7dca3240c698aa79c98362e'.format(job_name),
            headers=headers_without_content_type)
        if (await resp.json())['status'] != 'running':
            break

    return resp


def set_patches(monkeypatch):
    monkeypatch.setattr('swaggerit.models.orm._jobs_meta.random.getrandbits',
        mock.MagicMock(return_value=131940827655846590526331314439483569710))
    monkeypatch.setattr('swaggerit.models.orm._jobs_meta.datetime', datetime_mock())


class TestEngineObjectsModelsDataImporter(object):

    async def test_importer_post(self, init_db, headers, headers_without_content_type, client, monkeypatch):
        set_patches(monkeypatch)
        client = await client
        resp = await client.post('/engine_objects/1/import_data', headers=headers_without_content_type)

        assert resp.status == 201
        assert await resp.json() == {'job_hash': '6342e10bd7dca3240c698aa79c98362e'}
        await _wait_job_finish(client, headers_without_content_type, 'import_data')

    async def test_importer_get_running(self, init_db, headers_without_content_type, client, monkeypatch):
        set_patches(monkeypatch)
        client = await client
        await client.post('/engine_objects/1/import_data', headers=headers_without_content_type)
        resp = await client.get('/engine_objects/1/import_data?job_hash=6342e10bd7dca3240c698aa79c98362e',
            headers=headers_without_content_type)

        assert await resp.json() == {'status': 'running'}
        await _wait_job_finish(client, headers_without_content_type, 'import_data')


    async def test_importer_get_done(self, init_db, headers_without_content_type, client, monkeypatch):
        set_patches(monkeypatch)
        client = await client
        await client.post('/engine_objects/1/import_data', headers=headers_without_content_type)

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
        monkeypatch.setattr('tests.integration.fixtures.TopSellerArrayTest.get_data',
                            mock.MagicMock(side_effect=Exception('testing')))
        client = await client
        await client.post('/engine_objects/1/import_data', headers=headers_without_content_type)

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
    resp = await client.post('/item_types/1/items?store_id=1',
                        data=ujson.dumps(products), headers=headers)
    resp = await client.post('/item_types/1/update_filters?store_id=1',
                        headers=headers_without_content_type)

    sleep(0.05)
    while True:
        resp = await client.get(
            '/item_types/1/update_filters?store_id=1&job_hash=6342e10bd7dca3240c698aa79c98362e',
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

    monkeypatch.setattr(
        'myreco.engine_objects.object_base.EngineObjectBase._build_csv_readers',
        mock_
    )


class TestEngineObjectsModelsObjectsExporter(object):

    async def test_exporter_post(self, init_db, headers_without_content_type, headers, client, monkeypatch):
        set_patches(monkeypatch)
        set_readers_builders_patch(monkeypatch)
        
        client = await client
        await _post_products(client, headers, headers_without_content_type)
        resp = await client.post('/engine_objects/1/export', headers=headers_without_content_type)

        assert await resp.json() == {'job_hash': '6342e10bd7dca3240c698aa79c98362e'}
        await _wait_job_finish(client, headers_without_content_type)

    async def test_exporter_get_running(self, init_db, headers_without_content_type, headers, client, monkeypatch, loop):
        set_patches(monkeypatch)

        prods = [ujson.dumps({'value': i, 'item_key': 'test{}'.format(i)}).encode() for i in range(100)]
        set_readers_builders_patch(monkeypatch,  b'\n'.join(prods))

        client = await client
        products = [{'sku': 'test{}'.format(i)} for i in range(10)]

        await _post_products(client, headers, headers_without_content_type, products)
        await client.post('/engine_objects/1/export', headers=headers_without_content_type)

        resp = await client.get(
            '/engine_objects/1/export?job_hash=6342e10bd7dca3240c698aa79c98362e', headers=headers_without_content_type)

        assert await resp.json() == {'status': 'running'}
        await _wait_job_finish(client, headers_without_content_type)

    async def test_exporter_get_done(self, init_db, headers_without_content_type, headers, client, monkeypatch):
        set_patches(monkeypatch)
        client = await client
        await _post_products(client, headers, headers_without_content_type)

        set_readers_builders_patch(monkeypatch)

        await client.post('/engine_objects/1/export', headers=headers_without_content_type)

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
        await client.post('/engine_objects/1/export', headers=headers_without_content_type)

        resp = await _wait_job_finish(client, headers_without_content_type)

        assert await resp.json() == {
            'status': 'error',
            'result': {
                'message': "No data found for engine object 'Top Seller Object'",
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

    monkeypatch.setattr('tests.integration.fixtures.TopSellerArrayTest.get_data', mock_)
    return mock_


class TestEngineObjectsModelsObjectsExporterWithImport(object):

    async def test_exporter_post_with_import(self, init_db, headers, headers_without_content_type, client, monkeypatch):
        set_patches(monkeypatch)
        client = await client
        await _post_products(client, headers, headers_without_content_type)

        set_readers_builders_patch(monkeypatch)
        get_data_patch = set_data_importer_patch(monkeypatch)
        get_data_patch.return_value = {}

        resp = await client.post('/engine_objects/1/export?import_data=true',
                                      headers=headers_without_content_type)
        hash_ = await resp.json()

        await _wait_job_finish(client, headers_without_content_type)

        called = bool(TopSellerArrayTest.get_data.called)
        TopSellerArrayTest.get_data.reset_mock()

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
        await client.post('/engine_objects/1/export?import_data=true',
                            headers=headers_without_content_type)

        resp = await client.get(
            '/engine_objects/1/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
            headers=headers_without_content_type)

        assert await resp.json() == {'status': 'running'}
        await _wait_job_finish(client, headers_without_content_type)

    async def test_exporter_get_done_with_import(self, init_db, headers, headers_without_content_type, client, monkeypatch):
        set_patches(monkeypatch)
        client = await client
        await _post_products(client, headers, headers_without_content_type)

        set_readers_builders_patch(monkeypatch)
        await client.post('/engine_objects/1/export?import_data=true',
                            headers=headers_without_content_type)

        await _wait_job_finish(client, headers_without_content_type)

        resp = await client.get(
            '/engine_objects/1/export?job_hash=6342e10bd7dca3240c698aa79c98362e',
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
        await client.post('/engine_objects/1/export?import_data=true', headers=headers_without_content_type)

        await _wait_job_finish(client, headers_without_content_type)

        resp = await client.get(
            '/engine_objects/1/export?job_hash=6342e10bd7dca3240c698aa79c98362e', headers=headers_without_content_type)

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
        await client.post('/engine_objects/1/export?import_data=true', headers=headers_without_content_type)

        await _wait_job_finish(client, headers_without_content_type)

        resp = await client.get(
            '/engine_objects/1/export?job_hash=6342e10bd7dca3240c698aa79c98362e', headers=headers_without_content_type)

        assert await resp.json() == {
            'status': 'error',
            'result': {
                'message': "No data found for engine object 'Top Seller Object'",
                'name': 'EngineError'
            },
            'time_info': {
                'elapsed': '0:00',
                'start': '1900-01-01 00:00',
                'end': '1900-01-01 00:00'
            }
        }
