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
    ItemsTypesModel, EnginesModel, EnginesTypesNamesModel, DataImporter)
from myreco.factory import ModelsFactory
from myreco.engines.types.items_indices_map import ItemsIndicesMap
from pytest_falcon.plugin import Client
from falconswagger.http_api import HttpAPI
from base64 import b64encode
from fakeredis import FakeStrictRedis
from unittest import mock
from time import sleep
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

    EnginesTypesNamesModel.insert(session, {'name': 'top_seller'})

    item_type = {
        'name': 'products',
        'schema': {
            'type': 'object',
            'id_names': ['sku'],
            'properties': {'sku': {'type': 'string'}}
        },
        'stores': [{'id': 1}]
    }
    ItemsTypesModel.insert(session, item_type)

    return HttpAPI([EnginesModel], session.bind, FakeStrictRedis())


@pytest.fixture
def headers():
    return {
        'Authorization': b64encode('test:test'.encode()).decode()
    }



class TestEnginesModelPost(object):

    def test_post_without_body(self, client, headers):
        resp = client.post('/engines/', headers=headers)
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is missing'}

    def test_post_with_invalid_body(self, client, headers):
        resp = client.post('/engines/', headers=headers, body='[{}]')
        assert resp.status_code == 400
        assert json.loads(resp.body) ==  {
            'error': {
                'input': {},
                'message': "'configuration' is a required property",
                'schema': {
                    'type': 'object',
                    'additionalProperties': False,
                    'required': ['configuration', 'store_id', 'type_name_id', 'item_type_id'],
                    'properties': {
                        'name': {'type': 'string'},
                        'configuration': {'$ref': 'http://json-schema.org/draft-04/schema#'},
                        'store_id': {'type': 'integer'},
                        'type_name_id': {'type': 'integer'},
                        'item_type_id': {'type': 'integer'}
                    }
                }
            }
        }

    def test_post_with_invalid_grant(self, client):
        body = [{
            'name': 'Seven Days Top Seller',
            'configuration': {"days_interval": 7, 'data_importer_path': 'test.test'},
            'store_id': 1,
            'type_name_id': 1,
            'item_type_id': 1
        }]
        resp = client.post('/engines/', headers={'Authorization': 'invalid'},body=json.dumps(body))
        assert resp.status_code == 401
        assert json.loads(resp.body) ==  {'error': 'Invalid authorization'}

    def test_post(self, client, headers):
        body = [{
            'name': 'Seven Days Top Seller',
            'configuration': {"days_interval": 7, 'data_importer_path': 'test.test'},
            'store_id': 1,
            'type_name_id': 1,
            'item_type_id': 1
        }]
        resp = client.post('/engines/', headers=headers, body=json.dumps(body))
        body[0]['id'] = 1
        body[0]['variables'] = []
        body[0]['store'] = \
            {'id': 1, 'name': 'test', 'country': 'test', 'configuration': {'data_path': '/test'}}
        body[0]['type_name'] = {'id': 1, 'name': 'top_seller'}
        body[0]['item_type'] = {
            'id': 1,
            'stores': [{
                'configuration': {'data_path': '/test'},
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

        assert resp.status_code == 201
        assert json.loads(resp.body) ==  body


class TestEnginesModelGet(object):

    def test_get_not_found(self, client, headers):
        resp = client.get('/engines/', headers=headers)
        assert resp.status_code == 404

    def test_get_invalid_with_body(self, client, headers):
        resp = client.get('/engines/', headers=headers, body='{}')
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is not acceptable'}

    def test_get(self, client, headers):
        body = [{
            'name': 'Seven Days Top Seller',
            'configuration': {"days_interval": 7, 'data_importer_path': 'test.test'},
            'store_id': 1,
            'type_name_id': 1,
            'item_type_id': 1
        }]
        client.post('/engines/', headers=headers, body=json.dumps(body))
        body[0]['id'] = 1
        body[0]['variables'] = []
        body[0]['store'] = \
            {'id': 1, 'name': 'test', 'country': 'test', 'configuration': {'data_path': '/test'}}
        body[0]['type_name'] = {'id': 1, 'name': 'top_seller'}
        body[0]['item_type'] = {
            'id': 1,
            'stores': [{
                'configuration': {'data_path': '/test'},
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

        resp = client.get('/engines/', headers=headers)
        assert resp.status_code == 200
        assert json.loads(resp.body) ==  body


class TestEnginesModelUriTemplatePatch(object):

    def test_patch_without_body(self, client, headers):
        resp = client.patch('/engines/1/', headers=headers, body='')
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is missing'}

    def test_patch_with_invalid_body(self, client, headers):
        resp = client.patch('/engines/1/', headers=headers, body='{}')
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
                        'configuration': {'$ref': 'http://json-schema.org/draft-04/schema#'},
                        'store_id': {'type': 'integer'},
                        'type_name_id': {'type': 'integer'},
                        'item_type_id': {'type': 'integer'}
                    }
                }
            }
        }

    def test_patch_with_invalid_configuration(self, client, headers):
        body = [{
            'name': 'Seven Days Top Seller',
            'configuration': {"days_interval": 7, 'data_importer_path': 'test.test'},
            'store_id': 1,
            'type_name_id': 1,
            'item_type_id': 1
        }]
        client.post('/engines/', headers=headers, body=json.dumps(body))

        body = {
            'configuration': {}
        }
        resp = client.patch('/engines/1/', headers=headers, body=json.dumps(body))
        assert resp.status_code == 400
        assert json.loads(resp.body) ==  {
            'error': {
                'input': {},
                'message': "'days_interval' is a required property",
                'schema': {
                    'type': 'object',
                    'required': ['days_interval', 'data_importer_path'],
                    'properties': {
                        'days_interval': {'type': 'integer'},
                        'data_importer_path': {'type': 'string'}
                    }
                }
            }
        }

    def test_patch_not_found(self, client, headers):
        body = {
            'name': 'test',
            'store_id': 1
        }
        resp = client.patch('/engines/1/', headers=headers, body=json.dumps(body))
        assert resp.status_code == 404

    def test_patch(self, client, headers):
        body = [{
            'name': 'Seven Days Top Seller',
            'configuration': {"days_interval": 7, 'data_importer_path': 'test.test'},
            'store_id': 1,
            'type_name_id': 1,
            'item_type_id': 1
        }]
        obj = json.loads(client.post('/engines/', headers=headers, body=json.dumps(body)).body)[0]

        body = {
            'name': 'test2'
        }
        resp = client.patch('/engines/1/', headers=headers, body=json.dumps(body))
        obj['name'] = 'test2'

        assert resp.status_code == 200
        assert json.loads(resp.body) ==  obj


class TestEnginesModelUriTemplateDelete(object):

    def test_delete_with_body(self, client, headers):
        resp = client.delete('/engines/1/', headers=headers, body='{}')
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is not acceptable'}

    def test_delete(self, client, headers):
        body = [{
            'name': 'Seven Days Top Seller',
            'configuration': {"days_interval": 7, 'data_importer_path': 'test.test'},
            'store_id': 1,
            'type_name_id': 1,
            'item_type_id': 1
        }]
        client.post('/engines/', headers=headers, body=json.dumps(body))
        resp = client.get('/engines/1/', headers=headers)
        assert resp.status_code == 200

        resp = client.delete('/engines/1/', headers=headers)
        assert resp.status_code == 204

        resp = client.get('/engines/1/', headers=headers)
        assert resp.status_code == 404


class TestEnginesModelUriTemplateGet(object):

    def test_get_with_body(self, client, headers):
        resp = client.get('/engines/1/', headers=headers, body='{}')
        assert resp.status_code == 400
        assert json.loads(resp.body) == {'error': 'Request body is not acceptable'}

    def test_get_not_found(self, client, headers):
        resp = client.get('/engines/1/', headers=headers)
        assert resp.status_code == 404

    def test_get(self, client, headers):
        body = [{
            'name': 'Seven Days Top Seller',
            'configuration': {"days_interval": 7, 'data_importer_path': 'test.test'},
            'store_id': 1,
            'type_name_id': 1,
            'item_type_id': 1
        }]
        client.post('/engines/', headers=headers, body=json.dumps(body))

        resp = client.get('/engines/1/', headers=headers)
        body[0]['id'] = 1
        body[0]['variables'] = []
        body[0]['store'] = \
            {'id': 1, 'name': 'test', 'country': 'test', 'configuration': {'data_path': '/test'}}
        body[0]['type_name'] = {'id': 1, 'name': 'top_seller'}
        body[0]['item_type'] = {
            'id': 1,
            'stores': [{
                'configuration': {'data_path': '/test'},
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

        assert resp.status_code == 200
        assert json.loads(resp.body) == body[0]


@pytest.fixture
def data_importer_app(session):
    table_args = {'mysql_engine':'innodb'}
    factory = ModelsFactory('myreco', commons_models_attributes={'__table_args__': table_args},
                            commons_tables_attributes=table_args)
    models = factory.make_all_models('importer')
    user = {
        'name': 'test',
        'email': 'test',
        'password': 'test',
        'admin': True
    }
    models['users'].insert(session, user)

    store = {
        'name': 'test',
        'country': 'test',
        'configuration': {'data_path': '/test'}
    }
    models['stores'].insert(session, store)

    models['engines_types_names'].insert(session, {'name': 'top_seller'})

    item_type = {
        'name': 'products',
        'stores': [{'id': 1}],
        'schema': {
            'type': 'object',
            'id_names': ['sku'],
            'properties': {'sku': {'type': 'string'}}
        }
    }
    models['items_types'].insert(session, item_type)

    engine = {
        'name': 'Seven Days Top Seller',
        'configuration': {'days_interval': 7, 'data_importer_path': 'tests.integration.fixtures_models.DataImporter'},
        'store_id': 1,
        'type_name_id': 1,
        'item_type_id': 1
    }
    models['engines'].insert(session, engine)

    api = HttpAPI([models['engines'], models['items_types']], session.bind, FakeStrictRedis())
    models['items_types'].associate_all_items(session)

    return api


@pytest.fixture
def data_importer_client(data_importer_app):
    return Client(data_importer_app)


@mock.patch('falconswagger.models.base.random.getrandbits',
    new=mock.MagicMock(return_value=131940827655846590526331314439483569710))
class TestEnginesModelsDataImporter(object):

    def test_importer_post(self, data_importer_client, headers):
        DataImporter.get_data.return_value = {}
        resp = data_importer_client.post('/engines/1/import_data', headers=headers)
        hash_ = json.loads(resp.body)

        while True:
            resp = data_importer_client.get(
                '/engines/1/import_data?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        called = bool(DataImporter.get_data.called)
        DataImporter.get_data.reset_mock()

        assert hash_ == {'hash': '6342e10bd7dca3240c698aa79c98362e'}
        assert called

    def test_importer_get_running(self, data_importer_client, headers):
        def func(x, y, z):
            sleep(1)
            return {}

        DataImporter.get_data = func
        data_importer_client.post('/engines/1/import_data', headers=headers)

        resp = data_importer_client.get(
            '/engines/1/import_data?hash=6342e10bd7dca3240c698aa79c98362e', headers=headers)
        sleep(0.1)
        DataImporter.get_data = mock.MagicMock()

        assert json.loads(resp.body) == {'status': 'running'}

        while True:
            resp = data_importer_client.get(
                '/engines/1/import_data?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break


    def test_importer_get_done(self, data_importer_client, headers):
        DataImporter.get_data.return_value = 'testing'
        data_importer_client.post('/engines/1/import_data', headers=headers)

        while True:
            resp = data_importer_client.get(
                '/engines/1/import_data?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        resp = data_importer_client.get(
            '/engines/1/import_data?hash=6342e10bd7dca3240c698aa79c98362e', headers=headers)
        DataImporter.get_data = mock.MagicMock()

        assert json.loads(resp.body) == {'status': 'done', 'result': 'testing'}

    def test_importer_get_with_error(self, data_importer_client, headers):
        DataImporter.get_data.side_effect = Exception('testing')
        data_importer_client.post('/engines/1/import_data', headers=headers)

        while True:
            resp = data_importer_client.get(
                '/engines/1/import_data?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        resp = data_importer_client.get(
            '/engines/1/import_data?hash=6342e10bd7dca3240c698aa79c98362e', headers=headers)
        DataImporter.get_data = mock.MagicMock()

        assert json.loads(resp.body) == \
            {'status': 'error', 'result': {'message': 'testing', 'name': 'Exception'}}


@pytest.fixture
def objects_exporter_app(session):
    table_args = {'mysql_engine':'innodb'}
    factory = ModelsFactory('myreco', commons_models_attributes={'__table_args__': table_args},
                            commons_tables_attributes=table_args)
    models = factory.make_all_models('exporter')
    user = {
        'name': 'test',
        'email': 'test',
        'password': 'test',
        'admin': True
    }
    models['users'].insert(session, user)

    store = {
        'name': 'test',
        'country': 'test',
        'configuration': {'data_path': '/test'}
    }
    models['stores'].insert(session, store)

    models['engines_types_names'].insert(session, {'name': 'top_seller'})

    item_type = {
        'name': 'products',
        'stores': [{'id': 1}],
        'schema': {
            'type': 'object',
            'id_names': ['sku'],
            'properties': {'sku': {'type': 'string'}}
        }
    }
    models['items_types'].insert(session, item_type)

    engine = {
        'name': 'Seven Days Top Seller',
        'configuration': {'days_interval': 7, 'data_importer_path': 'tests.integration.fixtures_models.DataImporter'},
        'store_id': 1,
        'type_name_id': 1,
        'item_type_id': 1
    }
    models['engines'].insert(session, engine)

    api = HttpAPI([models['engines'], models['items_types']], session.bind, FakeStrictRedis())
    models['items_types'].associate_all_items(session)

    return api


@pytest.fixture
def objects_exporter_client(objects_exporter_app):
    return Client(objects_exporter_app)


@mock.patch('myreco.engines.types.top_seller.engine.build_csv_readers')
@mock.patch('falconswagger.models.base.random.getrandbits',
    new=mock.MagicMock(return_value=131940827655846590526331314439483569710))
class TestEnginesModelsObjectsExporter(object):

    def test_exporter_post(self, readers_builder, objects_exporter_client, headers):
        products = [{'sku': 'test'}]
        objects_exporter_client.post('/products?store_id=1',
                                    body=json.dumps(products), headers=headers)
        readers_builder.return_value = [[{'value': 1, 'sku': 'test'}]]

        resp = objects_exporter_client.post('/engines/1/export_objects', headers=headers)
        assert json.loads(resp.body) == {'hash': '6342e10bd7dca3240c698aa79c98362e'}

    def test_exporter_get_running(self, readers_builder, objects_exporter_client, headers):
        products = [{'sku': 'test'}]
        objects_exporter_client.post('/products?store_id=1',
                                    body=json.dumps(products), headers=headers)

        readers_builder.return_value = \
            [[{'value': 1, 'sku': 'test{}'.format(i)}] for i in range(1000)]
        objects_exporter_client.post('/engines/1/export_objects', headers=headers)

        resp = objects_exporter_client.get(
            '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e', headers=headers)
        assert json.loads(resp.body) == {'status': 'running'}

    def test_exporter_get_done(self, readers_builder, objects_exporter_client, headers):
        products = [{'sku': 'test'}]
        objects_exporter_client.post('/products?store_id=1',
                                    body=json.dumps(products), headers=headers)

        readers_builder.return_value = [[{'value': 1, 'sku': 'test'}]]
        resp = objects_exporter_client.post('/products/update_filters?store_id=1', headers=headers)
        objects_exporter_client.post('/engines/1/export_objects', headers=headers)

        while True:
            resp = objects_exporter_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        resp = objects_exporter_client.get(
            '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e', headers=headers)

        assert json.loads(resp.body) == {'status': 'done', 'result': [{'test': 1}]}

    def test_exporter_get_with_error(
            self, readers_builder, objects_exporter_client, headers):
        products = [{'sku': 'test'}]
        objects_exporter_client.post('/products?store_id=1',
                                    body=json.dumps(products), headers=headers)

        readers_builder.return_value = [[]]
        objects_exporter_client.post('/engines/1/export_objects', headers=headers)

        while True:
            resp = objects_exporter_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        resp = objects_exporter_client.get(
            '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e', headers=headers)

        assert json.loads(resp.body) == {
            'status': 'error',
            'result': {
                'message': "No data found for engine 'Seven Days Top Seller'",
                'name': 'EngineError'
            }
        }


@mock.patch('myreco.engines.types.top_seller.engine.build_csv_readers')
@mock.patch('falconswagger.models.base.random.getrandbits',
    new=mock.MagicMock(return_value=131940827655846590526331314439483569710))
class TestEnginesModelsObjectsExporterWithImport(object):

    def test_exporter_post_with_import(self, readers_builder, objects_exporter_client, headers):
        readers_builder.return_value = [[{'sku': 'test', 'value': 1}]]
        DataImporter.get_data.return_value = {}
        resp = objects_exporter_client.post('/engines/1/export_objects?import_data=true', headers=headers)
        hash_ = json.loads(resp.body)

        while True:
            resp = objects_exporter_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        called = bool(DataImporter.get_data.called)
        DataImporter.get_data.reset_mock()

        assert hash_ == {'hash': '6342e10bd7dca3240c698aa79c98362e'}
        assert called

    def test_exporter_get_running_with_import(self, readers_builder, objects_exporter_client, headers):
        def func(x, y, z):
            sleep(1)
            return {}

        readers_builder.return_value = [[{'sku': 'test', 'value': 1}]]
        DataImporter.get_data = func
        objects_exporter_client.post('/engines/1/export_objects?import_data=true', headers=headers)

        resp = objects_exporter_client.get(
            '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e', headers=headers)
        DataImporter.get_data = mock.MagicMock()

        assert json.loads(resp.body) == {'status': 'running'}

        while True:
            resp = objects_exporter_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

    def test_exporter_get_done_with_import(self, readers_builder, objects_exporter_client, headers):
        products = [{'sku': 'test'}]
        objects_exporter_client.post('/products?store_id=1',
                                    body=json.dumps(products), headers=headers)

        readers_builder.return_value = [[{'value': 1, 'sku': 'test'}]]
        objects_exporter_client.post('/products/update_filters?store_id=1', headers=headers)
        objects_exporter_client.post('/engines/1/export_objects?import_data=true', headers=headers)

        while True:
            resp = objects_exporter_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        resp = objects_exporter_client.get(
            '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e', headers=headers)

        assert json.loads(resp.body) == {'status': 'done', 'result': [{'test': 1}]}

    def test_exporter_get_with_error_in_import_with_import(
            self, readers_builder, objects_exporter_client, headers):
        DataImporter.get_data.side_effect = Exception('testing')
        objects_exporter_client.post('/engines/1/export_objects?import_data=true', headers=headers)

        while True:
            resp = objects_exporter_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        resp = objects_exporter_client.get(
            '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e', headers=headers)
        DataImporter.get_data = mock.MagicMock()

        assert json.loads(resp.body) == \
            {'status': 'error', 'result': {'message': 'testing', 'name': 'Exception'}}

    def test_exporter_get_with_error_in_export_with_import(
            self, readers_builder, objects_exporter_client, headers):
        products = [{'sku': 'test'}]
        objects_exporter_client.post('/products?store_id=1',
                                    body=json.dumps(products), headers=headers)

        readers_builder.return_value = [[]]
        objects_exporter_client.post('/engines/1/export_objects?import_data=true', headers=headers)

        while True:
            resp = objects_exporter_client.get(
                '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e',
                headers=headers)
            if json.loads(resp.body)['status'] != 'running':
                break

        resp = objects_exporter_client.get(
            '/engines/1/export_objects?hash=6342e10bd7dca3240c698aa79c98362e', headers=headers)

        assert json.loads(resp.body) == {
            'status': 'error',
            'result': {
                'message': "No data found for engine 'Seven Days Top Seller'",
                'name': 'EngineError'
            }
        }