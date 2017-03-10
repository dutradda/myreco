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


from myreco.factory import ModelsFactory
from myreco.api import MyrecoAPI
from swaggerit.models.orm.session import Session
from swaggerit.models.orm.binds import ElSearchBind
from unittest import mock
from sqlalchemy import create_engine
from aioredis import create_redis
from base64 import b64encode
from sqlalchemy.schema import DDL
import sqlalchemy as sa
import pytest
import pymysql
import asyncio
import uvloop


@pytest.fixture(scope='session')
def base_model(api):
    return api.models_factory.base_model


@pytest.fixture(scope='session')
def models(api):
    return api.all_models


@pytest.fixture(scope='session')
def loop():
    loop = uvloop.new_event_loop()
    # loop = asyncio.new_event_loop() # just for debugging
    asyncio.set_event_loop(loop)
    return loop


@pytest.fixture(scope='session')
def redis(variables, loop):
    coro = create_redis(
        (variables['redis']['host'], variables['redis']['port']),
        db=variables['redis']['db'],
        loop=loop
    )
    return loop.run_until_complete(coro)


@pytest.fixture(scope='session')
def pymysql_conn(variables):
    database = variables['database'].pop('database')
    conn = pymysql.connect(**variables['database'])

    with conn.cursor() as cursor:
        try:
            cursor.execute('drop database {};'.format(database))
        except:
            pass
        cursor.execute('create database {};'.format(database))
        cursor.execute('use {};'.format(database))
    conn.commit()
    variables['database']['database'] = database

    return conn


@pytest.fixture(scope='session')
def engine(variables, pymysql_conn):
    if variables['database']['password']:
        url = 'mysql+pymysql://{user}:{password}'\
            '@{host}:{port}/{database}'.format(**variables['database'])
    else:
        variables['database'].pop('password')
        url = 'mysql+pymysql://{user}'\
            '@{host}:{port}/{database}'.format(**variables['database'])
        variables['database']['password'] = None

    return create_engine(url)


@pytest.fixture(scope='session')
def elsearch(variables, loop):
    es = ElSearchBind(**variables['elsearch'])
    loop.run_until_complete(es.create_index())
    return es


@pytest.fixture(scope='session')
def api(engine, redis, elsearch, loop):
    api = MyrecoAPI(sqlalchemy_bind=engine, redis_bind=redis,
                    elsearch_bind=elsearch, title='Myreco API',
                    loop=loop, debug=True, type_='objects_exporter')

    class ModelTest(api.models_factory.base_model):
        __tablename__ = 'test'
        id = sa.Column(sa.Integer, primary_key=True)

        @classmethod
        async def do_nothing(cls, req, resp, **kwargs):
            return cls._build_response(200)

        __swagger_json__ = {
            'paths': {
                '/testing': {
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
                '/testing/{id}': {
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
        }

    api.add_model(ModelTest)
    api.router._resources = api.router._resources[-8:] + api.router._resources[:-8]
    return api


@pytest.fixture
def client(api, test_client):
    return test_client(api)


@pytest.fixture
def session(variables, redis, engine, pymysql_conn, base_model, loop, elsearch):
    base_model.metadata.bind = engine
    base_model.metadata.create_all()

    with pymysql_conn.cursor() as cursor:
        cursor.execute('SET FOREIGN_KEY_CHECKS = 0;')
        for table in base_model.metadata.tables.values():
            cursor.execute('delete from {};'.format(table))

            try:
                cursor.execute('alter table {} auto_increment=1;'.format(table))
            except:
                pass
        cursor.execute('SET FOREIGN_KEY_CHECKS = 1;')
        cursor.execute('ALTER TABLE slots_filters CHANGE id id INT AUTO_INCREMENT;')
        cursor.execute('ALTER TABLE slots_variables CHANGE id id INT AUTO_INCREMENT;')

    pymysql_conn.commit()
    loop.run_until_complete(redis.flushdb())
    loop.run_until_complete(elsearch.flush_index())
    session = Session(bind=engine, redis_bind=redis, elsearch_bind=elsearch, loop=loop)
    yield session
    session.close()


@pytest.fixture
def headers():
    return {
        'Authorization': 'Basic ' + b64encode('test:test'.encode()).decode(),
        'Content-Type': 'application/json'
    }


@pytest.fixture
def headers_without_content_type():
    return {
        'Authorization': 'Basic ' + b64encode('test:test'.encode()).decode()
    }
