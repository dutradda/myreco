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


from myreco.exceptions import ModuleObjectLoaderError
from swaggerit.models.orm.factory import FactoryOrmModels
from swaggerit.utils import get_model, get_swagger_json
from importlib import import_module
from copy import deepcopy
import asyncio
import os
import re


class ModuleObjectLoader(object):
    _objects = dict()

    @classmethod
    def load(cls, config):
        key = '{}.{}'.format(config['path'], config['object_name'])
        obj = cls._objects.get(key)

        if obj is None:
            try:
                module = import_module(config['path'])
                obj = getattr(module, config['object_name'])
                cls._objects[key] = obj

            except Exception as error:
                raise ModuleObjectLoaderError(
                    "Error loading module '{}.{}'.\nError Class: {}. Error Message: {}".format(
                        config['path'], config['object_name'],
                        error.__class__.__name__, str(error)
                    )
                )

        return obj


def build_item_key(name, *args):
    name = name.lower().replace(' ', '_')
    for arg in args:
        name += '_{}'.format(arg)
    return name


def get_items_model(item_type, store_id):
    item_types_model = get_model('item_types')
    return item_types_model.get_store_items_model(item_type, store_id)


def build_class_name(*names):
    final_name = ''
    for name in names:
        name = name.split(' ')
        for in_name in name:
            final_name += in_name.capitalize()

    return final_name + 'Model'


def extend_swagger_json(original, current_filename, swagger_json_name=None, by_method=False):
    swagger_json = deepcopy(original)
    additional_swagger = get_swagger_json(current_filename, swagger_json_name)

    if by_method:
        for path_name, addit_path in additional_swagger['paths'].items():
            if path_name in swagger_json['paths']:
                path = swagger_json['paths'][path_name]

                for method_name, method in addit_path.items():
                    if method_name in path:
                        path[method_name].update(method)
                    else:
                        path[method_name] = method

    else:
        swagger_json['paths'].update(additional_swagger['paths'])

    definitions = swagger_json.get('definitions')
    additional_definitions = additional_swagger.get('definitions')

    if additional_definitions:
        if definitions:
            definitions.update(additional_definitions)
        else:
            swagger_json['definitions'] = additional_definitions

    return swagger_json


def build_engine_object_key(engine_object):
    return '{}_{}_{}'.format(
        engine_object['strategy']['name'],
        engine_object['type'],
        engine_object['id']
    )


def makedirs(dir_):
    try:
        os.makedirs(dir_)
    except OSError as e:
        if os.errno.EEXIST != e.errno:
            raise


def run_coro(coro, session):
    if not asyncio.iscoroutine(coro):
        coro = _convert_future_to_coro(coro)

    if session.loop.is_running():
        return asyncio.run_coroutine_threadsafe(coro, session.loop).result()
    else:
        return session.loop.run_until_complete(coro)

async def _convert_future_to_coro(fut):
    return await fut
