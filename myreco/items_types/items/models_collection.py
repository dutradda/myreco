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


from myreco.engines.cores.items_indices_map import ItemsIndicesMap
from myreco.engines.cores.filters.filters import BooleanFilterBy
from myreco.utils import build_item_key
from swaggerit.models.orm.jobs import JobsModel
from swaggerit.request import SwaggerRequest
from swaggerit.response import SwaggerResponse
from collections import defaultdict


class ItemsModelsCollection(JobsModel):
    __methods__ = defaultdict(dict)

    async def swagger_insert(self, req, session):
        items_model = self._get_model(req.query)
        if items_model is None:
            return SwaggerResponse(404)

        resp = await items_model.swagger_insert(req, session)
        await self._set_stock_filter(session, items_model)
        return resp

    def _get_model(self, query):
        store_id = query.pop('store_id')
        return self.get_model(build_item_key(self.__item_type__['name'], store_id))

    async def _set_stock_filter(self, session, items_model):
        items_indices_map = await ItemsIndicesMap(items_model).get_all(session)

        if items_indices_map.values():
            items_keys = set(await session.redis_bind.hkeys(items_model.__key__))
            items_indices_keys = set(items_indices_map.keys())
            remaining_keys = items_indices_keys.intersection(items_keys)
            old_keys = items_indices_keys.difference(items_keys)

            items = []
            self._set_stock_item(remaining_keys, items_model, items_indices_map, True, items)
            self._set_stock_item(old_keys, items_model, items_indices_map, False, items)

            stock_filter = BooleanFilterBy(items_model, 'stock')
            await stock_filter.update(session, items)

    def _set_stock_item(self, keys, items_model, items_indices_map, value, items):
        for key in keys:
            item = {}
            items_model.set_instance_ids(item, key)
            item.update({'stock': value, 'index': int(items_indices_map[key])})
            items.append(item)

    async def swagger_update_many(self, req, session):
        items_model = self._get_model(req.query)
        if items_model is None:
            return SwaggerResponse(404)

        resp = await items_model.swagger_update_many(req, session)
        await self._set_stock_filter(session, items_model)
        return resp

    async def swagger_get(self, req, session):
        items_model = self._get_model(req.query)
        if items_model is None:
            return SwaggerResponse(404)

        req = SwaggerRequest(
            req.url, req.method,
            path_params=list(req.path_params.values())[0],
            query=req.query,
            headers=req.headers,
            body=req.body,
            body_schema=req.body_schema,
            context=req.context
        )
        return await items_model.swagger_get(req, session)

    async def swagger_get_all(self, req, session):
        items_model = self._get_model(req.query)
        if items_model is None:
            return SwaggerResponse(404)

        return await items_model.swagger_get_all(req, session)


def build_items_models_collection_schema_base(base_uri, schema, patch_schema, id_names_uri):
    return {
        base_uri: {
            'parameters': [{
                'name': 'Authorization',
                'in': 'header',
                'required': True,
                'type': 'string'
            },{
                'name': 'store_id',
                'in': 'query',
                'required': True,
                'type': 'integer'
            }],
            'post': {
                'parameters': [{
                    'name': 'body',
                    'in': 'body',
                    'required': True,
                    'schema': {
                        'type': 'array',
                        'minItems': 1,
                        'items': schema
                    }
                }],
                'operationId': 'swagger_insert',
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
                        'items': patch_schema
                    }
                }],
                'operationId': 'swagger_update_many',
                'responses': {'200': {'description': 'Updated'}}
            },
            'get': {
                'parameters': [{
                    'name': 'page',
                    'in': 'query',
                    'type': 'integer',
                    'default': 1
                },{
                    'name': 'items_per_page',
                    'in': 'query',
                    'type': 'integer',
                    'default': 1000
                },{
                    'name': 'item_key',
                    'in': 'query',
                    'type': 'array',
                    'items': {'type': 'string'}
                }],
                'operationId': 'swagger_get_all',
                'responses': {'200': {'description': 'Got'}}
            },
        },
        id_names_uri: {
            'parameters': [{
                'name': 'Authorization',
                'in': 'header',
                'required': True,
                'type': 'string'
            },{
                'name': 'store_id',
                'in': 'query',
                'required': True,
                'type': 'integer'
            },{
                'name': 'item_key',
                'in': 'path',
                'required': True,
                'type': 'string'
            }],
            'get': {
                'operationId': 'swagger_get',
                'responses': {'200': {'description': 'Got'}}
            }
        }
    }