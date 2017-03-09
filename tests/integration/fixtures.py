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


from myreco.engines.cores.top_seller.core import TopSellerEngineCore
from myreco.engines.cores.utils import build_engine_data_path
from unittest import mock
from jsonschema import ValidationError
from os import makedirs
import os.path
import gzip
import ujson
import asyncio


def CoroMock():
    coro = mock.MagicMock(name="CoroutineResult")
    corofunc = mock.MagicMock(name="CoroutineFunction", side_effect=asyncio.coroutine(coro))
    corofunc.coro = coro
    return corofunc


class EngineCoreTest(TopSellerEngineCore):

    def get_data(self, session):
        asyncio.run_coroutine_threadsafe(asyncio.sleep(0.5), session.loop).result()
        data_path = build_engine_data_path(self.engine)
        if not os.path.isdir(data_path):
            makedirs(data_path)

        data = [{'item_key': '2|test2', 'value': 1},
                {'item_key': '1|test1', 'value': 3},
                {'item_key': '3|test3', 'value': 2}]
        data = map(ujson.dumps, data)
        data = '\n'.join(data)

        filename_prefix = 'top_seller'
        file_ = gzip.open(os.path.join(data_path, filename_prefix) + '-000000001.gz', 'wt')
        file_.write(data)
        file_.close()
        return {'lines_count': 3}


class EngineCoreTestWithVars(EngineCoreTest):
    __configuration_schema__ = {
        "type": "object",
        "required": ["item_id_name", "aggregators_ids_name"],
        "properties": {
            "item_id_name": {"type": "string"},
            "aggregators_ids_name": {"type": "string"}
        }
    }

    def get_variables(self):
        item_id_name = self.engine['configuration']['item_id_name']
        aggregators_ids_name = self.engine['configuration']['aggregators_ids_name']
        item_type_schema_props = self.engine['item_type']['schema']['properties']
        return [{
            'name': item_id_name,
            'schema': item_type_schema_props[item_id_name]
        },{
            'name': aggregators_ids_name,
            'schema': item_type_schema_props[aggregators_ids_name]
        }]

    def _validate_config(self):
        item_id_name = self.engine['configuration']['item_id_name']
        aggregators_ids_name = self.engine['configuration']['aggregators_ids_name']
        item_type_schema_props = self.engine['item_type']['schema']['properties']
        message = "Configuration key '{}' not in item_type schema"

        if item_id_name not in item_type_schema_props:
            raise ValidationError(message.format('item_id_name'),
                instance=self.engine['configuration'], schema=item_type_schema_props)

        elif aggregators_ids_name not in item_type_schema_props:
            raise ValidationError(message.format('aggregators_ids_name'),
                instance=self.engine['configuration'], schema=item_type_schema_props)

    async def _build_items_vector(self):
        pass

    get_items = CoroMock()


class MyProducts(object):
    test = 1

    @classmethod
    def insert(cls, session, prods):
        for prod in prods:
            v = prod.get('filter_integer')
            if v is not None:
                prod['base_prop'] = v + cls.test

        return type(cls).insert(cls, session, prods)
