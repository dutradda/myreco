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


from myreco.engines.cores.items_indices_map import ItemsIndicesMap
from myreco.engines.cores.filters.filters import BooleanFilterBy
from falconswagger.models.orm.redis import ModelRedisMeta
from falcon import HTTPNotFound


class ItemsModelBaseMeta(ModelRedisMeta):

    def __init__(cls, name, bases, attrs):
        ModelRedisMeta.__init__(cls, name, bases, attrs)
        cls.index = None

    def get(cls, session, ids=None, limit=None, offset=None, **kwargs):
        items_per_page, page = kwargs.get('items_per_page', 1000), kwargs.get('page', 1)
        limit = items_per_page * page
        offset = items_per_page * (page-1)
        return ModelRedisMeta.get(cls, session, ids=ids, limit=limit, offset=offset, **kwargs)

    def get_all(cls, session, **kwargs):
        return ModelRedisMeta.get(cls, session, **kwargs)


class ItemsCollectionsModelBaseMeta(ModelRedisMeta):

    def insert(cls, session, objs, **kwargs):
        items_model = cls._get_model(kwargs)
        ret_values = items_model.insert(session, objs, **kwargs)
        cls._set_stock_filter(session, items_model)
        return ret_values

    def _get_model(cls, kwargs):
        store_id = kwargs.pop('store_id')
        items_model = cls.__models__.get(store_id)
        if items_model is None:
            raise HTTPNotFound()
        return items_model

    def _set_stock_filter(cls, session, items_model):
        items_keys = set(session.redis_bind.hkeys(items_model.__key__))
        items_indices_map = ItemsIndicesMap(items_model).get_all(session)
        items_indices_keys = set(items_indices_map.keys())
        remaining_keys = items_indices_keys.intersection(items_keys)
        old_keys = items_indices_keys.difference(items_keys)

        items = []
        cls._set_stock_item(remaining_keys, items_model, items_indices_map, True, items)
        cls._set_stock_item(old_keys, items_model, items_indices_map, False, items)

        stock_filter = BooleanFilterBy(items_model, 'stock')
        stock_filter.update(session, items)

    def _set_stock_item(cls, keys, items_model, items_indices_map, value, items):
        for key in keys:
            item = {}
            items_model.set_ids(item, key)
            item.update({'stock': value, 'index': int(items_indices_map[key])})
            items.append(item)

    def update(cls, session, objs, ids=None, **kwargs):
        items_model = cls._get_model(kwargs)
        ret_values = items_model.update(session, objs, ids=ids, **kwargs)
        cls._set_stock_filter(session, items_model)
        return ret_values

    def get(cls, session, ids=None, limit=None, offset=None, **kwargs):
        items_model = cls._get_model(kwargs)
        return items_model.get(session, ids=ids, limit=limit, offset=offset, **kwargs)
