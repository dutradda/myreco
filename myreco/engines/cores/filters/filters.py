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
from zlib import decompress, compress
from collections import defaultdict
import numpy as np


class FilterBaseBy(object):
    dtype = np.bool

    def __init__(self, items_model, name, is_inclusive=True, id_names=None, skip_values=None):
        self.key = items_model.__key__ + '_' + name + '_filter'
        self.items_model = items_model
        self.name = name
        self.is_inclusive = is_inclusive
        self.id_names = id_names
        self.skip_values = set(skip_values) if skip_values is not None else skip_values

    def _unpack_filter(self, filter_, new_size=None):
        filter_ = np.fromstring(filter_, dtype=type(self).dtype)

        if new_size is not None:
            self._resize_vector(filter_, new_size)

        return filter_

    def _resize_vector(self, vector, new_size):
        if vector.size != new_size:
            vector.resize(new_size, refcheck=False)

    def _filter(self, filter_, items_vector):
        if not self.is_inclusive:
            filter_ = np.invert(filter_)

        items_vector *= filter_

    def _pack_filter(self, filter_):
        return filter_.tobytes()

    def _build_empty_array(self, size):
        return np.zeros(size, dtype=np.bool)

    def _build_output_ids(self, item):
        ids = [item.get(id_name) for id_name in self.items_model.__id_names__]
        return ' | '.join([str(i) for i in ids])

    def _list_cast(self, obj):
        return obj if isinstance(obj, list) or isinstance(obj, tuple) else (obj,)

    def _not_skip_value(self, value):
        return (self.skip_values is None or value not in self.skip_values)


class BooleanFilterBy(FilterBaseBy):

    async def update(self, session, items, array_size):
        filter_ = self._build_empty_array(array_size)

        for item in items:
            value = item.get(self.name)
            if value is not None and self._not_skip_value(value):
                filter_[item['index']] = value

        await session.redis_bind.set(self.key, self._pack_filter(filter_))
        return {'true_values': np.nonzero(filter_)[0].size}

    async def filter(self, session, items_vector, *args, **kwargs):
        filter_ = await session.redis_bind.get(self.key)
        if filter_ is not None:
            filter_ = self._unpack_filter(filter_, items_vector.size)
            self._filter(filter_, items_vector)


class MultipleFilterBy(FilterBaseBy):

    async def filter(self, session, items_vector, ids):
        ids = self._list_cast(ids)

        if ids:
            filters = await session.redis_bind.hmget(self.key, *ids)
            filters = [self._unpack_filter(filter_, items_vector.size)
                        for filter_ in filters if filter_ is not None]
            final_filter = np.zeros(items_vector.size, dtype=np.bool)

            for filter_ in filters:
                final_filter += filter_

            self._filter(final_filter, items_vector)

    async def update(self, session, items, array_size):
        filter_map = defaultdict(list)
        set_data = dict()
        size = array_size

        [self._update_filter(filter_map, item) for item in items]
        for filter_id, items_indices in filter_map.items():
            filter_ = self._build_filter_array(items_indices, size)
            set_data[filter_id] = self._pack_filter(filter_)

        if set_data:
            await session.redis_bind.hmset_dict(self.key, set_data)

        return {'filters_quantity': len(set_data)}

    def _update_filter(self, filter_map, value, item):
        if value is not None and self._not_skip_value(value):
            filter_map[value].append(item['index'])

    def _build_filter_array(self, items_indices, size):
        filter_ = self._build_empty_array(size)
        filter_[np.array(items_indices, dtype=np.int32)] = True
        return filter_


class SimpleFilterBy(MultipleFilterBy):

    def _update_filter(self, filter_map, item):
        MultipleFilterBy._update_filter(self, filter_map, item.get(self.name), item)


class ObjectFilterBy(MultipleFilterBy):

    def _update_filter(self, filter_map, item):
        value = self._get_id_from_property(item)
        MultipleFilterBy._update_filter(self, filter_map, value, item)

    def _get_id_from_property(self, item):
        property_obj = item.get(self.name)
        if property_obj is not None:
            ids = [property_obj[id_name] for id_name in self.id_names]
            return repr(tuple([id_ for _, id_ in sorted(zip(self.id_names, ids), key=lambda x: x[0])]))

    async def filter(self, session, items_vector, properties):
        properties = self._list_cast(properties)
        ids = [self._get_id_from_property({self.name: prop}) for prop in properties]
        await MultipleFilterBy.filter(self, session, items_vector, ids)


class ArrayFilterBy(SimpleFilterBy):

    def _update_filter(self, filter_map, item):
        property_list = item.get(self.name, [])
        for value in property_list:
            MultipleFilterBy._update_filter(self, filter_map, value, item)


class SimpleFilterOf(SimpleFilterBy):

    async def filter(self, session, items_vector, items_keys):
        items = await self.items_model.get(session, items_keys)
        filter_ids = [item.get(self.name) for item in items]
        await SimpleFilterBy.filter(self, session, items_vector, filter_ids)


class ObjectFilterOf(ObjectFilterBy):

    async def filter(self, session, items_vector, items_keys):
        items = await self.items_model.get(session, items_keys)
        filter_ids = [item.get(self.name) for item in items]
        await ObjectFilterBy.filter(self, session, items_vector, filter_ids)


class ArrayFilterOf(ArrayFilterBy):

    async def filter(self, session, items_vector, items_keys):
        items = await self.items_model.get(session, items_keys)
        filter_ids = []
        [filter_ids.extend(item[self.name]) for item in items]
        await ArrayFilterBy.filter(self, session, items_vector, filter_ids)


class IndexFilterOf(FilterBaseBy):

    async def update(self, *args, **kwargs):
        return 'OK'

    async def filter(self, session, items_vector, items_keys):
        items_indices_map = ItemsIndicesMap(self.items_model)
        indices = await items_indices_map.get_indices(items_keys, session)
        if indices:
            indices = np.array(indices, dtype=np.int32)
            self._filter_by_indices(items_vector, indices)

    def _filter_by_indices(self, items_vector, indices):
        if max(indices) >= len(items_vector):
            indices = np.where(indices < len(items_vector))

        if not self.is_inclusive:
            items_vector[indices] = 0
        else:
            items_vector[:] = 0
            items_vector[indices] = 1


class IndexFilterByPropertyOf(SimpleFilterOf, IndexFilterOf):
    dtype = np.int32

    def _build_filter_array(self, items_indices, size):
        return np.array(items_indices, dtype=np.int32)

    async def filter(self, session, items_vector, items_keys):
        items = await self.items_model.get(session, items_keys)
        filter_ids = [item[self.name] for item in items]
        if filter_ids:
            filters = await session.redis_bind.hmget(self.key, *filter_ids)
            filters = [self._unpack_filter(filter_) for filter_ in filters if filter_ is not None]

            if filters:
                indices = np.concatenate(filters)
                self._filter_by_indices(items_vector, indices)
