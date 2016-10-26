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


from zlib import decompress, compress
from collections import defaultdict
import numpy as np


class FilterBaseBy(object):

    def __init__(self, items_model, name, is_inclusive=True, id_names=None):
        self.key = items_model.__key__ + '_' + name + '_filter'
        self.items_model = items_model
        self.name = name
        self.is_inclusive = is_inclusive
        self.id_names = id_names

    def _unpack_filter(self, filter_, new_size):
        filter_ = np.fromstring(decompress(filter_), dtype=np.bool)
        self._resize_vector(filter_, new_size)
        return filter_

    def _resize_vector(self, vector, new_size):
        if vector.size != new_size:
            vector.resize(new_size, refcheck=False)

    def _filter(self, filter_, rec_vector):
        if not self.is_inclusive:
            filter_ = np.invert(filter_)

        if np.sum(filter_):
            rec_vector *= filter_

    def _pack_filter(self, filter_):
        return compress(filter_.tobytes())

    def _build_empty_array(self, size):
        return np.zeros(size, dtype=np.bool)

    def _build_output_ids(self, item):
        return ' | '.join([str(i) for i in item.get_ids_values()])

    def _list_cast(self, obj):
        return obj if isinstance(obj, list) or isinstance(obj, tuple) else (obj,)


class BooleanFilterBy(FilterBaseBy):

    def update(self, session, items):
        filter_ret = {}
        filter_ = self._build_empty_array(len(items))

        for item in items:
            value = item.get_(self.name)
            if value is not None:
                filter_[item.index] = value
                filter_ret[self._build_output_ids(item)] = value

        session.redis_bind.set(self.key, self._pack_filter(filter_))
        return filter_ret

    def filter(self, session, rec_vector, *args, **kwargs):
        filter_ = session.redis_bind.get(self.key)
        if filter_ is not None:
            filter_ = self._unpack_filter(filter_, rec_vector.size)
            self._filter(filter_, rec_vector)


class MultipleFilterBy(FilterBaseBy):

    def filter(self, session, rec_vector, ids):
        ids = self._list_cast(ids)
        filters = session.redis_bind.hmget(self.key, ids)
        filters = [self._unpack_filter(filter_, rec_vector.size)
                    for filter_ in filters if filter_ is not None]
        final_filter = np.zeros(rec_vector.size, dtype=np.bool)

        for filter_ in filters:
            final_filter = np.logical_or(final_filter, filter_)

        self._filter(final_filter, rec_vector)

    def update(self, session, items):
        filter_ret = defaultdict(list)
        filter_map = defaultdict(list)
        set_data = dict()
        size = len(items)

        [self._update_filter(filter_map, filter_ret, item) for item in items]

        for filter_id, items_indices in filter_map.items():
            filter_ = self._build_empty_array(size)
            filter_[np.array(items_indices, dtype=np.int32)] = True
            set_data[filter_id] = self._pack_filter(filter_)

        if set_data:
            session.redis_bind.hmset(self.key, set_data)

        return filter_ret

    def _update_filter(self, filter_map, filter_ret, value, item):
        if value is not None:
            filter_map[value].append(item.index)
            filter_ret[value].append(self._build_output_ids(item))


class SimpleFilterBy(MultipleFilterBy):

    def _update_filter(self, filter_map, filter_ret, item):
        MultipleFilterBy._update_filter(self, filter_map, filter_ret, item.get_(self.name), item)


class ObjectFilterBy(MultipleFilterBy):

    def _update_filter(self, filter_map, filter_ret, item):
        value = self._get_id_from_property(item)
        MultipleFilterBy._update_filter(self, filter_map, filter_ret, value, item)

    def _get_id_from_property(self, item):
        property_obj = item.get_(self.name)
        if property_obj is not None:
            ids = [property_obj[id_name] for id_name in self.id_names]
            return repr(tuple([id_ for _, id_ in sorted(zip(self.id_names, ids), key=lambda x: x[0])]))

    def filter(self, session, rec_vector, properties):
        properties = self._list_cast(properties)
        ids = [self._get_id_from_property(self.items_model({self.name: prop})) for prop in properties]
        MultipleFilterBy.filter(self, session, rec_vector, ids)


class ArrayFilterBy(SimpleFilterBy):

    def _update_filter(self, filter_map, filter_ret, item):
        property_list = item.get_(self.name, [])
        for value in property_list:
            MultipleFilterBy._update_filter(self, filter_map, filter_ret, value, item)


class SimpleFilterOf(SimpleFilterBy):

    def filter(self, session, rec_vector, items_ids):
        items = self.items_model.get(session, items_ids)
        filter_ids = [item[self.name] for item in items]
        SimpleFilterBy.filter(self, session, rec_vector, filter_ids)


class ObjectFilterOf(ObjectFilterBy):

    def filter(self, session, rec_vector, items_ids):
        items = self.items_model.get(session, items_ids)
        filter_ids = [item.get(self.name) for item in items]
        ObjectFilterBy.filter(self, session, rec_vector, filter_ids)


class ArrayFilterOf(ArrayFilterBy):

    def filter(self, session, rec_vector, items_ids):
        items = self.items_model.get(session, items_ids)
        filter_ids = []
        [filter_ids.extend(item[self.name]) for item in items]
        ArrayFilterBy.filter(self, session, rec_vector, filter_ids)
