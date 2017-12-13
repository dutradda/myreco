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


from collections import defaultdict


class FilterBy(object):

    def __init__(self, items_model, name, is_inclusive=True, skip_values=None, id_names=None):
        self.items_model = items_model
        self.name = name
        self.is_inclusive = is_inclusive
        self.skip_values = set(skip_values) if skip_values is not None else skip_values
        self.id_names = id_names

    async def update(self, session, items):
        filters = self._build_new_filters(items)
        result = await self._update_redis(session, filters)
        return self._format_result(result)

    def _validate_value(self, value):
        return (self.skip_values is None or value not in self.skip_values)

    def _build_filter_key(self, value):
        value = self._cast_filter_key_value(value)
        return '{}_{}_{}_filter'.format(self.items_model.__key__, self.name, value)

    def _cast_filter_key_value(self, value):
        return str(value)

    def _build_new_filters(self, items):
        filters = defaultdict(set)

        for item in items:
            values = self._list_cast(item.get(self.name))

            for value in values:
                if self._validate_value(value):
                    item_key = self.items_model.get_instance_key(item)
                    filter_key = self._build_filter_key(value)
                    filters[filter_key].add(item_key)

        return filters

    def _list_cast(self, obj):
        return obj if isinstance(obj, list) or isinstance(obj, tuple) else (obj,)

    async def _update_redis(self, session, new_filters):
        for filter_key, new_filter in new_filters.items():
            old_filter = set(await session.redis_bind.smembers(filter_key))
            old_filter -= new_filter

            if new_filter:
                await session.redis_bind.sadd(filter_key, *new_filter)

            if old_filter:
                await session.redis_bind.srem(filter_key, *old_filter)

        return {'filters_quantity': len(new_filters)}

    def _format_result(self, result):
        return result

    async def get_filtering_keys(self, session, var_value):
        values = self._list_cast(var_value)
        return set([self._build_filter_key(value) for value in values])


class BooleanFilterBy(FilterBy):

    def _validate_value(self, value):
        return value

    def _cast_filter_key_value(self, value):
        return 'bool'


class ObjectFilterBy(FilterBy):

    def _cast_filter_key_value(self, value):
        value = [value[id_name] for id_name in self.id_names]
        return repr(tuple([
                id_ for _, id_ in sorted(zip(self.id_names, value), key=lambda x: x[0])
            ]))

    def _validate_value(self, value):
        return value is not None and super()._validate_value(value)


class FilterOf(FilterBy):

    async def get_filtering_keys(self, session, items_keys):
        items = await self.items_model.get(session, items_keys)
        return set([
                self._build_filter_key(value)
                    for item in items
                        for value in self._list_cast(item.get(self.name, []))
        ])


class BooleanFilterOf(FilterOf, BooleanFilterBy):
    pass


class ObjectFilterOf(FilterOf, ObjectFilterBy):
    pass
