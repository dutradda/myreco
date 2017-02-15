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


class ItemsIndicesDict(dict):

    def __init__(self, items_indices_map, items_model):
        self.items_model = items_model
        dict.__init__(self, items_indices_map)

    def __len__(self):
        values = self.values()
        if values:
            return max(self.values())+1
        else:
            return 0

    def get(self, key, default=None):
        if isinstance(key, str):
            key = key.encode()
        v = dict.get(self, key, default)
        return v


class ItemsIndicesMap(object):

    def __init__(self, items_model):
        self.items_model = items_model
        self.key = items_model.__key__ + '_indices_map'
        self.indices_items_key = items_model.__key__ + '_items_map'
        self.length_key = items_model.__key__ + '_indices_length'

    async def get_all(self, session):
        items_indices_map = await session.redis_bind.hgetall(self.key)
        items_indices_map = dict([(k, int(v.decode())) for k, v in items_indices_map.items()])
        return ItemsIndicesDict(items_indices_map, self.items_model)

    async def get_indices_items_map(self, session):
        map_ = await session.redis_bind.hgetall(self.indices_items_key)
        return {int(k): v.decode() for k, v in map_.items()}

    async def update(self, session):
        items_indices_map = await self.get_all(session)
        indices_items_map = await self.get_indices_items_map(session)

        items = await self.items_model.get_all(session)
        items_keys = self._build_keys(items)

        new_keys = [key for key in items_keys if key not in items_indices_map]
        old_keys = set([key for key in items_keys if key in items_indices_map])
        keys_to_delete = set(items_indices_map.keys()).difference(old_keys)
        free_indices = [v for k, v in items_indices_map.items() if k in keys_to_delete]

        [items_indices_map.pop(k) for k in keys_to_delete]
        [indices_items_map.pop(i) for i in free_indices]

        free_indices_length = len(free_indices)

        for key, index in zip(new_keys[:free_indices_length], free_indices):
            items_indices_map[key] = index
            indices_items_map[index] = key

        counter = len(items_indices_map)

        for key in new_keys[free_indices_length:]:
            items_indices_map[key] = counter
            indices_items_map[counter] = key
            counter += 1

        if keys_to_delete:
            await session.redis_bind.hdel(self.key, *keys_to_delete)
            await session.redis_bind.hdel(self.indices_items_key, *free_indices)

        if items_indices_map:
            await session.redis_bind.hmset_dict(self.key, items_indices_map)
            await session.redis_bind.hmset_dict(self.indices_items_key, indices_items_map)
            await session.redis_bind.set(self.length_key, len(items_indices_map))

        return self._format_output(await self.get_all(session))

    def _build_keys(self, items):
        return set([self.items_model.get_instance_key(item) for item in items])

    def _format_output(self, output):
        maximum_index = max(output.values()) if len(output.keys()) else None
        ret = {'total_items': len(output.keys()), 'maximum_index': maximum_index}
        return ret

    async def get_items(self, indices, session):
        if indices:
            return [item for item in \
                await session.redis_bind.hmget(self.indices_items_key, *indices) if item is not None]
        else:
            return []

    async def get_indices(self, keys, session):
        return [int(index.decode()) for index in \
            await session.redis_bind.hmget(self.key, *keys) if index is not None]

    async def get_length(self, session):
        len_ = await session.redis_bind.get(self.length_key)
        return None if len_ is None else int(len_)
