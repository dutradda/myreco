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


from falconswagger.mixins import LoggerMixin


class ItemsIndicesDict(dict):

    def __init__(self, items_indices_map, items_model):
        self.items_model = items_model
        dict.__init__(self, items_indices_map)

    def __len__(self):
        values = self.values()
        if values:
            return max(self.values())
        else:
            return 0


class ItemsIndicesMap(LoggerMixin):

    def __init__(self, items_model):
        self._build_logger()
        self.items_model = items_model
        self.key = items_model.__key__ + '_indices_map'
        self.indices_items_key = items_model.__key__ + '_items_map'

    def get_all(self, session):
        items_indices_map = session.redis_bind.hgetall(self.key)
        items_indices_map = {k.decode(): int(v.decode()) for k, v in items_indices_map.items()}
        return ItemsIndicesDict(items_indices_map, self.items_model)

    def get_indices_items_map(self, session):
        map_ = session.redis_bind.hgetall(self.indices_items_key)
        return {int(k): v.decode() for k, v in map_.items()}

    def update(self, session):
        self._logger.info('Updating...')
        items_indices_map = session.redis_bind.hgetall(self.key)
        indices_items_map = session.redis_bind.hgetall(self.indices_items_key)

        items = self.items_model.get_all(session)
        items_keys = self._build_keys(items)

        new_keys = [key for key in items_keys if key not in items_indices_map]
        old_keys = set([key for key in items_keys if key in items_indices_map])
        keys_to_delete = set(items_indices_map.keys()).difference(old_keys)
        free_indices = [int(v) for k, v in items_indices_map.items() if k in keys_to_delete]

        [items_indices_map.pop(k) for k in keys_to_delete]
        [indices_items_map.pop(i, None) for i in free_indices]

        if old_keys:
            iterable = (int(v) for k, v in items_indices_map.items() if k in old_keys)
            counter = max(iterable)
        else:
            counter = 0

        free_indices_length = len(free_indices)

        for key, index in zip(new_keys[:free_indices_length], free_indices):
            items_indices_map[key] = index
            indices_items_map[index] = key

        for key in new_keys[free_indices_length:]:
            items_indices_map[key] = counter
            indices_items_map[counter] = key
            counter += 1

        if keys_to_delete:
            session.redis_bind.hdel(self.key, *keys_to_delete)
            session.redis_bind.hdel(self.indices_items_key, *free_indices)

        if items_indices_map:
            session.redis_bind.hmset(self.key, items_indices_map)
            session.redis_bind.hmset(self.indices_items_key, indices_items_map)

        return self._format_output(self.get_all(session))

    def _build_keys(self, items):
        return set([self.items_model(item).get_key().encode() for item in items])

    def _format_output(self, output):
        return {'total_items': len(output.keys()), 'maximum_index': max(output.values())}

    def get_items(self, indices, session):
        if indices:
            return [item.decode() for item in \
                session.redis_bind.hmget(self.indices_items_key, indices) if item is not None]
        else:
            return []

    def get_indices(self, ids, session):
        keys = self._build_keys(ids)
        return [index.decode() for index in \
            session.redis_bind.hmget(self.key, keys) if index is not None]
