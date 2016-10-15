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


class ItemsIndicesDict(dict):

    def __init__(self, items_indices_map, item_model):
        self._item_model = item_model
        dict.__init__(self, items_indices_map)

    def get(self, keys, default=None):
        key = self._item_model(keys).get_key().encode()
        return dict.get(self, key, default)


class ItemsIndicesMap(object):

    def __init__(self, session, item_model):
        self.session = session
        self.item_model = item_model

    def get_all(self):
        items_indices_map = self.session.redis_bind.hgetall(self._build_key())
        return ItemsIndicesDict(items_indices_map, self.item_model)

    def _build_key(self):
        return self.item_model.__key__ + '_indices_map'

    def get_indices_items_map(self):
        map_ = self.session.redis_bind.hgetall(self._build_indices_items_key())
        return {int(k): v.decode() for k, v in map_.items()}

    def _build_indices_items_key(self):
        return self.item_model.__key__ + '_items_map'

    def update(self):
        redis_key = self._build_key()
        indices_items_key = self._build_indices_items_key()
        items_indices_map = self.session.redis_bind.hgetall(redis_key)
        indices_items_map = self.session.redis_bind.hgetall(indices_items_key)

        items = self.item_model.get(self.session)
        items_keys = set([self.item_model(item).get_key().encode() for item in items])

        new_keys = [key for key in items_keys if key not in items_indices_map]
        old_keys = set([key for key in items_keys if key in items_indices_map])
        keys_to_delete = set(items_indices_map.keys()).difference(old_keys)
        free_indices = [int(v) for k, v in items_indices_map.items() if k in keys_to_delete]

        [items_indices_map.pop(k) for k in keys_to_delete]
        [indices_items_map.pop(i) for i in free_indices]

        if old_keys:
            iterable = (int(v) for k, v in items_indices_map.items() if k in old_keys)
            counter = max(iterable)
        else:
            counter = 0

        i = 0
        for i, key, index in enumerate(zip(new_keys, free_indices)):
            items_indices_map[key] = index
            indices_items_map[index] = key

        for key in new_keys[i:]:
            items_indices_map[key] = counter
            indices_items_map[counter] = key
            counter += 1

        if keys_to_delete:
            self.session.redis_bind.hdel(redis_key, *keys_to_delete)
            self.session.redis_bind.hdel(indices_items_key, *free_indices)

        if items_indices_map:
            self.session.redis_bind.hmset(redis_key, items_indices_map)
            self.session.redis_bind.hmset(indices_items_key, indices_items_map)
