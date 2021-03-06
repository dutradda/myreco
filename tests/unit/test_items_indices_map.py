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


import asyncio
from collections import OrderedDict
from types import MethodType
from unittest import mock

import pytest


def CoroMock():
    coro = mock.MagicMock(name="CoroutineResult")
    corofunc = mock.MagicMock(name="CoroutineFunction", side_effect=asyncio.coroutine(coro))
    corofunc.coro = coro
    return corofunc


@pytest.fixture
def indices_map(monkeypatch):
    items_model = mock.MagicMock()
    items_model.get_keys = CoroMock()
    items_model.get_keys.coro.return_value = [b'a', b'b', b'c', b'd']
    items_model.__key__ = 'test'

    monkeypatch.setattr('builtins.dict', OrderedDict)
    from myreco.item_types.indices_map import ItemsIndicesMap, ItemsIndicesDict

    m = ItemsIndicesMap(items_model)

    async def _get_items_keys_mock(self, session):
        return await self.items_model.get_keys(session)

    m._get_items_keys = MethodType(_get_items_keys_mock, m)
    return m


@pytest.fixture
def session_first_update():
    m = mock.MagicMock()
    m.redis_bind.hgetall = CoroMock()
    m.redis_bind.hgetall.coro.side_effect = [{}, {}, {}]
    m.redis_bind.hmset_dict = CoroMock()
    m.redis_bind.set = CoroMock()
    m.redis_bind.hdel = CoroMock()
    return m


class TestItemsIndicesMapFirstUpdate(object):

    async def test_if_update_builds_items_indices_map_correctly(self, indices_map, session_first_update):
        await indices_map.update(session_first_update)
        assert session_first_update.redis_bind.hmset_dict.coro.call_args_list[0] == \
            mock.call('test_indices_map', {b'a': 0, b'b': 1, b'c': 2, b'd': 3})

    async def test_if_update_builds_indices_items_map_correctly(self, indices_map, session_first_update):
        await indices_map.update(session_first_update)
        assert session_first_update.redis_bind.hmset_dict.coro.call_args_list[1] == \
            mock.call('test_items_map', {0: b'a', 1: b'b', 2: b'c', 3: b'd'})

    async def test_if_update_builds_length_correctly(self, indices_map, session_first_update):
        await indices_map.update(session_first_update)
        assert session_first_update.redis_bind.set.coro.call_args_list == \
            [mock.call('test_indices_length', 4)]

    async def test_if_update_dont_calls_hdel(self, indices_map, session_first_update):
        await indices_map.update(session_first_update)
        assert session_first_update.redis_bind.hdel.coro.call_args_list == []


@pytest.fixture
def session(session_first_update):
    session_first_update.redis_bind.hgetall.coro.side_effect = [
        OrderedDict([(b'a', b'0'), (b'b', b'1'), (b'c', b'2'), (b'd', b'3')]),
        OrderedDict([(b'0', b'a'), (b'1', b'b'), (b'2', b'c'), (b'3', b'd')]),
        {}
    ]
    return session_first_update


class TestItemsIndicesMapWithSameItems(object):

    async def test_if_update_builds_items_indices_map_correctly(self, indices_map, session):
        await indices_map.update(session)
        assert session.redis_bind.hmset_dict.coro.call_args_list[0] == \
            mock.call('test_indices_map', {b'a': 0, b'b': 1, b'c': 2, b'd': 3})

    async def test_if_update_builds_indices_items_map_correctly(self, indices_map, session):
        await indices_map.update(session)
        assert session.redis_bind.hmset_dict.coro.call_args_list[1] == \
            mock.call('test_items_map', {0: 'a', 1: 'b', 2: 'c', 3: 'd'})

    async def test_if_update_builds_length_correctly(self, indices_map, session):
        await indices_map.update(session)
        assert session.redis_bind.set.coro.call_args_list == \
            [mock.call('test_indices_length', 4)]

    async def test_if_update_dont_calls_hdel(self, indices_map, session):
        await indices_map.update(session)
        assert session.redis_bind.hdel.coro.call_args_list == []


@pytest.fixture
def indices_map_new_item(indices_map):
    ret = indices_map.items_model.get_keys.coro.return_value
    ret.append(b'e')
    return indices_map


class TestItemsIndicesMapWithNewItem(object):

    async def test_if_update_builds_items_indices_map_correctly(self, indices_map_new_item, session):
        await indices_map_new_item.update(session)
        assert session.redis_bind.hmset_dict.coro.call_args_list[0] == \
            mock.call('test_indices_map', {b'a': 0, b'b': 1, b'c': 2, b'd': 3, b'e': 4})


    async def test_if_update_builds_indices_items_map_correctly_2(self, indices_map_new_item, session):
        await indices_map_new_item.update(session)
        assert session.redis_bind.hmset_dict.coro.call_args_list[1] == \
            mock.call('test_items_map', {0: 'a', 1: 'b', 2: 'c', 3: 'd', 4: b'e'})

    async def test_if_update_builds_length_correctly(self, indices_map_new_item, session):
        await indices_map_new_item.update(session)
        assert session.redis_bind.set.coro.call_args_list == \
            [mock.call('test_indices_length', 5)]

    async def test_if_update_dont_calls_hdel(self, indices_map_new_item, session):
        await indices_map_new_item.update(session)
        assert session.redis_bind.hdel.coro.call_args_list == []


@pytest.fixture
def indices_map_removed_item(indices_map):
    ret = indices_map.items_model.get_keys.coro.return_value
    ret.remove(ret[0])
    return indices_map


class TestItemsIndicesMapWithItemRemoved(object):

    async def test_if_update_builds_items_indices_map_correctly(self, indices_map_removed_item, session):
        await indices_map_removed_item.update(session)
        assert session.redis_bind.hmset_dict.coro.call_args_list[0] == \
            mock.call('test_indices_map', {b'b': 1, b'c': 2, b'd': 3})

    async def test_if_update_builds_indices_items_map_correctly(self, indices_map_removed_item, session):
        await indices_map_removed_item.update(session)
        assert session.redis_bind.hmset_dict.coro.call_args_list[1] == \
            mock.call('test_items_map', {1: 'b', 2: 'c', 3: 'd'})

    async def test_if_update_builds_length_correctly(self, indices_map_removed_item, session):
        await indices_map_removed_item.update(session)
        assert session.redis_bind.set.coro.call_args_list == \
            [mock.call('test_indices_length', 4)]

    async def test_if_update_dont_calls_hdel(self, indices_map_removed_item, session):
        await indices_map_removed_item.update(session)
        assert session.redis_bind.hdel.coro.call_args_list == [
            mock.call('test_indices_map', b'a'), mock.call('test_items_map', 0)
        ]


@pytest.fixture
def indices_map_removed_and_added_item_in_freed_index(indices_map):
    ret = indices_map.items_model.get_keys.coro.return_value
    ret.remove(ret[0])
    ret.append(b'e')
    return indices_map


class TestItemsIndicesMapWithItemRemovedAndAddedInFreedIndex(object):

    async def test_if_update_builds_items_indices_map_correctly(self, indices_map_removed_and_added_item_in_freed_index, session):
        await indices_map_removed_and_added_item_in_freed_index.update(session)
        assert session.redis_bind.hmset_dict.coro.call_args_list[0] == \
            mock.call('test_indices_map', {b'e': 0, b'b': 1, b'c': 2, b'd': 3})

    async def test_if_update_builds_indices_items_map_correctly(self, indices_map_removed_and_added_item_in_freed_index, session):
        await indices_map_removed_and_added_item_in_freed_index.update(session)
        assert session.redis_bind.hmset_dict.coro.call_args_list[1] == \
            mock.call('test_items_map', {0: b'e', 1: 'b', 2: 'c', 3: 'd'})

    async def test_if_update_builds_length_correctly(self, indices_map_removed_and_added_item_in_freed_index, session):
        await indices_map_removed_and_added_item_in_freed_index.update(session)
        assert session.redis_bind.set.coro.call_args_list == \
            [mock.call('test_indices_length', 4)]

    async def test_if_update_dont_calls_hdel(self, indices_map_removed_and_added_item_in_freed_index, session):
        await indices_map_removed_and_added_item_in_freed_index.update(session)
        assert session.redis_bind.hdel.coro.call_args_list == [
            mock.call('test_indices_map', b'a'), mock.call('test_items_map', 0)
        ]


@pytest.fixture
def indices_map_removed_and_added_two_items(indices_map):
    ret = indices_map.items_model.get_keys.coro.return_value
    ret.remove(ret[0])
    ret.append(b'e')
    ret.append(b'f')
    return indices_map


class TestItemsIndicesMapWithItemRemovedAndAddedTwoItems(object):

    async def test_if_update_builds_items_indices_map_correctly(self, indices_map_removed_and_added_two_items, session):
        await indices_map_removed_and_added_two_items.update(session)
        assert session.redis_bind.hmset_dict.coro.call_args_list[0] == \
            mock.call('test_indices_map', {b'e': 0, b'b': 1, b'c': 2, b'd': 3, b'f': 4})

    async def test_if_update_builds_indices_items_map_correctly(self, indices_map_removed_and_added_two_items, session):
        await indices_map_removed_and_added_two_items.update(session)
        assert session.redis_bind.hmset_dict.coro.call_args_list[1] == \
            mock.call('test_items_map', {0: b'e', 1: 'b', 2: 'c', 3: 'd', 4: b'f'})

    async def test_if_update_builds_length_correctly(self, indices_map_removed_and_added_two_items, session):
        await indices_map_removed_and_added_two_items.update(session)
        assert session.redis_bind.set.coro.call_args_list == \
            [mock.call('test_indices_length', 5)]

    async def test_if_update_dont_calls_hdel(self, indices_map_removed_and_added_two_items, session):
        await indices_map_removed_and_added_two_items.update(session)
        assert session.redis_bind.hdel.coro.call_args_list == [
            mock.call('test_indices_map', b'a'), mock.call('test_items_map', 0)
        ]


@pytest.fixture
def indices_map_free_indices(indices_map):
    ret = indices_map.items_model.get_keys.coro.return_value
    ret.remove(ret[0])
    ret.remove(ret[1])
    ret.pop()
    ret.append(b'e')
    ret.append(b'f')
    ret.append(b'g')
    ret.append(b'h')
    ret.append(b'i')
    return indices_map


class TestItemsIndicesMapFreeIndices(object):

    async def test_if_update_builds_items_indices_map_correctly(self, indices_map_free_indices, session):
        await indices_map_free_indices.update(session)
        assert session.redis_bind.hmset_dict.coro.call_args_list[0] == \
            mock.call('test_indices_map', {b'e': 0, b'b': 1, b'f': 2, b'g': 3, b'h': 4, b'i': 5})

    async def test_if_update_builds_indices_items_map_correctly(self, indices_map_free_indices, session):
        await indices_map_free_indices.update(session)
        assert session.redis_bind.hmset_dict.coro.call_args_list[1] == \
            mock.call('test_items_map', {0: b'e', 1: 'b', 2: b'f', 3: b'g', 4: b'h', 5: b'i'})

    async def test_if_update_builds_length_correctly(self, indices_map_free_indices, session):
        await indices_map_free_indices.update(session)
        assert session.redis_bind.set.coro.call_args_list == \
            [mock.call('test_indices_length', 6)]

    async def test_if_update_dont_calls_hdel(self, indices_map_free_indices, session):
        await indices_map_free_indices.update(session)
        assert session.redis_bind.hdel.coro.call_args_list[0][0][0] == 'test_indices_map'
        assert set(session.redis_bind.hdel.coro.call_args_list[0][0][1:]) == {b'a', b'c', b'd'}
        assert session.redis_bind.hdel.coro.call_args_list[1] == mock.call('test_items_map', 0, 2, 3)
