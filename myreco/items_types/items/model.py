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


from swaggerit.models.orm.redis_elsearch import ModelRedisElSearchMeta


class ItemsModelBaseMeta(ModelRedisElSearchMeta):

    def __init__(cls, name, bases, attrs):
        ModelRedisElSearchMeta.__init__(cls, name, bases, attrs)
        cls.index = None

    async def get(cls, session, ids=None, limit=None, offset=None, **kwargs):
        items_per_page, page = kwargs.get('items_per_page', 1000), kwargs.get('page', 1)
        limit = items_per_page * page
        offset = items_per_page * (page-1)
        return await \
            ModelRedisElSearchMeta.get(cls, session, ids=ids, limit=limit, offset=offset, **kwargs)

    async def get_all(cls, session, **kwargs):
        return await ModelRedisElSearchMeta.get(cls, session, **kwargs)

    async def search(cls, session, pattern, page=1, size=100):
        if page < 1:
            return []

        return await ModelRedisElSearchMeta.search(cls, session, pattern, page-1, size)
