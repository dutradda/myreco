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


from myreco.engine_objects.object_base import EngineObjectBase
from myreco.exceptions import EngineError
import ujson


class TopSellerMap(EngineObjectBase):

    def __init__(self, *args, **kwargs):
        self.map = dict()
        super().__init__(*args, **kwargs)

    def export(self, items_model, session):
        self._logger.info("Started export objects")

        readers = self._build_csv_readers()
        ret = self.update(readers, session)

        self._logger.info("Finished export objects")
        return ret

    def update(self, readers, session):
        self._build_top_seller_map(readers, session)

        self._run_coro(
            session.redis_bind.hmset_dict(
                self._redis_key,
                self.map
            ),
            session
        )

        self._run_coro(
            self._del_old_keys(
                self._redis_key,
                set(self.map.keys()),
                session
            ),
            session
        )

        return {
            'length': len(self.map),
            'max_sells': max(self.map.values()),
            'min_sells': min(self.map.values())
        }

    def _build_top_seller_map(self, readers, session):
        error_message = "No data found for engine object '{}'".format(self._engine_object['name'])
        if not len(readers):
            raise EngineError(error_message)

        for reader in readers:
            for line in reader:
                line = ujson.loads(line)
                self.map[line['item_key']] = int(line['value'])

        if not self.map:
            raise EngineError(error_message)

    async def get_map(self, session):
        return await session.redis_bind.hgetall(self._redis_key)
