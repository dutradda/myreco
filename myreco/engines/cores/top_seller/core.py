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


from myreco.engines.cores.top_seller.redis_object import TopSellerRedisObject
from myreco.engines.cores.objects_exporter import EngineCoreObjectsExporter
from myreco.engines.cores.recommender import EngineCoreRecommender


class TopSellerEngineCore(EngineCoreObjectsExporter, EngineCoreRecommender):
    __configuration_schema__ = {
        "type": "object",
        "required": ["days_interval"],
        "properties": {
            "days_interval": {"type": "integer"}
        }
    }

    async def _build_items_vector(self, session, **external_variables):
        return await TopSellerRedisObject(self).get_numpy_array(session)

    def export_objects(self, session):
        self._logger.info("Started export objects")

        readers = self._run_coro(
            self._build_csv_readers(),
            session.loop
        )

        items_indices_map_dict = self._run_coro(
            self._get_items_indices_map_dict(session),
            session.loop
        )

        top_seller = TopSellerRedisObject(self)
        ret = self._run_coro(
            top_seller.update(readers, session, items_indices_map_dict),
            session.loop
        )

        self._logger.info("Finished export objects")
        return ret
