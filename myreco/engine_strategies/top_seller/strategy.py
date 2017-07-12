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


from myreco.engine_strategies.top_seller.array import TopSellerArray
from myreco.engine_strategies.strategy_base import EngineStrategyBase


class TopSellerEngineStrategy(EngineStrategyBase):
    configuration_schema = {
        'type': 'object',
        'required': ['top_seller_array'],
        'additionalProperties': False,
        'properties': {
            'top_seller_array': {
                'type': 'object',
                'required': ['days_interval'],
                'additionalProperties': False,
                'properties': {
                    'days_interval': {'type': 'integer'}
                }
            }
        }
    }
    object_types = {'top_seller_array': TopSellerArray}

    async def _build_items_vector(self, session, items_model, **external_variables):
        return await self.objects['top_seller_array'].get_numpy_array(session)
