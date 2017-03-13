# MIT License

# Copyright (c) 2017 Diogo Dutra <dutradda@gmail.com>

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


from sqlalchemy.ext.declarative import AbstractConcreteBase
from myreco.utils import ModuleObjectLoader
from swaggerit.utils import get_swagger_json
import sqlalchemy as sa
import ujson


class EngineCoresModelBase(AbstractConcreteBase):
    __tablename__ = 'engine_cores'
    __swagger_json__ = get_swagger_json(__file__)
    _jobs = dict()

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(255), unique=True, nullable=False)
    strategy_class_json = sa.Column(sa.Text, nullable=False)

    @property
    def strategy_class(self):
        if not hasattr(self, '_strategy_class'):
            self._strategy_class = ujson.loads(self.strategy_class_json)
        return self._strategy_class

    async def _setattr(self, attr_name, value, session, input_):
        if attr_name == 'strategy_class':
            value = ujson.dumps(value)
            attr_name = 'strategy_class_json'

        await super()._setattr(attr_name, value, session, input_)

    def _format_output_json(self, dict_inst, schema):
        if schema.get('strategy_class') is not False:
            dict_inst.pop('strategy_class_json')
            dict_inst['strategy_class'] = self.strategy_class

    def _validate(self):
        ModuleObjectLoader.load({
            'path': self.strategy_class['module'],
            'object_name': self.strategy_class['class_name']
        })
