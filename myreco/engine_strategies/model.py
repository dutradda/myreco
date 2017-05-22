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


class EngineStrategiesModelBase(AbstractConcreteBase):
    __tablename__ = 'engine_strategies'
    __swagger_json__ = get_swagger_json(__file__)
    _jobs = dict()
    __table_args__ = (sa.UniqueConstraint('class_name', 'class_module'),)

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(255), unique=True, nullable=False)
    class_name = sa.Column(sa.String(255), nullable=False)
    class_module = sa.Column(sa.String(255), nullable=False)

    async def _validate(self, session, input_):
        self.get_class()

    @classmethod
    def _get_class(cls, strategy):
        return ModuleObjectLoader.load({
            'path': strategy['class_module'],
            'object_name': strategy['class_name']
        })

    def get_class(self):
        if not hasattr(self, '_class'):
            self._class = type(self)._get_class(self.todict({'object_types': False}))

        return self._class

    @property
    def object_types(self):
        return self.get_class().object_types.keys()

    def _format_output_json(self, dict_inst, schema):
        if schema.get('object_types') is not False:
            dict_inst['object_types'] = self.object_types

    @classmethod
    def get_instance(cls, engine, items_model=None):
        return cls._get_class(engine['strategy'])(engine, items_model)
