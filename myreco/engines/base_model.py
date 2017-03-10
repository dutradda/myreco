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


from swaggerit.utils import get_swagger_json
from swaggerit.exceptions import SwaggerItModelError
from myreco.utils import ModuleObjectLoader, get_items_model
from sqlalchemy.ext.declarative import AbstractConcreteBase, declared_attr
import sqlalchemy as sa
import ujson


class EnginesModelBase(AbstractConcreteBase):
    __tablename__ = 'engines'
    __swagger_json__ = get_swagger_json(__file__)
    _jobs = dict()

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(255), unique=True, nullable=False)
    configuration_json = sa.Column(sa.Text, nullable=False)

    @property
    def configuration(self):
        if not hasattr(self, '_configuration'):
            self._configuration = ujson.loads(self.configuration_json)
        return self._configuration

    @declared_attr
    def store_id(cls):
        return sa.Column(sa.ForeignKey('stores.id'), nullable=False)

    @declared_attr
    def core_id(cls):
        return sa.Column(sa.ForeignKey('engines_cores.id'), nullable=False)

    @declared_attr
    def item_type_id(cls):
        return sa.Column(sa.ForeignKey('items_types.id'), nullable=False)

    @declared_attr
    def core(cls):
        return sa.orm.relationship('EnginesCoresModel')

    @declared_attr
    def item_type(cls):
        return sa.orm.relationship('ItemsTypesModel')

    @declared_attr
    def store(cls):
        return sa.orm.relationship('StoresModel')

    @property
    def core_instance(self):
        if not hasattr(self, '_core_instance'):
            self._set_core_instance()

        return self._core_instance

    def _set_core_instance(self):
        core_class_ = self.get_core_class()
        self_dict = self._build_self_dict()
        store_items_model = get_items_model(self_dict)
        self._core_instance = core_class_(self_dict, store_items_model)

    def get_core_class(self):
        return ModuleObjectLoader.load(self.core.configuration['core_module'])

    def _build_self_dict(self):
        todict_schema = {'variables': False}
        return self.todict(todict_schema)

    async def _setattr(self, attr_name, value, session, input_):
        if attr_name == 'configuration':
            value = ujson.dumps(value)
            attr_name = 'configuration_json'

        if attr_name == 'core_id':
            value = {'id': value}
            attr_name = 'core'

        if attr_name == 'item_type_id':
            value = {'id': value}
            attr_name = 'item_type'

        await super()._setattr(attr_name, value, session, input_)

    def _validate(self):
        if self.core is not None:
            self.core_instance.validate_config()

    def _format_output_json(self, dict_inst, schema):
        if schema.get('configuration') is not False:
            dict_inst.pop('configuration_json')
            dict_inst['configuration'] = self.configuration

        if schema.get('variables') is not False:
            dict_inst['variables'] = self.core_instance.get_variables()

    async def init(self, session, input_=None, **kwargs):
        store = kwargs.get('store')
        store_id = kwargs.get('store_id')

        if store is None and store_id is not None:
            store = await type(self).get_model('stores').get(session, {'id': store_id})

            if not store:
                raise SwaggerItModelError("Invalid store_id {}".format(store_id), instance=input_)
            else:
                kwargs['store'] = store[0]

        await super().init(session, input_=input_, **kwargs)
