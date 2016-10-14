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


from falconswagger.models.base import get_model_schema
from sqlalchemy.ext.declarative import AbstractConcreteBase
from jsonschema import Draft4Validator
import sqlalchemy as sa
import json


class StoresModelBase(AbstractConcreteBase):
    __tablename__ = 'stores'
    __schema__ = get_model_schema(__file__)
    __config_validator__ = Draft4Validator(__schema__['definitions']['configuration'])

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(255), unique=True, nullable=False)
    country = sa.Column(sa.String(255), unique=True, nullable=False)
    configuration_json = sa.Column(sa.Text, nullable=False)

    @property
    def configuration(self):
        if not hasattr(self, '_configuration'):
            self._configuration = json.loads(self.configuration_json)
        return self._configuration

    def __init__(self, session, input_=None, **kwargs):
        super().__init__(session, input_=input_, **kwargs)
        if self.configuration_json:
            self.__config_validator__.validate(self.configuration)

    def _setattr(self, attr_name, value, session, input_):
        if attr_name == 'configuration':
            value = json.dumps(value)
            attr_name = 'configuration_json'

        super()._setattr(attr_name, value, session, input_)

    def _format_output_json(self, dict_inst, schema):
        if schema.get('configuration') is not False:
            config = dict_inst.pop('configuration_json')
            dict_inst['configuration'] = self.configuration
