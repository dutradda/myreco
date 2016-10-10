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


from falconswagger.models.redis import RedisModelBuilder
from falconswagger.models.base import get_model_schema
from sqlalchemy.ext.declarative import AbstractConcreteBase
import sqlalchemy as sa
import json


class ItemsTypesModelBase(AbstractConcreteBase):
    __tablename__ = 'items_types'
    __schema__ = get_model_schema(__file__)

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(255), unique=True, nullable=False)
    id_names_json = sa.Column(sa.String(255), nullable=False)
    schema_json = sa.Column(sa.Text, nullable=False)

    def _setattr(self, attr_name, value, session, input_):
        if attr_name == 'id_names':
            value = json.dumps(value)
            attr_name = 'id_names_json'

        elif attr_name == 'schema':
            value = json.dumps(value)
            attr_name = 'schema_json'

        super()._setattr(attr_name, value, session, input_)

    def _format_output_json(self, dict_inst):
        if 'id_names_json' in dict_inst:
            dict_inst['id_names'] = json.loads(dict_inst.pop('id_names_json'))

        if 'schema_json' in dict_inst:
            dict_inst['schema'] = json.loads(dict_inst.pop('schema_json'))

        schema_properties = dict_inst['schema'].get('properties', {})
        schema_properties_names = sorted(schema_properties.keys())
        dict_inst['available_filters'] = [{'name': name, 'schema': schema_properties[name]} \
            for name in schema_properties_names]


class ItemsModelsBuilder(object):

    def __new__(cls, models_types):
        models = set()
        for model_type in models_types:
            model = RedisModelBuilder(
                model_type['name'], model_type['id_names'], model_type['schema'])
            models.add(model)
        return models
