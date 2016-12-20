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


from myreco.users.models import (GrantsModelBase, URIsModelBase, MethodsModelBase,
    UsersModelBase, build_users_grants_table, build_users_stores_table)
from myreco.stores.model import StoresModelBase
from myreco.variables.model import VariablesModelBase
from myreco.placements.models import (PlacementsModelBase, VariationsModelBase,
    ABTestUsersModelBase, build_variations_slots_table)
from myreco.slots.models import (SlotsVariablesModelBase,
    SlotsModelBase, build_slots_fallbacks_table)
from myreco.engines.models import EnginesModelBase, EnginesCoresModelBase
from myreco.engines.cores.base import EngineCore, AbstractDataImporter
from myreco.engines.cores.utils import build_engine_data_path
from myreco.items_types.models import ItemsTypesModelBase
from myreco.factory import ModelsFactory
from unittest.mock import MagicMock
from os import makedirs
from csv import DictWriter
import os.path
import gzip
import json


table_args = {'mysql_engine':'innodb'}
factory = ModelsFactory('myreco', commons_models_attributes={'__table_args__': table_args},
    					commons_tables_attributes=table_args)
models = factory.make_all_models()

GrantsModel = models['grants']

URIsModel = models['uris']

MethodsModel = models['methods']

UsersModel = models['users']

StoresModel = models['stores']

VariablesModel = models['variables']

PlacementsModel = models['placements']

VariationsModel = models['variations']

ABTestUsersModel = models['ab_test_users']

SlotsVariablesModel = models['slots_variables']

SlotsModel = models['slots']

EnginesModel = models['engines']

EnginesCoresModel = models['engines_cores']

ItemsTypesModel = models['items_types']
    
SQLAlchemyRedisModelBase = factory.base_model

DataImporter = MagicMock()


class TestDataImporter(AbstractDataImporter):

        def get_data(self, items_indices_map, session):
            data_path = build_engine_data_path(self._engine)
            if not os.path.isdir(data_path):
                makedirs(data_path)

            data = [{'item_key': '2|test2', 'value': 1},
                    {'item_key': '1|test1', 'value': 3},
                    {'item_key': '3|test3', 'value': 2}]
            data = map(json.dumps, data)
            data = '\n'.join(data)

            filename_prefix = 'top_seller'
            file_ = gzip.open(os.path.join(data_path, filename_prefix) + '-000000001.gz', 'wt')
            file_.write(data)
            file_.close()
            return {'lines_count': 3}


class TestEngine(EngineCore):
    __configuration_schema__ = {
        "type": "object",
        "required": ["item_id_name", "aggregators_ids_name"],
        "properties": {
            "item_id_name": {"type": "string"},
            "aggregators_ids_name": {"type": "string"}
        }
    }

    def get_variables(self):
        item_id_name = self.engine['configuration']['item_id_name']
        aggregators_ids_name = self.engine['configuration']['aggregators_ids_name']
        item_type_schema_props = self.engine['item_type']['schema']['properties']
        return [{
            'name': item_id_name,
            'schema': item_type_schema_props[item_id_name]
        },{
            'name': aggregators_ids_name,
            'schema': item_type_schema_props[aggregators_ids_name]
        }]

    def _validate_config(self, engine):
        item_id_name = engine['configuration']['item_id_name']
        aggregators_ids_name = engine['configuration']['aggregators_ids_name']
        item_type_schema_props = engine['item_type']['schema']['properties']
        message = "Configuration key '{}' not in item_type schema"

        if item_id_name not in item_type_schema_props:
            raise ValidationError(message.format('item_id_name'),
                instance=engine['configuration'], schema=item_type_schema_props)

        elif aggregators_ids_name not in item_type_schema_props:
            raise ValidationError(message.format('aggregators_ids_name'),
                instance=engine['configuration'], schema=item_type_schema_props)

    def _build_rec_vector(self):
        pass

    get_recommendations = MagicMock()