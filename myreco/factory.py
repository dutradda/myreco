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


from myreco.users.models import (GrantsModelBase, URIsModelBase, MethodsModelBase,
    UsersModelBase, build_users_grants_table, build_users_stores_table)
from myreco.stores.model import StoresModelBase
from myreco.variables.model import VariablesModelBase
from myreco.placements.models import (PlacementsModelBase, VariationsModelBase,
    ABTestUsersModelBase, build_variations_slots_table)
from myreco.slots.models import (SlotsVariablesModelBase,
    SlotsModelBase, build_slots_fallbacks_table)
from myreco.engines.base_model import EnginesModelBase
from myreco.engines.import_data_model import EnginesModelImportDataBase
from myreco.engines.export_objects_model import EnginesModelExportObjectsBase
from myreco.engines.cores.model import EnginesCoresModelBase
from myreco.items_types.base_model import ItemsTypesModelBase, build_items_types_stores_table
from myreco.items_types.import_data_file_model import ItemsTypesModelImportDataFileBase
from myreco.items_types.update_filters_model import ItemsTypesModelUpdateFiltersBase
from swaggerit.models.orm.factory import FactoryOrmModels


class FactoryError(Exception):
    pass


class ModelsFactory(object):

    def __init__(self, commons_models_attributes=None, commons_tables_attributes=None):
        self.base_model = FactoryOrmModels.make_sqlalchemy_redis_base()
        self.meta_class = type(self.base_model)
        self._commons_models_attrs = self._init_attributes(commons_models_attributes)
        self._commons_tables_attrs = self._init_attributes(commons_tables_attributes)

    def _init_attributes(self, attrs=None, update=None):
        if attrs is None:
            if update is None:
                attrs = dict()
            else:
                attrs = update
        elif update is not None:
            attrs.update(update)
        return attrs

    def make_all_models(self, app_type='recommender'):
        self.make_all_tables()
        app_types = {'recommender', 'data_importer', 'objects_exporter'}
        if (app_type not in app_types):
            raise FactoryError(
                "Invalid application type '{}'. Valid types: {}".format(
                    app_type, ', '.join(app_types)))

        return {
            'engines': self.make_engines_model(app_type),
            'engines_cores': self.make_engines_cores_model(),
            'slots': self.make_slots_model(),
            'slots_variables': self.make_slots_variables_model(),
            'items_types': self.make_items_types_model(app_type),
            'placements': self.make_placements_model(),
            'variations': self.make_variations_model(),
            'ab_test_users': self.make_ab_test_users_model(),
            'stores': self.make_stores_model(),
            'uris': self.make_uris_model(),
            'users': self.make_users_model(),
            'methods': self.make_methods_model(),
            'grants': self.make_grants_model(),
            'variables': self.make_variables_model()
        }

    def make_all_tables(self):
        return {
            'users_grants': self.make_users_grants_table(),
            'users_stores': self.make_users_stores_table(),
            'variations_slots': self.make_variations_slots_table(),
            'slots_fallbacks': self.make_slots_fallbacks_table(),
            'items_types_stores': self.make_items_types_stores_table()
        }

    def make_users_grants_table(self, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_tables_attrs)
        return build_users_grants_table(self.base_model.metadata, **attributes)

    def make_users_stores_table(self, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_tables_attrs)
        return build_users_stores_table(self.base_model.metadata, **attributes)

    def make_variations_slots_table(self, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_tables_attrs)
        return build_variations_slots_table(self.base_model.metadata, **attributes)

    def make_slots_fallbacks_table(self, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_tables_attrs)
        return build_slots_fallbacks_table(self.base_model.metadata, **attributes)

    def make_items_types_stores_table(self, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_tables_attrs)
        return build_items_types_stores_table(self.base_model.metadata, **attributes)

    def make_engines_model(self, app_type, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)

        if app_type == 'recommender':
            bases_classes = (EnginesModelBase, self.base_model)
        elif app_type == 'data_importer':
            bases_classes = (EnginesModelImportDataBase, self.base_model)
        elif app_type == 'objects_exporter':
            bases_classes = (EnginesModelExportObjectsBase, self.base_model)

        return self.meta_class('EnginesModel', bases_classes, attributes)

    def make_engines_cores_model(self, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'EnginesCoresModel', (EnginesCoresModelBase, self.base_model), attributes)

    def make_slots_model(self, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'SlotsModel', (SlotsModelBase, self.base_model), attributes)

    def make_slots_variables_model(self, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'SlotsVariablesModel',
            (SlotsVariablesModelBase, self.base_model), attributes)

    def make_items_types_model(self, app_type, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)

        if app_type == 'recommender':
            bases_classes = (ItemsTypesModelBase, self.base_model)
        elif app_type == 'data_importer':
            bases_classes = (ItemsTypesModelImportDataFileBase, self.base_model)
        elif app_type == 'objects_exporter':
            bases_classes = (ItemsTypesModelUpdateFiltersBase, self.base_model)

        return self.meta_class('ItemsTypesModel', bases_classes, attributes)

    def make_placements_model(self, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'PlacementsModel', (PlacementsModelBase, self.base_model), attributes)

    def make_variations_model(self, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'VariationsModel', (VariationsModelBase, self.base_model), attributes)

    def make_ab_test_users_model(self, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'ABTestUsersModel', (ABTestUsersModelBase, self.base_model), attributes)

    def make_stores_model(self, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'StoresModel', (StoresModelBase, self.base_model), attributes)

    def make_uris_model(self, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'URIsModel', (URIsModelBase, self.base_model), attributes)

    def make_users_model(self, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'UsersModel', (UsersModelBase, self.base_model), attributes)

    def make_methods_model(self, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'MethodsModel', (MethodsModelBase, self.base_model), attributes)

    def make_grants_model(self, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'GrantsModel', (GrantsModelBase, self.base_model), attributes)

    def make_variables_model(self, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'VariablesModel', (VariablesModelBase, self.base_model), attributes)