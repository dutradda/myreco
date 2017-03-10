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
from myreco.external_variables.model import ExternalVariablesModelBase
from myreco.placements.models import (PlacementsModelBase, VariationsModelBase,
    ABTestUsersModelBase, build_variations_slots_table)
from myreco.slots.models import (SlotsVariablesModelBase, SlotsFiltersModelBase,
    SlotsModelBase, build_slots_fallbacks_table)
from myreco.engines.base_model import EnginesModelBase
from myreco.engines.data_importer.model import EnginesDataImporterModelBase
from myreco.engines.objects_exporter.model import EnginesObjectsExporterModelBase
from myreco.engines.cores.model import EnginesCoresModelBase
from myreco.items_types.model import ItemsTypesModelBase, build_items_types_stores_table
from myreco.items_types.data_file_importer.model import ItemsTypesDataFileImporterModelBase
from myreco.items_types.filters_updater.model import ItemsTypesFiltersUpdaterModelBase
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

    def make_all_models(self, app_type='recommender',
                        engines_recommender_base=EnginesModelBase,
                        engines_data_importer_base=EnginesDataImporterModelBase,
                        engines_objects_exporter_base=EnginesObjectsExporterModelBase,
                        engines_core_base=EnginesCoresModelBase,
                        slots_base=SlotsModelBase,
                        slots_variables_base=SlotsVariablesModelBase,
                        slots_filters_base=SlotsFiltersModelBase,
                        items_types_recommender_base=ItemsTypesModelBase,
                        items_types_data_importer_base=ItemsTypesDataFileImporterModelBase,
                        items_types_objects_exporter_base=ItemsTypesFiltersUpdaterModelBase,
                        placements_base=PlacementsModelBase,
                        variations_base=VariationsModelBase,
                        ab_test_users_base=ABTestUsersModelBase,
                        stores_base=StoresModelBase,
                        uris_base=URIsModelBase,
                        users_base=UsersModelBase,
                        methods_base=MethodsModelBase,
                        grants_base=GrantsModelBase,
                        external_variables_base=ExternalVariablesModelBase):
        self.make_all_tables()
        app_types = {'recommender', 'data_importer', 'objects_exporter'}
        if (app_type not in app_types):
            raise FactoryError(
                "Invalid application type '{}'. Valid types: {}".format(
                    app_type, ', '.join(app_types)))

        return {
            'engines': self.make_engines_model(
                app_type,
                recommender_base=engines_recommender_base,
                data_importer_base=engines_data_importer_base,
                objects_exporter_base=engines_objects_exporter_base
            ),
            'engines_cores': self.make_engines_cores_model(engines_core_base),
            'slots': self.make_slots_model(slots_base),
            'slots_variables': self.make_slots_variables_model(slots_variables_base),
            'slots_filters': self.make_slots_filters_model(slots_filters_base),
            'items_types': self.make_items_types_model(
                app_type,
                recommender_base=items_types_recommender_base,
                data_importer_base=items_types_data_importer_base,
                objects_exporter_base=items_types_objects_exporter_base
            ),
            'placements': self.make_placements_model(placements_base),
            'variations': self.make_variations_model(variations_base),
            'ab_test_users': self.make_ab_test_users_model(ab_test_users_base),
            'stores': self.make_stores_model(stores_base),
            'uris': self.make_uris_model(uris_base),
            'users': self.make_users_model(users_base),
            'methods': self.make_methods_model(methods_base),
            'grants': self.make_grants_model(grants_base),
            'external_variables': self.make_external_variables_model(external_variables_base)
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

    def make_engines_model(self, app_type,
                           recommender_base=EnginesModelBase,
                           data_importer_base=EnginesDataImporterModelBase,
                           objects_exporter_base=EnginesObjectsExporterModelBase,
                           attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)

        if app_type == 'recommender':
            bases_classes = (recommender_base, self.base_model)
        elif app_type == 'data_importer':
            bases_classes = (data_importer_base, self.base_model)
        elif app_type == 'objects_exporter':
            bases_classes = (objects_exporter_base, self.base_model)

        return self.meta_class('EnginesModel', bases_classes, attributes)

    def make_engines_cores_model(self, base=EnginesCoresModelBase, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'EnginesCoresModel', (base, self.base_model), attributes)

    def make_slots_model(self, base=SlotsModelBase, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'SlotsModel', (base, self.base_model), attributes)

    def make_slots_variables_model(self, base=SlotsVariablesModelBase, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class('SlotsVariablesModel', (base, self.base_model), attributes)

    def make_slots_filters_model(self, base=SlotsFiltersModelBase, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class('SlotsFiltersModel', (base, self.base_model), attributes)

    def make_items_types_model(self, app_type,
                               recommender_base=ItemsTypesModelBase,
                               data_importer_base=ItemsTypesDataFileImporterModelBase,
                               objects_exporter_base=ItemsTypesFiltersUpdaterModelBase,
                               attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)

        if app_type == 'recommender':
            bases_classes = (recommender_base, self.base_model)
        elif app_type == 'data_importer':
            bases_classes = (data_importer_base, self.base_model)
        elif app_type == 'objects_exporter':
            bases_classes = (objects_exporter_base, self.base_model)

        return self.meta_class('ItemsTypesModel', bases_classes, attributes)

    def make_placements_model(self, base=PlacementsModelBase, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'PlacementsModel', (base, self.base_model), attributes)

    def make_variations_model(self, base=VariationsModelBase, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'VariationsModel', (base, self.base_model), attributes)

    def make_ab_test_users_model(self, base=ABTestUsersModelBase, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'ABTestUsersModel', (base, self.base_model), attributes)

    def make_stores_model(self, base=StoresModelBase, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'StoresModel', (base, self.base_model), attributes)

    def make_uris_model(self, base=URIsModelBase, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'URIsModel', (base, self.base_model), attributes)

    def make_users_model(self, base=UsersModelBase, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'UsersModel', (base, self.base_model), attributes)

    def make_methods_model(self, base=MethodsModelBase, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'MethodsModel', (base, self.base_model), attributes)

    def make_grants_model(self, base=GrantsModelBase, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'GrantsModel', (base, self.base_model), attributes)

    def make_external_variables_model(self, base=ExternalVariablesModelBase, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'ExternalVariablesModel', (base, self.base_model), attributes)