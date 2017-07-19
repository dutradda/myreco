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
from myreco.slots.models import (SlotVariablesModelBase, SlotFiltersModelBase,
    SlotsModelBase, build_slots_fallbacks_table)
from myreco.engines.model import EnginesModelBase, build_engines_objects_table
from myreco.engine_objects.model import EngineObjectsModelBase
from myreco.engine_objects.data_importer.model import EngineObjectsDataImporterModelBase
from myreco.engine_objects.exporter.model import EngineObjectsExporterModelBase
from myreco.engine_strategies.model import EngineStrategiesModelBase
from myreco.item_types.model import ItemTypesModelBase, build_item_types_stores_table
from myreco.item_types.data_file_importer.model import ItemTypesDataFileImporterModelBase
from myreco.item_types.filters_updater.model import ItemTypesFiltersUpdaterModelBase
from swaggerit.models.orm.factory import FactoryOrmModels


class FactoryError(Exception):
    pass


class ModelsFactory(object):
    engine_strategies_base = EngineStrategiesModelBase
    engine_objects_recommender_base = EngineObjectsModelBase
    engine_objects_data_importer_base = EngineObjectsDataImporterModelBase
    engine_objects_exporter_base = EngineObjectsExporterModelBase
    engines_base = EnginesModelBase
    slots_base = SlotsModelBase
    slot_variables_base = SlotVariablesModelBase
    slot_filters_base = SlotFiltersModelBase
    item_types_recommender_base = ItemTypesModelBase
    item_types_data_importer_base = ItemTypesDataFileImporterModelBase
    item_types_objects_exporter_base = ItemTypesFiltersUpdaterModelBase
    placements_base = PlacementsModelBase
    variations_base = VariationsModelBase
    ab_test_users_base = ABTestUsersModelBase
    stores_base = StoresModelBase
    uris_base = URIsModelBase
    users_base = UsersModelBase
    methods_base = MethodsModelBase
    grants_base = GrantsModelBase
    external_variables_base = ExternalVariablesModelBase

    def __init__(self, data_path,
                 commons_models_attributes=None,
                 commons_tables_attributes=None):
        self.base_model = FactoryOrmModels.make_sqlalchemy_redis_base()
        self.base_model.__data_path__ = data_path
        self.meta_class = type(self.base_model)
        self._commons_models_attrs = self._init_attributes(commons_models_attributes)
        self._commons_tables_attrs = self._init_attributes(commons_tables_attributes)

    def _init_attributes(self, attrs=None, update=None):
        if attrs is None:
            attrs = dict()

        if update is not None:
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
            'engine_strategies': self.make_engine_strategies_model(),
            'engine_objects': self.make_engine_objects_model(app_type),
            'engines': self.make_engines_model(),
            'slots': self.make_slots_model(),
            'slot_variables': self.make_slot_variables_model(),
            'slot_filters': self.make_slot_filters_model(),
            'item_types': self.make_item_types_model(app_type),
            'placements': self.make_placements_model(),
            'variations': self.make_variations_model(),
            'ab_test_users': self.make_ab_test_users_model(),
            'stores': self.make_stores_model(),
            'uris': self.make_uris_model(),
            'users': self.make_users_model(),
            'methods': self.make_methods_model(),
            'grants': self.make_grants_model(),
            'external_variables': self.make_external_variables_model()
        }

    def make_all_tables(self):
        return {
            'users_grants': self.make_users_grants_table(),
            'users_stores': self.make_users_stores_table(),
            'variations_slots': self.make_variations_slots_table(),
            'slots_fallbacks': self.make_slots_fallbacks_table(),
            'item_types_stores': self.make_item_types_stores_table(),
            'engines_objects': self.make_engines_objects_table()
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

    def make_item_types_stores_table(self, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_tables_attrs)
        return build_item_types_stores_table(self.base_model.metadata, **attributes)

    def make_engines_objects_table(self, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_tables_attrs)
        return build_engines_objects_table(self.base_model.metadata, **attributes)

    def make_engine_strategies_model(self, base=EngineStrategiesModelBase, attributes=None):
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class('EngineStrategiesModel', (base, self.base_model), attributes)

    def make_engine_objects_model(self, app_type, attributes=None):
        recommender_base = type(self).engine_objects_recommender_base
        data_importer_base = type(self).engine_objects_data_importer_base
        objects_exporter_base = type(self).engine_objects_exporter_base
        attributes = self._init_attributes(attributes, self._commons_models_attrs)

        if app_type == 'recommender':
            bases_classes = (recommender_base, self.base_model)
        elif app_type == 'data_importer':
            bases_classes = (data_importer_base, self.base_model)
        elif app_type == 'objects_exporter':
            bases_classes = (objects_exporter_base, self.base_model)

        return self.meta_class('EngineObjectsModel', bases_classes, attributes)

    def make_engines_model(self, attributes=None):
        base = type(self).engines_base
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class('EnginesModel', (base, self.base_model), attributes)

    def make_slots_model(self, attributes=None):
        base = type(self).slots_base
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class('SlotsModel', (base, self.base_model), attributes)

    def make_slot_variables_model(self, attributes=None):
        base = type(self).slot_variables_base
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class('SlotVariablesModel', (base, self.base_model), attributes)

    def make_slot_filters_model(self, attributes=None):
        base = type(self).slot_filters_base
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class('SlotFiltersModel', (base, self.base_model), attributes)

    def make_item_types_model(self, app_type, attributes=None):
        recommender_base = type(self).item_types_recommender_base
        data_importer_base = type(self).item_types_data_importer_base
        objects_exporter_base = type(self).item_types_objects_exporter_base
        attributes = self._init_attributes(attributes, self._commons_models_attrs)

        if app_type == 'recommender':
            bases_classes = (recommender_base, self.base_model)
        elif app_type == 'data_importer':
            bases_classes = (data_importer_base, self.base_model)
        elif app_type == 'objects_exporter':
            bases_classes = (objects_exporter_base, self.base_model)

        return self.meta_class('ItemTypesModel', bases_classes, attributes)

    def make_placements_model(self, attributes=None):
        base = type(self).placements_base
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'PlacementsModel', (base, self.base_model), attributes)

    def make_variations_model(self, attributes=None):
        base = type(self).variations_base
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'VariationsModel', (base, self.base_model), attributes)

    def make_ab_test_users_model(self, attributes=None):
        base = type(self).ab_test_users_base
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'ABTestUsersModel', (base, self.base_model), attributes)

    def make_stores_model(self, attributes=None):
        base = type(self).stores_base
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'StoresModel', (base, self.base_model), attributes)

    def make_uris_model(self, attributes=None):
        base = type(self).uris_base
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class(
            'URIsModel', (base, self.base_model), attributes)

    def make_users_model(self, attributes=None):
        base = type(self).users_base
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class('UsersModel', (base, self.base_model), attributes)

    def make_methods_model(self, attributes=None):
        base = type(self).methods_base
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class('MethodsModel', (base, self.base_model), attributes)

    def make_grants_model(self, attributes=None):
        base = type(self).grants_base
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class('GrantsModel', (base, self.base_model), attributes)

    def make_external_variables_model(self, attributes=None):
        base = type(self).external_variables_base
        attributes = self._init_attributes(attributes, self._commons_models_attrs)
        return self.meta_class('ExternalVariablesModel', (base, self.base_model), attributes)
