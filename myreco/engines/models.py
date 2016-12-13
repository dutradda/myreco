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


from falconswagger.utils import get_model_schema
from falconswagger.exceptions import ModelBaseError
from myreco.engines.cores.items_indices_map import ItemsIndicesMap
from myreco.items_types.models import build_item_key
from myreco.utils import ModuleClassLoader, get_items_model_from_api
from types import MethodType, FunctionType
from jsonschema import ValidationError
from sqlalchemy.ext.declarative import AbstractConcreteBase, declared_attr
from falcon import HTTPNotFound
from copy import deepcopy
import sqlalchemy as sa
import json


class EnginesModelBase(AbstractConcreteBase):
    __tablename__ = 'engines'
    __schema__ = get_model_schema(__file__)
    _jobs = dict()

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String(255), unique=True, nullable=False)
    configuration_json = sa.Column(sa.Text, nullable=False)

    @property
    def configuration(self):
        if not hasattr(self, '_configuration'):
            self._configuration = json.loads(self.configuration_json)
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
        core_class_ = ModuleClassLoader.load(self.core.configuration['core_module'])
        self_dict = self._build_self_dict()
        items_model = get_items_model_from_api(type(self).__api__, self_dict)
        self._core_instance = core_class_(self_dict, items_model)

    def _build_self_dict(self):
        todict_schema = {'variables': False}
        return self.todict(todict_schema)

    def _setattr(self, attr_name, value, session, input_):
        if attr_name == 'configuration':
            value = json.dumps(value)
            attr_name = 'configuration_json'

        if attr_name == 'core_id':
            value = {'id': value}
            attr_name = 'core'

        if attr_name == 'item_type_id':
            value = {'id': value}
            attr_name = 'item_type'

        super()._setattr(attr_name, value, session, input_)

    def _validate(self):
        if self.core is not None:
            self.core_instance.validate_config()

    def _format_output_json(self, dict_inst, schema):
        if schema.get('configuration') is not False:
            dict_inst.pop('configuration_json')
            dict_inst['configuration'] = self.configuration

        if schema.get('variables') is not False:
            dict_inst['variables'] = self.core_instance.get_variables()


data_importer_schema = get_model_schema(__file__, 'data_importer_schema.json')
data_importer_schema.update(deepcopy(EnginesModelBase.__schema__))


class EnginesModelDataImporterBase(EnginesModelBase):
    __schema__ = data_importer_schema

    @classmethod
    def _run_job(cls, req, resp):
        job_session = req.context['job_session']
        engine = cls._get_engine(req, job_session)
        items_indices_map = cls._build_items_indices_map(engine)
        data_importer = cls._build_data_importer(engine)
        return data_importer.get_data(items_indices_map, job_session)

    @classmethod
    def _get_engine(cls, req, job_session):
        id_ = req.context['parameters']['path']['id']
        engines = cls.get(job_session, {'id': id_}, todict=False)

        if not engines:
            raise HTTPNotFound()

        return engines[0]

    @classmethod
    def _build_items_indices_map(cls, engine):
        items_model = get_items_model_from_api(cls.__api__, engine._build_self_dict())
        return ItemsIndicesMap(items_model)

    @classmethod
    def _build_data_importer(cls, engine):
        data_importer_config = engine.core.configuration['data_importer_module']
        data_importer_class = ModuleClassLoader.load(data_importer_config)
        return data_importer_class(engine.todict())


objects_exporter_schema = get_model_schema(__file__, 'objects_exporter_schema.json')
objects_exporter_schema.update(deepcopy(EnginesModelDataImporterBase.__schema__))


class EnginesModelObjectsExporterBase(EnginesModelDataImporterBase):
    __schema__ = objects_exporter_schema

    @classmethod
    def _run_job(cls, req, resp):
        job_session = req.context['job_session']
        import_data = req.context['parameters']['query_string'].get('import_data')
        engine = cls._get_engine(req, job_session)
        items_indices_map = cls._build_items_indices_map(engine)

        if import_data:
            data_importer = cls._build_data_importer(engine)
            data_importer.get_data(items_indices_map, job_session)
            return engine.core_instance.export_objects(job_session, items_indices_map)
        else:
            return engine.core_instance.export_objects(job_session, items_indices_map)


class EnginesCoresModelBase(AbstractConcreteBase):
    __tablename__ = 'engines_cores'
    __schema__ = get_model_schema(__file__, 'engines_cores_schema.json')

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(255), unique=True, nullable=False)
    configuration_json = sa.Column(sa.Text, nullable=False)

    @property
    def configuration(self):
        if not hasattr(self, '_configuration'):
            self._configuration = json.loads(self.configuration_json)
        return self._configuration

    def _setattr(self, attr_name, value, session, input_):
        if attr_name == 'configuration':
            value = json.dumps(value)
            attr_name = 'configuration_json'

        super()._setattr(attr_name, value, session, input_)

    def _format_output_json(self, dict_inst, schema):
        if schema.get('configuration') is not False:
            dict_inst.pop('configuration_json')
            dict_inst['configuration'] = self.configuration
