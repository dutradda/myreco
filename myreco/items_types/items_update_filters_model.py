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


from myreco.engines.cores.items_indices_map import ItemsIndicesMap
from myreco.engines.cores.filters.factory import FiltersFactory
from myreco.engines.cores.filters.filters import BooleanFilterBy
from myreco.items_types.items_data_importer_model import (
    ItemsModelCollectionDataImporter,
    ItemsTypesModelDataImporterBase)


class ItemsModelCollectionFiltersUpdater(ItemsModelCollectionDataImporter):

    async def post_update_filters_job(self, req, session):
        return self._create_job(self._run_update_filters_job, req, session, '_updater')

    async def _run_update_filters_job(self, req, session, items_model, store_id, **kwargs):
        self._logger.info("Started update filters for '{}'".format(items_model.__key__))

        items_indices_map = ItemsIndicesMap(items_model)
        items_indices_map_ret = await items_indices_map.update(session)

        filters_factory = FiltersFactory()
        enabled_filters = await self._get_enabled_filters(session, store_id)
        filters_ret = dict()
        items_indices_map_dict = await items_indices_map.get_all(session)
        items = await self._get_items_with_indices_and_stock(
            session, items_model, items_indices_map_dict)

        stock_filter = BooleanFilterBy(items_model, 'stock')
        await stock_filter.update(session, items)

        for slot_var, schema in enabled_filters:
            filter_ = filters_factory.make(items_model, slot_var, schema, slot_var['skip_values'])
            filters_ret[filter_.name] = await filter_.update(session, items)

        self._logger.info("Finished update filters for '{}'".format(items_model.__key__))
        return {'items_indices_map': items_indices_map_ret, 'filters': filters_ret}

    async def _get_enabled_filters(self, session, store_id):
        slots_model = self.get_model('slots')
        slots = await slots_model.get(session, **{'store_id': store_id})
        filters_variables = []

        # used to aggregate filters inclusive and exclusive with same property
        filters_names_set = set()

        for slot in slots:
            if self.__item_type__['id'] == slot['engine']['item_type_id']:
                for slot_var in slot['slot_variables']:
                    if slot_var['is_filter']:
                        filter_name = slot_var['inside_engine_name']
                        if filter_name not in filters_names_set:
                            schema = self.__item_type__['schema']['properties'][filter_name]
                            filters_variables.append((slot_var, schema))
                            filters_names_set.add(filter_name)

        return filters_variables

    async def _get_items_with_indices_and_stock(self, job_session, items_model, items_indices_map_dict):
        items = []
        page = 1
        items_part = await items_model.get(job_session, page=page, items_per_page=100000)

        while items_part:
            items.extend(items_part)
            page += 1
            items_part = await items_model.get(job_session, page=page, items_per_page=100000)

        for item in items:
            item_key = items_model.get_instance_key(item)
            index = items_indices_map_dict.get(item_key)
            if index is not None:
                item['index'] = index
                item['stock'] = True

        return items

    async def get_update_filters_job(self, req, session):
        jobs_id = self._get_model(req.query).__key__ + '_updater'
        return await self._get_job(jobs_id, req, session)


class ItemsTypesModelFiltersUpdaterBase(ItemsTypesModelDataImporterBase):

    @classmethod
    def _build_items_model_collection_schema(cls, key, schema, id_names):
        update_filters_uri = '{}/update_filters'.format(key)
        schema = ItemsTypesModelDataImporterBase.\
            _build_items_model_collection_schema(key, schema, id_names)

        schema[update_filters_uri] = {
            'parameters': [{
                'name': 'Authorization',
                'in': 'header',
                'required': True,
                'type': 'string'
            },{
                'name': 'store_id',
                'in': 'query',
                'required': True,
                'type': 'integer'
            }],
            'post': {
                'operationId': 'post_update_filters_job',
                'responses': {'201': {'description': 'Executing'}}
            },
            'get': {
                'parameters': [{
                    'name': 'job_hash',
                    'in': 'query',
                    'type': 'string'
                }],
                'operationId': 'get_update_filters_job',
                'responses': {'200': {'description': 'Got'}}
            }
        }
        return schema

    @classmethod
    def _get_items_model_collection_class(cls):
        return ItemsModelCollectionFiltersUpdater
