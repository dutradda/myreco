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


from myreco.engines.cores.filters.filters import BooleanFilterBy
from myreco.engines.cores.filters.factory import FiltersFactory
from myreco.items_types.data_file_importer.model import ItemsTypesDataFileImporterModelBase
from myreco.utils import extend_swagger_json
from swaggerit.exceptions import SwaggerItModelError


class ItemsTypesFiltersUpdaterModelBase(ItemsTypesDataFileImporterModelBase):
    __swagger_json__ = extend_swagger_json(
        ItemsTypesDataFileImporterModelBase.__swagger_json__,
        __file__
    )

    @classmethod
    async def post_update_filters_job(cls, req, session):
        return await cls._create_job(cls._run_update_filters_job, req, session, '_updater')

    @classmethod
    async def _run_update_filters_job(cls, req, session, store_items_model, store_id, **kwargs):
        cls._logger.info("Started update filters for '{}'".format(store_items_model.__key__))

        return await cls._update_enabled_filters(store_items_model, session, store_id)

    @classmethod
    async def get_update_filters_job(cls, req, session):
        return await cls._get_job('_updater', req, session)

    @classmethod
    async def _update_enabled_filters(cls, store_items_model, session, store_id):
        items_indices_map_ret = await store_items_model.items_indices_map.update(session)
        items_indices_map_len = await store_items_model.items_indices_map.get_length(session)

        filters_factory = cls.get_model('slots_filters').__factory__
        enabled_filters = await cls._get_enabled_filters(store_items_model, session, store_id)
        filters_ret = dict()
        items_indices_map_dict = await store_items_model.items_indices_map.get_all(session)
        items = await cls._get_items_with_indices_and_stock(
            store_items_model, session, items_indices_map_dict
        )

        stock_filter = BooleanFilterBy(store_items_model, 'stock')
        await stock_filter.update(session, items, items_indices_map_len)

        for slot_filter, schema in enabled_filters:
            filter_ = filters_factory.make(
                store_items_model, slot_filter,
                schema, slot_filter['skip_values']
            )
            filters_ret[filter_.name] = await filter_.update(session, items, items_indices_map_len)

        cls._logger.info("Finished update filters for '{}'".format(store_items_model.__key__))
        return {'items_indices_map': items_indices_map_ret, 'filters': filters_ret}

    @classmethod
    async def _get_enabled_filters(cls, store_items_model, session, store_id):
        slots_model = cls.get_model('slots')
        slots = await slots_model.get(session, **{'store_id': store_id})
        filters_external_variables = []

        # used to aggregate filters inclusive and exclusive with same property
        filters_names_set = set()

        for slot in slots:
            if store_items_model.item_type['id'] == slot['engine']['item_type_id']:
                for slot_filter in slot['slot_filters']:
                    filter_name = slot_filter['property_name']
                    if filter_name not in filters_names_set:
                        schema = \
                            store_items_model.item_type['schema']['properties'][filter_name]
                        filters_external_variables.append((slot_filter, schema))
                        filters_names_set.add(filter_name)

        return filters_external_variables

    @classmethod
    async def _get_items_with_indices_and_stock(cls, store_items_model, session, items_indices_map_dict):
        items = []
        page = 1
        items_part = await store_items_model.get(session, page=page, items_per_page=100000)

        while items_part:
            items.extend(items_part)
            page += 1
            items_part = await store_items_model.get(session, page=page, items_per_page=100000)

        for item in items:
            item_key = store_items_model.get_instance_key(item)
            index = items_indices_map_dict.get(item_key)
            if index is not None:
                item['index'] = index
                item['stock'] = True

        return items
