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


from myreco.engines.cores.items_indices_map import ItemsIndicesMap
from myreco.engines.cores.filters.factory import FiltersFactory
from myreco.engines.cores.filters.filters import BooleanFilterBy
from myreco.items_types.items_data_importer_model import (
    ItemsCollectionsModelDataImporterBaseMeta,
    ItemsTypesModelDataImporterBase)


class ItemsCollectionsModelFiltersUpdaterBaseMeta(ItemsCollectionsModelDataImporterBaseMeta):

    def _run_job(cls, req, resp):
        job_session = req.context['job_session']
        query_string = req.context['parameters']['query_string']
        store_id = query_string['store_id']
        items_model = cls._get_model(query_string)
        items_indices_map = ItemsIndicesMap(items_model)
        items_indices_map_ret = items_indices_map.update(job_session)

        filters_factory = FiltersFactory()
        enabled_filters = cls._get_enabled_filters(job_session, store_id)
        filters_ret = dict()
        items_indices_map = items_indices_map.get_all(job_session)
        items = cls._get_items_with_indices_and_stock(job_session, items_model, items_indices_map)

        stock_filter = BooleanFilterBy(items_model, 'stock')
        stock_filter.update(job_session, items)

        for slot_var, schema in enabled_filters:
            filter_ = filters_factory.make(items_model, slot_var, schema, slot_var['skip_values'])
            filters_ret[filter_.name] = filter_.update(job_session, items)

        return {'items_indices_map': items_indices_map_ret, 'filters': filters_ret}

    def _get_enabled_filters(cls, session, store_id):
        slots_model = cls.__all_models__['slots']
        slots = slots_model.get(session, **{'store_id': store_id})
        filters_variables = []

        # used to aggregate filters inclusive and exclusive with same property
        filters_names_set = set()

        for slot in slots:
            if cls.__item_type__['id'] == slot['engine']['item_type_id']:
                for slot_var in slot['slot_variables']:
                    if slot_var['is_filter']:
                        filter_name = slot_var['inside_engine_name']
                        if filter_name not in filters_names_set:
                            schema = cls.__item_type__['schema']['properties'][filter_name]
                            filters_variables.append((slot_var, schema))
                            filters_names_set.add(filter_name)

        return filters_variables

    def _get_items_with_indices_and_stock(cls, job_session, items_model, items_indices_map):
        items = []
        page = 1
        items_part = items_model.get(job_session, page=page, items_per_page=100000)

        while items_part:
            items.extend(items_part)
            page += 1
            items_part = items_model.get(job_session, page=page, items_per_page=100000)

        for item in items:
            item_key = items_model.get_instance_key(item)
            index = items_indices_map.get(item_key)
            if index is not None:
                item['index'] = index
                item['stock'] = True

        return items


class ItemsTypesModelFiltersUpdaterBase(ItemsTypesModelDataImporterBase):

    @classmethod
    def _build_items_collections_schema(cls, key, schema, id_names):
        update_filters_uri = '/{}/update_filters'.format(key)
        schema = ItemsTypesModelDataImporterBase.\
            _build_items_collections_schema(key, schema, id_names)

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
                'operationId': 'post_job',
                'responses': {'201': {'description': 'Executing'}}
            },
            'get': {
                'parameters': [{
                    'name': 'hash',
                    'in': 'query',
                    'required': True,
                    'type': 'string'
                }],
                'operationId': 'get_job',
                'responses': {'200': {'description': 'Got'}}
            }
        }
        return schema

    @classmethod
    def _get_items_collections_metaclass(cls):
        return ItemsCollectionsModelFiltersUpdaterBaseMeta
