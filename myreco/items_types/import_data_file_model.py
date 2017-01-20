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


from myreco.items_types.base_model import ItemsTypesModelBase
from myreco.items_types.items.data_importer_models_collection import ItemsModelsCollectionDataImporter


class ItemsTypesModelImportDataFileBase(ItemsTypesModelBase):

    @classmethod
    def _build_items_models_collection_schema(self, key, schema, id_names):
        import_data_file_uri = '{}/import_data_file'.format(key)
        schema = ItemsTypesModelBase._build_items_models_collection_schema(key, schema, id_names)
        schema[import_data_file_uri] = {
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
                },{
                    'name': 'upload_file',
                    'in': 'query',
                    'default': True,
                    'type': 'boolean'
                }],
                'post': {
                    'parameters': [{
                        'name': 'data_file',
                        'in': 'body',
                        'required': True,
                        'schema': {}
                    }],
                    'consumes': ['application/zip'],
                    'operationId': 'post_import_data_file_job',
                    'responses': {'200': {'description': 'Posted'}}
                },
                'get': {
                    'parameters': [{
                        'name': 'job_hash',
                        'in': 'query',
                        'type': 'string'
                    }],
                    'operationId': 'get_import_data_file_job',
                    'responses': {'200': {'description': 'Got'}}
                }
            }
        return schema

    @classmethod
    def _get_items_models_collection_class(self):
        return ItemsModelsCollectionDataImporter
