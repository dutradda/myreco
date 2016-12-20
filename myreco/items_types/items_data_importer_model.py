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


from myreco.items_types.items_model import ItemsCollectionsModelBaseMeta
from myreco.items_types.models import ItemsTypesModelBase
from falconswagger.exceptions import ModelBaseError
from jsonschema import Draft4Validator
from concurrent.futures import ThreadPoolExecutor
from tempfile import NamedTemporaryFile
from zipfile import ZipFile
from io import BytesIO
import json
import boto3
import os


class ItemsCollectionsModelDataImporterBaseMeta(ItemsCollectionsModelBaseMeta):

    def import_data_file(cls, req, resp):
        if req.content_type != 'application/zip':
            raise ModelBaseError("Invalid content type '{}'".format(req.content_type))

        store_id = req.context['parameters']['query_string']['store_id']
        upload_file = req.context['parameters']['query_string'].get('upload_file', True)
        items_model = cls._get_model(req.context['parameters']['query_string'])

        session = req.context['session']
        session = type(session)(
            bind=session.bind.engine.connect(),
            redis_bind=session.redis_bind)

        stream = BytesIO()
        stream.write(req.stream.read())
        stream.seek(0)

        if upload_file:
            cls._put_file_on_s3(stream, items_model, session, store_id)

        stream.seek(0)
        ThreadPoolExecutor(1).submit(
            cls._update_items_from_zipped_file, stream, items_model, session)

    def _put_file_on_s3(cls, stream, items_model, session, store_id):
        cls._logger.info("Started put file on S3 for '{}'".format(items_model.__key__))

        store = cls.__all_models__['stores'].get(session, [{'id': store_id}])[0]
        s3_bucket = store['configuration']['aws']['s3']['bucket']
        access_key_id = store['configuration']['aws'].get('access_key_id')
        secret_access_key = store['configuration']['aws'].get('secret_access_key')
        s3_key = '{}.zip'.format(items_model.__key__)

        boto3.resource(
            's3',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key
        ).Bucket(s3_bucket).put_object(Body=stream, Key=s3_key)

        cls._logger.info("Finished put file on S3 for '{}'".format(items_model.__key__))

    def _update_items_from_zipped_file(cls, stream, items_model, session):
        try:
            tempfile = NamedTemporaryFile(delete=False)
            filename = tempfile.name
            tempfile.write(stream.read())
            tempfile.close()

            zipfile = ZipFile(filename)
            feed = zipfile.open(zipfile.namelist()[0])

            cls._update_items_from_file(feed, items_model, session)

            feed.close()
            zipfile.close()
            os.remove(filename)

        except Exception as error:
            cls._logger.exception('ERROR importing data file')

    def _update_items_from_file(cls, stream, items_model, session):
        cls._logger.info("Started update items from file for '{}'".format(items_model.__key__))

        warning_message = "Invalid line for model '{}': ".format(items_model.__key__) + '{}'
        validator = Draft4Validator(items_model.__item_type__['schema'])
        lines = []

        old_keys = set(session.redis_bind.hkeys(items_model.__key__))
        new_keys = set()

        for line in stream:
            try:
                line = json.loads(line.decode())
                validator.validate(line)
            except:
                cls._logger.warning(warning_message.format(line))
                continue
            else:
                new_keys.add(items_model.get_instance_key(line))
                lines.append(line)

            if len(lines) == 1000:
                items_model.insert(session, lines)
                lines = []

        if lines:
            items_model.insert(session, lines)

        del lines
        old_keys.difference_update(new_keys)

        if old_keys:
            session.redis_bind.hdel(items_model.__key__, *old_keys)

        cls._set_stock_filter(session, items_model)

        cls._logger.info("Finished update items from file for '{}'".format(items_model.__key__))


class ItemsTypesModelDataImporterBase(ItemsTypesModelBase):

    @classmethod
    def _build_items_collections_schema(cls, key, schema, id_names):
        import_data_file_uri = '/{}/import_data_file'.format(key)
        schema = ItemsTypesModelBase._build_items_collections_schema(key, schema, id_names)
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
                    'consumes': ['application/zip'],
                    'operationId': 'import_data_file',
                    'responses': {'200': {'description': 'Posted'}}
                }
            }
        return schema

    @classmethod
    def _get_items_collections_metaclass(cls):
        return ItemsCollectionsModelDataImporterBaseMeta
