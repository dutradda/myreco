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


from myreco.items_types.model import ItemsTypesModelBase
from myreco.utils import extend_swagger_json
from swaggerit.exceptions import SwaggerItModelError
from jsonschema import Draft4Validator
from tempfile import NamedTemporaryFile
from aiofiles import zip_open, open as aiopen
from io import BytesIO
from collections import namedtuple
import ujson
import boto3
import os
import asyncio
import gc


class ItemsTypesDataFileImporterModelBase(ItemsTypesModelBase):
    __swagger_json__ = extend_swagger_json(
        ItemsTypesModelBase.__swagger_json__,
        __file__
    )

    @classmethod
    async def post_import_data_file_job(cls, req, session):
        content_type = req.headers.get('content-type')
        if content_type != 'application/zip':
            raise SwaggerItModelError("Invalid content type '{}'".format(content_type))

        stream = BytesIO()
        async for line in req.body:
            stream.write(line)
        stream.seek(0)

        return await cls._create_job(
            cls._run_import_data_file_job,
            req, session, '_importer', stream=stream)

    @classmethod
    async def _create_job(cls, func, req, session, jobs_id_prefix, stream=None):
        store_id = req.query['store_id']
        store_items_model = await cls._get_store_items_model(req, session)
        if store_items_model is None:
            return cls._build_response(404)

        session = cls._copy_session(session)
        jobs_id = store_items_model.__key__ + jobs_id_prefix
        return super()._create_job(
            func, jobs_id,
            req, session,
            store_items_model, store_id,
            stream=stream
        )

    @classmethod
    def _run_import_data_file_job(cls, req, session, store_items_model, store_id, stream):
        upload_file = req.query.get('upload_file', True)
        if upload_file:
            cls._put_file_on_s3(stream, store_items_model, session, store_id)
            stream.seek(0)

        result = asyncio.run_coroutine_threadsafe(
            cls._update_items_from_zipped_file(stream, store_items_model, session),
            session.loop
        ).result()

        gc.collect()
        return result

    @classmethod
    def _put_file_on_s3(cls, stream, store_items_model, session, store_id):
        cls._logger.info("Started put file on S3 for '{}'".format(store_items_model.__key__))

        store = asyncio.run_coroutine_threadsafe(
            cls.get_model('stores').get(session, [{'id': store_id}]),
            session.loop
        ).result()[0]

        s3_bucket = store['configuration']['aws']['s3']['bucket']
        access_key_id = store['configuration']['aws'].get('access_key_id')
        secret_access_key = store['configuration']['aws'].get('secret_access_key')
        s3_key = '{}.zip'.format(store_items_model.__key__)

        boto3.resource(
            's3',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key
        ).Bucket(s3_bucket).put_object(Body=stream, Key=s3_key)

        cls._logger.info("Finished put file on S3 for '{}'".format(store_items_model.__key__))

    @classmethod
    async def _update_items_from_zipped_file(cls, stream, store_items_model, session):
        feed = await cls._build_async_file(zip_open, stream)
        result = await cls._update_items_from_file(feed, store_items_model, session)

        await feed.close()
        os.remove(feed.name)
        return result

    @classmethod
    async def _build_async_file(cls, file_func, stream=None, mode=None):
        tempfile = NamedTemporaryFile(delete=False)
        filename = tempfile.name
        kwargs = {'mode': mode} if mode else {}
        if stream is not None:
            tempfile.write(stream.read())

        tempfile.close()
        af = await file_func(filename, **kwargs)
        af.name = filename
        return af

    @classmethod
    async def _update_items_from_file(cls, feed, store_items_model, session):
        cls._logger.info(
            "Started update items from file for '{}'".format(store_items_model.__key__)
        )
        warning_message = "Invalid line for model '{}': ".format(store_items_model.__key__) + '{}'
        validator = Draft4Validator(store_items_model.item_type['schema'])
        new_keys = set()
        success_lines = 0
        errors_lines = 0
        new_keys = set()
        success_lines = 0
        errors_lines = 0
        empty_lines = 0
        validator = Draft4Validator(store_items_model.item_type['schema'])
        new_keys = set()
        success_lines = 0
        errors_lines = 0
        empty_lines = 0
        old_keys = set(await session.redis_bind.hkeys(store_items_model.__key__))
        lines = []

        async for line in feed:
            try:
                line = cls._try_to_process_line(line, validator)
                if line is None:
                    empty_lines += 1
                    continue
                else:
                    success_lines += 1
                    new_keys.add(store_items_model.get_instance_key(line))
                    lines.append(line)

                    if len(lines) == 1000:
                        await store_items_model.insert(session, lines)
                        lines = []

            except Exception as error:
                errors_lines += 1
                cls._logger.warning(warning_message.format(line))
                cls._logger.warning(error)
                continue

        if lines:
            await store_items_model.insert(session, lines)

        del lines
        await feed.close()
        old_keys.difference_update(new_keys)

        if old_keys:
            await session.redis_bind.hdel(store_items_model.__key__, *old_keys)

        await cls._set_stock_filter(store_items_model, session)

        cls._logger.info(
            "Finished update items from file for '{}'".format(store_items_model.__key__)
        )

        return {
            'success_lines': success_lines,
            'errors_lines': errors_lines,
            'empty_lines': empty_lines
        }

    @classmethod
    def _try_to_process_line(cls, line, validator):
        line = line.strip().decode()
        if not line:
            return None

        line = ujson.loads(line)
        validator.validate(line)

        return line

    @classmethod
    async def get_import_data_file_job(cls, req, session):
        return await cls._get_job('_importer', req, session)

    @classmethod
    async def _get_job(cls, sufix, req, session):
        store_items_model = await cls._get_store_items_model(req, session)
        if store_items_model is None:
            return cls._build_response(404)

        jobs_id = store_items_model.__key__ + sufix
        return await super()._get_job(jobs_id, req, session)
