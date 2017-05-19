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


from myreco.engine_objects.model import EngineObjectsModelBase
from myreco.utils import extend_swagger_json, get_items_model
from copy import deepcopy
import asyncio


class EngineObjectsDataImporterModelBase(EngineObjectsModelBase):
    __swagger_json__ = extend_swagger_json(
        EngineObjectsModelBase.__swagger_json__,
        __file__
    )

    @classmethod
    async def post_import_data_job(cls, req, session):
        session = cls._copy_session(session)
        engine_object = await cls._get_engine_object(req, session)

        if engine_object is None:
            return cls._build_response(404)

        jobs_id = cls._get_jobs_id_importer(engine_object)
        response = cls._create_job(cls._run_import_data_job, jobs_id, req, session, engine_object)
        return response

    @classmethod
    async def _get_engine_object(cls, req, session):
        id_ = req.path_params['id']
        engine_objects = await cls.get(session, {'id': id_}, todict=False)

        if not engine_objects:
            return None

        return engine_objects[0]

    @classmethod
    def _get_jobs_id_importer(cls, engine_object):
        return cls._get_jobs_id(engine_object) + '_importer'

    @classmethod
    def _get_jobs_id(cls, engine_object):
        return '{}_{}_{}'.format(
            engine_object.strategy.name,
            engine_object.type,
            engine_object.id
        )

    @classmethod
    def _run_import_data_job(cls, req, session, engine_object):
        items_model = cls._get_items_model(engine_object)
        engine_object = cls._get_engine_object_instance(engine_object)
        return engine_object.get_data(
            items_model,
            session
        )

    @classmethod
    def _get_items_model(cls, engine_object):
        return get_items_model(
            engine_object.item_type.todict(),
            engine_object.store_id
        )

    @classmethod
    def _get_engine_object_instance(cls, engine_object):
        strategy_class = engine_object.strategy.get_class()
        object_class = strategy_class.object_types[engine_object.type]
        return object_class(engine_object.todict(), cls.__data_path__)

    @classmethod
    async def get_import_data_job(cls, req, session):
        engine_object = await cls._get_engine_object(req, session)
        if engine_object is None:
            return cls._build_response(404)

        jobs_id = cls._get_jobs_id_importer(engine_object)
        return await cls._get_job(jobs_id, req, session)
