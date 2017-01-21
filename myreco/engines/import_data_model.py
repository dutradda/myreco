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


from swaggerit.utils import get_model_schema
from swaggerit.response import SwaggerResponse
from myreco.engines.cores.utils import build_engine_key_prefix
from myreco.engines.base_model import EnginesModelBase
from copy import deepcopy
import asyncio


import_data_schema = get_model_schema(__file__, 'import_data_swagger_schema.json')
import_data_schema.update(deepcopy(EnginesModelBase.__schema__))


class EnginesModelImportDataBase(EnginesModelBase):
    __schema__ = import_data_schema

    @classmethod
    async def post_import_data_job(cls, req, session):
        session = cls._copy_session(session)
        engine = await cls._get_engine(req, session)
        if engine is None:
            return SwaggerResponse(404)

        jobs_id = cls._get_jobs_id_importer(engine)
        response = cls._create_job(cls._run_import_data_job, jobs_id, req, session, engine)
        return response

    @classmethod
    async def _get_engine(cls, req, session):
        id_ = req.path_params['id']
        engines = await cls.get(session, {'id': id_}, todict=False)

        if not engines:
            return None

        return engines[0]

    @classmethod
    def _get_jobs_id_importer(cls, engine):
        return cls._get_jobs_id(engine) + '_importer'

    @classmethod
    def _get_jobs_id(cls, engine):
        return build_engine_key_prefix({'id': engine.id, 'core': {'name': engine.core.name}})

    @classmethod
    def _run_import_data_job(cls, req, session, engine):
        return engine.core_instance.get_data(session)

    @classmethod
    async def get_import_data_job(cls, req, session):
        engine = await cls._get_engine(req, session)
        if engine is None:
            return SwaggerResponse(404)

        jobs_id = cls._get_jobs_id_importer(engine)
        return await cls._get_job(jobs_id, req, session)
