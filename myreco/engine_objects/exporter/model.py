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


from myreco.utils import extend_swagger_json, get_items_model
from myreco.engine_objects.data_importer.model import EngineObjectsDataImporterModelBase
from copy import deepcopy
import asyncio


class EngineObjectsExporterModelBase(EngineObjectsDataImporterModelBase):
    __swagger_json__ = extend_swagger_json(
        EngineObjectsDataImporterModelBase.__swagger_json__,
        __file__
    )

    @classmethod
    async def post_export_job(cls, req, session):
        session = cls._copy_session(session)
        engine_object = await cls._get_engine_object(req, session)

        if engine_object is None:
            return cls._build_response(404)

        jobs_id = cls._get_jobs_id_exporter(engine_object)
        return cls._create_job(cls._run_export_objects_job, jobs_id, req, session, engine_object)

    @classmethod
    def _get_jobs_id_exporter(cls, engine_object):
        return cls._get_jobs_id(engine_object) + '_exporter'

    @classmethod
    def _run_export_objects_job(cls, req, session, engine_object):
        import_data = req.query.get('import_data')
        items_model = cls._get_items_model(engine_object)
        engine_object = cls.get_engine_object_instance(engine_object)

        if import_data:
            importer_result = engine_object.get_data(items_model, session)
            exporter_result = engine_object.export(items_model, session)
            return {
                'importer': importer_result,
                'exporter': exporter_result
            }

        else:
            return engine_object.export(items_model, session)

    @classmethod
    async def get_export_job(cls, req, session):
        engine_object = await cls._get_engine_object(req, session)
        if engine_object is None:
            return cls._build_response(404)

        jobs_id = cls._get_jobs_id_exporter(engine_object)
        return await cls._get_job(jobs_id, req, session)
