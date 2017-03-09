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


from myreco.utils import extend_swagger_json
from myreco.engines.data_importer.model import EnginesDataImporterModelBase
from copy import deepcopy
import asyncio


class EnginesObjectsExporterModelBase(EnginesDataImporterModelBase):
    __swagger_json__ = extend_swagger_json(
        EnginesDataImporterModelBase.__swagger_json__,
        __file__
    )

    @classmethod
    async def post_export_objects_job(cls, req, session):
        session = cls._copy_session(session)
        engine = await cls._get_engine(req, session)
        if engine is None:
            return cls._build_response(404)

        jobs_id = cls._get_jobs_id_exporter(engine)
        return cls._create_job(cls._run_export_objects_job, jobs_id, req, session, engine)

    @classmethod
    def _get_jobs_id_exporter(cls, engine):
        return cls._get_jobs_id(engine) + '_exporter'

    @classmethod
    def _run_export_objects_job(cls, req, session, engine):
        import_data = req.query.get('import_data')

        if import_data:
            importer_result = engine.core_instance.get_data(session)
            exporter_result = engine.core_instance.export_objects(session)

            return {
                'importer': importer_result,
                'exporter': exporter_result
            }
        else:
            return engine.core_instance.export_objects(session)

    @classmethod
    async def get_export_objects_job(cls, req, session):
        engine = await cls._get_engine(req, session)
        if engine is None:
            return cls._build_response(404)

        jobs_id = cls._get_jobs_id_exporter(engine)
        return await cls._get_job(jobs_id, req, session)
