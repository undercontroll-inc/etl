from __future__ import annotations

import os
import unittest
from unittest import mock

from ml_etl import config as config_module
from ml_etl.db import engine as engine_module
from ml_etl.pipeline import orchestrator, run_pipeline
from tests.fake_api import FakeMercadoLivreApi

TARGET_CATEGORIES = ["MLB5726", "MLB432825"]


class PipelineTestCase(unittest.TestCase):
    extra_env: dict[str, str] = {}

    def setUp(self) -> None:
        self._env = dict(os.environ)
        os.environ.clear()
        os.environ.update(
            {
                "DB_BACKEND": "memory",
                "ML_CLIENT_ID": "fake-client-id",
                "ML_CLIENT_SECRET": "fake-client-secret",
                "ETL_TARGET_CATEGORIES": ",".join(TARGET_CATEGORIES),
                "ETL_SNAPSHOT_BUCKET": "month",
                "ETL_MAX_WORKERS": "2",
                "ETL_LOG_LEVEL": "CRITICAL",
                **self.extra_env,
            }
        )
        config_module.reset_settings_cache()
        engine_module.dispose_engine()
        self.settings = config_module.load_settings(use_dotenv=False)
        self.assertTrue(self.settings.database.is_memory, "a suite deve rodar apenas em memoria")

    def tearDown(self) -> None:
        engine_module.dispose_engine()
        config_module.reset_settings_cache()
        os.environ.clear()
        os.environ.update(self._env)

    def _run(self, **kwargs):
        with mock.patch.object(orchestrator, "MercadoLivreApi", FakeMercadoLivreApi):
            return run_pipeline(settings=self.settings, **kwargs)
