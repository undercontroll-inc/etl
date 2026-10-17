from __future__ import annotations

import unittest

from ml_etl.domain.numbers import as_float, as_int, as_positive_float
from ml_etl.domain.rejection import Layer, Rejection, Severity, Stage
from ml_etl.pipeline.enrich import categories_from_raw, category_row
from ml_etl.pipeline.extract import STAGE_ENRICH, RawPayload


class NumbersTest(unittest.TestCase):
    def test_as_float_accepts_numeric_strings_and_rejects_garbage(self) -> None:
        self.assertEqual(12.5, as_float("12.5"))
        self.assertEqual(0.0, as_float(0))
        self.assertIsNone(as_float(None))
        self.assertIsNone(as_float("abc"))

    def test_as_positive_float_drops_zero_and_negative(self) -> None:
        self.assertEqual(10.0, as_positive_float(10))
        self.assertIsNone(as_positive_float(0))
        self.assertIsNone(as_positive_float(-3))
        self.assertIsNone(as_positive_float("x"))

    def test_as_int(self) -> None:
        self.assertEqual(7, as_int("7"))
        self.assertIsNone(as_int(None))
        self.assertIsNone(as_int("7.5"))


class RejectionFactoryTest(unittest.TestCase):
    def test_factories_set_severity(self) -> None:
        blocking = Rejection.blocking(Stage.EXTRACT, Layer.ESTRUTURAL, "regra", external_id="MLB1")
        warning = Rejection.warning(Stage.ENRICH, Layer.INTEGRIDADE, "regra", detail="d")

        self.assertEqual(Severity.BLOQUEANTE, blocking.severity)
        self.assertEqual("MLB1", blocking.external_id)
        self.assertEqual(Severity.AVISO, warning.severity)
        self.assertEqual("d", warning.as_row("run")["detail"])


class CategoryRowTest(unittest.TestCase):
    BODY = {
        "id": "MLB73057",
        "name": "Micro-ondas",
        "total_items_in_this_category": "31000",
        "path_from_root": [{"id": "MLB5726"}, {"id": "MLB73057"}],
    }

    def test_category_row_resolves_parent(self) -> None:
        row = category_row("MLB73057", self.BODY)
        self.assertEqual("MLB5726", row["parent_id"])
        self.assertEqual(31000, row["total_items"])

    def test_root_category_has_no_parent(self) -> None:
        self.assertIsNone(category_row("MLB1", {"name": "Raiz", "path_from_root": [{"id": "MLB1"}]})["parent_id"])

    def test_categories_from_raw_only_reads_category_payloads(self) -> None:
        payloads = [
            RawPayload(stage=STAGE_ENRICH, endpoint="/categories/MLB73057", status_code=200, payload=self.BODY),
            RawPayload(stage=STAGE_ENRICH, endpoint="/products/MLB1", status_code=200, payload={"id": "MLB1"}),
            RawPayload(stage=STAGE_ENRICH, endpoint="/categories/MLBX", status_code=404, payload=None),
        ]
        self.assertEqual(["MLB73057"], list(categories_from_raw(payloads)))


if __name__ == "__main__":
    unittest.main()
