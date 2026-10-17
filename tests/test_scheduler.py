from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from scheduler import build_trigger


class SchedulerTriggerTest(unittest.TestCase):
    def test_trigger_is_pinned_to_utc(self) -> None:
        trigger = build_trigger("0 3 1 * *")
        self.assertEqual(timedelta(0), trigger.timezone.utcoffset(datetime(2026, 1, 1)))

    def test_next_fire_time_is_in_utc(self) -> None:
        trigger = build_trigger("0 3 1 * *")
        now = datetime(2026, 8, 15, 12, 0, tzinfo=timezone.utc)
        fire = trigger.get_next_fire_time(None, now)
        self.assertEqual(datetime(2026, 9, 1, 3, 0, tzinfo=timezone.utc), fire)
        self.assertEqual(timedelta(0), fire.utcoffset())

    def test_invalid_expression_raises(self) -> None:
        with self.assertRaises(ValueError):
            build_trigger("invalid")


if __name__ == "__main__":
    unittest.main()
