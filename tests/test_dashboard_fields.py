import json
from pathlib import Path
import unittest
from src.test_dashboard import assert_calculated_fields_acyclic


class DashboardCalculatedFieldsTest(unittest.TestCase):
    def test_original_case_insensitive_collision_is_rejected(self):
        with self.assertRaisesRegex(AssertionError, "Circular"):
            assert_calculated_fields_acyclic({"columns":[{"displayName":"Revenue","expression":"SUM(`revenue`)"}]})

    def test_indirect_cycle_is_rejected(self):
        with self.assertRaisesRegex(AssertionError, "Circular"):
            assert_calculated_fields_acyclic({"columns":[{"displayName":"A","expression":"SUM(`B`)"},
                                                       {"displayName":"B","expression":"SUM(`a`)"}]})

    def test_current_dashboard_has_no_calculated_field_cycles(self):
        dashboard=json.loads(Path("src/dashboards/payments.lvdash.json").read_text())
        for dataset in dashboard["datasets"]:
            assert_calculated_fields_acyclic(dataset)


if __name__=="__main__":unittest.main()
