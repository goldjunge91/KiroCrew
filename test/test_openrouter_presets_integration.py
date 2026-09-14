import tempfile
import unittest
from pathlib import Path

from kiro_crew.dashboard.chat_handlers import _model_rejected_reason
from kiro_crew.openrouter_byok import OpenRouterBYOKManager


class TestOpenRouterPresetsIntegration(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self.tmp_dir.name)
        self.mgr = OpenRouterBYOKManager(base_dir=self.base_dir)

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_preset_lookup_and_resolution(self):
        key_info = self.mgr.add_key("Test Key", "sk-or-v1-1234567890abcdef")
        preset = self.mgr.add_preset(
            name="My Custom Fast Model",
            key_id=key_info["id"],
            model_name="anthropic/claude-3.5-haiku",
        )

        found_by_name = self.mgr.get_preset_by_name_or_id("My Custom Fast Model")
        self.assertIsNotNone(found_by_name)
        self.assertEqual(found_by_name["id"], preset["id"])

        found_by_id = self.mgr.get_preset_by_name_or_id(preset["id"])
        self.assertIsNotNone(found_by_id)

        res_key_id, res_raw_key, res_model = self.mgr.resolve_model(
            task_override=("", "My Custom Fast Model")
        )
        self.assertEqual(res_key_id, key_info["id"])
        self.assertEqual(res_raw_key, "sk-or-v1-1234567890abcdef")
        self.assertEqual(res_model, "anthropic/claude-3.5-haiku")

    def test_preset_validation_guard(self):
        default_mgr = OpenRouterBYOKManager()
        preset = default_mgr.add_preset(
            name="Test Preset Guard",
            key_id="",
            model_name="openai/gpt-4o-mini",
        )
        try:
            reason = _model_rejected_reason("Test Preset Guard")
            self.assertIsNone(reason)
        finally:
            default_mgr.delete_preset(preset["id"])


if __name__ == "__main__":
    unittest.main()
