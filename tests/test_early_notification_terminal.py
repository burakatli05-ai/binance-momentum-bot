import importlib
import json
import os
import tempfile
import unittest
from pathlib import Path


class EarlyNotificationPolicyTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.state_path = Path(self.tempdir.name) / 'early-notification.json'
        self.old_path = os.environ.get('EARLY_NOTIFICATION_STATE_PATH')
        self.old_default = os.environ.get('EARLY_SIGNAL_TELEGRAM_ENABLED')
        os.environ['EARLY_NOTIFICATION_STATE_PATH'] = str(self.state_path)
        os.environ['EARLY_SIGNAL_TELEGRAM_ENABLED'] = '0'
        import notification_policy
        self.policy = importlib.reload(notification_policy)

    def tearDown(self):
        if self.old_path is None:
            os.environ.pop('EARLY_NOTIFICATION_STATE_PATH', None)
        else:
            os.environ['EARLY_NOTIFICATION_STATE_PATH'] = self.old_path
        if self.old_default is None:
            os.environ.pop('EARLY_SIGNAL_TELEGRAM_ENABLED', None)
        else:
            os.environ['EARLY_SIGNAL_TELEGRAM_ENABLED'] = self.old_default
        self.tempdir.cleanup()

    def test_environment_default_is_used_before_terminal_override(self):
        self.assertFalse(self.policy.early_signal_notifications_enabled())
        os.environ['EARLY_SIGNAL_TELEGRAM_ENABLED'] = '1'
        self.assertTrue(self.policy.early_signal_notifications_enabled())
        self.assertEqual(
            self.policy.early_signal_notification_status()['source'],
            'environment',
        )

    def test_terminal_toggle_persists_across_reload(self):
        self.assertTrue(
            self.policy.set_early_signal_notifications_enabled(True)
        )
        self.assertEqual(
            json.loads(self.state_path.read_text())['enabled'],
            True,
        )
        self.policy = importlib.reload(self.policy)
        self.assertTrue(self.policy.early_signal_notifications_enabled())
        self.assertEqual(
            self.policy.early_signal_notification_status()['source'],
            'persistent',
        )
        self.assertFalse(
            self.policy.set_early_signal_notifications_enabled(False)
        )
        self.assertFalse(self.policy.early_signal_notifications_enabled())

    def test_malformed_override_falls_back_and_non_boolean_is_rejected(self):
        os.environ['EARLY_SIGNAL_TELEGRAM_ENABLED'] = '1'
        payloads = (
            'not-json',
            json.dumps([]),
            json.dumps({}),
            json.dumps({'enabled': 'false'}),
        )
        for payload in payloads:
            with self.subTest(payload=payload):
                self.state_path.write_text(payload, encoding='utf-8')
                self.policy = importlib.reload(self.policy)
                self.assertTrue(
                    self.policy.early_signal_notifications_enabled()
                )
        with self.assertRaises(TypeError):
            self.policy.set_early_signal_notifications_enabled('false')


if __name__ == '__main__':
    unittest.main()
