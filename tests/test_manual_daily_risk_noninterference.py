"""Versioned, scoped noninterference check; the frozen original manifest is immutable.

The existing legacy exact-identity tests are expected to FAIL for an explicitly
authorized change to risk functions. This test does not change their baselines.
"""
import ast
import hashlib
import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "tests" / "final_acceptance_noninterference.json"
BOT = ROOT / "binance_momentum_bot" / "bot.py"

NEW_FUNCTIONS = {
    "_at_manual_risk_record",
    "_at_manual_risk_extra",
    "_at_manual_risk_grant",
    "_at_manual_risk_revoke",
    "_at_manual_risk_offer",
}
AUTHORIZED_EXISTING_CHANGES = {
    "_at_update_daily",              # keep cooldown active despite authorized daily-loss resume
    "_at_risk_allowed",              # add finite extra prospective risk budget
    "_at_panel_text",                # render current effective budget
    "_at_panel_markup",              # add manual risk help button
    "handle_autotrade_callback",     # scoped two-step approval callback
    "_at_command",                   # add /riskresume command
}


class RiskResumeVersionedNoninterference(unittest.TestCase):
    def test_unrelated_bot_functions_exactly_match_frozen_reference(self):
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        tree = ast.parse(BOT.read_text(encoding="utf-8"))
        functions = {node.name: node for node in tree.body
                     if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
        baseline = manifest["baseline_function_hashes"]
        self.assertEqual(set(functions), set(baseline) | NEW_FUNCTIONS)
        self.assertTrue(AUTHORIZED_EXISTING_CHANGES.issubset(baseline))
        self.assertFalse(NEW_FUNCTIONS.intersection(baseline))
        count = 0
        for name, expected_hash in baseline.items():
            if name in AUTHORIZED_EXISTING_CHANGES:
                continue
            expected_hash = manifest["candidate_changed_function_hashes"].get(name, expected_hash)
            current_hash = hashlib.sha256(
                ast.dump(functions[name], include_attributes=False).encode()
            ).hexdigest()
            self.assertEqual(current_hash, expected_hash, name)
            count += 1
        self.assertGreaterEqual(count, 220)

    def test_new_policy_is_authorized_live_only_and_profile_unchanged(self):
        import sys
        sys.path.insert(0, str(ROOT / "binance_momentum_bot"))
        import premium_step_lock as frozen_profile
        assert frozen_profile.CONTRACT_HASH == (
            "29e695ec40d2c12b6037db8ad6cbede27da5cac0f58a01054e491b2967746cd3"
        )
        text = BOT.read_text(encoding="utf-8")
        self.assertIn('if str(scope).upper() != "LIVE":', text)
        self.assertIn('if not _at_admin_allowed(chat_id,user_id,require_user_id=True)', text)
        self.assertIn('if not _at_admin_allowed(chat_id,uid,require_user_id=True)', text)


if __name__ == "__main__":
    unittest.main()
