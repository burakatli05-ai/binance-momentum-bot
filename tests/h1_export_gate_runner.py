"""CI evidence collector; preserves existing failure IDs and full tracebacks."""
import argparse
import json
import os
from pathlib import Path
import sys
import unittest

p = argparse.ArgumentParser()
p.add_argument("--root", required=True)
p.add_argument("--output", required=True)
args = p.parse_args()
root = Path(args.root).resolve()
output = Path(args.output).resolve()
os.chdir(root)
sys.path[:0] = [str(root), str(root / "binance_momentum_bot"), str(root / "tests")]
suite = unittest.defaultTestLoader.discover(str(root / "tests"), pattern="test*.py")
result = unittest.TextTestRunner(verbosity=2).run(suite)
def evidence(items):
    return [{"test_id": test.id(), "traceback": detail,
             "terminal": detail.strip().splitlines()[-1]}
            for test, detail in items]
record = {"tests_run": result.testsRun, "failures": evidence(result.failures),
          "errors": evidence(result.errors),
          "skipped": [{"test_id": t.id(), "reason": why} for t, why in result.skipped],
          "unexpected_successes": [t.id() for t in result.unexpectedSuccesses]}
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text(json.dumps(record, indent=2), encoding="utf-8")
print("REGRESSION_EVIDENCE " + json.dumps({k: len(v) if isinstance(v, list) else v for k, v in record.items()}))
# Evidence collection intentionally completes even with baseline failures.
# A separate mandatory comparison step decides regression acceptance.
