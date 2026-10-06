"""Fail on any head failure not independently reproduced in the pinned base."""
import json
from pathlib import Path
import sys
base = json.loads(Path(sys.argv[1]).read_text())
head = json.loads(Path(sys.argv[2]).read_text())
def signatures(data):
    return {(category, item["test_id"], item["terminal"])
            for category in ("failures", "errors") for item in data[category]}
new = sorted(signatures(head) - signatures(base))
new_unexpected = sorted(set(head["unexpected_successes"]) - set(base["unexpected_successes"]))
report = {"base_tests": base["tests_run"], "head_tests": head["tests_run"],
          "base_failures": len(base["failures"]), "base_errors": len(base["errors"]),
          "head_failures": len(head["failures"]), "head_errors": len(head["errors"]),
          "new_failure_signatures": new, "new_unexpected_successes": new_unexpected,
          "status": "DEPLOYMENT_BLOCKED_NEW_REGRESSION" if new or new_unexpected else "NO_NEW_REGRESSION"}
Path(sys.argv[3]).write_text(json.dumps(report, indent=2), encoding="utf-8")
print(json.dumps(report, indent=2))
sys.exit(1 if new or new_unexpected else 0)
