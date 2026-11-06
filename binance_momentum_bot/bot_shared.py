"""Launch production bot with shared Public H1 execution and supervised runtime."""

import runpy

import early_v2_adapter
from early_public_execution import PublicH1PremiumIntegration
from runtime_supervisor import run_runtime

early_v2_adapter.Integration = PublicH1PremiumIntegration

if __name__ == "__main__":
    runtime = runpy.run_path("bot.py", run_name="momentum_runtime")
    run_runtime(runtime)
