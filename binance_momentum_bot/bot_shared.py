"""Launch the production bot with the Early selector bound to Premium AutoTrade."""

import runpy

import early_v2_adapter
from early_shared_execution import SharedPremiumIntegration


early_v2_adapter.Integration = SharedPremiumIntegration

if __name__ == "__main__":
    runpy.run_path("bot.py", run_name="__main__")
