"""Launch the production bot with Public H1 Early bound to Premium AutoTrade."""

import runpy

import early_v2_adapter
from early_public_execution import PublicH1PremiumIntegration


early_v2_adapter.Integration = PublicH1PremiumIntegration

if __name__ == "__main__":
    runpy.run_path("bot.py", run_name="__main__")
