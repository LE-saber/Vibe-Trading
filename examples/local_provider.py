"""Local entrypoint for the strict ninef adapter.

Credentials stay in process environment.  Real research is separately loaded
from ``VIBE_RESEARCH_PROVIDER=module:factory``; absence fails honestly and
never falls back to the DEMO strategy.
"""

from vibe_trading.adapters.ninef import create_provider

__all__ = ["create_provider"]
