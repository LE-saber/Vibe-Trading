"""IMPLEMENTATION BOUNDARY, not a working ninef connector.

Local Codex must map CURRENT local source and verified raw contracts here.
Only this trusted local implementation may read credentials. Never expose raw
headers/errors to the browser/logs; bound network timeouts; never retry writes.
"""
from vibe_trading.models import Capabilities


class LocalProvider:
    def capabilities(self):
        return Capabilities(provider='ninef-local', environment='NINEF_SIMULATION',
                            verified=False, verification_ref='')

    def context(self, symbols):
        raise NotImplementedError('Map current account, all order pages, quotes, fees and rules.')

    def leaderboard(self):
        raise NotImplementedError('Map actual contest top-30 and own rank separately.')

    def news(self):
        raise NotImplementedError('Map news IDs, sources, timestamps, symbols and bodies.')

    def research(self, request, report, cancelled):
        raise NotImplementedError('Wrap existing research with progress, holding review and news.')

    def submit(self, order, client_order_id):
        raise NotImplementedError('Only the execution service may call this after approval.')

    def get_order(self, order_id):
        raise NotImplementedError('Read the exact authoritative broker ID; missing is not cancelled.')

    def cancel(self, order_id):
        raise NotImplementedError('One explicitly approved cancel attempt; no internal retries.')


def create_provider():
    return LocalProvider()
