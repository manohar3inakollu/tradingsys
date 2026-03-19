import os
import sys

# Ensure project root is on the path before importing project modules.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import health  # noqa: E402


class DummyCursor:
    def execute(self, q):
        return None


class DummyConn:
    def cursor(self):
        return DummyCursor()


class DummyDB:
    def __enter__(self):
        return DummyConn()

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyAlpaca:
    def get_latest_bar(self, sym):
        return {'c': 123.45}


def test_check_all_smoke(monkeypatch):
    # Patch DB connection used inside health module
    monkeypatch.setattr(health, 'db_connection', lambda: DummyDB())

    # Patch Alpaca client
    monkeypatch.setattr(health, 'AlpacaClient', lambda: DummyAlpaca())

    # Ensure optional env vars are unset to avoid real network calls
    for k in ('FINNHUB_API_KEY', 'TRADIER_TOKEN', 'TWILIO_ACCOUNT_SID',
              'TWILIO_AUTH_TOKEN', 'ANTHROPIC_API_KEY', 'GOOGLE_SERVICE_ACCOUNT_JSON',
              'SHEETS_SPREADSHEET_ID'):
        os.environ.pop(k, None)

    results = health.check_all()

    assert isinstance(results, dict)

    # All expected keys present and string-valued
    for key in ('postgres', 'alpaca', 'finviz', 'finnhub', 'tradier', 'twilio', 'anthropic', 'sheets'):
        assert key in results, f"missing key: {key}"
        assert isinstance(results[key], str), f"{key} should be a string"

    # Required checks pass with our dummies
    assert results['postgres'] == 'ok'
    assert results['alpaca'].startswith('ok')

    # Optional checks skipped when env vars are absent
    for key in ('finnhub', 'tradier', 'twilio', 'anthropic', 'sheets'):
        assert results[key].startswith('skipped'), f"{key} should be skipped when unconfigured"
