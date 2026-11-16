"""Wires settings, secrets, warehouse, landing and ops into the Context every command runs with."""

from __future__ import annotations

from storeops.contracts import load_contracts
from storeops.landing import GcsLanding, LocalLanding
from storeops.ops import Ops
from storeops.runner import Context
from storeops.secrets import provider_from_spec
from storeops.settings import Settings
from storeops.warehouse import connect, ensure_ops


def build_context(settings: Settings | None = None, *, secrets=None, wh=None, landing=None,
                  connector_factory=None) -> Context:
    settings = settings or Settings.from_env()
    secrets = secrets or provider_from_spec(settings.secrets)
    wh = wh or connect(settings, secrets)
    ensure_ops(wh)
    if landing is None:
        landing = (GcsLanding(settings.landing_bucket, settings.restricted_bucket) if settings.landing == "gcs"
                   else LocalLanding(settings.local_landing_dir))
    ops = Ops(wh, settings.alert_webhook_url)
    ctx = Context(settings=settings, contracts=load_contracts(settings.contracts_dir), wh=wh, landing=landing,
                  secrets=secrets, ops=ops, now=settings.now, connector_factory=connector_factory)
    ops.now = lambda: ctx.now()
    return ctx
