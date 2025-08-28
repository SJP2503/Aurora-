#!/usr/bin/env python3
"""
Emit combined RiskEvent payloads to Aurora /risk/feed.
Safe behavior: if AURORA_API_URL is unset, we skip POST and just print a notice.
"""

import os, sys, csv, json, uuid, hashlib
from datetime import datetime, timezone
from typing import List, Dict

import requests
from pydantic import BaseModel, HttpUrl
# ---------- Config via env ----------
API = (os.environ.get("AURORA_API_URL") or "").strip().rstrip("/")
TOKEN = (os.environ.get("AURORA_API_TOKEN") or "").strip()

# ---------- Models (align with aurora/models) ----------
class Evidence(BaseModel):
    url: HttpUrl | None = None
    hash: str | None = None
    excerpt: str | None = None

class RiskEvent(BaseModel):
    event_id: str
    kind: str
    entity_id: str
    severity: str            # info|low|medium|high|critical
    confidence: float        # 0..1
    occurred_at: str         # ISO datetime (UTC)
    evidence: List[Evidence] = []
    meta: Dict[str, object] = {}

# ---------- Helpers ----------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def sha256_bytes(b: bytes) -> str:
    return "sha256:" + hashlib.sha256(b).hexdigest()

def load_csv(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def post_events(events: List[RiskEvent]) -> None:
    if not API:
        print("[emit_risk] AURORA_API_URL empty; skipping POST. Collected events:", len(events))
        return
    url = f"{API}/risk/feed"
    headers = {"Content-Type": "application/json"}
    if TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"
    body = [e.model_dump(exclude_none=True) for e in events]
    r = requests.post(url, headers=headers, data=json.dumps(body), timeout=60)
    r.raise_for_status()
    print("[emit_risk] risk/feed accepted:", r.json())

# ---------- Mappers ----------
def map_legislator_deltas() -> List[RiskEvent]:
    """Snapshot counts (low-noise indicator)."""
    events: List[RiskEvent] = []
    for label, path in [("house","data/philippines_house_members.csv"),
                        ("senate","data/philippines_senators.csv")]:
        rows = load_csv(path)
        if not rows:
            continue
        ev = RiskEvent(
            event_id=str(uuid.uuid4()),
            kind=f"roster_snapshot_{label}",
            entity_id=f"legislature::{label}",
            severity="info",
            confidence=0.9,
            occurred_at=now_iso(),
            meta={"count": len(rows), "source": "wikipedia"}
        )
        events.append(ev)
    return events

def map_pnp_most_wanted() -> List[RiskEvent]:
    """Presence snapshot only (NOT an accusation)."""
    path = "data/pnp/pnp_most_wanted_raw.csv"
    rows = load_csv(path)
    events: List[RiskEvent] = []
    if not rows:
        return events
    # fingerprint first N rows of raw text to avoid massive payloads
    text_concat = "\n".join([r.get("text","") for r in rows[:500]])[:8000].encode("utf-8")
    ev = RiskEvent(
        event_id=str(uuid.uuid4()),
        kind="pnp_most_wanted_snapshot",
        entity_id="pnp::national",
        severity="info",
        confidence=0.7,
        occurred_at=now_iso(),
        evidence=[Evidence(url=None, hash=sha256_bytes(text_concat), excerpt="PNP most-wanted listing snapshot")],
        meta={"rows": len(rows)}
    )
    events.append(ev)
    return events

def map_coa_findings() -> List[RiskEvent]:
    """COA AAR/ND findings (metadata source)."""
    rows = load_csv("data/coa/coa_findings.csv")
    events: List[RiskEvent] = []
    if not rows:
        return events
    # Cap to avoid flooding on first run
    for r in rows[:200]:
        kind = (r.get("kind") or "report").strip()
        sev  = (r.get("severity") or ("high" if kind == "ND_issued" else "medium")).strip()
        ev = RiskEvent(
            event_id=str(uuid.uuid4()),
            kind=kind,  # e.g., ND_issued, AAR_adverse
            entity_id=(r.get("entity_id") or "entity::unknown"),
            severity=sev,
            confidence=0.85 if kind == "ND_issued" else 0.7,
            occurred_at=now_iso(),
            evidence=[Evidence(url=r.get("url") or None, hash=r.get("doc_hash") or None, excerpt=r.get("ref") or None)],
            meta={"ref": r.get("ref")}
        )
        events.append(ev)
    return events

def map_philgeps_awards() -> List[RiskEvent]:
    """Daily volume snapshot for awards (red-flag rules are downstream)."""
    rows = load_csv("data/procurement/philgeps_awards.csv")
    events: List[RiskEvent] = []
    if rows:
        ev = RiskEvent(
            event_id=str(uuid.uuid4()),
            kind="proc_awards_snapshot",
            entity_id="procurement::philgeps",
            severity="info",
            confidence=0.8,
            occurred_at=now_iso(),
            meta={"rows": len(rows)}
        )
        events.append(ev)
    return events

def map_comelec_soce() -> List[RiskEvent]:
    """SOCE snapshot (counts). Linking logic via MEG comes later."""
    rows = load_csv("data/comelec/soce.csv")
    events: List[RiskEvent] = []
    if rows:
        ev = RiskEvent(
            event_id=str(uuid.uuid4()),
            kind="soce_snapshot",
            entity_id="comelec::soce",
            severity="info",
            confidence=0.8,
            occurred_at=now_iso(),
            meta={"rows": len(rows)}
        )
        events.append(ev)
    return events

def map_court_cases() -> List[RiskEvent]:
    """Court events metadata (Ombudsman, Sandiganbayan, SC, CA)."""
    rows = load_csv("data/courts/case_events.csv")
    events: List[RiskEvent] = []
    if not rows:
        return events
    for r in rows[:200]:
        ev = RiskEvent(
            event_id=str(uuid.uuid4()),
            kind="court_case",
            entity_id=(r.get("entity_id") or "entity::unknown"),
            severity="medium",
            confidence=0.75,
            occurred_at=now_iso(),
            evidence=[Evidence(url=r.get("url") or None, excerpt=r.get("ref") or None)],
            meta={"court": r.get("court"), "case_no": r.get("case_no")}
        )
        events.append(ev)
    return events

def map_dbm_releases() -> List[RiskEvent]:
    """DBM releases snapshot (counts)."""
    rows = load_csv("data/dbm/releases.csv")
    events: List[RiskEvent] = []
    if rows:
        ev = RiskEvent(
            event_id=str(uuid.uuid4()),
            kind="dbm_releases_snapshot",
            entity_id="dbm::releases",
            severity="info",
            confidence=0.8,
            occurred_at=now_iso(),
            meta={"rows": len(rows)}
        )
        events.append(ev)
    return events

# ---------- Main ----------
def main():
    events: List[RiskEvent] = []
    # light signals
    events += map_legislator_deltas()
    events += map_pnp_most_wanted()
    # audits / procurement
    events += map_coa_findings()
    events += map_philgeps_awards()
    # campaign finance / courts / budget
    events += map_comelec_soce()
    events += map_court_cases()
    events += map_dbm_releases()

    print(f"[emit_risk] collected events: {len(events)}")
    if events:
        post_events(events)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[emit_risk] ERROR:", e, file=sys.stderr)
        # don't fail the whole workflow on emit; comment next line if you want strict behavior
        # sys.exit(1)
