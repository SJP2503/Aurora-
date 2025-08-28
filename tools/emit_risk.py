# --- add near the top imports if missing ---
import hashlib, uuid
from typing import List, Dict

# --- add helper to load CSV if not present ---
def load_csv(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path): return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

# --- new: COA mapper ---
def map_coa_findings() -> List[RiskEvent]:
    path = "data/coa/coa_findings.csv"
    rows = load_csv(path)
    events: List[RiskEvent] = []
    for r in rows[:200]:  # cap to avoid flooding
        sev = r.get("severity","medium")
        kind = r.get("kind","report")
        ev = RiskEvent(
            event_id=str(uuid.uuid4()),
            kind=kind,  # e.g., ND_issued, AAR_adverse
            entity_id=r.get("entity_id") or "entity::unknown",
            severity=sev,
            confidence=0.85 if kind == "ND_issued" else 0.7,
            occurred_at=now_iso(),
            evidence=[Evidence(url=r.get("url") or None, hash=r.get("doc_hash") or None, excerpt=r.get("ref") or None)],
            meta={"ref": r.get("ref")}
        )
        events.append(ev)
    return events

# --- new: PhilGEPS mapper ---
def map_philgeps_awards() -> List[RiskEvent]:
    path = "data/procurement/philgeps_awards.csv"
    rows = load_csv(path)
    events: List[RiskEvent] = []
    if not rows: return events

    # Example: low-severity info event with daily snapshot size
    ev = RiskEvent(
        event_id=str(uuid.uuid4()),
        kind="proc_awards_snapshot",
        entity_id="procurement::philgeps",
        severity="info",
        confidence=0.8,
        occurred_at=now_iso(),
        evidence=[],
        meta={"rows": len(rows)}
    )
    events.append(ev)
    return events

# --- in main(): add to the events list ---
    events += map_coa_findings()
    events += map_philgeps_awards()
