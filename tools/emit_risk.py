def map_comelec_soce() -> List[RiskEvent]:
    rows = load_csv("data/comelec/soce.csv")
    events: List[RiskEvent] = []
    # Example: emit donor-link snapshot (low severity). Later you can add
    # per-entity events when MEG links donors -> vendors/officials.
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
    rows = load_csv("data/courts/case_events.csv")
    events: List[RiskEvent] = []
    for r in rows[:200]:
        ev = RiskEvent(
            event_id=str(uuid.uuid4()),
            kind="court_case",
            entity_id=r.get("entity_id") or "entity::unknown",
            severity="medium",
            confidence=0.75,
            occurred_at=now_iso(),
            evidence=[Evidence(url=r.get("url") or None, excerpt=r.get("ref") or None)],
            meta={"court": r.get("court"), "case_no": r.get("case_no")}
        )
        events.append(ev)
    return events

def map_dbm_releases() -> List[RiskEvent]:
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

# in main():
    events += map_comelec_soce()
    events += map_court_cases()
    events += map_dbm_releases()
