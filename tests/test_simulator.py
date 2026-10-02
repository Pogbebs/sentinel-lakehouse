from collections import defaultdict
from datetime import datetime, timezone

from sentinel.generator.geo import haversine_km
from sentinel.generator.simulator import ATTACK_TYPES, OFFICE_NAT_IP, SimConfig, Simulator
from sentinel.generator.sinks import breach_dump_csv

START = datetime(2026, 10, 1, 8, tzinfo=timezone.utc)


def _events(minutes=60, **kw):
    cfg = SimConfig(users=800, events_per_minute=80, **kw)
    return list(Simulator(cfg, START).run(minutes))


def test_same_seed_same_output():
    assert _events(10, seed=7) == _events(10, seed=7)
    assert _events(10, seed=7) != _events(10, seed=8)


def test_events_are_time_ordered():
    ts = [e["event_ts"] for e in _events(30) if e["event_ts"]]
    assert ts == sorted(ts)


def test_every_attack_type_is_generated():
    seen = {e["attack_type"] for e in _events(60, attacks_per_hour=30)}
    assert set(ATTACK_TYPES) <= seen


def test_noise_is_injected():
    events = _events(60, malformed_rate=0.01, duplicate_rate=0.01)
    ids = [e["event_id"] for e in events if e["event_id"]]
    assert len(ids) > len(set(ids)), "expected producer-retry duplicates"
    assert any(
        e["event_ts"] is None
        or e["outcome"] == "SUCESS"
        or e["geo_lat"] == 999.0
        or e["event_id"] is None
        for e in events
    )


def test_breach_dump_contains_only_hashes():
    sim = Simulator(SimConfig(users=200, breach_corpus_size=100), START)
    body = breach_dump_csv(sim.breach_records)
    assert "@" not in body
    header, first = body.splitlines()[:2]
    assert header == "email_sha256,password_sha1,source,breach_date"
    assert len(first.split(",")[0]) == 64


def test_legitimate_travel_is_physically_possible():
    """Hard negative: benign users who travel must never look like impossible travel."""
    by_user = defaultdict(list)
    for e in _events(240, attacks_per_hour=0, malformed_rate=0, duplicate_rate=0):
        if e["outcome"] == "success" and e["attack_type"] == "none":
            by_user[e["user_id"]].append(e)
    for logins in by_user.values():
        for a, b in zip(logins, logins[1:], strict=False):
            km = haversine_km(a["geo_lat"], a["geo_lon"], b["geo_lat"], b["geo_lon"])
            hours = (
                datetime.fromisoformat(b["event_ts"][:-1])
                - datetime.fromisoformat(a["event_ts"][:-1])
            ).total_seconds() / 3600
            if km > 500:
                assert km / max(hours, 1 / 60) < 900


def test_office_nat_ip_is_shared_by_many_users():
    users = {e["username"] for e in _events(30) if e["src_ip"] == OFFICE_NAT_IP}
    assert len(users) > 50
