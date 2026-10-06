"""Static checks on the provisioned Grafana alerting, since a bad file stops Grafana starting."""

from __future__ import annotations

import json
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
ALERTING = ROOT / "grafana" / "provisioning" / "alerting" / "sentinel-alerts.yml"


def load() -> dict:
    return yaml.safe_load(ALERTING.read_text())


def test_only_the_intended_environment_variable_is_used():
    """Grafana expands $VARS in provisioning files: a stray $ would corrupt a query or template."""
    text = "\n".join(
        line for line in ALERTING.read_text().splitlines() if not line.lstrip().startswith("#")
    )
    assert sorted(set(re.findall(r"\$\{?\w+\}?", text))) == [
        "${ALERT_EMAIL_TO}",
        "${TEAMS_SEVERITIES}",
        "${TEAMS_WEBHOOK_URL}",
    ]


def test_rules_are_well_formed():
    cfg = load()
    datasources = {"sentinel-pg", "__expr__"}
    uids = []
    for group in cfg["groups"]:
        assert group["folder"] and group["interval"]
        for rule in group["rules"]:
            uids.append(rule["uid"])
            refs = {d["refId"] for d in rule["data"]}
            assert rule["condition"] in refs
            for d in rule["data"]:
                assert d["datasourceUid"] in datasources
                assert d["model"]["refId"] == d["refId"]
                if d["datasourceUid"] == "__expr__":
                    assert d["model"]["expression"] in refs
                else:
                    assert d["model"]["rawSql"].lower().startswith("select")
            assert rule["labels"]["severity"] in {"critical", "warning"}
            assert rule["annotations"]["summary"]
    assert len(uids) == len(set(uids))
    assert all(len(u) <= 40 for u in uids)  # Grafana's uid length limit


def test_contact_points_and_routes_match():
    cfg = load()
    receivers = {cp["name"] for cp in cfg["contactPoints"]}
    assert receivers == {"sentinel-email", "sentinel-teams"}
    (root,) = cfg["policies"]
    used = {root["receiver"]} | {r["receiver"] for r in root["routes"]}
    assert used == receivers


def _route_targets(severity: str, teams_severities: str) -> list[str]:
    """Mimic Alertmanager routing for our tree: first match wins unless `continue`."""
    (root,) = load()["policies"]
    targets = []
    for route in root["routes"]:
        ((label, op, pattern),) = route["object_matchers"]
        assert (label, op) == ("severity", "=~")
        pattern = pattern.replace("${TEAMS_SEVERITIES}", teams_severities)
        if re.fullmatch(pattern, severity):
            targets.append(route["receiver"])
            if not route.get("continue"):
                break
    return targets or [root["receiver"]]


def test_teams_is_off_by_default_and_email_always_gets_alerts():
    assert _route_targets("critical", "none") == ["sentinel-email"]
    assert _route_targets("warning", "none") == ["sentinel-email"]


def test_teams_receives_only_the_configured_severities():
    assert _route_targets("critical", "critical") == ["sentinel-email", "sentinel-teams"]
    assert _route_targets("warning", "critical") == ["sentinel-email"]
    assert _route_targets("warning", "critical|warning") == ["sentinel-email", "sentinel-teams"]


def test_datasource_uid_matches_provisioned_datasource():
    ds = yaml.safe_load((ROOT / "grafana/provisioning/datasources/postgres.yml").read_text())
    assert "sentinel-pg" in {d["uid"] for d in ds["datasources"]}
    dash = json.loads((ROOT / "grafana/dashboards/sentinel.json").read_text())
    assert dash["panels"]
