"""Synthetic authentication traffic with labelled attack campaigns.

The simulator is deterministic for a given seed. Every event carries ground-truth labels
(`attack_type`, `attack_id`) that the pipeline never uses for detection, only to score
how well the detections perform (see the `mart_detection_quality` dbt model).
"""

from __future__ import annotations

import hashlib
import heapq
import itertools
import random
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from sentinel.generator.geo import (
    ATTACKER_CITIES,
    CITIES,
    HOME_WEIGHTS,
    City,
    haversine_km,
)

APPS = ("employee-portal", "vpn", "email", "hr-system", "pos-backoffice")
AUTH_METHODS = ("password", "password+mfa", "sso")
USER_AGENTS = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/129.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0) Mobile/15E148",
    "Mozilla/5.0 (Linux; Android 14) Chrome/129.0 Mobile",
)
ATTACK_USER_AGENTS = ("python-requests/2.32", "curl/8.9", "Go-http-client/2.0", "okhttp/4.12")

ATTACK_TYPES = ("brute_force", "credential_stuffing", "password_spray", "impossible_travel")
OFFICE_NAT_IP = "23.10.0.10"


@dataclass
class User:
    user_id: str
    email: str
    home: City
    device_id: str
    user_agent: str
    home_ip: str
    location: City = field(init=False)
    trip: tuple[City, datetime] | None = None

    def __post_init__(self) -> None:
        self.location = self.home


@dataclass(frozen=True)
class SimConfig:
    seed: int = 42
    users: int = 5000
    events_per_minute: int = 300
    # Expected attack campaigns per simulated hour, per attack type.
    attacks_per_hour: float = 4.0
    breach_corpus_size: int = 20000
    breached_user_share: float = 0.10
    malformed_rate: float = 0.003
    duplicate_rate: float = 0.004


def sha256_hex(value: str) -> str:
    return hashlib.sha256(value.strip().lower().encode()).hexdigest()


def sha1_hex(value: str) -> str:
    return hashlib.sha1(value.encode()).hexdigest()  # noqa: S324 - mirrors HIBP format


def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Simulator:
    def __init__(self, config: SimConfig, start: datetime) -> None:
        self.cfg = config
        self.rng = random.Random(config.seed)
        self.start = start
        self._seq = itertools.count()
        self._heap: list[tuple[datetime, int, dict]] = []
        self._attack_seq = itertools.count(1)
        self.users = self._build_users()
        self.breach_records = self._build_breach_corpus()

    # ------------------------------------------------------------------ population
    def _rand_ip(self, city: City) -> str:
        r = self.rng
        return f"{city.ip_prefix}.{r.randint(0, 255)}.{r.randint(0, 255)}.{r.randint(1, 254)}"

    def _build_users(self) -> list[User]:
        weights = [HOME_WEIGHTS.get(c.name, 1.0) for c in CITIES if not c.datacenter]
        homes = [c for c in CITIES if not c.datacenter]
        users = []
        for i in range(self.cfg.users):
            home = self.rng.choices(homes, weights=weights)[0]
            users.append(
                User(
                    user_id=f"u{i:06d}",
                    email=f"user{i:06d}@gwh-demo.org",
                    home=home,
                    device_id=uuid.UUID(int=self.rng.getrandbits(128)).hex[:16],
                    user_agent=self.rng.choice(USER_AGENTS),
                    home_ip=self._rand_ip(home),
                )
            )
        return users

    def _build_breach_corpus(self) -> list[dict]:
        """A third-party breach dump: mostly strangers, plus a slice of our own users."""
        r = self.rng
        breached = r.sample(self.users, int(len(self.users) * self.cfg.breached_user_share))
        emails = [u.email for u in breached]
        emails += [
            f"victim{i:07d}@{r.choice(('mail.com', 'webmail.net', 'inbox.io'))}"
            for i in range(self.cfg.breach_corpus_size)
        ]
        sources = ("shopfast-2024", "gamerzone-2025", "fitlife-2023")
        records = []
        for email in emails:
            src = r.choice(sources)
            records.append(
                {
                    "email": email,
                    "email_sha256": sha256_hex(email),
                    "password_sha1": sha1_hex(f"pw-{r.getrandbits(32)}"),
                    "source": src,
                    "breach_date": f"20{src[-2:]}-0{r.randint(1, 9)}-1{r.randint(0, 9)}",
                }
            )
        return records

    # ------------------------------------------------------------------ events
    def _event(
        self,
        ts: datetime,
        *,
        user: User | None,
        username: str,
        city: City,
        ip: str,
        device_id: str,
        user_agent: str,
        outcome: str,
        failure_reason: str | None,
        attack_type: str = "none",
        attack_id: str | None = None,
        app: str | None = None,
        auth_method: str | None = None,
    ) -> dict:
        r = self.rng
        return {
            "event_id": str(uuid.UUID(int=r.getrandbits(128))),
            "event_ts": iso(ts),
            "user_id": user.user_id if user else None,
            "username": username,
            "src_ip": ip,
            "geo_country": city.country,
            "geo_city": city.name,
            "geo_lat": round(city.lat + r.uniform(-0.05, 0.05), 5),
            "geo_lon": round(city.lon + r.uniform(-0.05, 0.05), 5),
            "device_id": device_id,
            "user_agent": user_agent,
            "app": app or r.choice(APPS),
            "auth_method": auth_method or r.choices(AUTH_METHODS, weights=(5, 3, 2))[0],
            "outcome": outcome,
            "failure_reason": failure_reason,
            "attack_type": attack_type,
            "attack_id": attack_id,
        }

    def _push(self, event: dict, ts: datetime) -> None:
        heapq.heappush(self._heap, (ts, next(self._seq), event))

    def _normal_session(self, ts: datetime) -> None:
        r = self.rng
        u = r.choice(self.users)
        # Legitimate travel: once a trip has had time to happen, the user is there.
        if u.trip and ts >= u.trip[1]:
            u.location, u.trip = u.trip[0], None
        elif u.trip is None and r.random() < 0.0005:
            dest = r.choice([c for c in CITIES if not c.datacenter and c != u.location])
            hours = haversine_km(u.location.lat, u.location.lon, dest.lat, dest.lon) / 700 + 3
            u.trip = (dest, ts + timedelta(hours=hours))
        ip = u.home_ip if u.location == u.home else self._rand_ip(u.location)
        # Hard negative #1: Houston HQ staff share one NAT egress IP, so a single IP sees
        # hundreds of distinct users. IP rules must not mistake that for stuffing/spraying.
        if u.location.name == "Houston" and u.location == u.home and int(u.user_id[1:]) % 3 == 0:
            ip = OFFICE_NAT_IP
        common = dict(
            user=u,
            username=u.email,
            city=u.location,
            ip=ip,
            device_id=u.device_id,
            user_agent=u.user_agent,
        )
        t = ts
        if r.random() < 0.0004:
            # Hard negative #2: a user who forgot their password after a reset and keeps
            # trying. Realistic noise that can trip the brute-force rule (a false positive).
            for _ in range(r.randint(8, 14)):
                self._push(
                    self._event(t, outcome="failure", failure_reason="bad_password", **common), t
                )
                t += timedelta(seconds=r.uniform(5, 25))
        elif r.random() < 0.06:  # fat-fingered password once or twice, then gets in
            for _ in range(r.randint(1, 2)):
                self._push(
                    self._event(t, outcome="failure", failure_reason="bad_password", **common), t
                )
                t += timedelta(seconds=r.uniform(3, 20))
        if r.random() < 0.01:
            self._push(
                self._event(
                    t,
                    outcome="failure",
                    failure_reason="mfa_failed",
                    auth_method="password+mfa",
                    **common,
                ),
                t,
            )
            t += timedelta(seconds=r.uniform(10, 40))
        self._push(self._event(t, outcome="success", failure_reason=None, **common), t)

    def _attack_id(self, kind: str) -> str:
        return f"{kind[:2].upper()}-{next(self._attack_seq):05d}"

    def _brute_force(self, ts: datetime) -> None:
        r = self.rng
        aid, victim, city = (
            self._attack_id("brute_force"),
            r.choice(self.users),
            r.choice(ATTACKER_CITIES),
        )
        ip, ua, dev = self._rand_ip(city), r.choice(ATTACK_USER_AGENTS), f"atk{r.getrandbits(40):x}"
        t = ts
        for _ in range(r.randint(30, 80)):
            self._push(
                self._event(
                    t,
                    user=victim,
                    username=victim.email,
                    city=city,
                    ip=ip,
                    device_id=dev,
                    user_agent=ua,
                    outcome="failure",
                    failure_reason="bad_password",
                    attack_type="brute_force",
                    attack_id=aid,
                    app="vpn",
                    auth_method="password",
                ),
                t,
            )
            t += timedelta(seconds=r.uniform(1, 6))
        if r.random() < 0.25:
            self._push(
                self._event(
                    t,
                    user=victim,
                    username=victim.email,
                    city=city,
                    ip=ip,
                    device_id=dev,
                    user_agent=ua,
                    outcome="success",
                    failure_reason=None,
                    attack_type="brute_force",
                    attack_id=aid,
                    app="vpn",
                    auth_method="password",
                ),
                t,
            )

    def _credential_stuffing(self, ts: datetime) -> None:
        r = self.rng
        aid, city = self._attack_id("credential_stuffing"), r.choice(ATTACKER_CITIES)
        ip, ua, dev = self._rand_ip(city), r.choice(ATTACK_USER_AGENTS), f"atk{r.getrandbits(40):x}"
        by_email = {u.email: u for u in self.users}
        t = ts
        for rec in r.sample(self.breach_records, r.randint(60, 200)):
            user = by_email.get(rec["email"])
            if user is None:
                outcome, reason = "failure", "unknown_user"
            elif r.random() < 0.04:  # password reuse: the leaked password still works
                outcome, reason = "success", None
            else:
                outcome, reason = "failure", "bad_password"
            self._push(
                self._event(
                    t,
                    user=user,
                    username=rec["email"],
                    city=city,
                    ip=ip,
                    device_id=dev,
                    user_agent=ua,
                    outcome=outcome,
                    failure_reason=reason,
                    attack_type="credential_stuffing",
                    attack_id=aid,
                    app="employee-portal",
                    auth_method="password",
                ),
                t,
            )
            t += timedelta(seconds=r.uniform(0.5, 2.5))

    def _password_spray(self, ts: datetime) -> None:
        r = self.rng
        aid, city = self._attack_id("password_spray"), r.choice(ATTACKER_CITIES)
        ip, ua, dev = self._rand_ip(city), r.choice(ATTACK_USER_AGENTS), f"atk{r.getrandbits(40):x}"
        t = ts
        for victim in r.sample(self.users, r.randint(25, 60)):
            ok = r.random() < 0.03
            self._push(
                self._event(
                    t,
                    user=victim,
                    username=victim.email,
                    city=city,
                    ip=ip,
                    device_id=dev,
                    user_agent=ua,
                    outcome="success" if ok else "failure",
                    failure_reason=None if ok else "bad_password",
                    attack_type="password_spray",
                    attack_id=aid,
                    app="email",
                    auth_method="password",
                ),
                t,
            )
            t += timedelta(seconds=r.uniform(2, 7))

    def _impossible_travel(self, ts: datetime) -> None:
        r = self.rng
        aid, victim = self._attack_id("impossible_travel"), r.choice(self.users)
        far = [
            c
            for c in CITIES
            if haversine_km(c.lat, c.lon, victim.location.lat, victim.location.lon) > 3000
        ]
        city = r.choice(far)
        # The real user signs in from where they are...
        self._push(
            self._event(
                ts,
                user=victim,
                username=victim.email,
                city=victim.location,
                ip=victim.home_ip,
                device_id=victim.device_id,
                user_agent=victim.user_agent,
                outcome="success",
                failure_reason=None,
            ),
            ts,
        )
        # ...and shortly after, the stolen session/credential is used from another continent.
        t2 = ts + timedelta(minutes=r.uniform(8, 50))
        self._push(
            self._event(
                t2,
                user=victim,
                username=victim.email,
                city=city,
                ip=self._rand_ip(city),
                device_id=f"atk{r.getrandbits(40):x}",
                user_agent=r.choice(USER_AGENTS),
                outcome="success",
                failure_reason=None,
                attack_type="impossible_travel",
                attack_id=aid,
            ),
            t2,
        )

    # ------------------------------------------------------------------ data quality noise
    def _corrupt(self, event: dict) -> dict:
        bad = dict(event)
        choice = self.rng.randint(0, 3)
        if choice == 0:
            bad["event_ts"] = None
        elif choice == 1:
            bad["outcome"] = "SUCESS"
        elif choice == 2:
            bad["geo_lat"] = 999.0
        else:
            bad["event_id"] = None
        return bad

    # ------------------------------------------------------------------ public API
    def run(self, minutes: int) -> Iterator[dict]:
        """Yield events in event-time order for `minutes` of simulated time."""
        r, cfg = self.rng, self.cfg
        attackers = (
            self._brute_force,
            self._credential_stuffing,
            self._password_spray,
            self._impossible_travel,
        )
        p_attack = cfg.attacks_per_hour / 60.0
        for minute in range(minutes):
            tick = self.start + timedelta(minutes=minute)
            for _ in range(cfg.events_per_minute):
                self._normal_session(tick + timedelta(seconds=r.uniform(0, 60)))
            for attack in attackers:
                if r.random() < p_attack:
                    attack(tick + timedelta(seconds=r.uniform(0, 60)))
            horizon = tick + timedelta(minutes=1)
            yield from self._drain(until=horizon)
        yield from self._drain(until=None)

    def _drain(self, until: datetime | None) -> Iterator[dict]:
        while self._heap and (until is None or self._heap[0][0] < until):
            _, _, event = heapq.heappop(self._heap)
            roll = self.rng.random()
            if roll < self.cfg.malformed_rate:
                yield self._corrupt(event)
                continue
            yield event
            if roll > 1 - self.cfg.duplicate_rate:  # at-least-once producer retry
                yield event
