"""Reference geography used by the simulator. Coordinates are city centres."""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class City:
    name: str
    country: str
    lat: float
    lon: float
    ip_prefix: int  # first octet used to fabricate IPs that "belong" to this city
    datacenter: bool = False  # cities attackers commonly rent infrastructure in


CITIES: tuple[City, ...] = (
    City("Houston", "US", 29.7604, -95.3698, 23),
    City("Dallas", "US", 32.7767, -96.7970, 24),
    City("Austin", "US", 30.2672, -97.7431, 47),
    City("San Antonio", "US", 29.4241, -98.4936, 50),
    City("New York", "US", 40.7128, -74.0060, 64),
    City("Chicago", "US", 41.8781, -87.6298, 66),
    City("Atlanta", "US", 33.7490, -84.3880, 68),
    City("Denver", "US", 39.7392, -104.9903, 71),
    City("Los Angeles", "US", 34.0522, -118.2437, 72),
    City("Seattle", "US", 47.6062, -122.3321, 73),
    City("Miami", "US", 25.7617, -80.1918, 74),
    City("Toronto", "CA", 43.6532, -79.3832, 99),
    City("Mexico City", "MX", 19.4326, -99.1332, 187),
    City("London", "GB", 51.5074, -0.1278, 81),
    City("Lagos", "NG", 6.5244, 3.3792, 102),
    City("Abuja", "NG", 9.0765, 7.3986, 105),
    City("Nairobi", "KE", -1.2921, 36.8219, 154),
    City("Sao Paulo", "BR", -23.5505, -46.6333, 177),
    City("Frankfurt", "DE", 50.1109, 8.6821, 85, datacenter=True),
    City("Amsterdam", "NL", 52.3676, 4.9041, 88, datacenter=True),
    City("Bucharest", "RO", 44.4268, 26.1025, 89, datacenter=True),
    City("Moscow", "RU", 55.7558, 37.6173, 95, datacenter=True),
    City("Singapore", "SG", 1.3521, 103.8198, 128, datacenter=True),
    City("Hanoi", "VN", 21.0278, 105.8342, 113, datacenter=True),
    City("Sao Luis", "BR", -2.5307, -44.3068, 179, datacenter=True),
    City("Mumbai", "IN", 19.0760, 72.8777, 115),
    City("Tokyo", "JP", 35.6762, 139.6503, 126),
    City("Sydney", "AU", -33.8688, 151.2093, 139),
)

# Most of the simulated workforce lives in Texas, like a regional employer would.
HOME_WEIGHTS: dict[str, float] = {
    "Houston": 40,
    "Dallas": 15,
    "Austin": 10,
    "San Antonio": 8,
}

ATTACKER_CITIES: tuple[City, ...] = tuple(c for c in CITIES if c.datacenter)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))
