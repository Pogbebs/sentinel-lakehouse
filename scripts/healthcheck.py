"""Docker healthcheck for the streaming container: exit 0 if every stream is heartbeating.

Reads the heartbeat file the streaming job writes (sentinel.streaming.health). Needs no
JVM and no network, so it is cheap enough to run every 30 seconds.
"""

import os
import sys

from sentinel.streaming.health import DEFAULT_HEALTH_FILE, evaluate_health_file

if __name__ == "__main__":
    ok, reason = evaluate_health_file(os.environ.get("HEALTH_FILE", DEFAULT_HEALTH_FILE))
    print(reason)
    sys.exit(0 if ok else 1)
