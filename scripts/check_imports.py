"""Build-time smoke test for the Spark image: every runtime import must resolve.

Run through spark-submit (so pyspark and py4j are on the path, exactly as at runtime).
A missing module fails the image build instead of crash-looping the container later.
"""

import delta.tables  # noqa: F401
import psycopg2  # noqa: F401

import sentinel.streaming.pipeline  # noqa: F401
import sentinel.streaming.replay  # noqa: F401

print("sentinel spark image: imports OK")
