"""spark-submit entry point for a backfill: rebuild silver and alerts from bronze."""

from sentinel.streaming.replay import main

if __name__ == "__main__":
    main([])
