"""spark-submit entry point for the streaming pipeline (spark-submit cannot run `-m module`)."""

from sentinel.streaming.pipeline import main

if __name__ == "__main__":
    main()
