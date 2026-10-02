FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
ENV SPARK_PACKAGES="io.delta:delta-spark_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4" \
    IVY_HOME=/opt/ivy \
    PYTHONUNBUFFERED=1

RUN python3 -m pip install --no-cache-dir "delta-spark==3.2.1" "psycopg2-binary>=2.9"

WORKDIR /opt/sentinel
COPY pyproject.toml README.md ./
COPY src ./src
COPY scripts ./scripts
RUN python3 -m pip install --no-cache-dir --no-deps . \
 # Resolve and cache the JVM dependencies at build time so containers start offline.
 && echo "print('warm')" > /tmp/warm.py \
 && /opt/spark/bin/spark-submit --conf spark.jars.ivy=${IVY_HOME} \
      --packages ${SPARK_PACKAGES} /tmp/warm.py \
 && chown -R spark:spark ${IVY_HOME}

USER spark
CMD /opt/spark/bin/spark-submit \
      --master "local[*]" \
      --driver-memory 2g \
      --conf spark.jars.ivy=${IVY_HOME} \
      --packages ${SPARK_PACKAGES} \
      --conf spark.ui.port=4040 \
      /opt/sentinel/scripts/run_streaming.py --stages bronze,silver,detect
