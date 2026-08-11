FROM eclipse-temurin:11-jdk-focal

LABEL maintainer="FoodStream Analytics"
LABEL description="PySpark Structured Streaming processor for food order enrichment"

# Install Python 3 and pip
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-dev \
        procps \
        curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Make python3 the default
RUN ln -sf /usr/bin/python3 /usr/bin/python

WORKDIR /app

# Install PySpark 3.3.x — best Java 11 compatibility
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy streaming job
COPY spark_streaming.py .

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# spark-submit with --add-opens flags required for Spark 3.x on Java 11+
CMD ["python3", "-c", "\
import subprocess, sys, os; \
spark_home = __import__('pyspark').__path__[0]; \
submit = os.path.join(spark_home, 'bin', 'spark-submit'); \
sys.exit(subprocess.call([ \
    submit, \
    '--packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.apache.kafka:kafka-clients:3.3.2', \
    '--master', 'local[2]', \
    '--driver-memory', '1g', \
    '--driver-java-options', \
    '-Dio.netty.tryReflectionSetAccessible=true', \
    '--conf', 'spark.executor.extraJavaOptions=-Dio.netty.tryReflectionSetAccessible=true', \
    '/app/spark_streaming.py' \
]))"]
