FROM spark:3.5.3-python3

USER root

# Install Python and required packages
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-dev && \
    pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir psycopg2-binary==2.9.9 numpy==1.24.3

# Create directories
RUN mkdir -p \
    /opt/spark/apps \
    /opt/spark/checkpoints \
    /opt/spark/logs \
    /opt/spark/.ivy2 \
    && chmod -R 777 /opt/spark/apps \
    /opt/spark/checkpoints \
    /opt/spark/logs \
    /opt/spark/.ivy2

WORKDIR /opt/spark
USER 185