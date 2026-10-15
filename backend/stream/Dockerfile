FROM apache/spark:4.0.4-scala2.13-java17-python3-r-ubuntu
USER root
WORKDIR /opt/echoguard
COPY requirements.txt /opt/echoguard/backend/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /opt/echoguard/backend/requirements.txt
RUN python3 -c "import confluent_kafka"
COPY . /opt/echoguard/backend
RUN mkdir -p /opt/echoguard/checkpoints /opt/echoguard/ivy \
    && chown -R 185:0 /opt/echoguard/checkpoints /opt/echoguard/ivy
ENV PYTHONPATH=/opt/echoguard PYSPARK_PYTHON=python3
USER 185:0
