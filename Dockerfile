FROM python:3.7-stretch

ENV PYTHONUNBUFFERED=1 \
    POSTGRES_USER=root \
    POSTGRES_DB=telemetry \
    PORT=5000

EXPOSE $PORT

# Update stretch repositories
# https://stackoverflow.com/a/76095392
RUN sed -i -e 's/deb.debian.org/archive.debian.org/g' \
           -e 's|security.debian.org|archive.debian.org/|g' \
           -e '/stretch-updates/d' /etc/apt/sources.list

# Install Java
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends openjdk-8-jdk

# Install Cloud SDK: https://cloud.google.com/sdk/docs/quickstart-debian-ubuntu
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | \
    tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
    apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && \
    apt-get update -y && \
    apt-get install google-cloud-sdk -y

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
        # production only libs on next line.
        gcc awscli net-tools \
        libsnappy-dev liblzma-dev g++ curl libpq-dev bzip2 libffi-dev \
        libblas-dev liblapack-dev wget ca-certificates openssl libssl-dev \
        postgresql && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# add a non-privileged user for installing and running the application
RUN mkdir /app && \
    chown 10001:10001 /app && \
    groupadd --gid 10001 app && \
    useradd --no-create-home --uid 10001 --gid 10001 --home-dir /app app

# Install Python dependencies
COPY requirements/*.txt /tmp/requirements/

# Switch to /tmp to install dependencies outside home dir
WORKDIR /tmp
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements/build.txt

ENV PYSPARK_PYTHON=python \
    SPARK_HOME=/usr/local/lib/python3.7/site-packages/pyspark

RUN wget --directory-prefix $SPARK_HOME/jars/ https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-latest.jar
RUN wget --directory-prefix $SPARK_HOME/jars/ https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar
RUN wget --directory-prefix $SPARK_HOME/jars/ https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.3/hadoop-aws-2.7.3.jar
RUN wget --directory-prefix $SPARK_HOME/jars/ https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar
RUN wget --directory-prefix $SPARK_HOME/jars/ https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.11/2.4.3/spark-avro_2.11-2.4.3.jar

# Switch back to home directory
WORKDIR /app

COPY . /app

RUN chown -R 10001:10001 /app

USER 10001

ENTRYPOINT ["/usr/local/bin/gunicorn"]

CMD ["mozaggregator.service:app", "-k", "gevent", "--bind", "0.0.0.0:5000"]
