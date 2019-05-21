FROM python:2-stretch

ENV PYTHONUNBUFFERED=1 \
    POSTGRES_USER=root \
    POSTGRES_DB=telemetry \
    PORT=5000

EXPOSE $PORT

# Install Java
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends openjdk-8-jdk

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
        # production only libs on next line.
        gcc awscli net-tools \
        libsnappy-dev liblzma-dev g++ curl libpq-dev bzip2 libffi-dev \
        python-numpy python-pandas python-scipy wget ca-certificates openssl libssl-dev \
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

# Switch back to home directory
WORKDIR /app

COPY . /app

RUN chown -R 10001:10001 /app

USER 10001

ENTRYPOINT ["/usr/local/bin/gunicorn"]

CMD ["mozaggregator.service:app", "-k", "gevent", "--bind", "0.0.0.0:5000"]
