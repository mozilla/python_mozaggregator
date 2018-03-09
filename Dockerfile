FROM python:2-slim

ENV PYTHONUNBUFFERED=1 \
    POSTGRES_USER=root \
    POSTGRES_DB=telemetry \
    PORT=5000

EXPOSE $PORT

# python-slim base image has missing directories required for psql install
RUN mkdir -p /usr/share/man/man1
RUN mkdir -p /usr/share/man/man7

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        # Production only libs on next line.
        gcc awscli net-tools \
        libsnappy-dev liblzma-dev g++ redis-server curl libpq-dev bzip2 libffi-dev \
        python-numpy python-pandas python-scipy wget ca-certificates openssl libssl-dev \
        postgresql-9.4 postgresql-contrib-9.4 postgresql-server-dev-9.4 \
        openjdk-7-jdk && \
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

ENTRYPOINT ["/bin/bash", "/app/bin/run"]

CMD ["web"]
