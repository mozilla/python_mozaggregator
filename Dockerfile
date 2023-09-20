FROM mozilla/python_mozaggregator:latest
COPY patch /tmp/patch
WORKDIR /app
RUN patch -p1 < /tmp/patch
