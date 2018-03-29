# python_mozaggregator
Aggregator job for Telemetry. See this [blog](http://robertovitillo.com/2015/07/02/telemetry-metrics-roll-ups/) post for details. 

[![CircleCI](https://circleci.com/gh/mozilla/python_mozaggregator/tree/master.svg?style=svg)](https://circleci.com/gh/mozilla/python_mozaggregator/tree/master)

## Development and deployment

To clean, build, and run all containers:
```
make up
```

To build containers and ssh into web container:
```
make shell
```

To manually ssh into running web container (e.g. after a 'make up'):
```
docker ps
docker exec -it <CONTAINER_ID of web container> /bin/bash
```

To build and run tests inside dev container:
```
make test
```

To manually run tests on running web container:
```
make shell
./bin/run test
```

## Deployment
The following Env vars need to be set up in hiera-sops POSTGRES_HOST, POSTGRES_RO_HOST, POSTGRES_PASS
There are jenkins pipeline jobs to deploy this.  See cloudops-deployment/projects/mozaggregator for details

## API
Aggregates are made available through a HTTP API. There are two kinds of aggregates: per submission date (date a ping is received by the server) and per build-id (date the submitting product was built).

To access the aggregates use the ```aggregates_by/build_id/``` and ```aggregates_by/submission_date/``` prefix respectively.

In the URLs below, replace `SERVICE` with the origin of this service's instance. The official service is `https://aggregates.telemetry.mozilla.org`.

The following examples are based on build-id aggregates. Replace `build_id` with `submission_date` to use aggregates per submission date instead.

##### Get available channels:
```bash
curl -X GET https://SERVICE/aggregates_by/build_id/channels/
["nightly","beta","release"]
```

##### Get a list of options for the available dimensions on a given channel and version:
```bash
curl -X GET "https://SERVICE/filters/?channel=nightly&version=42"
{"metric":["A11Y_CONSUMERS","A11Y_IATABLE_USAGE_FLAG",...], 
 "application":["Fennec","Firefox"],
 ...}
```

##### Get a list of available build-ids for a given channel:
```bash
curl -X GET "https://SERVICE/aggregates_by/build_id/channels/nightly/dates/"
[{"date":"20150630","version":"42"}, {"date":"20150629","version":"42"}]
```

##### Given a set of build-ids, retrieve for each of build-id the aggregated histogram that complies with the requested filters:
```bash
curl -X GET "https://SERVICE/aggregates_by/build_id/channels/nightly/?version=41&dates=20150615,20150616&metric=GC_MS&os=Windows_NT"
{"buckets":[0, ..., 10000],
 "data":[{"date":"20150615",
          "count":239459,
          "sum": 412346123,
          "histogram":[309, ..., 5047],
          "label":""},
         {"date":"20150616",
          "count":233688,
          "sum": 402241121,
          "histogram":[306, ..., 7875],
          "label":""}],
 "kind":"exponential",
 "description":"Time spent running JS GC (ms)"}
```

The available filters are:
- `metric`, e.g. JS_TELEMETRY_ADDON_EXCEPTIONS
- `application`, e.g. Firefox
- `architecture`, e.g. x86
- `os`, e.g. Windows_NT
- `osVersion`, e.g. 6.1
- `label`, e.g Adblock-Plus
- `child`, e.g. true, meaningful only if e10s is enabled

A reply has the following attributes:
- `buckets`, which represents the bucket labels of the histogram
- `kind`, the kind of histogram (e.g. exponential)
- `data`, which is an array of metric objects with the following attributes:
  - `date`: a build-id
  - `count`: number of metrics aggregated
  - `sum`: sum of accumulated values
  - `histogram`: bucket values
  - `description`: histogram description
  - `label`: for keyed histograms, the key the entry belongs to, or otherwise a blank string

Keyed histograms have the same format as unkeyed histograms, but there can possibly be multiple metric objects with the same date, each with a different key (`label`).
