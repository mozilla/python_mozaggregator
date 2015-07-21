[![Build Status](https://travis-ci.org/vitillo/python_mozaggregator.svg?branch=master)](https://travis-ci.org/vitillo/python_mozaggregator)

# python_mozaggregator
Aggregator job for Telemetry. See this [blog](http://robertovitillo.com/2015/07/02/telemetry-metrics-roll-ups/) post for details. 

## Development and deployment

To start hacking on your local machine:
```bash
vagrant up
vagrant ssh
```

To deploy a new version of the aggregation service to the cloud:
```bash
ansible-playbook ansible/deploy.yml -i ansible/inventory
```

## API
Aggregates are made available through a HTTP API. There are two kinds of aggregates: per submission date and per build-id. 
To access the aggregates use the ```aggregates_by/build_id/``` and ```aggregates_by/submission_date/``` prefix respectively.

The following examples are based on build-id aggregates.

##### Get available channels:
```bash
curl -X GET http://SERVICE/aggregates_by/build_id/channels/
["nightly","beta","aurora"]
```

##### Get a list of options for the available dimensions on a given channel:
```bash
curl -X GET "http://SERVICE/aggregates_by/build_id/channels/nightly/filters/"
{"metric":["A11Y_CONSUMERS","A11Y_IATABLE_USAGE_FLAG",...], 
 "application":["Fennec","Firefox"],
 ...}
```

##### Get a list of available build-ids for a given channel:
```bash
curl -X GET "http://SERVICE/aggregates_by/build_id/channels/nightly/dates/"
[{"date":"20150630","version":"42"}, {"date":"20150629","version":"42"}]
```

##### Given a set of build-ids, retrieve for each of build-id the aggregated histogram that complies with the requested filters:
```bash
curl -X GET "http://SERVICE/aggregates_by/build_id/channels/nightly/?version=41&dates=20150615,20150616&metric=GC_MS&os=Windows_NT"
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
- metric, e.g. JS_TELEMETRY_ADDON_EXCEPTIONS
- application, e.g. Firefox
- architecture, e.g. x86
- os, e.g. Windows_NT
- osVersion, e.g. 6.1
- e10sEnabled, e.g. true
- label, e.g Adblock-Plus
- child, e.g. true, meaningful only if e10s is enabled

A reply has the following attributes:
- buckets, which represents the bucket labels of the histogram
- kind, the kind of histogram (e.g. exponential)
- data, which is an array of metric objects with the following attributes:
  - date: a build-id
  - count: number of metrics aggregated
  - sum: sum of accumulated values
  - histogram: bucket values
  - description: histogram description
