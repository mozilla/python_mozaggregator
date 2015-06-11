import pyspark
import uuid
import logging
import pandas as pd
import re

from collections import defaultdict
from mozaggregator.aggregator import _aggregate_metrics, scalar_histogram_labels

NUM_CHILDREN_PER_PING = 3
NUM_PINGS_PER_DIMENSIONS = 2

ping_dimensions = {"submission_date": [u"20150601", u"20150602"],
                   "channel": [u"nightly", u"aurora", u"beta", u"release"],
                   "version": [u"40.0a1", u"41"],
                   "build_id": [u"20150601000000", u"20150602000000"],
                   "application": [u"Firefox", u"Fennec"],
                   "arch": [u"x86", u"x86-64"],
                   "revision": [u"https://hg.mozilla.org/mozilla-central/rev/ac277e615f8f",
                                u"https://hg.mozilla.org/mozilla-central/rev/bc277e615f9f"],
                   "os": [u"Linux", u"Windows_NT"],
                   "os_version": [u"6.1", u"3.1.12"],
                   "e10s": [True, False]}

histograms_template = {u"EVENTLOOP_UI_LAG_EXP_MS": {u'bucket_count': 20,
                                                    u'histogram_type': 0,
                                                    u'log_sum': 0,
                                                    u'log_sum_squares': 0,
                                                    u'range': [50, 60000],
                                                    u'sum': 9362,
                                                    u'values': {u'0': 0,
                                                                u'110': 16,
                                                                u'1170': 0,
                                                                u'163': 8,
                                                                u'242': 5,
                                                                u'359': 2,
                                                                u'50': 18,
                                                                u'74': 16,
                                                                u'789': 1}},
                       u"UPDATE_PING_COUNT_EXTERNAL": {u'bucket_count': 3,
                                                       u'histogram_type': 4,
                                                       u'range': [1, 2],
                                                       u'sum': 42,
                                                       u'sum_squares_hi': 0,
                                                       u'sum_squares_lo': 1,
                                                       u'values': {u'0': 42, u'1': 0}}}

keyed_histograms_template = {u'BLOCKED_ON_PLUGIN_INSTANCE_DESTROY_MS':
                             {u'Shockwave Flash17.0.0.188': {u'bucket_count': 20,
                                                             u'histogram_type': 0,
                                                             u'log_sum': 696.68039953709,
                                                             u'log_sum_squares': 3202.8813306447,
                                                             u'range': [1, 10000],
                                                             u'sum': 19568,
                                                             u'values': {u'103': 78,
                                                                         u'13': 4,
                                                                         u'1306': 0,
                                                                         u'171': 12,
                                                                         u'2': 0,
                                                                         u'22': 2,
                                                                         u'284': 6,
                                                                         u'3': 8,
                                                                         u'37': 4,
                                                                         u'472': 1,
                                                                         u'5': 22,
                                                                         u'62': 21,
                                                                         u'785': 1,
                                                                         u'8': 8}}}}

simple_measurements_template = {"uptime": 42, "addonManager": {u'XPIDB_parseDB_MS': 42}}


def generate_pings():
    for submission_date in ping_dimensions["submission_date"]:
        for channel in ping_dimensions["channel"]:
            for version in ping_dimensions["version"]:
                for build_id in ping_dimensions["build_id"]:
                    for application in ping_dimensions["application"]:
                        for arch in ping_dimensions["arch"]:
                            for revision in ping_dimensions["revision"]:
                                for os in ping_dimensions["os"]:
                                    for os_version in ping_dimensions["os_version"]:
                                        for e10s in ping_dimensions["e10s"]:
                                            for i in range(NUM_PINGS_PER_DIMENSIONS):
                                                dimensions = {u"submission_date": submission_date,
                                                              u"channel": channel,
                                                              u"version": version,
                                                              u"build_id": build_id,
                                                              u"application": application,
                                                              u"arch": arch,
                                                              u"revision": revision,
                                                              u"os": os,
                                                              u"os_version": os_version,
                                                              u"e10s": e10s}
                                                yield generate_payload(dimensions)


def generate_payload(dimensions):
    meta = {u"submissionDate": dimensions["submission_date"]}
    application = {u"channel": dimensions["channel"],
                   u"version": dimensions["version"],
                   u"buildId": dimensions["build_id"],
                   u"name": dimensions["application"],
                   u"architecture": dimensions["arch"]}

    child_payloads = [{"histograms": histograms_template,
                       "keyedHistograms": keyed_histograms_template}
                      for i in range(NUM_CHILDREN_PER_PING)]

    payload = {u"info": {u"revision": dimensions["revision"]},
               u"simpleMeasurements": simple_measurements_template,
               u"histograms": histograms_template,
               u"keyedHistograms": keyed_histograms_template,
               u"childPayloads": child_payloads}
    environment = {u"system": {u"os": {u"name": dimensions["os"],
                                       u"version": dimensions["os_version"]}},
                   u"settings": {u"e10sEnabled": dimensions["e10s"]}}

    return {u"clientId": str(uuid.uuid4()),
            u"meta": meta,
            u"application": application,
            u"payload": payload,
            u"environment": environment}


def setup_module():
    global aggregates

    logger = logging.getLogger("py4j")
    logger.setLevel(logging.ERROR)

    sc = pyspark.SparkContext(master="local[*]")
    raw_pings = list(generate_pings())
    aggregates = _aggregate_metrics(sc.parallelize(raw_pings)).collect()
    sc.stop()


def test_count():
    assert(len(list(generate_pings()))/NUM_PINGS_PER_DIMENSIONS == len(aggregates))


def test_keys():
    for aggregate in aggregates:
        submission_date, channel, version, build_id, app, arch, revision, os, os_version, e10s = aggregate[0]

        assert(submission_date in ping_dimensions["submission_date"])
        assert(channel in ping_dimensions["channel"])
        assert(version in [x.split('.')[0] for x in ping_dimensions["version"]])
        assert(build_id in [x[:8] for x in ping_dimensions["build_id"]])
        assert(app in ping_dimensions["application"])
        assert(arch in ping_dimensions["arch"])
        assert(revision in [x[-12:] for x in ping_dimensions["revision"]])
        assert(os in ping_dimensions["os"])
        if os == "Linux":
            assert(os_version in [x[:3] for x in ping_dimensions["os_version"]])
        else:
            assert(os_version in ping_dimensions["os_version"])


def test_simple_measurements():
    metric_count = defaultdict(int)
    labels = set([unicode(x) for x in scalar_histogram_labels])

    for aggregate in aggregates:
        for key, value in aggregate[1].iteritems():
            metric, label, child = key

            if re.match("^SIMPLE_MEASURES.*_SCALAR$", metric):
                metric_count[metric] += 1
                assert(label == "")
                assert(child is False)
                assert(value["count"] == NUM_PINGS_PER_DIMENSIONS)
                assert(set(value["histogram"].keys()) == labels)
                assert(value["histogram"]["35"] == value["count"])

    assert len(metric_count) == len(simple_measurements_template)
    for v in metric_count.values():
        assert(v == len(aggregates))


def test_classic_histograms():
    metric_count = defaultdict(int)
    histograms = {k: v for k, v in histograms_template.iteritems() if v["histogram_type"] != 4}

    for aggregate in aggregates:
        for key, value in aggregate[1].iteritems():
            metric, label, child = key
            histogram = histograms.get(metric, None)

            if histogram:
                metric_count[metric] += 1
                assert(label == "")
                assert(value["count"] == NUM_PINGS_PER_DIMENSIONS*(NUM_CHILDREN_PER_PING if child else 1))
                assert(set(histogram["values"].keys()) == set(value["histogram"].keys()))
                assert((pd.Series(histogram["values"])*value["count"] == pd.Series(value["histogram"])).all())

    assert(len(metric_count) == len(histograms))
    for v in metric_count.values():
        assert(v == 2*len(aggregates))  # Count both child and parent metrics


def test_count_histograms():
    metric_count = defaultdict(int)
    labels = set([unicode(x) for x in scalar_histogram_labels])
    histograms = {"{}_SCALAR".format(k): v for k, v in histograms_template.iteritems() if v["histogram_type"] == 4}

    for aggregate in aggregates:
        for key, value in aggregate[1].iteritems():
            metric, label, child = key
            histogram = histograms.get(metric, None)

            if histogram:
                metric_count[metric] += 1
                assert(label == "")
                assert(value["count"] == NUM_PINGS_PER_DIMENSIONS*(NUM_CHILDREN_PER_PING if child else 1))
                assert(set(value["histogram"].keys()) == labels)
                assert(value["histogram"]["35"] == value["count"])

    assert len(metric_count) == len(histograms)
    for v in metric_count.values():
        assert(v == 2*len(aggregates))  # Count both child and parent metrics


def test_keyed_histograms():
    metric_count = defaultdict(int)

    for aggregate in aggregates:
        for key, value in aggregate[1].iteritems():
            metric, label, child = key

            if metric in keyed_histograms_template.keys():
                metric_count["{}_{}".format(metric, label)] += 1
                assert(label != "")
                assert(value["count"] == NUM_PINGS_PER_DIMENSIONS*(NUM_CHILDREN_PER_PING if child else 1))

                histogram_template = keyed_histograms_template[metric][label]["values"]
                assert(set(histogram_template.keys()) == set(value["histogram"].keys()))
                assert((pd.Series(histogram_template)*value["count"] == pd.Series(value["histogram"])).all())

    assert(len(metric_count) == len(keyed_histograms_template))  # Assume one label per keyed histogram
    for v in metric_count.values():
        assert(v == 2*len(aggregates))  # Count both child and parent metrics
