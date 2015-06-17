#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import binascii

from moztelemetry.spark import get_pings
from moztelemetry.histogram import cached_exponential_buckets
from collections import defaultdict

# Simple measurement and count histogram labels
simple_measures_labels = cached_exponential_buckets(1, 30000, 50)
count_histogram_labels = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 21, 23, 25, 27, 29, 31, 34, 37, 40, 43, 46, 50, 54, 58, 63, 68, 74, 80, 86, 93, 101, 109, 118, 128, 138, 149, 161, 174, 188, 203, 219, 237, 256, 277, 299, 323, 349, 377, 408, 441, 477, 516, 558, 603, 652, 705, 762, 824, 891, 963, 1041, 1125, 1216, 1315, 1422, 1537, 1662, 1797, 1943, 2101, 2271, 2455, 2654, 2869, 3102, 3354, 3626, 3920, 4238, 4582, 4954, 5356, 5791, 6261, 6769, 7318, 7912, 8554, 9249, 10000]


def aggregate_metrics(sc, channels, submission_date, fraction=1):
    """ Returns the build-id and submission date aggregates for a given submission date.

    :param sc: A SparkContext instance
    :param channel: Either the name of a channel or a list/tuple of names
    :param submission-date: The submission date for which the data will be aggregated
    :param fraction: An approximative fraction of submissions to consider for aggregation
    """
    if not isinstance(channels, (tuple, list)):
        channels = [channels]

    channels = set(channels)
    rdds = [get_pings(sc, channel=ch, submission_date=submission_date, doc_type="saved_session", schema="v4", fraction=fraction) for ch in channels]
    pings = reduce(lambda x, y: x.union(y), rdds)
    return _aggregate_metrics(pings)


def _aggregate_metrics(pings):
    trimmed = pings.filter(_sample_clients).map(_map_ping_to_dimensions).filter(lambda x: x)
    build_id_aggregates = trimmed.aggregateByKey(defaultdict(dict), _aggregate_ping, _aggregate_aggregates)
    submission_date_aggregates = build_id_aggregates.map(_map_build_id_key_to_submission_date_key).reduceByKey(_aggregate_aggregates)
    return build_id_aggregates, submission_date_aggregates


def _map_build_id_key_to_submission_date_key(aggregate):
    return tuple(aggregate[0][:3] + aggregate[0][4:]), aggregate[1]


def _sample_clients(ping):
    client_id = ping.get("clientId", None)

    if not client_id:
        return False

    channel = ping["application"]["channel"]
    percentage = {"nightly": 100,
                  "aurora": 100,
                  "beta": 100,
                  "release": 100}

    if channel not in percentage:
        return False

    # Use meta/sampleid once Heka spits it out correctly
    return client_id and ((binascii.crc32(client_id) % 100) < percentage[channel])


def _extract_histograms(state, payload, is_child=False):
    if type(payload) != dict:
        return

    histograms = payload.get("histograms", {})
    _extract_main_histograms(state, histograms, is_child)

    keyed_histograms = payload.get("keyedHistograms", {})
    if type(keyed_histograms) != dict:
        return

    for name, histograms in keyed_histograms.iteritems():
        _extract_keyed_histograms(state, name, histograms, is_child)


def _extract_histogram(state, histogram, histogram_name, label, is_child):
    if type(histogram) != dict:
        return

    values = histogram.get("values", None)
    if type(values) != dict:
        return

    histogram_type = histogram.get("histogram_type", None)
    if type(histogram_type) != int:
        return

    if histogram_type == 4:  # Count histogram
        count = values.get("0", 0)
        count = count if isinstance(count, (int, long)) else 0
        return _extract_scalar_value(state, u"[[COUNT]]_{}".format(histogram_name), label, count, count_histogram_labels, is_child=is_child)

    # Note that some dimensions don't vary within a single submissions
    # (e.g. channel) while some do (e.g. process type).
    # The latter should appear within the key of a single metric.
    accessor = (histogram_name, label, is_child)
    aggregated_histogram = state[accessor]["histogram"] = state[accessor].get("histogram", {})

    state[accessor]["count"] = state[accessor].get("count", 0) + 1
    for k, v in values.iteritems():
        try:
            int(k)
        except:
            # We have seen some histograms with non-integer bucket keys...
            continue

        v = v if isinstance(v, (int, long)) else 0
        aggregated_histogram[k] = aggregated_histogram.get(k, 0) + v


def _extract_main_histograms(state, histograms, is_child):
    if type(histograms) != dict:
        return

    for histogram_name, histogram in histograms.iteritems():
        _extract_histogram(state, histogram, histogram_name, u"", is_child)


def _extract_keyed_histograms(state, histogram_name, histograms, is_child):
    if type(histograms) != dict:
        return

    for key, histogram in histograms.iteritems():
        _extract_histogram(state, histogram, histogram_name, key, is_child)


def _extract_simple_measures(state, simple):
    if type(simple) != dict:
        return

    for name, value in simple.iteritems():
        if type(value) == dict:
            for sub_name, sub_value in value.iteritems():
                if type(sub_value) in (int, float, long):
                    _extract_scalar_value(state, u"SIMPLE_MEASURES_{}_{}".format(name.upper(), sub_name.upper()), u"", sub_value, simple_measures_labels)
        elif type(value) in (int, float, long):
            _extract_scalar_value(state, u"SIMPLE_MEASURES_{}".format(name.upper()), u"", value, simple_measures_labels)


def _extract_scalar_value(state, name, label, value, bucket_labels, is_child=False):
    accessor = (name, label, is_child)
    aggregated_histogram = state[accessor]["histogram"] = state[accessor].get("histogram", {})
    state[accessor]["count"] = state[accessor].get("count", 0) + 1

    insert_bucket = bucket_labels[0]  # Initialized to underflow bucket
    for bucket in reversed(bucket_labels):
        if value >= bucket:
            insert_bucket = bucket
            break

    aggregated_histogram[unicode(insert_bucket)] = aggregated_histogram.get(unicode(insert_bucket), 0) + 1


def _extract_children_histograms(state, child_payloads):
    if not isinstance(child_payloads, (list, tuple)):
        return

    for child in child_payloads:
        _extract_histograms(state, child, True)


def _aggregate_ping(state, ping):
    if type(ping) != dict:
        return

    _extract_histograms(state, ping.get("payload", {}))
    _extract_simple_measures(state, ping.get("payload", {}).get("simpleMeasurements", {}))
    _extract_children_histograms(state, ping.get("payload", {}).get("childPayloads", {}))
    return state


def _aggregate_aggregates(agg1, agg2):
    for metric, payload in agg2.iteritems():
        if metric == "count":
            continue

        if metric not in agg1:
            agg1[metric] = payload

        agg1[metric]["count"] += payload["count"]

        for k, v in payload["histogram"].iteritems():
            agg1[metric]["histogram"][k] = agg1[metric]["histogram"].get(k, 0) + v

    return agg1


def _trim_payload(payload):
    return {k: v for k, v in payload.iteritems() if k in ["histograms", "keyedHistograms", "simpleMeasurements"]}


def _map_ping_to_dimensions(ping):
    try:
        submission_date = ping["meta"]["submissionDate"]
        channel = ping["application"]["channel"]
        version = ping["application"]["version"].split('.')[0]
        build_id = ping["application"]["buildId"][:8]
        application = ping["application"]["name"]
        architecture = ping["application"]["architecture"]
        os = ping["environment"]["system"]["os"]["name"]
        os_version = ping["environment"]["system"]["os"]["version"]
        e10s = ping["environment"]["settings"]["e10sEnabled"]
        if os == "Linux":
            os_version = str(os_version)[:3]

        subset = {}
        subset["payload"] = _trim_payload(ping["payload"])
        subset["payload"]["childPayloads"] = [_trim_payload(c) for c in ping["payload"].get("childPayloads", [])]

        # Note that some dimensions don't vary within a single submissions
        # (e.g. channel) while some do (e.g. process type).
        # Dimensions that don't vary should appear in the submission key, while
        # the ones that do vary should appear within the key of a single metric.
        return ((submission_date, channel, version, build_id, application, architecture, os, os_version, e10s), subset)
    except KeyError:
        return None
