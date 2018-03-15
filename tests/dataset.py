import uuid
from itertools import chain, product, repeat

from mozaggregator.aggregator import PROCESS_TYPES


NUM_CHILDREN_PER_PING = 3
NUM_AGGREGATED_CHILD_PINGS = 2
NUM_PINGS_PER_DIMENSIONS = 3
assert(NUM_AGGREGATED_CHILD_PINGS <= NUM_PINGS_PER_DIMENSIONS)
NUM_PROCESS_TYPES = len(PROCESS_TYPES)
SCALAR_VALUE = 42
SIMPLE_SCALAR_BUCKET = 35
COUNT_SCALAR_BUCKET = 40
NUMERIC_SCALAR_BUCKET = 40

ping_dimensions = {
    "submission_date": [u"20150601", u"20150603"],
    "channel": [u"nightly", u"beta"],
    "version": [u"40.0b1", u"41"],
    "build_id": [u"20150601000000", u"20150602000000"],
    "application": [u"Firefox", u"Fennec"],
    "arch": [u"x86", u"x86-64"],
    "os": [u"Linux", u"Windows_NT"],
    "os_version": [u"6.1", u"3.1.12"],
    "e10s": [True, False]
}

histograms_template = {
    u"EVENTLOOP_UI_ACTIVITY_EXP_MS": {u"bucket_count": 20,
                                      u"histogram_type": 0,
                                      u"log_sum": 0,
                                      u"log_sum_squares": 0,
                                      u"range": [50, 60000],
                                      u"sum": 9362,
                                      u"values": {u"0": 0,
                                                  u"110": 16,
                                                  u"1170": 0,
                                                  u"163": 8,
                                                  u"242": 5,
                                                  u"359": 2,
                                                  u"50": 18,
                                                  u"74": 16,
                                                  u"789": 1}},
    u"UPDATE_PING_COUNT_EXTERNAL": {u"bucket_count": 3,
                                    u"histogram_type": 4,
                                    u"range": [1, 2],
                                    u"sum": SCALAR_VALUE,
                                    u"values": {u"0": SCALAR_VALUE, u"1": 0}},
    u"USE_COUNTER2_PROPERTY_FILL_PAGE": {u'bucket_count': 3,
                                         u'histogram_type': 2,
                                         u'range': [1, 2],
                                         u'sum': 2,
                                         u'values': {u'0': 0, u'1': 2, u'2': 0}},
    u"USE_COUNTER2_ISNULL_PAGE": None,
    u"USE_COUNTER2_PROPERTY_FILL_DOCUMENT": {u'bucket_count': 3,
                                             u'histogram_type': 2,
                                             u'range': [1, 2],
                                             u'sum': 1,
                                             u'values': {u'0': 0, u'1': 1, u'2': 0}},
    u"CONTENT_DOCUMENTS_DESTROYED": {u"bucket_count": 3,
                                     u"histogram_type": 4,
                                     u"range": [1, 2],
                                     u"sum": 17,
                                     u"values": {u"0": 17, u"1": 0}},
    u"TOP_LEVEL_CONTENT_DOCUMENTS_DESTROYED": {u"bucket_count": 3,
                                               u"histogram_type": 4,
                                               u"range": [1, 2],
                                               u"sum": 19,
                                               u"values": {u"0": 19, u"1": 0}},
    u"USE_COUNTER_PROPERTY_FILL_DOCUMENT": {u'bucket_count': 3,
                                            u'histogram_type': 2,
                                            u'range': [1, 2],
                                            u'sum': 1,
                                            u'values': {u'0': 0, u'1': 1}},
    u"TELEMETRY_TEST_CATEGORICAL": {u"bucket_count": 4,
                                    u"histogram_type": 5,
                                    u"range": [1, 2],
                                    u"sum": 3,
                                    u"values": {u"0": 1, u"1": 1, u"2": 1, u"3": 0}},
    u"GC_MAX_PAUSE_MS_2": {"bucket_count": 50,
                           "histogram_type": 1,
                           "range": [1, 1000],
                           "sum": 554,
                           "values": {"0": 0, "1": 4, "22": 2, "43": 1, "63": 1, "272": 1, "292": 0}}
}

keyed_histograms_template = {
    u"DEVTOOLS_PERFTOOLS_RECORDING_FEATURES_USED": {
        "withMarkers": {
            "range": [1, 2],
            "bucket_count": 3,
            "histogram_type": 2,
            "values": {
                "0": 0,
                "1": 1,
                "2": 0
            },
            "sum": 1
        }
    }
}

ignored_keyed_histograms_template = {
    u"MESSAGE_MANAGER_MESSAGE_SIZE": {u"foo": {u"bucket_count": 20,
                                               u"histogram_type": 0,
                                               u"sum": 0,
                                               u"values": {u"0": 0}}},
    "VIDEO_DETAILED_DROPPED_FRAMES_PROPORTION": {u"foo": {u"bucket_count": 20,
                                                          u"histogram_type": 0,
                                                          u"sum": 0,
                                                          u"values": {u"0": 0}}}
}


simple_measurements_template = {
    "uptime": SCALAR_VALUE,
    "addonManager": {
        u"XPIDB_parseDB_MS": SCALAR_VALUE
    }
}

scalars_template = {
    "browser.engagement.total_uri_count": SCALAR_VALUE,
    "browser.engagement.tab_open_event_count": SCALAR_VALUE
}

ignored_scalars_template = {
    "browser.engagement.navigation": SCALAR_VALUE,
    "browser.engagement.navigation.test": SCALAR_VALUE,
    "telemetry.test.string_kind": "IGNORED_STRING"
}

keyed_scalars_template = {
    "telemetry.test.keyed_release_optout": {
        "search_enter": SCALAR_VALUE
    },
    "telemetry.test.keyed_unsigned_int": {
        "first": SCALAR_VALUE,
        "second": SCALAR_VALUE
    }
}

ignored_keyed_scalars_template = {
    "browser.engagement.navigation.searchbar": {
        "first": SCALAR_VALUE,
        "second": SCALAR_VALUE
    },
    "fake.keyed.string": {
        "first": "IGNORE_ME"
    }
}


def generate_pings():
    for dimensions in [dict(x) for x in product(*[zip(repeat(k), v) for k, v in ping_dimensions.iteritems()])]:
        for i in range(NUM_PINGS_PER_DIMENSIONS):
            yield generate_payload(dimensions, i < NUM_AGGREGATED_CHILD_PINGS)


def generate_payload(dimensions, aggregated_child_histograms):
    meta = {u"submissionDate": dimensions["submission_date"],
            u"sampleId": 42}
    application = {u"channel": dimensions["channel"],
                   u"version": dimensions["version"],
                   u"buildId": dimensions["build_id"],
                   u"name": dimensions["application"],
                   u"architecture": dimensions["arch"]}

    child_payloads = [{"simpleMeasurements": simple_measurements_template}
                      for i in range(NUM_CHILDREN_PER_PING)]

    scalars = dict(chain(scalars_template.iteritems(), ignored_scalars_template.iteritems()))
    keyed_scalars = dict(chain(keyed_scalars_template.iteritems(), ignored_keyed_scalars_template.iteritems()))

    processes_payload = {
        u"parent": {
            u"scalars": scalars,
            u"keyedScalars": keyed_scalars
        }
    }

    if aggregated_child_histograms:
        processes_payload[u"content"] = {
            u"histograms": histograms_template,
            u"keyedHistograms": keyed_histograms_template,
            u"scalars": scalars,
            u"keyedScalars": keyed_scalars
        }
        processes_payload[u"gpu"] = {
            u"histograms": histograms_template,
            u"keyedHistograms": keyed_histograms_template,
            u"scalars": scalars,
            u"keyedScalars": keyed_scalars
        }
    else:
        for i in range(NUM_CHILDREN_PER_PING):
            child_payloads[i][u"histograms"] = histograms_template
            child_payloads[i][u"keyedHistograms"] = keyed_histograms_template

    payload = {u"simpleMeasurements": simple_measurements_template,
               u"histograms": histograms_template,
               u"keyedHistograms": dict(keyed_histograms_template.items() +
                                        ignored_keyed_histograms_template.items()),
               u"childPayloads": child_payloads,
               u"processes": processes_payload}

    environment = {u"system": {u"os": {u"name": dimensions["os"],
                                       u"version": dimensions["os_version"]}},
                   u"settings": {u"telemetryEnabled": True,
                                 u"e10sEnabled": dimensions["e10s"]}}

    return {u"clientId": str(uuid.uuid4()),
            u"meta": meta,
            u"application": application,
            u"payload": payload,
            u"environment": environment}


def expected_count(process_type, scalar=False):
    if process_type == "parent":
        return NUM_PINGS_PER_DIMENSIONS
    elif process_type == "gpu":
        return NUM_AGGREGATED_CHILD_PINGS
    elif process_type == "content" and not scalar:
        return (NUM_PINGS_PER_DIMENSIONS - NUM_AGGREGATED_CHILD_PINGS) * NUM_CHILDREN_PER_PING + NUM_AGGREGATED_CHILD_PINGS
    elif process_type == "content" and scalar:
        return NUM_AGGREGATED_CHILD_PINGS
    else:
        return -1
