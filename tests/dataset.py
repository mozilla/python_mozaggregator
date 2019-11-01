import datetime
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

DATE_FMT = '%Y%m%d'
DATETIME_FMT = '%Y%m%d%H%M%S'

TODAY = datetime.date.today()
BUILD_ID_1 = TODAY - datetime.timedelta(days=1)
BUILD_ID_2 = TODAY - datetime.timedelta(days=2)
SUBMISSION_DATE_1 = TODAY
SUBMISSION_DATE_2 = TODAY - datetime.timedelta(days=2)

ping_dimensions = {
    "submission_date": [SUBMISSION_DATE_2.strftime(DATE_FMT),
                        SUBMISSION_DATE_1.strftime(DATE_FMT)],
    "channel": ["nightly", "beta", "release"],
    "version": ["40.0b1", "41"],
    "build_id": [BUILD_ID_2.strftime(DATETIME_FMT),
                 BUILD_ID_1.strftime(DATETIME_FMT)],
    "application": ["Firefox", "Fennec"],
    "arch": ["x86", "x86-64"],
    "os": ["Linux", "Windows_NT"],
    "os_version": ["6.1", "3.1.12"],
}

histograms_template = {
    "EVENTLOOP_UI_ACTIVITY_EXP_MS": {"bucket_count": 20,
                                      "histogram_type": 0,
                                      "log_sum": 0,
                                      "log_sum_squares": 0,
                                      "range": [50, 60000],
                                      "sum": 9362,
                                      "values": {"0": 0,
                                                  "110": 16,
                                                  "1170": 0,
                                                  "163": 8,
                                                  "242": 5,
                                                  "359": 2,
                                                  "50": 18,
                                                  "74": 16,
                                                  "789": 1}},
    "UPDATE_PING_COUNT_EXTERNAL": {"bucket_count": 3,
                                    "histogram_type": 4,
                                    "range": [1, 2],
                                    "sum": SCALAR_VALUE,
                                    "values": {"0": SCALAR_VALUE, "1": 0}},
    "USE_COUNTER2_PROPERTY_FILL_PAGE": {'bucket_count': 3,
                                         'histogram_type': 2,
                                         'range': [1, 2],
                                         'sum': 2,
                                         'values': {'0': 0, '1': 2, '2': 0}},
    "USE_COUNTER2_ISNULL_PAGE": None,
    "USE_COUNTER2_PROPERTY_FILL_DOCUMENT": {'bucket_count': 3,
                                             'histogram_type': 2,
                                             'range': [1, 2],
                                             'sum': 1,
                                             'values': {'0': 0, '1': 1, '2': 0}},
    "CONTENT_DOCUMENTS_DESTROYED": {"bucket_count": 3,
                                     "histogram_type": 4,
                                     "range": [1, 2],
                                     "sum": 17,
                                     "values": {"0": 17, "1": 0}},
    "TOP_LEVEL_CONTENT_DOCUMENTS_DESTROYED": {"bucket_count": 3,
                                               "histogram_type": 4,
                                               "range": [1, 2],
                                               "sum": 19,
                                               "values": {"0": 19, "1": 0}},
    "USE_COUNTER_PROPERTY_FILL_DOCUMENT": {'bucket_count': 3,
                                            'histogram_type': 2,
                                            'range': [1, 2],
                                            'sum': 1,
                                            'values': {'0': 0, '1': 1}},
    "TELEMETRY_TEST_CATEGORICAL": {"bucket_count": 4,
                                    "histogram_type": 5,
                                    "range": [1, 2],
                                    "sum": 3,
                                    "values": {"0": 1, "1": 1, "2": 1, "3": 0}},
    "GC_MAX_PAUSE_MS_2": {"bucket_count": 50,
                           "histogram_type": 1,
                           "range": [1, 1000],
                           "sum": 554,
                           "values": {"0": 0, "1": 4, "22": 2, "43": 1, "63": 1, "272": 1, "292": 0}}
}

keyed_histograms_template = {
    "DEVTOOLS_PERFTOOLS_RECORDING_FEATURES_USED": {
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
    },
}

ignored_keyed_histograms_template = {
    "MESSAGE_MANAGER_MESSAGE_SIZE": {"foo": {"bucket_count": 20,
                                               "histogram_type": 0,
                                               "sum": 0,
                                               "values": {"0": 0}}},
    "VIDEO_DETAILED_DROPPED_FRAMES_PROPORTION": {"foo": {"bucket_count": 20,
                                                          "histogram_type": 0,
                                                          "sum": 0,
                                                          "values": {"0": 0}}},
    "SEARCH_COUNTS": {"ddg.urlbar": {"range": [1, 2],
                                      "bucket_count": 3,
                                      "histogram_type": 4,
                                      "values": {"0": 1, "1": 0},
                                      "sum": 1}},
}


simple_measurements_template = {
    "uptime": SCALAR_VALUE,
    "addonManager": {
        "XPIDB_parseDB_MS": SCALAR_VALUE
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

private_keyed_scalars_template = {
    "telemetry.event_counts": {
        "some#event#happened": SCALAR_VALUE
    },
    "telemetry.dynamic_event_counts": {
        "some#dynamic#event": SCALAR_VALUE
    }
}


def generate_pings():
    for dimensions in [
        dict(x) for x in product(
            *[list(zip(repeat(k), v)) for k, v in ping_dimensions.items()]
        )
    ]:
        for i in range(NUM_PINGS_PER_DIMENSIONS):
            yield generate_payload(dimensions, i < NUM_AGGREGATED_CHILD_PINGS)


def generate_payload(dimensions, aggregated_child_histograms):
    meta = {
        "submissionDate": dimensions["submission_date"],
        "sampleId": 42,
    }
    application = {
        "channel": dimensions["channel"],
        "version": dimensions["version"],
        "buildId": dimensions["build_id"],
        "name": dimensions["application"],
        "architecture": dimensions["arch"],
    }

    child_payloads = [{"simpleMeasurements": simple_measurements_template}
                      for i in range(NUM_CHILDREN_PER_PING)]

    scalars = dict(chain(iter(scalars_template.items()), iter(ignored_scalars_template.items())))
    keyed_scalars = dict(chain(iter(keyed_scalars_template.items()),
                               iter(ignored_keyed_scalars_template.items()),
                               iter(private_keyed_scalars_template.items())))

    processes_payload = {
        "parent": {
            "scalars": scalars,
            "keyedScalars": keyed_scalars
        }
    }

    if aggregated_child_histograms:
        processes_payload["content"] = {
            "histograms": histograms_template,
            "keyedHistograms": keyed_histograms_template,
            "scalars": scalars,
            "keyedScalars": keyed_scalars
        }
        processes_payload["gpu"] = {
            "histograms": histograms_template,
            "keyedHistograms": keyed_histograms_template,
            "scalars": scalars,
            "keyedScalars": keyed_scalars
        }
    else:
        for i in range(NUM_CHILDREN_PER_PING):
            child_payloads[i]["histograms"] = histograms_template
            child_payloads[i]["keyedHistograms"] = keyed_histograms_template

    payload = {
        "simpleMeasurements": simple_measurements_template,
        "histograms": histograms_template,
        "keyedHistograms": dict(list(keyed_histograms_template.items()) +
                                 list(ignored_keyed_histograms_template.items())),
        "childPayloads": child_payloads,
        "processes": processes_payload,
    }

    environment = {
        "system": {"os": {"name": dimensions["os"],
                            "version": dimensions["os_version"]}},
        "settings": {"telemetryEnabled": False,
                      "e10sEnabled": dimensions.get("e10s", True)}
    }

    return {
        "clientId": str(uuid.uuid4()),
        "meta": meta,
        "application": application,
        "payload": payload,
        "environment": environment,
    }


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
