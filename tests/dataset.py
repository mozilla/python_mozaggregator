import uuid

from itertools import product, repeat

NUM_CHILDREN_PER_PING = 3
NUM_AGGREGATED_CHILD_PINGS = 1
NUM_PINGS_PER_DIMENSIONS = 2
assert(NUM_AGGREGATED_CHILD_PINGS <= NUM_PINGS_PER_DIMENSIONS)
SCALAR_VALUE = 42
SIMPLE_SCALAR_BUCKET = 35
COUNT_SCALAR_BUCKET = 40
NUMERIC_SCALAR_BUCKET = 40

ping_dimensions = {"submission_date": [u"20150601", u"20150603"],
                   "channel": [u"nightly", u"aurora"],
                   "version": [u"40.0a1", u"41"],
                   "build_id": [u"20150601000000", u"20150602000000"],
                   "application": [u"Firefox", u"Fennec"],
                   "arch": [u"x86", u"x86-64"],
                   "os": [u"Linux", u"Windows_NT"],
                   "os_version": [u"6.1", u"3.1.12"],
                   "e10s": [True, False]}

histograms_template = {u"EVENTLOOP_UI_ACTIVITY_EXP_MS": {u'bucket_count': 20,
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
                                                       u'sum': SCALAR_VALUE,
                                                       u'values': {u'0': SCALAR_VALUE, u'1': 0}},
                       u"USE_COUNTER2_PROPERTY_FILL_PAGE": {u'bucket_count': 3,
                                                            u'histogram_type': 2,
                                                            u'range': [1, 2],
                                                            u'sum': 2,
                                                            u'values': {u'0': 0, u'1': 2, u'2': 0}},
                       u"USE_COUNTER2_PROPERTY_FILL_DOCUMENT": {u'bucket_count': 3,
                                                                u'histogram_type': 2,
                                                                u'range': [1, 2],
                                                                u'sum': 1,
                                                                u'values': {u'0': 0, u'1': 1, u'2': 0}},
                       u"CONTENT_DOCUMENTS_DESTROYED": {u'bucket_count': 3,
                                                        u'histogram_type': 4,
                                                        u'range': [1, 2],
                                                        u'sum': 17,
                                                        u'values': {u'0': 17, u'1': 0}},
                       u"TOP_LEVEL_CONTENT_DOCUMENTS_DESTROYED": {u'bucket_count': 3,
                                                                  u'histogram_type': 4,
                                                                  u'range': [1, 2],
                                                                  u'sum': 19,
                                                                  u'values': {u'0': 19, u'1': 0}},
                       u"USE_COUNTER_PROPERTY_FILL_DOCUMENT": {u'bucket_count': 3,
                                                               u'histogram_type': 2,
                                                               u'range': [1, 2],
                                                               u'sum': 1,
                                                               u'values': {u'0': 0, u'1': 1}},
                       u"TELEMETRY_TEST_CATEGORICAL": {u'bucket_count': 4,
                                                       u'histogram_type': 5,
                                                       u'range': [1, 2],
                                                       u'sum': 3,
                                                       u'values': {u'0': 1, u'1': 1, u'2':1, u'3':0}}}

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

ignored_keyed_histograms_template = {u'MESSAGE_MANAGER_MESSAGE_SIZE':
                                     {u'foo': {u'bucket_count': 20,
                                               u'histogram_type': 0,
                                               u'sum': 0,
                                               u'values': {u'0': 0}
                                     }},
                                     "VIDEO_DETAILED_DROPPED_FRAMES_PROPORTION":
                                     {u'foo': {u'bucket_count': 20,
                                               u'histogram_type': 0,
                                               u'sum': 0,
                                               u'values': {u'0': 0}
                                     }}}

simple_measurements_template = {"uptime": SCALAR_VALUE, "addonManager": {u'XPIDB_parseDB_MS': SCALAR_VALUE}}

scalars_template = {
    "browser.engagement.total_uri_count": SCALAR_VALUE, 
    "browser.engagement.tab_open_event_count": SCALAR_VALUE
}

keyed_scalars_template = {
    "browser.engagement.navigation.searchbar": {
        "search_enter": SCALAR_VALUE
    },
    "test.keyed.scalar": {
        "first": SCALAR_VALUE,
        "second": SCALAR_VALUE
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

    processes_payload = {
        u"parent": {
            u"scalars": scalars_template,
            u"keyedScalars": keyed_scalars_template
        }
    }


    if aggregated_child_histograms:
        processes_payload[u"content"] = {
            u"histograms": histograms_template,
            u"keyedHistograms": keyed_histograms_template,
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

def expected_count(is_child):
    if not is_child:
        return NUM_PINGS_PER_DIMENSIONS
    return (NUM_PINGS_PER_DIMENSIONS - NUM_AGGREGATED_CHILD_PINGS) * NUM_CHILDREN_PER_PING + NUM_AGGREGATED_CHILD_PINGS
