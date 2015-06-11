import uuid

NUM_CHILDREN_PER_PING = 3
NUM_PINGS_PER_DIMENSIONS = 2

ping_dimensions = {"submission_date": [u"20150601", u"20150602"],
                   "channel": [u"nightly", u"aurora", u"beta", u"release"],
                   "version": [u"40.0a1", u"41"],
                   "build_id": [u"20150601000000", u"20150602000000"],
                   "application": [u"Firefox", u"Fennec"],
                   "arch": [u"x86", u"x86-64"],
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

    payload = {u"simpleMeasurements": simple_measurements_template,
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
