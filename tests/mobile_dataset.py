import datetime
import uuid
from itertools import product, repeat


NUM_PINGS_PER_DIMENSIONS = 3

DATE_FMT = '%Y-%m-%d'
DATETIME_FMT = '%Y%m%d%H%M%S'

NOW = datetime.datetime.now()
SUBMISSION_DATE_1 = NOW
SUBMISSION_DATE_2 = NOW - datetime.timedelta(days=2)

ping_dimensions = {
    "arch": [u"arm64-v7a", u"arm64-v8a"],
    "osversion": [26, 27],
    "normalizedChannel": ["release", "nightly"],
}

meta_template = {
    u'Type': u'telemetry',
    u'appBuildId': u'323001230-GeckoView',
    u'appName': u'Focus',
    u'appUpdateChannel': u'nightly',
    u'appVersion': u'8.0',
    u'docType': u'mobile-metrics',
    u'normalizedAppName': u'Focus',
    u'normalizedOs': u'Android',
    u'sampleId': u'34.0',
    u'sourceName': u'telemetry',
    u'sourceVersion': u'1',
    u'submissionDate': SUBMISSION_DATE_1.strftime('%Y%m%d'),
}

histograms_template = {
    u'USE_COUNTER2_PROPERTY_FILL_PAGE': {
        u'bucket_count': 3,
        u'histogram_type': 2,
        u'range': [1, 2],
        u'sum': 96,
        u'values': {
            u'0': 0,
            u'1': 96,
            u'2': 0
        }
    },
    u'GC_MAX_PAUSE_MS_2': {
        u'bucket_count': 50,
        u'histogram_type': 0,
        u'range': [1, 10000],
        u'sum': 18587,
        u'values': {
            u'0': 0,
            u'1': 13,
            u'2': 7,
            u'3': 9,
            u'4': 13,
            u'5': 24,
            u'6': 19,
            u'7': 26,
            u'8': 76,
            u'10': 93,
            u'12': 55,
            u'14': 73,
            u'17': 74,
            u'20': 56,
            u'24': 54,
            u'29': 48,
            u'34': 48,
            u'40': 38,
            u'48': 60,
            u'57': 18,
            u'68': 8,
            u'81': 6,
            u'96': 2,
            u'114': 3,
            u'135': 0
        }
    },
}

keyed_histograms_template = {
    u'NETWORK_HTTP_REDIRECT_TO_SCHEME': {
        u'http': {
            u'bucket_count': 51,
            u'histogram_type': 5,
            u'range': [1, 50],
            u'sum': 2,
            u'values': {
                u'0': 34,
                u'1': 2,
                u'2': 0
            }
        },
        u'https': {
            u'bucket_count': 51,
            u'histogram_type': 5,
            u'range': [1, 50],
            u'sum': 55,
            u'values': {
                u'0': 89,
                u'1': 55,
                u'2': 0
            }
        }
    }
}

keyed_scalars_template = {
    u'telemetry.accumulate_clamped_values': {
        u'HTTP_CACHE_IO_QUEUE_2_EVICT': 18
    }
}

scalars_template = {
    u'media.page_count': 176,
    u'media.page_had_media_count': 2,
    u'telemetry.persistence_timer_hit_count': 230,
}


def generate_mobile_pings():

    for dimension in [
        dict(x)
        for x in product(*[zip(repeat(k), v)
                         for k, v in ping_dimensions.iteritems()])
    ]:
        for i in range(NUM_PINGS_PER_DIMENSIONS):
            yield generate_payload(dimension)


def generate_payload(dimension):

    metrics = {
        'content': {
            'histograms': histograms_template,
            'keyedHistograms': keyed_histograms_template,
            'keyedScalars': keyed_scalars_template,
            'scalars': scalars_template,
        },
        'dynamic': {
            'histograms': histograms_template,
            'keyedHistograms': keyed_histograms_template,
        },
        'extension': {
            'histograms': histograms_template,
            'keyedHistograms': keyed_histograms_template,
        },
        'gpu': {
            'histograms': histograms_template,
            'keyedHistograms': keyed_histograms_template,
        },
        'parent': {
            'histograms': histograms_template,
            'keyedHistograms': keyed_histograms_template,
            'keyedScalars': keyed_scalars_template,
            'scalars': scalars_template,
        },
    }

    meta = meta_template.copy()
    meta['normalizedChannel'] = dimension['normalizedChannel']

    return {
        u'arch': dimension['arch'],
        u'clientId': str(uuid.uuid4()),
        u'createdDate': SUBMISSION_DATE_1.strftime(DATE_FMT),
        u'createdTimestamp': SUBMISSION_DATE_1.strftime("%s"),
        u'device': u'Google-Pixel 2',
        u'locale': u'en-US',
        u'meta': meta,
        u'metrics': metrics,
        u'os': u'Android',
        u'osversion': dimension['osversion'],
        u'processStartTimestamp': SUBMISSION_DATE_1.strftime("%s"),
        u'profileDate': 17747,
        u'seq': 123,
        u'tz': 120,
        u'v': 1,
    }
