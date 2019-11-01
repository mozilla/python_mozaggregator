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
    "arch": ["arm64-v7a", "arm64-v8a"],
    "osversion": [26, 27],
    "normalizedChannel": ["release", "nightly"],
}

meta_template = {
    'Type': 'telemetry',
    'appBuildId': '323001230-GeckoView',
    'appName': 'Focus',
    'appUpdateChannel': 'nightly',
    'appVersion': '8.0',
    'docType': 'mobile-metrics',
    'normalizedAppName': 'Focus',
    'normalizedOs': 'Android',
    'sampleId': '34.0',
    'sourceName': 'telemetry',
    'sourceVersion': '1',
    'submissionDate': SUBMISSION_DATE_1.strftime('%Y%m%d'),
}

histograms_template = {
    'USE_COUNTER2_PROPERTY_FILL_PAGE': {
        'bucket_count': 3,
        'histogram_type': 2,
        'range': [1, 2],
        'sum': 96,
        'values': {
            '0': 0,
            '1': 96,
            '2': 0
        }
    },
    'GC_MAX_PAUSE_MS_2': {
        'bucket_count': 50,
        'histogram_type': 0,
        'range': [1, 10000],
        'sum': 18587,
        'values': {
            '0': 0,
            '1': 13,
            '2': 7,
            '3': 9,
            '4': 13,
            '5': 24,
            '6': 19,
            '7': 26,
            '8': 76,
            '10': 93,
            '12': 55,
            '14': 73,
            '17': 74,
            '20': 56,
            '24': 54,
            '29': 48,
            '34': 48,
            '40': 38,
            '48': 60,
            '57': 18,
            '68': 8,
            '81': 6,
            '96': 2,
            '114': 3,
            '135': 0
        }
    },
}

keyed_histograms_template = {
    'NETWORK_HTTP_REDIRECT_TO_SCHEME': {
        'http': {
            'bucket_count': 51,
            'histogram_type': 5,
            'range': [1, 50],
            'sum': 2,
            'values': {
                '0': 34,
                '1': 2,
                '2': 0
            }
        },
        'https': {
            'bucket_count': 51,
            'histogram_type': 5,
            'range': [1, 50],
            'sum': 55,
            'values': {
                '0': 89,
                '1': 55,
                '2': 0
            }
        }
    }
}

keyed_scalars_template = {
    'telemetry.accumulate_clamped_values': {
        'HTTP_CACHE_IO_QUEUE_2_EVICT': 18
    }
}

scalars_template = {
    'media.page_count': 176,
    'media.page_had_media_count': 2,
    'telemetry.persistence_timer_hit_count': 230,
}


def generate_mobile_pings():

    for dimension in [
        dict(x)
        for x in product(*[list(zip(repeat(k), v))
                         for k, v in ping_dimensions.items()])
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
        'arch': dimension['arch'],
        'clientId': str(uuid.uuid4()),
        'createdDate': SUBMISSION_DATE_1.strftime(DATE_FMT),
        'createdTimestamp': SUBMISSION_DATE_1.strftime("%s"),
        'device': 'Google-Pixel 2',
        'locale': 'en-US',
        'meta': meta,
        'metrics': metrics,
        'os': 'Android',
        'osversion': dimension['osversion'],
        'processStartTimestamp': SUBMISSION_DATE_1.strftime("%s"),
        'profileDate': 17747,
        'seq': 123,
        'tz': 120,
        'v': 1,
    }
