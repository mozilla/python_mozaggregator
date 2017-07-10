
from moztelemetry.histogram import cached_exponential_buckets

# Simple measurement, count histogram, and numeric scalars labels & prefixes
SIMPLE_MEASURES_LABELS = cached_exponential_buckets(1, 30000, 50)
COUNT_HISTOGRAM_LABELS = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 21, 23, 25, 27, 29, 31, 34, 37, 40, 43, 46, 50, 54, 58, 63, 68, 74, 80, 86, 93, 101, 109, 118, 128, 138, 149, 161, 174, 188, 203, 219, 237, 256, 277, 299, 323, 349, 377, 408, 441, 477, 516, 558, 603, 652, 705, 762, 824, 891, 963, 1041, 1125, 1216, 1315, 1422, 1537, 1662, 1797, 1943, 2101, 2271, 2455, 2654, 2869, 3102, 3354, 3626, 3920, 4238, 4582, 4954, 5356, 5791, 6261, 6769, 7318, 7912, 8554, 9249, 10000]
NUMERIC_SCALARS_LABELS = COUNT_HISTOGRAM_LABELS

SIMPLE_MEASURES_PREFIX = 'SIMPLE_MEASURES'
COUNT_HISTOGRAM_PREFIX = '[[COUNT]]'
SCALARS_PREFIX = 'SCALARS'

SCALAR_MEASURE_MAP = {
    SIMPLE_MEASURES_PREFIX: SIMPLE_MEASURES_LABELS,
    COUNT_HISTOGRAM_PREFIX: COUNT_HISTOGRAM_LABELS,
    SCALARS_PREFIX: NUMERIC_SCALARS_LABELS
}

PROCESS_TYPES = {"parent", "content", "gpu"}

STRING_SCALAR_WHITELIST = {
    "telemetry.test.string_kind"
}

HISTOGRAM_REVISION_MAP = {
    "nightly": "https://hg.mozilla.org/mozilla-central/rev/tip",
    "aurora": "https://hg.mozilla.org/releases/mozilla-aurora/rev/tip",
    "beta": "https://hg.mozilla.org/releases/mozilla-beta/rev/tip",
    "release": "https://hg.mozilla.org/releases/mozilla-release/rev/tip"
}
