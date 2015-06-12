import pyspark
import logging
import requests

from mozaggregator.aggregator import _aggregate_metrics
from mozaggregator.db import create_connection, submit_aggregates
from dataset import *


def setup_module():
    global aggregates
    global sc

    logger = logging.getLogger("py4j")
    logger.setLevel(logging.ERROR)

    sc = pyspark.SparkContext(master="local[*]")
    raw_pings = list(generate_pings())
    aggregates = _aggregate_metrics(sc.parallelize(raw_pings))


def teardown_module():
    sc.stop()


def test_connection():
    db = create_connection()
    assert(db)

SERVICE_URI = "http://localhost:5000"

def test_submit():
    count = submit_aggregates(aggregates)
    n_submission_dates = len(ping_dimensions["submission_date"])
    n_channels = len(ping_dimensions["channel"])
    n_versions = len(ping_dimensions["version"])
    n_build_ids = len(ping_dimensions["build_id"])
    assert(count == n_submission_dates*n_channels*n_versions*n_build_ids)


def test_channels():
    channels = requests.get("{}/channel/".format(SERVICE_URI)).json()
    assert(set(channels) == set(ping_dimensions["channel"]))


def test_buildids():
    template_channel = ping_dimensions["channel"]
    template_version = ping_dimensions["version"]
    template_build_id = ping_dimensions["build_id"]

    for channel in template_channel:
        buildids = requests.get("{}/channel/{}/buildid/".format(SERVICE_URI, channel)).json()
        assert(len(buildids) == len(template_version)*len(template_build_id))

        for buildid in buildids:
            assert(set(buildid.keys()) == set(["buildid", "version"]))
            assert(buildid["buildid"] in [x[:-6] for x in template_build_id])
            assert(buildid["version"] in [x.split('.')[0] for x in template_version])


def test_metric():
    template_channel = ping_dimensions["channel"]
    template_version = [x.split('.')[0] for x in ping_dimensions["version"]]
    template_build_id = [x[:-6] for x in ping_dimensions["build_id"]]

    for channel in template_channel:
        for version in template_version:
            for buildid in template_build_id:
                for histogram in histograms_template.iteritems():
                    metric, value = histogram
                    res = requests.get("{}/channel/{}/buildid/{}_{}?metric={}".format(SERVICE_URI, channel, version, buildid, metric)).json()
                    assert(len(res) == 1)
                    print metric
                    print res
                    assert(False)
                    #res[0]["count"] == 
