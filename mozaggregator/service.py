import argparse
import ujson as json

from flask import Flask, request, abort
from db import create_connection, histogram_revision_map
from moztelemetry.histogram import Histogram
from aggregator import scalar_histogram_labels

app = Flask(__name__)


def execute_query(query, params=tuple()):
    db = create_connection(host_override=host)
    cursor = db.cursor()
    cursor.execute(query, params)
    return cursor.fetchall()


@app.route('/channel/')
def get_channels():
    try:
        channels = execute_query("select * from list_channels()")
        return json.dumps([channel[0] for channel in channels])
    except:
        abort(404)


@app.route('/channel/<channel>/buildid/')
def get_buildids(channel):
    try:
        result = execute_query("select * from list_buildids(%s)", (channel, ))
        pretty_result = map(lambda r: {"version": r[0], "buildid": r[1]}, result)
        return json.dumps(pretty_result)
    except:
        abort(404)


@app.route('/channel/<channel>/buildid/<version>_<buildid>', methods=["GET"])
def get_buildid(channel, version, buildid):
    try:
        dimensions = json.dumps({k: v for k, v in request.args.iteritems()})
        result = execute_query("select * from get_buildid_metric(%s, %s, %s, %s)", (channel, version, buildid, dimensions))

        if not result:  # Metric not found
            abort(404)

        pretty_result = []
        for row in result:
            label = row[0]
            histogram = row[1][:-1]
            count = row[1][-1]

            # Retrieve labels for histogram
            revision = histogram_revision_map.get(channel, "nightly")  # Use nightly revision if the channel is unknown
            try:
                labels = Histogram(request.args["metric"], histogram, revision=revision).get_value().keys().tolist()
            except Exception as e:
                # Count histogram or simple measurement
                # TODO: deal properly with those
                labels = scalar_histogram_labels

            pretty_result.append({"label": label, "histogram": dict(zip(labels, histogram)), "count": count})

        return json.dumps(pretty_result)

    except:
        abort(404)


if __name__ == "__main__":
    global host

    parser = argparse.ArgumentParser(description="Aggregation REST service", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-d", "--debug", help="Debug mode", dest="debug", action="store_true")
    parser.add_argument("-o", "--host", help="DB hostname", default=None)

    parser.set_defaults(debug=False)
    args = parser.parse_args()
    host = args.host

    app.run("0.0.0.0", debug=args.debug)
