import argparse
import ujson as json

from flask import Flask, request, abort
from flask.ext.cors import CORS
from db import create_connection, histogram_revision_map
from moztelemetry.histogram import Histogram
from aggregator import simple_measures_labels, count_histogram_labels
from werkzeug.contrib.cache import SimpleCache
from joblib import Parallel, delayed
from functools import wraps

from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop

TIMEOUT = 24*60*60

app = Flask(__name__)
cache = SimpleCache()
CORS(app, resources=r'/*', allow_headers='Content-Type')


def cache_request(f):
    @wraps(f)
    def decorated_request(*args, **kwargs):
        rv = cache.get(request.url)
        if rv is None:
            rv = f(*args, **kwargs)
            cache.set(request.url, rv, timeout=TIMEOUT)
            return rv
        else:
            return rv
    return decorated_request


def execute_query(query, params=tuple()):
    db = create_connection(host_override=host)
    cursor = db.cursor()
    cursor.execute(query, params)
    return cursor.fetchall()


@app.route('/status')
def status():
    return "OK"


@app.route('/aggregates_by/<prefix>/channels/')
@cache_request
def get_channels(prefix):
    try:
        channels = execute_query("select * from list_channels(%s)", (prefix, ))
        if not channels:
            abort(404)

        return json.dumps([channel[0] for channel in channels])
    except:
        abort(404)


@app.route('/aggregates_by/<prefix>/channels/<channel>/dates/')
@cache_request
def get_dates(prefix, channel):
    try:
        result = execute_query("select * from list_buildids(%s, %s)", (prefix, channel))
        if not result:
            abort(404)

        pretty_result = map(lambda r: {"version": r[0], "date": r[1]}, result)
        return json.dumps(pretty_result)
    except:
        abort(404)


def get_filter_options(prefix, channel, filters, filter):
    options = execute_query("select * from list_filter_options(%s, %s, %s)", (prefix, channel, filter))
    if not options or (len(options) == 1 and options[0][0] is None):
        raise ValueError("Invalid option")

    pretty_opts = []
    for option in options:
        option = option[0]
        if filter == "metric" and option.startswith("[[COUNT]]_"):
            pretty_opts.append(option[10:])
        else:
            pretty_opts.append(option)

    filters[filter] = pretty_opts


@app.route('/aggregates_by/<prefix>/channels/<channel>/filters/')
@cache_request
def get_filters_options(prefix, channel):
    try:
        filters = {}
        dimensions = ["metric", "application", "architecture", "os", "e10sEnabled", "child"]

        # TODO: come up with a better strategy than using the nightly filter options on all channels
        Parallel(n_jobs=len(dimensions), backend="threading")(delayed(get_filter_options)("submission_date", "nightly", filters, f)
                                                              for f in dimensions)

        return json.dumps(filters)
    except:
        abort(404)


def get_filter_options_new(channel, version, filters, filter):
    options = execute_query("select * from get_filter_options(%s, %s, %s)", (channel, version, filter))
    if not options or (len(options) == 1 and options[0][0] is None):
        return

    pretty_opts = []
    for option in options:
        option = option[0]
        if filter == "metric" and option.startswith("[[COUNT]]_"):
            pretty_opts.append(option[10:])
        else:
            pretty_opts.append(option)

    filters[filter] = pretty_opts


@app.route('/filters/', methods=["GET"])
@cache_request
def get_filters_options_new():
    try:
        channel = request.args.get("channel", None)
        version = request.args.get("version", None)

        if not channel or not version:
            abort(404)

        filters = {}
        dimensions = ["metric", "application", "architecture", "os", "e10sEnabled", "child"]

        Parallel(n_jobs=len(dimensions), backend="threading")(delayed(get_filter_options_new)(channel, version, filters, f)
                                                              for f in dimensions)

        if not filters:
            abort(404)

        return json.dumps(filters)
    except:
        abort(404)


@app.route('/aggregates_by/<prefix>/channels/<channel>/', methods=["GET"])
@cache_request
def get_dates_metrics(prefix, channel):
    try:
        mapping = {"true": True, "false": False}
        dimensions = {k: mapping.get(v, v) for k, v in request.args.iteritems()}

        # Get dates
        dates = dimensions.pop('dates', "").split(',')
        version = dimensions.pop('version', None)

        if not dates or not version:
            abort(404)

        # Get bucket labels
        if dimensions["metric"].startswith("SIMPLE_MEASURES_"):
            labels = simple_measures_labels
            kind = "exponential"
            description = ""
        else:
            revision = histogram_revision_map.get(channel, "nightly")  # Use nightly revision if the channel is unknown
            definition = Histogram(dimensions["metric"], {"values": {}}, revision=revision)
            kind = definition.kind
            description = definition.definition.description()

            if kind == "count":
                labels = count_histogram_labels
                dimensions["metric"] = "[[COUNT]]_{}".format(dimensions["metric"])
            elif kind == "flag":
                labels = [0, 1]
            else:
                labels = definition.get_value().keys().tolist()

        # Fetch metrics
        result = execute_query("select * from batched_get_metric(%s, %s, %s, %s, %s)", (prefix, channel, version, dates, json.dumps(dimensions)))
        if not result:  # Metric not found
            abort(404)

        pretty_result = {"data": [], "buckets": labels, "kind": kind, "description": description}
        for row in result:
            date = row[0]
            label = row[1]
            histogram = row[2][:-2]
            sum = row[2][-2]
            count = row[2][-1]
            pretty_result["data"].append({"date": date, "label": label, "histogram": histogram, "count": count, "sum": sum})

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

    if args.debug:
        app.run("0.0.0.0", debug=args.debug, threaded=True)
    else:
        http_server = HTTPServer(WSGIContainer(app))
        http_server.listen(5000)
        IOLoop.instance().start()
