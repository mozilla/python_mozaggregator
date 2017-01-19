import ujson as json
import config
import logging

from flask import Flask, Response, request, abort
from flask.ext.cors import CORS
from flask.ext.cache import Cache
from flask_sslify import SSLify
from moztelemetry.histogram import Histogram
from joblib import Parallel, delayed
from functools import wraps
from gevent.monkey import patch_all
from psycogreen.gevent import patch_psycopg
from psycopg2.pool import SimpleConnectionPool
from aggregator import SIMPLE_MEASURES_LABELS, COUNT_HISTOGRAM_LABELS, NUMERIC_SCALARS_LABELS, \
                        SIMPLE_MEASURES_PREFIX, COUNT_HISTOGRAM_PREFIX, NUMERIC_SCALARS_PREFIX, \
                        SCALAR_MEASURE_MAP
from db import get_db_connection_string, histogram_revision_map
from scalar import Scalar
from logging.handlers import SysLogHandler

pool = None
app = Flask(__name__)
app.config.from_object('config')

CORS(app, resources=r'/*', allow_headers='Content-Type')
cache = Cache(app, config={'CACHE_TYPE': app.config["CACHETYPE"]})
sslify = SSLify(app, skips=['status'])

patch_all()
patch_psycopg()
cache.clear()

### Papertrail Logging Config ###
logger = logging.getLogger('RequestLogger')
logger.setLevel(logging.INFO)

syslog = SysLogHandler(address=('logs5.papertrailapp.com', 47698))
formatter = logging.Formatter('%(asctime)s -- %(message)s')

syslog.setFormatter(formatter)
logger.addHandler(syslog)


def cache_request(f):
    @wraps(f)
    def decorated_request(*args, **kwargs):
        rv = cache.get(request.url)
        if rv is None:
            rv = f(*args, **kwargs)
            cache.set(request.url, rv, timeout=app.config["TIMEOUT"])
            return rv
        else:
            return rv
    return decorated_request


def create_pool():
    global pool
    if pool is None:
        pool = SimpleConnectionPool(app.config["MINCONN"], app.config["MAXCONN"], dsn=get_db_connection_string())
    return pool


def execute_query(query, params=tuple()):
    pool = create_pool()
    db = pool.getconn()

    try:
        cursor = db.cursor()
        cursor.execute(query, params)
        return cursor.fetchall()
    except:
        abort(404)
    finally:
        pool.putconn(db)


@app.before_request
def log_request():
    """Log format: Referrer URL, Referrer, IP Address, URL
    """
    ip_addr = request.access_route[0] or request.remote_addr 
    data  = (request.values.get('url', ''),
            request.values.get('Referer', ''),
            ip_addr,
            request.url)

    if ip_addr != '127.0.0.1':
        logger.info(','.join([d.replace(',', '\,') for d in data]))


@app.route('/status')
def status():
    return "OK"


@app.route('/aggregates_by/<prefix>/channels/')
@cache_request
def get_channels(prefix):
    channels = execute_query("select * from list_channels(%s)", (prefix, ))
    return Response(json.dumps([channel[0] for channel in channels]), mimetype="application/json")


@app.route('/aggregates_by/<prefix>/channels/<channel>/dates/')
@cache_request
def get_dates(prefix, channel):
    result = execute_query("select * from list_buildids(%s, %s)", (prefix, channel))
    pretty_result = map(lambda r: {"version": r[0], "date": r[1]}, result)
    return Response(json.dumps(pretty_result), mimetype="application/json")


def get_filter_options(channel, version, filters, filter):
    try:
        options = execute_query("select * from get_filter_options(%s, %s, %s)", (channel, version, filter))
        if not options or (len(options) == 1 and options[0][0] is None):
            return

        pretty_opts = []
        for option in options:
            option = option[0]
            if filter == "metric" and option.startswith(COUNT_HISTOGRAM_PREFIX):
                pretty_opts.append(option[len(COUNT_HISTOGRAM_PREFIX) + 1:])
            else:
                pretty_opts.append(option)

        if filter == "child":
            # In the db, child is true, false, and other things.
            # We want to have content = true and parent = false.
            pretty_opts = ["content" if x == "true"
                           else "parent" if x == "false"
                           else x for x in pretty_opts]

        filters[filter] = pretty_opts
    except:
        pass


@app.route('/filters/', methods=["GET"])
@cache_request
def get_filters_options():
    channel = request.args.get("channel", None)
    version = request.args.get("version", None)

    if not channel or not version:
        abort(404)

    filters = {}
    dimensions = ["metric", "application", "architecture", "os", "e10sEnabled", "child"]

    Parallel(n_jobs=len(dimensions), backend="threading")(delayed(get_filter_options)(channel, version, filters, f)
                                                            for f in dimensions)

    if not filters:
        abort(404)

    return Response(json.dumps(filters), mimetype="application/json")


def _get_description(channel, prefix, metric):
    if prefix != NUMERIC_SCALARS_PREFIX:
        return ''

    metric = metric.replace(prefix + '_', '').lower()
    return Scalar(metric, 0, channel=channel).get_definition().get('description', '')


@app.route('/aggregates_by/<prefix>/channels/<channel>/', methods=["GET"])
@cache_request
def get_dates_metrics(prefix, channel):
    mapping = {"true": True, "false": False}
    dimensions = {k: mapping.get(v, v) for k, v in request.args.iteritems()}

    # Get dates
    dates = dimensions.pop('dates', "").split(',')
    version = dimensions.pop('version', None)
    metric = dimensions.get('metric', None)

    if not dates or not version or not metric:
        abort(404)

    if metric == "SEARCH_COUNTS":
        abort(404)

    # Get bucket labels
    for _prefix, _labels in SCALAR_MEASURE_MAP.iteritems():
        if metric.startswith(_prefix) and _prefix != COUNT_HISTOGRAM_PREFIX:
            labels = _labels
            kind = "exponential"
            description = _get_description(channel, _prefix, metric) 
            break
    else:
        revision = histogram_revision_map.get(channel, "nightly")  # Use nightly revision if the channel is unknown
        try:
            definition = Histogram(metric, {"values": {}}, revision=revision)
        except KeyError:
            # Couldn't find the histogram definition
            abort(404)

        kind = definition.kind
        description = definition.definition.description()

        if kind == "count":
            labels = COUNT_HISTOGRAM_LABELS
            dimensions["metric"] = "{}_{}".format(COUNT_HISTOGRAM_PREFIX, metric)
        elif kind == "flag":
            labels = [0, 1]
        else:
            labels = definition.get_value().keys().tolist()

    # Fetch metrics
    result = execute_query("select * from batched_get_metric(%s, %s, %s, %s, %s)", (prefix, channel, version, dates, json.dumps(dimensions)))
    if not result:
        abort(404)

    pretty_result = {"data": [], "buckets": labels, "kind": kind, "description": description}
    for row in result:
        date = row[0]
        label = row[1]
        histogram = row[2][:-2]
        sum = row[2][-2]
        count = row[2][-1]
        pretty_result["data"].append({"date": date, "label": label, "histogram": histogram, "count": count, "sum": sum})

    return Response(json.dumps(pretty_result), mimetype="application/json")


if __name__ == "__main__":
    app.run("0.0.0.0", debug=True, threaded=True)
