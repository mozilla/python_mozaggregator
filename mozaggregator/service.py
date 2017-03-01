import ujson as json
import config
import logging
import boto3
import time

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
from botocore.exceptions import ClientError
from copy import deepcopy

pool = None
app = Flask(__name__)
app.config.from_object('mozaggregator.config')

CORS(app, resources=r'/*', allow_headers='Content-Type')
cache = Cache(app, config={'CACHE_TYPE': app.config["CACHETYPE"]})
sslify = SSLify(app, skips=['status'])

patch_all()
patch_psycopg()
cache.clear()

### Cloudwatch Logging Config ###
MAX_RETRIES = 5
LOG_GROUP_NAME = 'telemetry-aggregation-service'
LOG_STREAM_NAME = 'requests'

sequence_token = None
log_client = boto3.client("logs", region_name='us-west-2')

### For caching - change this if after backfilling submission_date data
SUBMISSION_DATE_ETAG = "submission_date_v1"
CLIENT_CACHE_SLACK_SECONDS = 3600


def get_time_left_in_cache():
    assert app.config["CACHETYPE"] == "simple", "Only simple caches can be used with get_time_left_in_cache"

    # our cache (a flask cache), contains a cache (werkzeug SimpleCache), which contains a _cache (dict)
    # see https://github.com/pallets/werkzeug/blob/master/werkzeug/contrib/cache.py#L307
    expires_ts, _ = cache.cache._cache.get(request.url, (0, ""))

    # get seconds until expiry
    expires = int(expires_ts - time.time())

    if expires <= 0:
        return 0

    # add some slack
    expires += CLIENT_CACHE_SLACK_SECONDS
    return expires


def add_cache_header(add_etag=False):
    def decorator_func(f):
        @wraps(f)
        def decorated_request(*args, **kwargs):
            response = f(*args, **kwargs)

            prefix = kwargs.get('prefix')
            if prefix == 'submission_date' and add_etag:
                response.cache_control.max_age = app.config["TIMEOUT"]
                response.set_etag(SUBMISSION_DATE_ETAG)
            else:
                response.cache_control.max_age = get_time_left_in_cache()

            return response
        return decorated_request
    return decorator_func


def check_etag(f):
    @wraps(f)
    def decorated_request(*args, **kwargs):
        etag = request.headers.get('If-None-Match')
        prefix = kwargs.get('prefix')
        if prefix == 'submission_date' and etag == SUBMISSION_DATE_ETAG:
            return Response(status=304)
        return f(*args, **kwargs)
    return decorated_request


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
    """Log format: Origin, Referrer, IP Address, URL
    """
    global sequence_token, LOG_GROUP_NAME, LOG_STREAM_NAME

    origin  = request.headers.get('Origin') or ''
    referrer = request.headers.get('Referer') or request.referrer or ''
    ip_addr = request.access_route[0] or request.remote_addr or ''
    data  = (origin,
            referrer,
            ip_addr,
            request.url)

    millis = int(round(time.time() * 1000))
    log_line = ','.join([d.replace(',', '\,') for d in data])

    if ip_addr != '127.0.0.1':
        # See https://github.com/kislyuk/watchtower for inspiration

        kwargs = {
            'logGroupName': LOG_GROUP_NAME,
            'logStreamName': LOG_STREAM_NAME,
            'logEvents': [
                {
                    'timestamp': millis,
                    'message': log_line
                }
            ]
        }

        for retry in xrange(MAX_RETRIES + 1):
            if sequence_token is not None:
                kwargs['sequenceToken'] = sequence_token
            try:
                response = log_client.put_log_events(**kwargs)
                break
            except ClientError as e:
                if e.response.get("Error", {}).get("Code") in \
                   ("DataAlreadyAcceptedException", "InvalidSequenceTokenException"):
                    stream_describe = log_client.describe_log_streams(
                        logGroupName = LOG_GROUP_NAME,
                        logStreamNamePrefix = LOG_STREAM_NAME)
                    sequence_token = stream_describe['logStreams'][0]['uploadSequenceToken']
                else:
                    raise 

        if response and response.get('nextSequenceToken'):
            sequence_token = response['nextSequenceToken']

@app.route('/status')
def status():
    return "OK"


@app.route('/aggregates_by/<prefix>/channels/')
@add_cache_header()
@cache_request
def get_channels(prefix):
    channels = execute_query("select * from list_channels(%s)", (prefix, ))
    return Response(json.dumps([channel[0] for channel in channels]), mimetype="application/json")


@app.route('/aggregates_by/<prefix>/channels/<channel>/dates/')
@add_cache_header()
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
@add_cache_header()
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
@add_cache_header(True)
@check_etag
@cache_request
def get_dates_metrics(prefix, channel):
    mapping = {"true": True, "false": False}
    dimensions = {k: mapping.get(v, v) for k, v in request.args.iteritems()}

    if 'child' in dimensions:
        # Process types in the db are true/false, not content/process
        new_process_map = {"content": True, "parent": False}
        dimensions['child'] = new_process_map.get(dimensions['child'], dimensions['child'])

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

    altered_dimensions = deepcopy(dimensions)
    if 'child' in dimensions:
        # Bug 1339139 - when adding gpu processes, child process went from True/False to "true"/"false"/"gpu"
        reverse_map = {True: 'true', False: 'false'}
        altered_dimensions['child'] = reverse_map.get(altered_dimensions['child'], altered_dimensions['child'])

    # Fetch metrics
    result = execute_query("select * from batched_get_metric(%s, %s, %s, %s, %s, %s)", (prefix, channel, version, dates, json.dumps(dimensions), json.dumps(altered_dimensions)))

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
