import ujson as json
import config
import logging
import requests
import yaml

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
from aggregator import simple_measures_labels, count_histogram_labels, numeric_scalars_labels, \
                        simple_measures_prefix, count_histogram_prefix, numeric_scalars_prefix, \
                        scalar_measure_map
from db import get_db_connection_string, histogram_revision_map, scalar_revision_map
from expiringdict import ExpiringDict
from logging.handlers import SysLogHandler

definition_cache = ExpiringDict(max_len=2**10, max_age_seconds=3600)

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
            if filter == "metric" and option.startswith(count_histogram_prefix):
                pretty_opts.append(option[len(count_histogram_prefix) + 1:])
            else:
                pretty_opts.append(option)

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

def _yaml_unnest(defs, prefix=''):
    """The yaml definition file is nested - this functions unnests it.
    # example
    >>> test = {'browser.nav': {'clicks': {'description': 'a description', 'expires': 'never'}}}
    >>> yaml_unnest(test)
    # prints {'browser.nav.clicks': {'description': 'a description', 'expires': 'never'}}
    """
    stop = lambda x: type(x) is not dict or any((key in x for key in ['description', 'expires', 'kind']))
    new_defs, found = {}, list(defs.iteritems())

    while found:
        key, value = found.pop()
        if stop(value):
            new_defs[key] = value
        else:
            found += [('{}.{}'.format(key, k), v) for k, v in value.iteritems()]

    return new_defs

def _get_description(channel, prefix, metric):
    if prefix != numeric_scalars_prefix:
        return ''

    metric = metric.replace(prefix + '_', '').lower()
    revision = scalar_revision_map.get(channel, 'nightly')

    if revision not in definition_cache:
        content = requests.get(revision).content
        definitions = _yaml_unnest(yaml.load(content))
        definition_cache[revision] = json.dumps(definitions)
    else:
        definitions = json.loads(definition_cache[revision])

    return definitions.get(metric, {}).get('description', '').strip()


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
    for _prefix, _labels in scalar_measure_map.iteritems():
        if metric.startswith(_prefix) and _prefix != count_histogram_prefix:
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
            labels = count_histogram_labels
            dimensions["metric"] = "{}_{}".format(count_histogram_prefix, metric)
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
