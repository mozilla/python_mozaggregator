import os
import time
from copy import deepcopy
from functools import wraps
from urllib.parse import urlencode
from urllib.request import urlopen
from expiringdict import ExpiringDict

import re
import ujson as json
from dockerflow.flask import Dockerflow
from flask import Flask, Response, abort, jsonify, request, _request_ctx_stack
from flask_cache import Cache
from flask_cors import CORS
from flask_sslify import SSLify
from gevent.monkey import patch_all
from joblib import Parallel, delayed
from moztelemetry.histogram import Histogram
from moztelemetry.scalar import MissingScalarError, Scalar
from psycogreen.gevent import patch_psycopg
from psycopg2.pool import SimpleConnectionPool
from werkzeug.exceptions import MethodNotAllowed
from jose import jwt
from jose.jwt import JWTError

from .aggregator import (
    COUNT_HISTOGRAM_LABELS, COUNT_HISTOGRAM_PREFIX, NUMERIC_SCALARS_PREFIX, SCALAR_MEASURE_MAP)
from .db import get_db_connection_string, histogram_revision_map, _preparedb

pool = None
db_connection_string = get_db_connection_string(read_only=True)
app = Flask(__name__)
dockerflow = Dockerflow(app, version_path='/app')

app.config.from_pyfile('config.py')

CORS(app, resources=r'/*', allow_headers=['Authorization', 'Content-Type'])
cache = Cache(app, config={'CACHE_TYPE': app.config["CACHETYPE"]})
sslify = SSLify(app, permanent=True, skips=['__version__', '__heartbeat__', '__lbheartbeat__', 'status'])

patch_all()
patch_psycopg()
cache.clear()

# For caching - change this if after backfilling submission_date data
SUBMISSION_DATE_ETAG = 'submission_date_v1'
CLIENT_CACHE_SLACK_SECONDS = 3600

# If we get a query string not in this set we throw a 405.
ALLOWED_DIMENSIONS = ('application', 'architecture', 'child', 'dates', 'label',
                      'metric', 'os', 'osVersion', 'version')

# Disallowed metrics for serving - matches regex
METRICS_BLACKLIST_RE = [re.compile(x) for x in [
        r"SEARCH_COUNTS",
        r"SCALARS_BROWSER\.SEARCH\..+",
    ]]

NON_AUTH_METRICS_BLACKLIST = [
    "SCALARS_TELEMETRY.EVENT_COUNTS",
    "SCALARS_TELEMETRY.DYNAMIC_EVENT_COUNTS",
]

# Allowed public release metrics
RELEASE_CHANNEL = "release"
ALLOW_ALL_RELEASE_METRICS = os.environ.get("ALLOW_ALL_RELEASE_METRICS", "False") == "True"
PUBLIC_RELEASE_METRICS = {"SCALARS_TELEMETRY.TEST.KEYED_UNSIGNED_INT"}

# Auth0 Integration
AUTH0_DOMAIN = "auth.mozilla.auth0.com"
AUTH0_API_AUDIENCE = "https://aggregates.telemetry.mozilla.org/"
AUTH0_ALGORITHMS = ["RS256"]
AUTH0_REQUIRED_SCOPE = "read:aggregates"
auth0_cache = ExpiringDict(max_len=1000, max_age_seconds=15 * 60)

# CSP Headers
DEFAULT_CSP_POLICY = "frame-ancestors 'none'; default-src 'self'"
DEFAULT_X_FRAME_POLICY = "DENY"


# Error handler
class AuthError(Exception):
    def __init__(self, error, status_code):
        self.error = error
        self.status_code = status_code


@app.errorhandler(AuthError)
def handle_auth_error(ex):
    response = jsonify(ex.error)
    response.status_code = ex.status_code
    return response


def get_token_auth_header():
    """Obtains the Access Token from the Authorization Header
    """
    auth = request.headers.get("Authorization", None)
    if not auth:
        raise AuthError({"code": "authorization_header_missing",
                        "description":
                            "Authorization header is expected"}, 403)

    parts = auth.split()

    if parts[0] != "Bearer":
        raise AuthError({"code": "invalid_header",
                        "description":
                            "Authorization header must start with"
                            " Bearer"}, 403)
    elif len(parts) == 1:
        raise AuthError({"code": "invalid_header",
                        "description": "Token not found"}, 403)
    elif len(parts) > 2:
        raise AuthError({"code": "invalid_header",
                        "description":
                            "Authorization header must be"
                            " Bearer token"}, 403)

    token = parts[1]
    return token


def check_auth():
    """Determines if the Access Token is valid
    """
    domain_base = "https://" + AUTH0_DOMAIN + "/"
    token = get_token_auth_header()

    if token in auth0_cache:
        return auth0_cache[token]

    # check token validity
    jsonurl = urlopen(domain_base + ".well-known/jwks.json")
    jwks = json.loads(jsonurl.read())

    try:
        unverified_header = jwt.get_unverified_header(token)
    except JWTError:
        raise AuthError({"code": "improper_token",
                        "description": "Token cannot be validated"}, 403)

    rsa_key = {}
    for key in jwks["keys"]:
        if key["kid"] == unverified_header["kid"]:
            rsa_key = {
                "kty": key["kty"],
                "kid": key["kid"],
                "use": key["use"],
                "n": key["n"],
                "e": key["e"]
            }
            break
    else:
        raise AuthError({"code": "invalid_header",
                         "description": "Unable to find appropriate key"}, 403)

    try:
        payload = jwt.decode(
            token,
            rsa_key,
            algorithms=AUTH0_ALGORITHMS,
            audience=AUTH0_API_AUDIENCE,
            issuer=domain_base
        )
    except jwt.ExpiredSignatureError:
        raise AuthError({"code": "token_expired",
                        "description": "Token is expired"}, 403)
    except jwt.JWTClaimsError:
        raise AuthError({"code": "invalid_claims",
                        "description":
                            "Incorrect claims,"
                            "please check the audience and issuer"}, 403)
    except Exception:
        raise AuthError({"code": "invalid_header",
                        "description":
                            "Unable to parse authentication"
                            " token."}, 403)

    # check scope
    unverified_claims = jwt.get_unverified_claims(token)
    if unverified_claims.get("scope"):
        token_scopes = unverified_claims["scope"].split()
        for token_scope in token_scopes:
            if token_scope == AUTH0_REQUIRED_SCOPE:
                _request_ctx_stack.top.current_user = payload
                auth0_cache[token] = True
                return True

    raise AuthError({"code": "access_denied",
                     "description": "Access not allowed"}, 403)


def is_authed():
    try:
        return check_auth()
    except AuthError:
        return False


def get_time_left_in_cache():
    assert app.config["CACHETYPE"] == "simple", "Only simple caches can be used with get_time_left_in_cache"

    # our cache (a flask cache), contains a cache (werkzeug SimpleCache), which contains a _cache (dict)
    # see https://github.com/pallets/werkzeug/blob/master/werkzeug/contrib/cache.py#L307
    expires_ts, _ = cache.cache._cache.get((request.url, False), (0, ""))

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
        authed = is_authed()
        cache_key = (request.url, authed)
        rv = cache.get(cache_key)

        if rv is None:
            rv = f(*args, **kwargs)
            cache.set(cache_key, rv, timeout=app.config["TIMEOUT"])
            return rv
        else:
            return rv
    return decorated_request


def create_pool():
    global pool
    if pool is None:
        _preparedb()
        pool = SimpleConnectionPool(
            app.config["MINCONN"],
            app.config["MAXCONN"],
            dsn=db_connection_string)
    return pool


def execute_query(query, params=tuple()):
    pool = create_pool()
    db = pool.getconn()

    try:
        cursor = db.cursor()
        cursor.execute(query, params)
        return cursor.fetchall()
    except:  # noqa
        abort(404)
    finally:
        pool.putconn(db)


@app.after_request
def apply_headers(response):
    response.headers["X-Frame-Options"] = DEFAULT_X_FRAME_POLICY
    response.headers["Content-Security-Policy"] = DEFAULT_CSP_POLICY
    response.headers["X-Content-Security-Policy"] = DEFAULT_CSP_POLICY
    return response


@app.route('/status')
def status():
    return "OK"


@app.route('/authed')
def authed():
    check_auth()
    return "Authenticated"


@app.route('/clear-cache')
def clear_cache():
    check_auth()
    cache.clear()
    return "Cache Cleared"


@app.route('/aggregates_by/<prefix>/channels/')
@add_cache_header()
@cache_request
def get_channels(prefix):
    channels = execute_query("select * from list_channels(%s)", (prefix, ))
    channels = [channel[0] for channel in channels]

    if not is_authed():
        channels = [c for c in channels if c != RELEASE_CHANNEL]

    return Response(json.dumps(channels), mimetype="application/json")


@app.route('/aggregates_by/<prefix>/channels/<channel>/dates/')
@add_cache_header()
@cache_request
def get_dates(prefix, channel):
    if channel == RELEASE_CHANNEL:
        check_auth()
    result = execute_query("select * from list_buildids(%s, %s)", (prefix, channel))
    pretty_result = [{"version": r[0], "date": r[1]} for r in result]
    return Response(json.dumps(pretty_result), mimetype="application/json")


def matches_blacklist(string):
    return any((regex.match(string) for regex in METRICS_BLACKLIST_RE))


def get_filter_options(authed, channel, version, filters, filter):
    try:
        options = execute_query("select * from get_filter_options(%s, %s, %s)", (channel, version, filter))
        if not options or (len(options) == 1 and options[0][0] is None):
            return

        pretty_opts = []
        for option in options:
            option = option[0]
            if filter == "metric":
                if option.startswith(COUNT_HISTOGRAM_PREFIX):
                    option = option[len(COUNT_HISTOGRAM_PREFIX) + 1:]
                if option in NON_AUTH_METRICS_BLACKLIST and authed:
                    pretty_opts.append(option)
                elif not matches_blacklist(option) and option not in NON_AUTH_METRICS_BLACKLIST:
                    pretty_opts.append(option)
            else:
                pretty_opts.append(option)

        if filter == "child":
            # In the db, child is true, false, and other things.
            # We want to have content = true and parent = false.
            pretty_opts = ["content" if x == "true"
                           else "parent" if x == "false"
                           else x for x in pretty_opts]

        filters[filter] = pretty_opts
    except:  # noqa
        pass


@app.route('/filters/', methods=['GET'])
@add_cache_header()
@cache_request
def get_filters_options():
    channel = request.args.get("channel")
    version = request.args.get("version")

    if not channel or not version:
        abort(404)

    if channel == RELEASE_CHANNEL:
        authed = check_auth()
    else:
        authed = is_authed()

    filters = {}
    dimensions = ["metric", "application", "architecture", "os", "child"]

    Parallel(n_jobs=len(dimensions), backend="threading")(
        delayed(get_filter_options)(authed, channel, version, filters, f)
        for f in dimensions
    )

    if not filters:
        abort(404)

    return Response(json.dumps(filters), mimetype="application/json")


def _get_description(channel, prefix, metric):
    if prefix != NUMERIC_SCALARS_PREFIX:
        return ''

    metric = metric.replace(prefix + '_', '').lower()
    return Scalar(metric, 0, channel=channel).definition.description


def _allow_metric(channel, metric):
    if matches_blacklist(metric):
        return False
    elif channel == RELEASE_CHANNEL:
        if ALLOW_ALL_RELEASE_METRICS:
            return True
        elif metric in PUBLIC_RELEASE_METRICS:
            return True
        else:
            return check_auth()
    elif channel != RELEASE_CHANNEL:
        if metric in NON_AUTH_METRICS_BLACKLIST:
            return check_auth()
        else:
            return True


@app.route('/aggregates_by/<prefix>/channels/<channel>/', methods=['GET'])
@add_cache_header(True)
@check_etag
@cache_request
def get_dates_metrics(prefix, channel):
    mapping = {"true": True, "false": False}
    dimensions = {k: mapping.get(v, v) for k, v in request.args.items()}

    extra_dimensions = dimensions.keys() - ALLOWED_DIMENSIONS
    if extra_dimensions:
        # We received an unsupported query string to filter by, return 405.
        valid_url = '{}?{}'.format(
            request.path,
            urlencode({k: v for k, v in list(dimensions.items()) if k in ALLOWED_DIMENSIONS}))
        raise MethodNotAllowed(valid_methods=[valid_url])

    if 'child' in dimensions:
        # Process types in the db are true/false, not content/process
        new_process_map = {"content": True, "parent": False}
        dimensions['child'] = new_process_map.get(dimensions['child'], dimensions['child'])

    # Get dates
    dates = dimensions.pop('dates', '').split(',')
    version = dimensions.pop('version', None)
    metric = dimensions.get('metric')

    if not dates or not version or not metric:
        abort(404, description="Missing date or version or metric. All three are required.")

    if not _allow_metric(channel, metric):
        abort(404, description="This metric is not allowed to be served.")

    # Get bucket labels
    for _prefix, _labels in SCALAR_MEASURE_MAP.items():
        if metric.startswith(_prefix) and _prefix != COUNT_HISTOGRAM_PREFIX:
            labels = _labels
            kind = "exponential"
            try:
                description = _get_description(channel, _prefix, metric)
            except MissingScalarError:
                abort(404, description="Cannot find this scalar definition.")
            break
    else:
        revision = histogram_revision_map[channel]
        try:
            definition = Histogram(metric, {"values": {}}, revision=revision)
        except KeyError:
            # Couldn't find the histogram definition
            abort(404, description="Cannot find this histogram definition.")

        kind = definition.kind
        description = definition.definition.description()

        if kind == "count":
            labels = COUNT_HISTOGRAM_LABELS
            dimensions["metric"] = "{}_{}".format(COUNT_HISTOGRAM_PREFIX, metric)
        elif kind == "flag":
            labels = [0, 1]
        else:
            labels = list(definition.get_value().keys()).tolist()

    altered_dimensions = deepcopy(dimensions)
    if 'child' in dimensions:
        # Bug 1339139 - when adding gpu processes, child process went from True/False to "true"/"false"/"gpu"
        reverse_map = {True: 'true', False: 'false'}
        altered_dimensions['child'] = reverse_map.get(altered_dimensions['child'], altered_dimensions['child'])

    # Fetch metrics
    if metric.startswith("USE_COUNTER2_"):
        # Bug 1412382 - Use Counters need to be composed from reported True
        # values and False values supplied by *CONTENT_DOCUMENTS_DESTROYED.
        denominator = "TOP_LEVEL_CONTENT_DOCUMENTS_DESTROYED"
        if metric.endswith("_DOCUMENT"):
            denominator = "CONTENT_DOCUMENTS_DESTROYED"
        denominator = "{}_{}".format(COUNT_HISTOGRAM_PREFIX, denominator)
        denominator_dimensions = deepcopy(dimensions)
        denominator_dimensions["metric"] = denominator
        denominator_new_dimensions = deepcopy(altered_dimensions)
        denominator_new_dimensions["metric"] = denominator
        result = execute_query(
            "select * from batched_get_use_counter(%s, %s, %s, %s, %s, %s, %s, %s)", (
                prefix, channel, version, dates, json.dumps(denominator_dimensions),
                json.dumps(denominator_new_dimensions), json.dumps(dimensions), json.dumps(altered_dimensions)))
    else:
        result = execute_query(
            "select * from batched_get_metric(%s, %s, %s, %s, %s, %s)", (
                prefix, channel, version, dates, json.dumps(dimensions), json.dumps(altered_dimensions)))

    if not result:
        abort(404, description="No data found for this metric.")

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
