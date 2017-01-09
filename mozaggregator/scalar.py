import requests
import yaml
import json

from expiringdict import ExpiringDict

SCALARS_YAML_PATH = '/toolkit/components/telemetry/Scalars.yaml'
REVISIONS = {'nightly': 'https://hg.mozilla.org/mozilla-central/rev/tip',
             'aurora': 'https://hg.mozilla.org/releases/mozilla-aurora/rev/tip',
             'beta': 'https://hg.mozilla.org/releases/mozilla-beta/rev/tip',
             'release': 'https://hg.mozilla.org/releases/mozilla-release/rev/tip'}

definition_cache = ExpiringDict(max_len=2**10, max_age_seconds=3600)


def _yaml_unnest(defs, prefix=''):
    """The yaml definition file is nested - this functions unnests it.
    # example
    >>> test = {'browser.nav': {'clicks': {'description': 'a description', 'expires': 'never'}}}
    >>> yaml_unnest(test)
    # prints {'browser.nav.clicks': {'description': 'a description', 'expires': 'never'}}
    """
    stop = lambda x: type(x) is not dict or x.viewkeys() & Scalar.REQUIRED_FIELDS
    new_defs, found = {}, list(defs.iteritems())

    while found:
        key, value = found.pop()
        if stop(value):
            new_defs[key] = value
        else:
            found += [('{}.{}'.format(key, k), v) for k, v in value.iteritems()]

    return new_defs


def _get_scalar_definition(url, metric, additional_scalars=None):
    if url not in definition_cache:
        content = requests.get(url).content
        definitions = _yaml_unnest(yaml.load(content))
        definition_cache[url] = json.dumps(definitions)
    else:
        definitions = json.loads(definition_cache[url])

    if additional_scalars:
        definitions.update(_yaml_unnest(additional_scalars))

    return definitions.get(metric, {})


class Scalar:
    """A class representing a scalar"""

    REQUIRED_FIELDS = {'bug_numbers', 'description', 'expires', 'kind',
                       'notification_emails'}

    OPTIONAL_FIELDS = {'cpp_guard', 'release_channel_collection', 'keyed'}

    def __init__(self, name, value, channel=None, revision=None,
                 scalars_url=None, additional_scalars=None):
        """
        Initialize a scalar from it's name.

        :param channel: The channel for which the scalar is defined. One of
                        nightly, aurora, beta, release
        :param revision: The url of the revision to use for the scalar definition
        :param scalars_url: The url of the scalars.yml file to use for the 
                            scalar definitions
        :param additional_scalars: A dictionary describing additional scalars, 
                                   must be of the same form as scalars.yml
        """
        picked = sum([bool(channel), bool(revision), bool(scalars_url)])

        if picked == 0:
            channel = 'nightly'
        elif picked > 1:
            raise ValueError('Can only use one of (channel, revision, scalars_url)')

        if channel:
            revision = REVISIONS[channel] # raises error on improper channel
        if revision:
            scalars_url = revision.replace('rev', 'raw-file') + SCALARS_YAML_PATH

        json_definition = _get_scalar_definition(scalars_url, name, additional_scalars)

        missing_fields = Scalar.REQUIRED_FIELDS - json_definition.viewkeys()
        assert not missing_fields, \
                "Definition is missing required fields {}".format(','.join(missing_fields))

        self.name = name
        self.definition = json_definition
        self.value = value
        self.scalars_url = scalars_url

    def get_name(self):
        return self.name

    def get_value(self):
        return self.value

    def get_definition(self):
        return self.definition

    def get(self, arg, default=None):
        if arg == 'name':
            return self.get_name()
        if arg == 'value':
            return self.get_value()
        else:
            return self.definition.get(arg, default)

    def __str__(self):
        return str(self.get_value())

    def __add__(self, other):
        return Scalar(self.name, self.value + other.value, scalars_url=self.scalars_url)
