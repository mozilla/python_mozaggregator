import json
import re
import unittest

import pandas as pd
import pyspark
from dataset import (
    COUNT_SCALAR_BUCKET, NUM_AGGREGATED_CHILD_PINGS, NUM_CHILDREN_PER_PING, NUM_PINGS_PER_DIMENSIONS,
    NUM_PROCESS_TYPES, NUMERIC_SCALAR_BUCKET, SCALAR_VALUE, SIMPLE_SCALAR_BUCKET, generate_pings,
    histograms_template, keyed_histograms_template, keyed_scalars_template, ping_dimensions,
    scalars_template, simple_measurements_template)
from mozaggregator import config
from mozaggregator.aggregator import (
    COUNT_HISTOGRAM_LABELS, NUMERIC_SCALARS_LABELS, NUMERIC_SCALARS_PREFIX, SIMPLE_MEASURES_LABELS,
    SIMPLE_MEASURES_PREFIX, _aggregate_metrics)
from mozaggregator.db import submit_aggregates
from mozaggregator.service import (
    CLIENT_CACHE_SLACK_SECONDS, SUBMISSION_DATE_ETAG, app)
from moztelemetry.histogram import Histogram


class ServiceTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up the database once for the test run."""
        cls.sc = pyspark.SparkContext(master="local[*]")
        raw_pings = list(generate_pings())
        aggregates = _aggregate_metrics(cls.sc.parallelize(raw_pings), num_reducers=10)
        submit_aggregates(aggregates)

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()

    def setUp(self):
        self.app = app.test_client()
        self.app.application.config['TESTING'] = True
        self.app.application.config['DEBUG'] = True

    def as_json(self, resp):
        self.assertEqual(resp.status_code, 200, (
            'Response status code != 200, got %s. Cannot get JSON response.' % resp.status_code
        ))
        return json.loads(resp.data)

    # Test HTTP things.

    def test_status(self):
        resp = self.app.get('/status')
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.data, 'OK')

    def test_submission_dates_cache_control(self):
        resp = self.app.get(
            '/aggregates_by/submission_date/channels/nightly/?version=41&dates=20150603&metric=GC_MAX_PAUSE_MS_2')
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.headers.get('Cache-Control'), 'max-age=%s' % config.TIMEOUT)

    def test_submission_dates_etag(self):
        resp = self.app.get(
            '/aggregates_by/submission_date/channels/nightly/?version=41&dates=20150603&metric=GC_MAX_PAUSE_MS_2')
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.headers.get('etag').strip('"'), SUBMISSION_DATE_ETAG)

    def test_submission_dates_etag_header(self):
        resp = self.app.get(
            '/aggregates_by/submission_date/channels/nightly/?version=41&dates=20150603&metric=GC_MAX_PAUSE_MS_2',
            headers={'If-None-Match': SUBMISSION_DATE_ETAG})
        self.assertEqual(resp.status_code, 304, ('Expected, 304. Got %s' % resp.status_code))

    def test_submission_dates_etag_header_wrong(self):
        resp = self.app.get(
            '/aggregates_by/submission_date/channels/nightly/?version=41&dates=20150603&metric=GC_MAX_PAUSE_MS_2',
            headers={'If-None-Match': SUBMISSION_DATE_ETAG + '_'})
        self.assertEqual(resp.status_code, 200, ('Expected, 200. Got %s' % resp.status_code))

    def test_build_id_cache_control(self):
        resp = self.app.get(
            '/aggregates_by/build_id/channels/nightly/?version=41&dates=20150601&metric=GC_MAX_PAUSE_MS_2')
        self.assertEqual(resp.status_code, 200)

        matches = re.match(r'max-age=(\d+)', resp.headers.get('Cache-Control'))
        self.assertTrue(matches is not None, 'Cache Control response not set, but should be')
        self.assertTrue(int(matches.group(1)) > 0,
                        'max-age expected greater than 0, was %s' % matches.group(1))
        self.assertTrue(int(matches.group(1)) < config.TIMEOUT + CLIENT_CACHE_SLACK_SECONDS,
                        'max-age expected less than %s, was %s' % (
                            config.TIMEOUT + CLIENT_CACHE_SLACK_SECONDS, matches.group(1)))

    def test_build_id_dates_no_etag(self):
        resp = self.app.get(
            '/aggregates_by/build_id/channels/nightly/?version=41&dates=20150601&metric=GC_MAX_PAUSE_MS_2')
        self.assertTrue(resp.headers.get('etag') is None)

    def test_build_id_etag_header_ignored(self):
        # Etag should be ignored for build_id.
        resp = self.app.get(
            '/aggregates_by/build_id/channels/nightly/?version=41&dates=20150601&metric=GC_MAX_PAUSE_MS_2',
            headers={'If-None-Match': SUBMISSION_DATE_ETAG})
        self.assertEqual(resp.status_code, 200, ('Expected, 200. Got %s' % resp.status_code))

    def test_non_existent_scalars(self):
        # Non-existent scalars should 404.
        resp = self.app.get(
            '/aggregates_by/build_id/channels/nightly/?version=41&dates=20150601&metric=SCALARS_NONEXISTENT')
        self.assertEqual(resp.status_code, 404)

    # Test response content.

    def test_channels(self):
        resp = self.as_json(self.app.get('/aggregates_by/build_id/channels/'))
        self.assertEqual(set(resp), set(ping_dimensions['channel']))

        resp = self.as_json(self.app.get('/aggregates_by/submission_date/channels/'))
        self.assertEqual(set(resp), set(ping_dimensions['channel']))

    def test_build_ids(self):
        _versions = ping_dimensions['version']
        _build_ids = ping_dimensions['build_id']

        for channel in ping_dimensions["channel"]:
            resp = self.as_json(self.app.get('/aggregates_by/build_id/channels/%s/dates/' % channel))
            self.assertEqual(len(resp), len(_versions * len(_build_ids)))

            for build_id in resp:
                self.assertEqual(set(build_id.keys()), set(['date', 'version']))
                self.assertTrue(build_id['date'] in [x[:-6] for x in _build_ids])
                self.assertTrue(build_id['version'] in [x.split('.')[0] for x in _versions])

    def test_submission_dates(self):
        _versions = ping_dimensions["version"]
        _submission_dates = ping_dimensions["submission_date"]

        for channel in ping_dimensions["channel"]:
            resp = self.as_json(self.app.get('/aggregates_by/submission_date/channels/%s/dates/' % channel))
            self.assertEqual(len(resp), len(_versions) * len(_submission_dates))

            for submission_date in resp:
                self.assertEqual(set(submission_date.keys()), set(['date', 'version']))
                self.assertTrue(submission_date['date'] in _submission_dates)
                self.assertTrue(submission_date['version'] in [x.split('.')[0] for x in _versions])

    def test_filters(self):
        for channel in ping_dimensions['channel']:
            for version in [v.split('.')[0] for v in ping_dimensions['version']]:
                resp = self.as_json(self.app.get('/filters/?channel=%s&version=%s' % (channel, version)))

                # TODO: Test all filters.
                self.assertEqual(set(resp['application']), set(ping_dimensions['application']))
                self.assertEqual(set(resp['architecture']), set(ping_dimensions['arch']))
                self.assertEqual(set(resp['e10sEnabled']), set(['true', 'false']))
                self.assertEqual(set(resp['child']), set(['gpu', 'content', 'parent']))

    def test_changed_child_value(self):
        # See bug 1339139
        resp = self.as_json(self.app.get(
            '/aggregates_by/submission_date/channels/nightly/?version=41&dates=20150603&metric=GC_MAX_PAUSE_MS_2&child=true'))
        self.assertTrue(resp is not None)

    def test_using_content_types(self):
        resp = self.as_json(self.app.get(
            '/aggregates_by/submission_date/channels/nightly/?version=41&dates=20150603&metric=GC_MAX_PAUSE_MS_2&child=content'))
        self.assertTrue(resp is not None)

    def test_using_parent_types(self):
        resp = self.as_json(self.app.get(
            '/aggregates_by/submission_date/channels/nightly/?version=41&dates=20150603&metric=GC_MAX_PAUSE_MS_2&child=parent'))
        self.assertTrue(resp is not None)

    def test_using_gpu_types(self):
        resp = self.as_json(self.app.get(
            '/aggregates_by/submission_date/channels/nightly/?version=41&dates=20150603&metric=GC_MAX_PAUSE_MS_2&child=gpu'))
        self.assertTrue(resp is not None)

    def test_absent_use_counter(self):
        # A use counter that isn't in the aggregator should still get a response from the service.
        # This is a side-effect of bug 1412384
        channel = ping_dimensions['channel'][0]
        version = ping_dimensions['version'][0].split('.')[0]
        template_build_id = [ping_dimensions['build_id'][0][:-6]]
        metric = 'USE_COUNTER2_SIR_NOT_APPEARING_IN_THIS_DOCUMENT'
        value = {
            u'bucket_count': 3,
            u'histogram_type': 2,
            u'range': [1, 2],
            u'values': {u'0': 640, u'1': 0, u'2': 0},
            u'count': 0,
            u'sum': 0,
        }

        expected_count = 1
        for dimension, values in ping_dimensions.iteritems():
            if dimension not in ['channel', 'version', 'build_id']:
                expected_count *= len(values)

        histogram_expected_count = NUM_PINGS_PER_DIMENSIONS * expected_count

        self._histogram('build_id', channel, version, template_build_id,
                        metric, value, histogram_expected_count)

    def test_submission_dates_metrics(self):
        template_channel = ping_dimensions['channel']
        template_version = [x.split('.')[0] for x in ping_dimensions['version']]
        template_submission_date = ping_dimensions['submission_date']

        expected_count = 1
        for dimension, values in ping_dimensions.iteritems():
            if dimension not in ['channel', 'version', 'submission_date']:
                expected_count *= len(values)

        for channel in template_channel:
            for version in template_version:

                histogram_expected_count = NUM_PINGS_PER_DIMENSIONS * expected_count
                for metric, value in histograms_template.iteritems():
                    if value is None:
                        continue
                    self._histogram('submission_date', channel, version, template_submission_date,
                                    metric, value, histogram_expected_count)

                # Count = product(dimensions) * pings_per_dimensions
                # 1 Count for parent, then 1 Count for each NUM_CHILDREN_PER_PING
                simple_measure_expected_count = expected_count * NUM_PINGS_PER_DIMENSIONS * (NUM_CHILDREN_PER_PING + 1)

                for simple_measure, value in simple_measurements_template.iteritems():
                    if not isinstance(value, int):
                        continue

                    metric = '{}_{}'.format(SIMPLE_MEASURES_PREFIX, simple_measure.upper())
                    self._simple_measure('submission_date', channel, version, template_submission_date,
                                         metric, value, simple_measure_expected_count)

                # for gpu and content process, NUM_AGGREGATED_CHILD_PINGS * expected_count gets the expected number of counts
                # (we only add gpu and content scalars for aggregated child pings)
                # for parent processes, NUM_PINGS_PER_DIMENSIONS * expected_count
                numeric_scalar_expected_count = ((2 * NUM_AGGREGATED_CHILD_PINGS) + NUM_PINGS_PER_DIMENSIONS) * expected_count

                for scalar, value in scalars_template.iteritems():
                    if not isinstance(value, int):
                        continue
                    metric = '{}_{}'.format(NUMERIC_SCALARS_PREFIX, scalar.upper())
                    self._numeric_scalar('submission_date', channel, version, template_submission_date,
                                         metric, value, numeric_scalar_expected_count, NUMERIC_SCALAR_BUCKET,
                                         NUMERIC_SCALARS_LABELS, True)

                for metric, _dict in keyed_scalars_template.iteritems():
                    metric_name = '{}_{}'.format(NUMERIC_SCALARS_PREFIX, metric.upper())
                    self._keyed_numeric_scalar('submission_date', channel, version, template_submission_date,
                                               metric_name, _dict, numeric_scalar_expected_count)

                for metric, histograms in keyed_histograms_template.iteritems():
                    self._keyed_histogram('submission_date', channel, version, template_submission_date,
                                          metric, histograms, histogram_expected_count)

    def test_build_id_metrics(self):
        template_channel = ping_dimensions['channel']
        template_version = [x.split('.')[0] for x in ping_dimensions['version']]
        template_build_id = [x[:-6] for x in ping_dimensions['build_id']]

        expected_count = 1
        for dimension, values in ping_dimensions.iteritems():
            if dimension not in ['channel', 'version', 'build_id']:
                expected_count *= len(values)

        for channel in template_channel:
            for version in template_version:

                histogram_expected_count = NUM_PINGS_PER_DIMENSIONS * expected_count
                for metric, value in histograms_template.iteritems():
                    if value is None:
                        continue
                    self._histogram('build_id', channel, version, template_build_id, metric, value, histogram_expected_count)

                # Count = product(dimensions) * pings_per_dimensions
                # 1 Count for parent, then 1 Count for each NUM_CHILDREN_PER_PING
                simple_measure_expected_count = expected_count * NUM_PINGS_PER_DIMENSIONS * (NUM_CHILDREN_PER_PING + 1)

                for simple_measure, value in simple_measurements_template.iteritems():
                    if not isinstance(value, int):
                        continue

                    metric = '{}_{}'.format(SIMPLE_MEASURES_PREFIX, simple_measure.upper())
                    self._simple_measure('build_id', channel, version, template_build_id, metric, value, simple_measure_expected_count)

                # for gpu and content process, NUM_AGGREGATED_CHILD_PINGS * expected_count gets the expected number of counts
                # (we only add gpu and content scalars for aggregated child pings)
                # for parent processes, NUM_PINGS_PER_DIMENSIONS * expected_count
                numeric_scalar_expected_count = ((2 * NUM_AGGREGATED_CHILD_PINGS) + NUM_PINGS_PER_DIMENSIONS) * expected_count

                for scalar, value in scalars_template.iteritems():
                    if not isinstance(value, int):
                        continue

                    metric = '{}_{}'.format(NUMERIC_SCALARS_PREFIX, scalar.upper())
                    self._numeric_scalar('build_id', channel, version, template_build_id, metric,
                                         value, numeric_scalar_expected_count, NUMERIC_SCALAR_BUCKET,
                                         NUMERIC_SCALARS_LABELS, True)

                for metric, _dict in keyed_scalars_template.iteritems():
                    metric_name = '{}_{}'.format(NUMERIC_SCALARS_PREFIX, metric.upper())
                    self._keyed_numeric_scalar('build_id', channel, version, template_build_id,
                                               metric_name, _dict, numeric_scalar_expected_count)

                for metric, histograms in keyed_histograms_template.iteritems():
                    self._keyed_histogram('build_id', channel, version, template_build_id, metric,
                                          histograms, histogram_expected_count)

    # Helpers.

    def _histogram(self, prefix, channel, version, dates, metric, value, expected_count):
        if metric.endswith('CONTENT_DOCUMENTS_DESTROYED'):  # Ignore USE_COUNTER2_ support histograms
            return

        resp = self.as_json(self.app.get(
            '/aggregates_by/%s/channels/%s/?version=%s&dates=%s&metric=%s'
            % (prefix, channel, version, ','.join(dates), metric)))
        self.assertEqual(len(resp['data']), len(dates))

        bucket_index = COUNT_HISTOGRAM_LABELS.index(COUNT_SCALAR_BUCKET)

        for res in resp['data']:
            # From pings before bug 1218576 (old), `count` is the number of processes.
            # From pings after bug 1218576 (new), `count` is the number of process types.
            old_pings_expected_count = expected_count * (NUM_PINGS_PER_DIMENSIONS - NUM_AGGREGATED_CHILD_PINGS) / NUM_PINGS_PER_DIMENSIONS
            new_pings_expected_count = expected_count * NUM_AGGREGATED_CHILD_PINGS / NUM_PINGS_PER_DIMENSIONS
            self.assertEqual(
                res['count'],
                new_pings_expected_count * NUM_PROCESS_TYPES + old_pings_expected_count * (NUM_CHILDREN_PER_PING + 1))

            if value['histogram_type'] == 4:  # Count histogram
                current = pd.Series(res['histogram'], index=map(int, resp['buckets']))
                expected = pd.Series(index=COUNT_HISTOGRAM_LABELS, data=0)
                expected[COUNT_SCALAR_BUCKET] = res['count']

                self.assertEqual(res['histogram'][bucket_index], res['count'])
                self.assertEqual(res['sum'], value['sum'] * res['count'])
                self.assertTrue((current == expected).all())

            elif metric.startswith('USE_COUNTER2_'):
                if metric.endswith('_PAGE'):
                    destroyed = histograms_template['TOP_LEVEL_CONTENT_DOCUMENTS_DESTROYED']['sum']
                else:
                    destroyed = histograms_template['CONTENT_DOCUMENTS_DESTROYED']['sum']
                value['values']['0'] = destroyed - value['values']['1']

                current = pd.Series(res['histogram'], index=map(int, resp['buckets']))
                expected = Histogram(metric, value).get_value() * res['count']

                self.assertTrue((current == expected).all())
                self.assertEqual(res['sum'], value['sum'] * res['count'])

            else:
                ind_type = int if value['histogram_type'] != 5 else str  # Categorical histograms
                current = pd.Series(res['histogram'], index=map(ind_type, resp['buckets']))
                expected = Histogram(metric, value).get_value() * res['count']

                self.assertTrue((current == expected).all())
                self.assertEqual(res['sum'], value['sum'] * res['count'])

    def _simple_measure(self, prefix, channel, version, dates, metric, value, expected_count):
        self._numeric_scalar(prefix, channel, version, dates, metric, value, expected_count,
                             SIMPLE_SCALAR_BUCKET, SIMPLE_MEASURES_LABELS, False)

    def _numeric_scalar(self, prefix, channel, version, dates, metric, value, expected_count,
                        bucket, labels, has_def):
        resp = self.as_json(self.app.get(
            '/aggregates_by/%s/channels/%s/?version=%s&dates=%s&metric=%s'
            % (prefix, channel, version, ','.join(dates), metric)
        ))
        self.assertEqual(len(resp['data']), len(dates))
        self.assertTrue(not has_def or resp['description'] != '')

        bucket_index = labels.index(bucket)

        for res in resp['data']:
            self.assertEqual(res['count'], expected_count)

            current = pd.Series(res['histogram'], index=map(int, resp['buckets']))
            expected = pd.Series(index=labels, data=0)
            expected[bucket] = res['count']

            self.assertEqual(res['histogram'][bucket_index], res['count'])
            self.assertEqual(res['sum'], value * res['count'])
            self.assertTrue((current == expected).all())

    def _keyed_numeric_scalar(self, prefix, channel, version, dates, metric, histograms, expected_count):
        resp = self.as_json(self.app.get(
            '/aggregates_by/%s/channels/%s/?version=%s&dates=%s&metric=%s'
            % (prefix, channel, version, ','.join(dates), metric)))
        self.assertEqual(len(resp['data']), len(histograms) * len(dates))

        for label, value in histograms.iteritems():
            resp = self.as_json(self.app.get(
                '/aggregates_by/%s/channels/%s/?version=%s&dates=%s&metric=%s&label=%s'
                % (prefix, channel, version, ','.join(dates), metric, label.upper())))

            self.assertNotEqual(resp['description'], '')
            self.assertEqual(len(resp['data']), len(dates))
            for res in resp['data']:
                self.assertEqual(res['count'], expected_count)

                current = pd.Series(res['histogram'], index=map(int, resp['buckets']))
                expected = pd.Series(index=NUMERIC_SCALARS_LABELS, data=0)
                expected[NUMERIC_SCALAR_BUCKET] = expected_count

                self.assertTrue((current == expected).all())
                self.assertEqual(res['sum'], SCALAR_VALUE * res['count'])

    def _keyed_histogram(self, prefix, channel, version, dates, metric, histograms, expected_count):
        resp = self.as_json(self.app.get(
            '/aggregates_by/%s/channels/%s/?version=%s&dates=%s&metric=%s'
            % (prefix, channel, version, ','.join(dates), metric)))
        self.assertEqual(len(resp['data']), len(histograms) * len(dates))

        for label, value in histograms.iteritems():
            resp = self.as_json(self.app.get(
                '/aggregates_by/%s/channels/%s/?version=%s&dates=%s&metric=%s&label=%s'
                % (prefix, channel, version, ','.join(dates), metric, label)))
            self.assertEqual(len(resp['data']), len(dates))

            for res in resp['data']:
                old_pings_expected_count = expected_count * (NUM_PINGS_PER_DIMENSIONS - NUM_AGGREGATED_CHILD_PINGS) / NUM_PINGS_PER_DIMENSIONS
                new_pings_expected_count = expected_count * NUM_AGGREGATED_CHILD_PINGS / NUM_PINGS_PER_DIMENSIONS
                self.assertEqual(
                    res['count'],
                    new_pings_expected_count * NUM_PROCESS_TYPES + old_pings_expected_count * (NUM_CHILDREN_PER_PING + 1)
                )

                current = pd.Series(res['histogram'], index=map(int, resp['buckets']))
                expected = Histogram(metric, value).get_value() * res['count']

                self.assertTrue((current == expected).all())
                self.assertEqual(res['sum'], value['sum'] * res['count'])


if __name__ == '__main__':
    unittest.main()
