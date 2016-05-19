#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from setuptools import setup

setup(name='python_mozaggregator',
    version='0.2.5.4',
    author='Roberto Agostino Vitillo',
    author_email='rvitillo@mozilla.com',
    description='Telemetry aggregation job',
    url='https://github.com/vitillo/python_mozaggregator',
    packages=['mozaggregator'],
    package_dir={'mozaggregator': 'mozaggregator'},
    install_requires=[
        'awscli',
        'telemetry-tools',
        'python_moztelemetry',
        'Flask',
        'flask-cors',
        'flask-cache',
        'flask-sslify',
        'gevent',
        'gunicorn',
        'psycogreen',
        'psycopg2',
        'redis',
        'joblib',
        'boto',
        'ujson',
        'nose',
        'pandas>=0.15.2'
    ]
)
