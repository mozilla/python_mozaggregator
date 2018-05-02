#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from setuptools import setup

setup(name='python_mozaggregator',
    version='0.3.0.3',
    author='Roberto Agostino Vitillo',
    author_email='rvitillo@mozilla.com',
    description='Telemetry aggregation job',
    url='https://github.com/vitillo/python_mozaggregator',
    packages=['mozaggregator'],
    package_dir={'mozaggregator': 'mozaggregator'},
    install_requires=[
        'awscli',
        'boto3',
        'dockerflow',
        'Flask',
        'Flask-Cors',
        'Flask-Cache',
        'Flask-SSLify',
        'gevent',
        'gunicorn',
        'joblib',
        'matplotlib',
        'pandas',
        'psycogreen',
        'psycopg2-binary',
        'pyspark',
        'python-moztelemetry',
        'scipy',
        'ujson',
    ]
)
