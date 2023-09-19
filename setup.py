#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from setuptools import setup

setup(name='python_mozaggregator',
    version='0.3.0.4',
    author='Roberto Agostino Vitillo',
    author_email='rvitillo@mozilla.com',
    description='Telemetry aggregation job',
    url='https://github.com/vitillo/python_mozaggregator',
    packages=['mozaggregator'],
    package_dir={'mozaggregator': 'mozaggregator'},
    install_requires=[
        'Flask',
        'Flask-Cache',
        'Flask-Cors',
        'Flask-SSLify',
        'boto3',
        'click',
        'dockerflow',
        'gevent',
        'gunicorn',
        'joblib',
        'pandas',
        'psycogreen',
        'psycopg2-binary',
        'pyspark==2.4.4',
        'python-jose-cryptodome',
        # using git reference as python_moztelemetry
        # has been deleted from pypi repository
        # TODO: investigate python_moztelemetry usage
        # and remove this dependency if possible.
        'python-moztelemetry @ git+https://github.com/mozilla/python_moztelemetry.git@v0.10.4#egg=python-moztelemetry',
        # 'git+https://github.com/mozilla/python_moztelemetry.git@v0.10.4#egg=python-moztelemetry',
        'ujson',
    ]
)
