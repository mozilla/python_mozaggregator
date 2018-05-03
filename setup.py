#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from setuptools import setup

test_deps = [
    'configparser==3.5.0',
    'coverage==4.5.1',
    'enum34==1.1.6',
    'flake8==3.5.0',
    'mccabe==0.6.1',
    'nose==1.3.7',
    'pycodestyle==2.3.1',
    'pyflakes==1.6.0',
    'requests==2.18.4',
]

extras = {
    'testing': test_deps,
}

setup(name='python_mozaggregator',
    version='0.3.0.2',
    author='Roberto Agostino Vitillo',
    author_email='rvitillo@mozilla.com',
    description='Telemetry aggregation job',
    url='https://github.com/vitillo/python_mozaggregator',
    packages=['mozaggregator'],
    package_dir={'mozaggregator': 'mozaggregator'},
    install_requires=[
        'awscli==1.14.67',
        'blinker==1.4',
        'boto3==1.6.20',
        'botocore==1.9.20',
        'certifi==2018.1.18',
        'click==6.7',
        'dockerflow==2018.2.1',
        'Flask==0.12.2',
        'Flask-Cache==0.13.1',
        'Flask-Cors==3.0.3',
        'Flask-SSLify==0.1.5',
        'gevent==1.2.2',
        'greenlet==0.4.13',
        'gunicorn==19.7.1',
        'idna==2.6',
        'itsdangerous==0.24',
        'Jinja2==2.10',
        'joblib==0.11',
        'matplotlib==2.2.2',
        'numpy==1.14.2',
        'pandas==0.22.0',
        'protobuf==3.5.2.post1',
        'psycogreen==1.0',
        'psycopg2-binary==2.7.4',
        'pyspark==2.3.0',
        'python-dateutil==2.7.2',
        'python-moztelemetry==0.8.14',
        'python-snappy==0.5.2',
        'scipy==1.0.1',
        'ujson==1.35',
        'urllib3==1.22',
        'Werkzeug==0.14.1',
    ],
    extras_require=extras,
)
