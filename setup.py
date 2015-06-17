#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from distutils.core import setup

import urllib
import setuptools.command.install


setup(name='python_mozaggregator',
      version='0.2.0.0',
      author='Roberto Agostino Vitillo',
      author_email='rvitillo@mozilla.com',
      description='Telemetry aggregation job',
      url='https://github.com/vitillo/python_mozaggregator',
      packages=['mozaggregator'],
      package_dir={'mozaggregator': 'mozaggregator'},
      install_requires=['python_moztelemetry', 'Flask', 'boto', 'ujson', 'psycopg2', 'pandas>=0.15.2'])

