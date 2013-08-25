# -*- coding: utf-8 -*-

from setuptools import setup

version = '0.1'

setup(name='eoddata',
      version=version,
      description="Python wrapper around EOD Data's Webservice API",
      long_description=open('README.rst').read(),
      keywords='',
      author='Jason Kölker',
      author_email='jason@koelker.net',
      url='https://github.com/jkoelker/python-eoddata',
      license='MIT',
      packages=['eoddata'],
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'scio',
      ],
      )
