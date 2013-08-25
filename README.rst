A python wrapper for EOD Data
=============================

Requirements
============
A `eoddata.com <http://eoddata.com>`_ platinum account is required.

Usage
=====

.. code-block:: python

    import eoddata


    USERNAME = 'username'
    PASSWORD = 'password'


    client = eoddata.Client(USERNAME, PASSWORD)

    for symbol, quote in client.quotes('NASDAQ'):
        print symbol, quote['close']
