# -*- coding: utf-8 -*-

import os
import logging

import pandas as pd
import pytz
from tzlocal import windows_tz

import appdirs
import ws


LOG = logging.getLogger(__name__)


_TYPE_MAP = {'integer': int,
             'unicode': str,
             'string': str,
             'boolean': bool,
             'datetime': 'M8[ns]'}


def file_name(name, format):
    return '.'.join((name, format))


def get_file(name, expiration=None):
    if not os.path.exists(name):
        return

    expiration = pd.core.datetools.to_offset(expiration)
    if expiration:
        mtime = pd.datetime.utcfromtimestamp(os.path.getmtime(name))
        if (pd.datetime.now() - mtime) >= expiration:
            return

    return name


def cleanup(data):
    types = data.apply(lambda x: pd.lib.infer_dtype(x.values))

    for type_name, type_type in _TYPE_MAP.iteritems():
        for col in types[types == type_name].index:
            data[col] = data[col].astype(type_type)

    return data


def timetastic(ts, tz=None):
    if ts is None:
        return ts

    ts = pd.to_datetime(ts)

    if tz is not None and (not hasattr(ts, 'tzinfo') or ts.tzinfo is None):
        ts = ts.tz_localize(tz)

    return ts


class Manager(object):
    def __init__(self, client):
        self.client = client

    def _last_trade_date(self, exchange, expiration='1d'):
        exchanges = self.exchanges(expiration=expiration)
        return exchanges[exchange]['last_trade_date_time']

    def exchange_tz(self, exchange, exchanges=None):
        # NOTE(jkoelker) EODData's service is windows based, convert times here
        if exchanges is None:
            exchanges = self.exchanges()
        exchange_tz = exchanges[exchange]['time_zone']
        return pytz.timezone(windows_tz.tz_names[exchange_tz])

    def exchanges(self, expiration='1d'):
        LOG.info("Getting Exchanges")
        exchanges = self.client.exchanges()
        for exchange in exchanges:
            exchange_tz = self.exchange_tz(exchange, exchanges=exchanges)
            for col in ('intraday_start_date', 'last_trade_date_time'):
                exchanges[exchange][col] = timetastic(exchanges[exchange][col],
                                                      tz=exchange_tz)
        return pd.DataFrame(exchanges)

    def symbols(self, exchange, expiration='1d'):
        LOG.info("Getting Symbols for exchange %s" % exchange)
        return pd.DataFrame(self.client.symbols(exchange))

    def history(self, exchange, symbol, start, end=None, period='d'):
        symbols = self.symbols(exchange)

        if symbol not in symbols:
            return pd.DataFrame()

        tz = self.exchange_tz(exchange)
        start = timetastic(start, tz)
        end = timetastic(end, tz)

        exchange_end = self._last_trade_date(exchange)

        if end > exchange_end:
            end = exchange_end

        LOG.info("Getting History for %s:%s from %s to %s" % (exchange, symbol,
                                                              start, end))
        history = self.client.history(exchange, symbol, start, end, period)

        if not history:
            return pd.DataFrame()

        history = pd.DataFrame.from_records(history, index='date_time')

        # NOTE(jkoelker) Sometimes we'll get an extra period back
        if end is not None:
            history = history[history.index <= end]

        history.index = history.index.tz_localize(tz)

        return history

    def open(self, *args, **kwargs):
        pass

    def close(self, *args, **kwargs):
        pass


class CacheManager(Manager):
    def __init__(self, client, directory=None, name='eoddata',
                 *args, **kwargs):
        Manager.__init__(self, client)

        if directory is None:
            directory = appdirs.user_cache_dir(name)

        self.directory = directory

        if not os.path.exists(self.directory):
            os.makedirs(self.directory)


class PickleCache(CacheManager):
    @staticmethod
    def _get_key(*parts):
        return '/'.join(parts)

    def _get_file(self, key, create=True):
        filename = '.'.join(('/'.join((self.directory, key)), 'pkl'))

        if create:
            path = os.path.dirname(filename)
            if not os.path.exists(path):
                os.makedirs(path)

        return filename

    def _can_haz_cache(self, key, expiration=None):
        filename = self._get_file(key)
        if not os.path.exists(filename):
            return False

        if expiration is None:
            return True

        mtime = pd.to_datetime(os.path.getmtime(filename), unit='s')
        expiration = pd.core.datetools.to_offset(expiration)
        if (pd.datetime.now() - mtime) < expiration.delta:
            return True

        return False

    def exchanges(self, expiration='1d'):
        key = 'exchanges'
        filename = self._get_file(key)

        if self._can_haz_cache(key, expiration):
            return pd.read_pickle(filename)

        exchanges = CacheManager.exchanges(self, expiration)
        exchanges.to_pickle(filename)
        return exchanges

    # TODO(jkoelker) handle rename/delisting and the like
    def symbols(self, exchange, expiration='1d'):
        key = self._get_key('symbols', exchange)
        filename = self._get_file(key)

        if self._can_haz_cache(key, expiration):
            return pd.read_pickle(filename)

        symbols = CacheManager.symbols(self, exchange, expiration)
        symbols.to_pickle(filename)
        return symbols

    def _history(self, exchange, symbol, start, end=None, period='d'):
        return CacheManager.history(self, exchange, symbol, start, end, period)

    def history(self, exchange, symbol, start, end=None, period='d'):
        tz = self.exchange_tz(exchange)
        start = timetastic(start, tz)
        end = timetastic(end, tz)

        period_key = 'period_%s' % period
        key = self._get_key('history', exchange, symbol, period_key)
        filename = self._get_file(key)

        exchange_end = self._last_trade_date(exchange)

        if end is not None and end > exchange_end:
            end = exchange_end

        if not self._can_haz_cache(key):
            history = self._history(exchange, symbol, start, end, period)

            if history.empty:
                return history

            if os.path.exists(filename):
                cached_history = pd.read_pickle(filename)
                cached_history.combine_first(history).to_pickle(filename)

            else:
                history.to_pickle(filename)

            return history

        cached_history = pd.read_pickle(filename)

        if end is None:
            now = timetastic(pd.datetime.now(), tz)
            history = cached_history.ix[start:now]

        else:
            # NOTE(jkoelker) String date indexing allows any time on the date
            history = cached_history.ix[str(start):str(end.date())]

        if not history.empty:
            first_record = history.index[0]
            last_record = history.index[-1]

            # TODO(jkoelker) handle missing intraday data
            if start.date() < first_record.date():
                new_history = self._history(exchange, symbol, start,
                                            first_record, period)

                if not new_history.empty:
                    cached_history = cached_history.combine_first(new_history)
                    cached_history.to_pickle(filename)

                    history = history.combine_first(new_history)

            if end is None:
                search_end = timetastic(pd.datetime.now(), tz)

            else:
                search_end = end

            if last_record.date() < search_end.date():
                new_history = self._history(exchange, symbol, last_record,
                                            search_end, period)
                if not new_history.empty:
                    cached_history = cached_history.combine_first(new_history)
                    cached_history.to_pickle(filename)
                    history = history.combine_first(new_history)

        return history


class DataReader(object):
    def __init__(self, username, password, cache=None):
        client = ws.Client(username, password)
        self.datasource = None

        if not cache:
            self.datasource = Manager(client)
        elif cache is True:
            self.datasource = PickleCache(client)
        else:
            self.datasource = cache

    def __call__(self, exchange, symbol, start, end=None, period='d',
                 full_history=True):
        tz = self.datasource.exchange_tz(exchange)
        start = timetastic(start, tz)
        end = timetastic(end, tz)

        history = self.datasource.history(exchange, symbol, start, end, period)
        return history


def data(datareader, exchange, symbol, start, end, period):
    for history in datareader(exchange, symbol, start, end, period).iterrows():
        yield history
