# -*- coding: utf-8 -*-

import os

import pandas as pd

import appdirs
import ws


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


def ununicode(data):
    types = data.apply(lambda x: pd.lib.infer_dtype(x.values))

    for col in types[types == 'unicode'].index:
        data[col] = data[col].astype(str)

    return data


class Manager(object):
    def __init__(self, client):
        self.client = client

    def exchanges(self, expiration='1d'):
        return pd.DataFrame(self.client.exchanges())

    def symbols(self, exchange, expiration='1d'):
        return pd.DataFrame(self.client.symbols(exchange))

    def history(self, exchange, symbol, start, end=None, period='d'):
        start = pd.core.datetools.to_datetime(start)
        end = pd.core.datetools.to_datetime(end)

        history = self.client.history(exchange, symbol, start, end, period)
        history = pd.DataFrame.from_records(history, index='date_time')

        # NOTE(jkoelker) Sometimes we'll get an extra period back
        if end is not None:
            history = history[history.index <= end]

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


class HDFCache(CacheManager):
    def __init__(self, *args, **kwargs):
        CacheManager.__init__(self, *args, **kwargs)
        self._store_kwargs = {'path': os.path.join(self.directory,
                                                   'eoddata.h5'),
                              'complevel': kwargs.get('complevel', 9),
                              'complib': kwargs.get('complib', 'blosc'),
                              'fletcher32': kwargs.get('fletcher32', True)}
        self.store = pd.HDFStore(**self._store_kwargs)

    def _update_insert_time(self, key):
        series = pd.Series({key: pd.datetime.now()})

        if 'insert_time' in self.store:
            insert_time = self.store['insert_time']
            if key in insert_time:
                insert_time[key] = series[key]

            else:
                insert_time = insert_time.append(series)
        else:
            insert_time = series

        self.store['insert_time'] = insert_time

    def _can_haz_cache(self, key, expiration=None):
        if key not in self.store:
            return False

        if expiration is None:
            return True

        if 'insert_time' in self.store:
            insert_time = self.store['insert_time']

            if key not in insert_time:
                return False

            expiration = pd.core.datetools.to_offset(expiration)
            if (pd.datetime.now() - insert_time[key]) < expiration:
                return True

        return False

    @staticmethod
    def _get_key(*parts):
        return '/'.join(parts)

    @property
    def is_open(self):
        if self.store._handle is not None and self.store._handle.isopen:
            return True
        return False

    def exchanges(self, expiration='1d'):
        if not self.is_open:
            self.store.open()

        if self._can_haz_cache('exchanges', expiration):
            return self.store['exchanges']

        exchanges = CacheManager.exchanges(self, expiration)
        exchanges = ununicode(exchanges)
        self.store['exchanges'] = exchanges
        return exchanges

    # TODO(jkoelker) handle rename/delisting and the like
    def symbols(self, exchange, expiration='1d'):
        if not self.is_open:
            self.store.open()

        key = self._get_key('symbols', exchange)
        if self._can_haz_cache(key, expiration):
            return self.store[key]

        symbols = CacheManager.symbols(self, exchange, expiration)
        symbols = ununicode(symbols)
        self.store[key] = symbols
        return symbols

    def _history(self, exchange, symbol, start, end=None, period='d'):
        history = CacheManager.history(self, exchange, symbol, start, end,
                                       period)
        return ununicode(history)

    def history(self, exchange, symbol, start, end=None, period='d'):
        start = pd.core.datetools.to_datetime(start)
        end = pd.core.datetools.to_datetime(end)

        if not self.is_open:
            self.store.open()

        key = self._get_key('history', exchange, symbol, period)

        if not self._can_haz_cache(key):
            history = self._history(exchange, symbol, start, end, period)

            if key not in self.store:
                self.store.append(key, history)

            else:
                terms = [pd.Term('index', '>=', start)]

                if end is not None:
                    terms.append(pd.Term('index', '<=', end))

                cached_history = self.store.select(key, terms)
                criteria = ~history.index.isin(cached_history.index)
                new_history = history.ix[criteria]

                self.store.append(key, new_history)

            return history

        terms = [pd.Term('index', '>=', start)]

        if end is not None:
            terms.append(pd.Term('index', '<=', end))

        history = self.store.select(key, terms)

        if history:

            last_record = history.index[-1]

            if end is not None and last_record < end:
                start = last_record

            new_history = self._history(exchange, symbol, start, end, period)
            new_history = new_history[new_history.index > last_record]

            if new_history:
                self.store.append(key, new_history)
                history = history.append(new_history)

            return history

        history = self._history(exchange, symbol, start, end, period)
        self.store.append(key, history)
        return history

    def open(self, *args, **kwargs):
        return self.store.open(*args, **kwargs)

    def close(self, *args, **kwargs):
        return self.store.close(*args, **kwargs)


class DataReader(object):
    def __init__(self, username, password, cache=None):
        client = ws.Client(username, password)
        self.datasource = None

        if not cache:
            self.datasource = Manager(client)
        elif cache is True:
            self.datasource = HDFCache(client)
        else:
            self.datasource = cache

    def __call__(self, exchange, symbol, start, end=None, period='d'):
        return self.datasource.history(exchange, symbol, start, end, period)
