# -*- coding: utf-8 -*-
import itertools

from zipline.sources import data_source

import datareader


# NOTE(jkoelker) Adapted from https://github.com/quantopian/zipline/pull/249
class EODData(data_source.DataSource):
    def __init__(self, symbols, period, start, end, username, password,
                 cache=True):
        self.symbols = symbols
        self.start = start
        self.end = end
        self.period = period
        self.cache = cache
        self.datareader = datareader.DataReader(username, password, cache)

    @property
    def mapping(self):
        return {
            'dt': (lambda x: x, 'dt'),
            'sid': (lambda x: x, 'sid'),
            'price': (float, 'close'),
            'close_price': (float, 'close'),
            'open_price': (float, 'open'),
            'high': (float, 'high'),
            'low': (float, 'low'),
            'volume': (int, 'volume'),
        }

    @property
    def instance_hash(self):
        return "EODData"

    def raw_data_gen(self):
        histories = []
        for exchange, symbol in self.symbols:
            history = datareader.data(self.datareader, exchange, symbol,
                                      self.start, self.end, self.period)
            histories.append(history)

        def roundrobin(*iterables):
            "roundrobin('ABC', 'D', 'EF') --> A D E B F C"
            # Recipe credited to George Sakkis
            pending = len(iterables)
            nexts = itertools.cycle(iter(it).next for it in iterables)
            while pending:
                try:
                    for n in nexts:
                        yield n()
                except StopIteration:
                    pending -= 1
                    nexts = itertools.cycle(itertools.islice(nexts, pending))

        for dt, event in roundrobin(*histories):
            yield {'dt': dt,
                   'sid': event['symbol'],
                   'close': event['close'],
                   'open': event['open'],
                   'volume': event['volume'],
                   'high': event['high'],
                   'low': event['low'],
                   }

    @property
    def raw_data(self):
        if not self._raw_data:
            self._raw_data = self.raw_data_gen()

        return self._raw_data
