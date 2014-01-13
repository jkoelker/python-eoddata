# -*- coding: utf-8 -*-
import itertools

from zipline.sources import data_source
from zipline.finance import trading
from zipline.data import loader_utils

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

    def create_simulation_parameters(self, start=None, end=None,
                                     capital_base=float("1.0e5"),
                                     emission_rate=None,
                                     data_frequency=None):
        if emission_rate is None:
            emission_rate = 'minute' if str(self.period) == 1 else 'daily'

        if data_frequency is None:
            data_frequency = 'minute' if str(self.period) == 1 else 'daily'

        if start is None:
            start = datareader.timetastic(self.start, datareader.pytz.utc)

        if end is None:
            end = datareader.timetastic(self.end, datareader.pytz.utc)

        return trading.SimulationParameters(period_start=start,
                                            period_end=end,
                                            capital_base=capital_base,
                                            emission_rate=emission_rate,
                                            data_frequency=data_frequency)

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
            yield {'dt': loader_utils.get_utc_from_exchange_time(dt),
                   'sid': event['symbol'],
                   'close': event['close'],
                   'open': event['open'],
                   'volume': event['volume'],
                   'high': event['high'],
                   'low': event['low'],
                   }

    @property
    def raw_data(self):
        if not hasattr(self, '_raw_data') or not self._raw_data:
            self._raw_data = self.raw_data_gen()

        return self._raw_data
