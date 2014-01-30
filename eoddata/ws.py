# -*- coding: utf-8 -*-

import functools
import re
import urllib2

import scio

# NOTE(jkoelker) I hate soap so much

WSDL = 'http://ws.eoddata.com/data.asmx?wsdl'
FIRST_CAP_RE = re.compile('(.)([A-Z][a-z]+)')
ALL_CAP_RE = re.compile('([a-z0-9])([A-Z])')


class Error(Exception):
    pass


class LoginError(Error):
    pass


def convert_date(date):
    if not date:
        return date

    if not isinstance(date, basestring):
        try:
            return date.strftime('%Y%m%d')
        except:
            pass

    return date


def require_login(f):

    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        if not self.token:
            self.login()

        return f(self, *args, **kwargs)

    return wrapper


def success(obj, method):
    result = method + 'Result'
    obj = getattr(obj, result)

    if not hasattr(obj, 'Message'):
        return False

    if 'success' in obj.Message.lower():
        return obj

    return False


def convert(value):
    if isinstance(value, basestring):
        v = value.lower()

        if v == 'true':
            return True

        elif v == 'false':
            return False

    return value


# NOTE(jkoelker) http://stackoverflow.com/a/1176023/101801
def decamelize(name):
    return ALL_CAP_RE.sub(r'\1_\2', FIRST_CAP_RE.sub(r'\1_\2', name)).lower()


def dictify(obj):
    return dict([(decamelize(k.strip('_')), convert(v))
                 for k, v in obj.__dict__.iteritems()
                 if k.startswith('_') and k.endswith('_')])


def list_to_dictify(objs, key):
    res = {}

    for obj in objs:
        obj = dictify(obj)
        res[obj[key]] = obj

    return res


class Client(object):
    def __init__(self, username, password):
        self.client = scio.Client(urllib2.urlopen(WSDL))
        self.username = username
        self.password = password
        self.token = None
        self.last_response = None

    def _get(self, method, **kwargs):
        func = getattr(self.client.service, method)
        self.last_response = func(**kwargs)
        return self.last_response

    def _result(self, method, processor=None, **kwargs):
        obj = self._get(method, Token=self.token, **kwargs)

        result = success(obj, method)
        if not result:
            raise Error(str(self.last_response))

        if processor:
            result = processor(result)

        return result

    def login(self):
        method = 'Login'
        obj = self._get(method, Username=self.username, Password=self.password)

        result = success(obj, method)

        if not result:
            raise LoginError(str(self.last_response))

        self.token = result.Token
        return self.token

    @require_login
    def country_list(self):
        method = 'CountryList'
        processor = lambda obj: dict([(c.Code, c.Name)
                                      for c in obj.COUNTRIES.CountryBase])
        return self._result(method, processor)

    @require_login
    def exchange(self, exchange):
        method = 'ExchangeGet'
        processor = lambda obj: dictify(obj.EXCHANGE)
        return self._result(method, processor, Exchange=exchange)

    @require_login
    def exchanges(self):
        method = 'ExchangeList'
        processor = lambda obj: list_to_dictify(obj.EXCHANGES.EXCHANGE, 'code')
        return self._result(method, processor)

    @require_login
    def fundamentals(self, exchange):
        method = 'FundamentalList'
        processor = lambda obj: list_to_dictify(obj.FUNDAMENTALS.FUNDAMENTAL,
                                                'symbol')
        return self._result(method, processor, Exchange=exchange)

    @require_login
    def quote(self, exchange, symbol):
        method = 'QuoteGet'
        processor = lambda obj: dictify(obj.QUOTE)
        return self._result(method, processor, Exchange=exchange,
                            Symbol=symbol)

    # NOTE(jkoelker) Period queries don't seem to have intraday data. Need to
    #                investigate
    @require_login
    def quotes(self, exchange, symbols=None, date=None, period=None):
        method = 'QuoteList'
        kwargs = {'Exchange': exchange}

        if symbols is not None:
            if any((date, period)):
                raise TypeError("'symbols' cannot be specified with 'date' or "
                                "'period'")
            method = 'QuoteList2'
            kwargs['Symbols'] = ','.join(symbols)

        if date is not None:
            date = convert_date(date)
            method = 'QuoteListByDate'
            kwargs['QuoteDate'] = date

            if period is not None:
                method = 'QuoteListByDatePeriod'
                kwargs['period'] = period

        processor = lambda obj: list_to_dictify(obj.QUOTES.QUOTE, 'symbol')
        return self._result(method, processor, **kwargs)

    @require_login
    def history(self, exchange, symbol, start, end=None, period=None):
        method = 'SymbolHistory'
        kwargs = {'Exchange': exchange, 'Symbol': symbol,
                  'StartDate': convert_date(start)}

        # NOTE(jkoelker) Consistancy much?
        if period is not None:
            kwargs['Period'] = period

            if end is None:
                method = 'SymbolHistoryPeriod'
                kwargs['Date'] = convert_date(start)

            else:
                method = 'SymbolHistoryPeriodByDateRange'
                kwargs['EndDate'] = convert_date(end)

        elif end is not None:
            raise TypeError("'period' must be specified with 'end'")

        processor = lambda obj: [dictify(o) for o in obj.QUOTES.QUOTE]
        return self._result(method, processor, **kwargs)

    @require_login
    def symbols(self, exchange):
        method = 'SymbolList'
        processor = lambda obj: list_to_dictify(obj.SYMBOLS.SYMBOL, 'code')
        return self._result(method, processor, Exchange=exchange)

    @require_login
    def technicals(self, exchange):
        method = 'TechnicalList'
        processor = lambda obj: list_to_dictify(obj.TECHNICALS.TECHNICAL,
                                                'symbol')
        return self._result(method, processor, Exchange=exchange)
