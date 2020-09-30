"""
Kaiko API Wrapper
"""
from os import environ
import kaiko.utils as ut
import pandas as pd
import logging
from datetime import datetime
try:
    from cStringIO import StringIO      # Python 2
except ImportError:
    from io import StringIO

# Base URLs
_BASE_URL_KAIKO_US = 'https://us.market-api.kaiko.io/'
_BASE_URL_KAIKO_EU = 'https://eu.market-api.kaiko.io/'
_BASE_URL_RAPIDAPI = 'https://kaiko-cryptocurrency-market-data.p.rapidapi.com/'  # Not supported yet
_BASE_URLS = dict(us=_BASE_URL_KAIKO_US, eu=_BASE_URL_KAIKO_EU, rapidapi=_BASE_URL_RAPIDAPI)

# API endpoints
_URL_REFERENCE_DATA_API = 'https://reference-data-api.kaiko.io/v1/'

_URL_HISTORICAL_TRADES = 'v1/data/{commodity}.{data_version}/exchanges/{exchange}/{instrument_class}/{instrument}' \
                         '/trades'
_URL_ORDER_BOOK_FULL = 'v1/data/{commodity}.{data_version}/exchanges/{exchange}/{instrument_class}/{instrument}' \
                       '/snapshots/full'
_URL_ORDER_BOOK_AGGREGATIONS_FULL = 'v1/data/{commodity}.{data_version}/exchanges/{exchange}/{instrument_class}' \
                                    '/{instrument}/ob_aggregations/full'
_URL_CANDLES = 'v1/data/{commodity}.{data_version}/exchanges/{exchange}/{instrument_class}/{instrument}/aggregations' \
               '/count_ohlcv_vwap'
_URL_DIRECT_EXCHANGE_RATE = 'v1/data/{commodity}.{data_version}/spot_direct_exchange_rate/{base_asset}/{quote_asset}'
_URL_EXCHANGE_RATE = 'v1/data/trades.v1/spot_exchange_rate/{base_asset}/{quote_asset}'


# Default settings?

def init_param_dict(keys: list, values: dict = None):
    """
    Creates a dictionary filled with `value` and with keys corresponding to `keys`.

    :param keys: List of keys for the dictionary.
    :param values: Dictionary of values to fill (default is `None`).  If the values dictionary contains keys that
                    did not exist in the list `keys`, then it is added to the return dictionary.
    :type values: dict
    :return: Dictionary with `keys` as keys and `value` as values.
    :rtype: dict
    """
    # Initialize with None values
    output = dict(zip(keys, [None for i in keys]))

    # Overwrite default values
    if values is not None:
        for k in values.keys():
            output[k] = values[k]

    return output


class KaikoClient:
    """
    Kaiko Client: extracts API key from environment, sets base URL and constructs headers for API requests.

    In order to change your API key, you can use the setter method for `api_key_input`. `api_key` contains the key
    used by the client and cannot be set.  `api_key` and `headers` are automatically updated when changing
    `api_key_input`.

    Valid `base_url` include 'us', 'eu', and 'rapidapi' (Rapid API no longer supported).
    """

    def __init__(self, api_key: str = '', base_url: str = 'us'):
        self.base_url = _BASE_URLS[base_url]

        self._api_key_input = api_key

        self.headers = {
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip',
            'X-Api-Key': self.api_key,
        }

    @property
    def api_key(self) -> str:
        """
        Sets the API key from the environment variable $KAIKO_API_KEY if no key is provided.
        :param api_key: (optional) your API key
        :return: API key to be used in the requests
        """
        env = environ.get('KAIKO_API_KEY')
        kaiko_api_key = env or ''
        api_key = self.api_key_input or kaiko_api_key
        return api_key

    @property
    def api_key_input(self):
        return self._api_key_input

    @api_key_input.setter
    def api_key_input(self, newval):
        self._api_key_input = newval
        self.update_headers()

    def update_headers(self) -> dict:
        self.headers = {
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip',
            'X-Api-Key': self.api_key,
        }

    def load_catalogs(self):
        """
        Loads
        1) List of instruments -> self.all_instruments
        2) List of exchanges   -> self.all_exchanges
        3) List of assets      -> self.all_assets

        Those are public endpoints which do not require authentication.
        """
        print("Downloading Kaiko's catalog (lists of instruments, exchanges, assets)...")
        logging.info("Downloading catalogs...")

        # List of all instruments
        self.all_instruments = ut.request_df(_URL_REFERENCE_DATA_API + 'instruments')
        # replace None values by 'ongoing'
        self.all_instruments['trade_end_time'] = self.all_instruments['trade_end_time'].apply(lambda x: x or 'ongoing')

        # List of exchanges and assets
        self.all_exchanges = ut.request_df(_URL_REFERENCE_DATA_API + 'exchanges')
        self.all_assets = ut.request_df(_URL_REFERENCE_DATA_API + 'assets')

        print("\t...done! - available under client.all_{instruments, exchanges, assets}")
        logging.info("... catalogs imported!")

    def __repr__(self):
        return "Kaiko Client set up with \n\tBase URL: {}\n\tAPI Key : {}[...]".format(self.base_url, self.api_key[:5])


class KaikoData:
    """
    General data class
    Get query details from the json file as attributes
    For the definition of the endpoint, there are mandatory instrument descriptions (can we get it from API?)

    Attributes (draft)
     - endpoint = base + endpoint
     - params
    """

    def __repr__(self):
        return f"KaikoData setup with\n- URL\n\t {self.url},\n- Required parameters:\n\t{self.req_params}," \
               f"\n- Optional parameters:\n\t{self.params}"

    def __init__(self, endpoint, req_params: dict, params: dict = {}, client=None, pagination=True, **kwargs):
        self.client = client or KaikoClient()
        self.endpoint = self.client.base_url + endpoint
        self.params = params
        self.req_params = req_params
        self._form_url()

        self.pagination = pagination

        # catch parameters given to the class constructor
        self._add_to_params(**kwargs)
        self._add_to_req_params(**kwargs)

        self._form_url()

        logging.info(f"\n\nInitiated data object\n{self.__repr__()}\n")

    def _form_url(self):
        self.url = self.endpoint.format(**self.req_params)

    @staticmethod
    def _format_param_timestamps(params):
        for key in ['start_time', 'end_time']:
            if key in params:
                params[key] = ut.convert_timestamp_to_apiformat(params[key])
        return params

    @property
    def query(self):
        return dict(**self.params, **self.req_params)

    @property
    def params(self):
        return self._format_param_timestamps(self._params)

    @params.setter
    def params(self, params):
        self._params = params

    def _add_to_params(self, **kwargs):
        for key in kwargs:
            if key in self.parameter_space:
                self._params[key] = kwargs[key]

    def _add_to_req_params(self, **kwargs):
        for key in kwargs:
            if key in self.req_params.keys():
                self.req_params[key] = kwargs[key]

    @staticmethod
    def df_formatter(res):
        df = pd.DataFrame(res['data'], dtype='float')
        df.set_index('timestamp', inplace=True)
        df.index = ut.convert_timestamp_unix_to_datetime(df.index)
        return df

    def _request_api(self):
        self.df, self.query_api = ut.request_df(self.url,
                                                return_query=True,
                                                headers=self.client.headers,
                                                params=self.params,
                                                df_formatter=self.df_formatter,
                                                pagination=self.pagination,
                                                )

    def load_catalogs(self):
        """ Loads catalogs in the client """
        self.client.load_catalogs()


class TickTrades(KaikoData):
    """
    Tick-by-tick trade data
    """

    def __init__(self, exchange, instrument, instrument_class: str = 'spot', params: dict = dict(page_size=100000), client=None, **kwargs):
        # Initialize endpoint required parameters
        self.req_params = dict(commodity='trades',
                               data_version='latest',
                               exchange=exchange,
                               instrument_class=instrument_class,
                               instrument=instrument,
                               )

        self.parameter_space = 'start_time,end_time,page_size,continuation_token'.split(',')

        endpoint = _URL_HISTORICAL_TRADES

        KaikoData.__init__(self, endpoint, self.req_params, params, client, **kwargs)

        self._request_api()

    @staticmethod
    def df_formatter(res):
        df = pd.DataFrame(res['data'], dtype='float')
        df.set_index('timestamp', inplace=True)
        df.index = ut.convert_timestamp_unix_to_datetime(df.index)
        return df


class Candles(KaikoData):
    """
    Candles (Count OHLCV VWAP)
    """
    def __init__(self, exchange, instrument, instrument_class: str = 'spot', params: dict = dict(page_size=100000),
                 client=None,
                 **kwargs):

        # Initialize endpoint required parameters
        self.req_params = dict(commodity='trades',
                               data_version='latest',
                               exchange=exchange,
                               instrument_class=instrument_class,
                               instrument=instrument,
                               )

        self.parameter_space = 'interval,start_time,end_time,page_size,continuation_token,sort'.split(',')

        endpoint = _URL_CANDLES

        KaikoData.__init__(self, endpoint, self.req_params, params, client, **kwargs)

        self._request_api()

    @staticmethod
    def df_formatter(res):
        df = pd.DataFrame(res['data'], dtype='float')
        df.set_index('timestamp', inplace=True)
        df.index = ut.convert_timestamp_unix_to_datetime(df.index)
        return df


def add_price_levels(df):
    """
    Add order-book price levels corresponding to amounts given by the API:
     X_volume_Y where X is in {bid, ask} and Y is the price level relative to the midprice:
     0_1 ... 0_9 : 0.1% to 0.9% away from the mid price
     1 ... 10 : 1% to 10% away from the mid price
    """
    for side in ['bid', 'ask']:
        labs = [l for l in df.columns if l.startswith('%s_volume' % side)]
        for lab in labs:
            # calculate the level
            lvl_lab = lab.split('volume')[-1]
            lvl = float('.'.join(lvl_lab.split('_'))) / 100
            # side of the order book
            eps = -1 * (side == 'bid') + 1 * (side == 'ask')

            newlab = '%s_price%s' % (side, lvl_lab)

            df[newlab] = df["mid_price"] * (1 + eps * lvl)
    return df


class OrderBookSnapshots(KaikoData):
    """
    Order-book snapshot data
    """
    def __init__(self, exchange, instrument, instrument_class: str = 'spot', params: dict = dict(page_size=100),
                 client=None,
                 **kwargs):

        # Initialize endpoint required parameters
        self.req_params = dict(commodity='order_book_snapshots',
                               data_version='latest',
                               exchange=exchange,
                               instrument_class=instrument_class,
                               instrument=instrument,
                               )

        self.parameter_space = 'start_time,end_time,page_size,continuation_token,slippage,slippage_ref,orders,limit_orders'.split(',')

        endpoint = _URL_ORDER_BOOK_FULL

        KaikoData.__init__(self, endpoint, self.req_params, params, client, **kwargs)

        self._request_api()

        if len(self.df) == 0:
            print(f'No data was found for the time range selected. \n{self.query_api}')
            print('NB: only one month of historical order book snapshots is available from the API. Please setup a '
                  'Data Feed delivery if you are trying to access data older than a month.')

    @staticmethod
    def df_formatter(res):
        df = pd.DataFrame(res['data'], dtype='float')
        df.set_index('poll_timestamp', inplace=True)
        df.index = ut.convert_timestamp_unix_to_datetime(df.index)
        df = add_price_levels(df)
        return df


class OrderBookAggregations(KaikoData):
    """
    Order-book data statistics (averages)
    """
    def __init__(self, exchange, instrument, instrument_class: str = 'spot', params: dict = dict(page_size=100),
                 client=None,
                 **kwargs):

        # Initialize endpoint required parameters
        self.req_params = dict(commodity='order_book_snapshots',
                               data_version='latest',
                               exchange=exchange,
                               instrument_class=instrument_class,
                               instrument=instrument,
                               )

        self.parameter_space = 'start_time,end_time,page_size,continuation_token,slippage,slippage_ref,interval'.split(',')

        endpoint = _URL_ORDER_BOOK_AGGREGATIONS_FULL

        KaikoData.__init__(self, endpoint, self.req_params, params, client, **kwargs)
        self._request_api()
        if len(self.df) == 0:
            print(f'No data was found for the time range selected. \n{self.query_api}')
            print('NB: only one month of historical order book snapshots is available from the API. Please setup a '
                  'Data Feed delivery if you are trying to access data older than a month.')

    @staticmethod
    def df_formatter(res):
        df = pd.DataFrame(res['data'], dtype='float')
        df.set_index('poll_timestamp', inplace=True)
        df.index = ut.convert_timestamp_unix_to_datetime(df.index)
        df = add_price_levels(df)
        return df


if __name__ == '__main__':
    FORMAT = "%(asctime)-15s %(levelname)-8s | %(lineno)d %(filename)s: %(message)s"
    logging.basicConfig(filename='/var/tmp/kaiko.log', level=logging.DEBUG, format=FORMAT, filemode='a')
    # test = OrderBookAverages('cbse', 'btc-usd', start_time='2020-08-06', interval='10m')

    test = Candles('cbse', 'eth-usd', start_time='2020-08-06', interval='1d')
    print(test.df)
