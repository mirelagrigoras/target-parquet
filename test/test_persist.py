import pytest
import io
from datetime import datetime
import pandas as pd
from pandas.testing import assert_frame_equal
import simplejson as json

# from os import walk
import glob
import os
from target_parquet import persist_messages

#### TEMP DEBUG


def test_flatten():
    input_message = """{"type": "SCHEMA", "stream": "exchange_rate", "schema": {"type": "object", "properties": {"date": {"type": "string", "format": "date-time"}, "CAD": {"type": ["null", "number"]}, "HKD": {"type": ["null", "number"]}, "ISK": {"type": ["null", "number"]}, "PHP": {"type": ["null", "number"]}, "DKK": {"type": ["null", "number"]}, "HUF": {"type": ["null", "number"]}, "CZK": {"type": ["null", "number"]}, "GBP": {"type": ["null", "number"]}, "RON": {"type": ["null", "number"]}, "SEK": {"type": ["null", "number"]}, "IDR": {"type": ["null", "number"]}, "INR": {"type": ["null", "number"]}, "BRL": {"type": ["null", "number"]}, "RUB": {"type": ["null", "number"]}, "HRK": {"type": ["null", "number"]}, "JPY": {"type": ["null", "number"]}, "THB": {"type": ["null", "number"]}, "CHF": {"type": ["null", "number"]}, "EUR": {"type": ["null", "number"]}, "MYR": {"type": ["null", "number"]}, "BGN": {"type": ["null", "number"]}, "TRY": {"type": ["null", "number"]}, "CNY": {"type": ["null", "number"]}, "NOK": {"type": ["null", "number"]}, "NZD": {"type": ["null", "number"]}, "ZAR": {"type": ["null", "number"]}, "USD": {"type": ["null", "number"]}, "MXN": {"type": ["null", "number"]}, "SGD": {"type": ["null", "number"]}, "AUD": {"type": ["null", "number"]}, "ILS": {"type": ["null", "number"]}, "KRW": {"type": ["null", "number"]}, "PLN": {"type": ["null", "number"]}}}, "key_properties": ["date"]}
{"type": "RECORD", "stream": "exchange_rate", "record": {"CAD": 1.3171828596, "HKD": 7.7500212134, "ISK": 138.6508273229, "PHP": 48.5625795503, "DKK": 6.3139584217, "HUF": 309.7581671616, "CZK": 23.2040729741, "GBP": 0.7686720407, "RON": 4.1381417056, "SEK": 8.7889690284, "IDR": 14720.101824353, "INR": 73.3088672041, "BRL": 5.6121340687, "RUB": 77.5902418328, "HRK": 6.4340263046, "JPY": 105.311837081, "THB": 31.1803139584, "CHF": 0.9099703012, "EUR": 0.8485362749, "MYR": 4.1424692406, "BGN": 1.6595672465, "TRY": 7.8962240136, "CNY": 6.6836656767, "NOK": 9.2889266016, "NZD": 1.5062367416, "ZAR": 16.4451421298, "USD": 1.0, "MXN": 21.0537123462, "SGD": 1.3568095036, "AUD": 1.4064488757, "ILS": 3.3802291048, "KRW": 1138.1671616462, "PLN": 3.8797624098, "date": "2020-10-19T00:00:00Z"}}
{"type": "STATE", "value": {"start_date": "2020-10-19"}}
{"type": "SCHEMA", "stream": "exchange_rate", "schema": {"type": "object", "properties": {"date": {"type": "string", "format": "date-time"}, "CAD": {"type": ["null", "number"]}, "HKD": {"type": ["null", "number"]}, "ISK": {"type": ["null", "number"]}, "PHP": {"type": ["null", "number"]}, "DKK": {"type": ["null", "number"]}, "HUF": {"type": ["null", "number"]}, "CZK": {"type": ["null", "number"]}, "GBP": {"type": ["null", "number"]}, "RON": {"type": ["null", "number"]}, "SEK": {"type": ["null", "number"]}, "IDR": {"type": ["null", "number"]}, "INR": {"type": ["null", "number"]}, "BRL": {"type": ["null", "number"]}, "RUB": {"type": ["null", "number"]}, "HRK": {"type": ["null", "number"]}, "JPY": {"type": ["null", "number"]}, "THB": {"type": ["null", "number"]}, "CHF": {"type": ["null", "number"]}, "EUR": {"type": ["null", "number"]}, "MYR": {"type": ["null", "number"]}, "BGN": {"type": ["null", "number"]}, "TRY": {"type": ["null", "number"]}, "CNY": {"type": ["null", "number"]}, "NOK": {"type": ["null", "number"]}, "NZD": {"type": ["null", "number"]}, "ZAR": {"type": ["null", "number"]}, "USD": {"type": ["null", "number"]}, "MXN": {"type": ["null", "number"]}, "SGD": {"type": ["null", "number"]}, "AUD": {"type": ["null", "number"]}, "ILS": {"type": ["null", "number"]}, "KRW": {"type": ["null", "number"]}, "PLN": {"type": ["null", "number"]}}}, "key_properties": ["date"]}
{"type": "RECORD", "stream": "exchange_rate", "record": {"CAD": 1.3171828596, "HKD": 7.7500212134, "ISK": 138.6508273229, "PHP": 48.5625795503, "DKK": 6.3139584217, "HUF": 309.7581671616, "CZK": 23.2040729741, "GBP": 0.7686720407, "RON": 4.1381417056, "SEK": 8.7889690284, "IDR": 14720.101824353, "INR": 73.3088672041, "BRL": 5.6121340687, "RUB": 77.5902418328, "HRK": 6.4340263046, "JPY": 105.311837081, "THB": 31.1803139584, "CHF": 0.9099703012, "EUR": 0.8485362749, "MYR": 4.1424692406, "BGN": 1.6595672465, "TRY": 7.8962240136, "CNY": 6.6836656767, "NOK": 9.2889266016, "NZD": 1.5062367416, "ZAR": 16.4451421298, "USD": 1.0, "MXN": 21.0537123462, "SGD": 1.3568095036, "AUD": 1.4064488757, "ILS": 3.3802291048, "KRW": 1138.1671616462, "PLN": 3.8797624098, "date": "2020-10-19T00:00:00Z"}}
{"type": "STATE", "value": {"start_date": "2020-10-19"}}
"""

    # content of test_persist.expected.pkl based on : [{"CAD":1.3171828596,"HKD":7.7500212134,"ISK":138.6508273229,"PHP":48.5625795503,"DKK":6.3139584217,"HUF":309.7581671616,"CZK":23.2040729741,"GBP":0.7686720407,"RON":4.1381417056,"SEK":8.7889690284,"IDR":14720.101824353,"INR":73.3088672041,"BRL":5.6121340687,"RUB":77.5902418328,"HRK":6.4340263046,"JPY":105.311837081,"THB":31.1803139584,"CHF":0.9099703012,"EUR":0.8485362749,"MYR":4.1424692406,"BGN":1.6595672465,"TRY":7.8962240136,"CNY":6.6836656767,"NOK":9.2889266016,"NZD":1.5062367416,"ZAR":16.4451421298,"USD":1.0,"MXN":21.0537123462,"SGD":1.3568095036,"AUD":1.4064488757,"ILS":3.3802291048,"KRW":1138.1671616462,"PLN":3.8797624098,"date":"2020-10-19T00:00:00Z"},{"CAD":1.3171828596,"HKD":7.7500212134,"ISK":138.6508273229,"PHP":48.5625795503,"DKK":6.3139584217,"HUF":309.7581671616,"CZK":23.2040729741,"GBP":0.7686720407,"RON":4.1381417056,"SEK":8.7889690284,"IDR":14720.101824353,"INR":73.3088672041,"BRL":5.6121340687,"RUB":77.5902418328,"HRK":6.4340263046,"JPY":105.311837081,"THB":31.1803139584,"CHF":0.9099703012,"EUR":0.8485362749,"MYR":4.1424692406,"BGN":1.6595672465,"TRY":7.8962240136,"CNY":6.6836656767,"NOK":9.2889266016,"NZD":1.5062367416,"ZAR":16.4451421298,"USD":1.0,"MXN":21.0537123462,"SGD":1.3568095036,"AUD":1.4064488757,"ILS":3.3802291048,"KRW":1138.1671616462,"PLN":3.8797624098,"date":"2020-10-19T00:00:00Z"}]

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    input_messages = io.TextIOWrapper(
        io.BytesIO(input_message.encode()), encoding="utf-8"
    )

    persist_messages(input_messages, f"test_{timestamp}")

    filename = [f for f in glob.glob(f"test_{timestamp}/*.parquet")]

    df = pd.read_parquet(filename[0])
    df2 = pd.read_pickle("./test/test_persist.expected.pkl")

    for f in filename:
        os.remove(f)
    os.rmdir(f"test_{timestamp}")

    assert_frame_equal(df, df2)
