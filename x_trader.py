import pandas as pd
import backtrader as bt
import requests
import joblib
import hashlib
import datetime
import io
import os

# Step 1: Collect Data
def get_data(api_key, symbol, interval):
    # generate a unique cache key for the request
    cache_key = hashlib.sha256(f"{symbol}_{interval}_{datetime.date.today()}".encode()).hexdigest()
    
    # check if data is in cache and is not stale
    cached_data = joblib.load(f"{cache_key}.joblib")
    if cached_data is not None:
        cache_time = datetime.datetime.fromtimestamp(os.path.getmtime(f"{cache_key}.joblib"))
        current_time = datetime.datetime.now()
        time_since_update = current_time - cache_time
        if time_since_update < datetime.timedelta(days=1):
            return cached_data
    
    # if data is not in cache or is stale, make API request
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_{interval}&symbol={symbol}&apikey={api_key}&datatype=csv'
    response = requests.get(url)
    if response.status_code != 200:
        raise ValueError(f"API request failed with status code {response.status_code}")
    df = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
    df.set_index('timestamp', inplace=True)
    
    # cache the data
    joblib.dump(df, f"{cache_key}.joblib")
    return df

# Step 2a: Fill Missing Values
def fill_missing_values(df):
    df['close'] = df['close'].fillna(method='ffill')
    return df

# Step 2b: Calculate Returns
def calculate_returns(df):
    df['returns'] = df['close'].pct_change()
    return df

# Step 2: Data Preparation
def prepare_data(df):
    df = fill_missing_values(df)
    df = calculate_returns(df)
    return df

# Step 3: Technical Analysis
def calculate_technical_indicators(df):
    df['ma5'] = df['close'].rolling(window=5).mean()
    df['ma20'] = df['close'].rolling(window=20).mean()
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()
    rs = avg_gain / avg_loss
    df['rsi'] = 100 - (100 / (1 + rs))
    # add custom indicators here
    return df

# Step 4: Trading Strategy
class MovingAverageCross(bt.Strategy):
    params = (
        ('ma1', 5),
        ('ma2', 20),
    )

    def __init__(self):
        ma1 = bt.indicators.SimpleMovingAverage(self.data.close, period=self.params.ma1)
        ma2 = bt.indicators.SimpleMovingAverage(self.data.close, period=self.params.ma2)
        self.crossover = bt.indicators.CrossOver(ma1, ma2)

    def next(self):
        if self.crossover > 0:
            self.buy()
        elif self.crossover < 0:
            self.sell()

# Step 5: Backtesting
def backtest(df):
    cerebro = bt.Cerebro()
    data = bt.feeds.PandasData(dataname=df)
    cerebro.adddata(data)
    cerebro.addstrategy(MovingAverageCross)
    cerebro.run()
