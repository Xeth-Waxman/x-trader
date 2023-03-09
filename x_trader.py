import pandas as pd
import backtrader as bt
import requests
import joblib
import hashlib
import datetime
import io
import os

# Step 1: Collect Data
def get_data(api_key, symbol):
    # Generate cache key and cache file name
    cache_key = hashlib.sha256(f"{symbol}_{datetime.date.today()}".encode()).hexdigest()
    cache_file = f"{cache_key}.joblib"
    
    # Check if cached data exists and is not stale
    if os.path.exists(cache_file):
        cached_data = joblib.load(cache_file)
        cache_time = datetime.datetime.fromtimestamp(os.path.getmtime(cache_file))
        current_time = datetime.datetime.now()
        time_since_update = current_time - cache_time
        if time_since_update < datetime.timedelta(days=1):
            return cached_data
    
    # Make API request if cached data is not available or stale
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={symbol}outputsize=full&apikey={api_key}&datatype=csv'
    response = requests.get(url)

    #debug
    print(response.content)
    #/debug

    if response.status_code != 200:
        raise ValueError(f"API request failed with status code {response.status_code}")
    df = pd.DataFrame(pd.np.empty((0, 8)))
    df = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
    df.set_index(pd.to_datetime(df.index), inplace=True)
    
    # Set the index to the date string
    df.index = pd.to_datetime(df.index)
    
    # Cache the data
    joblib.dump(df, cache_file)
    
    return df



# Step 2a: Fill Missing Values
def fill_missing_values(df):
    df['4. close'] = df['4. close'].fillna(method='ffill')
    return df

# Step 2b: Calculate Returns
def calculate_returns(df):
    df['returns'] = df['4. close'].pct_change()
    return df

# Step 2: Data Preparation
def prepare_data(df):
    # Rename columns to a simpler format
    df.columns = ['Open', 'High', 'Low', 'Close', 'Adjusted Close', 'Volume', 'Dividend Amount', 'Split Coefficient']
    
    # Drop columns we don't need
    df = df.drop(columns=['Open', 'High', 'Low', 'Dividend Amount', 'Split Coefficient'])

    # Convert index to datetime and sort
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()

    # Fill missing values
    df = fill_missing_values(df)

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
    cerebro.run(tradehistory=True)

    # Print final portfolio value
    print(f"Final Portfolio Value: {cerebro.broker.getvalue():,.2f}")

if __name__ == '__main__':
    # Call your functions here
    api_key = 'ZG5MLKJMUZ2UEAJ8'
    symbol = 'LPLA'
    data = get_data(api_key, symbol)
    prepared_data = prepare_data(data)
    technical_data = calculate_technical_indicators(prepared_data)
    backtest(technical_data)
