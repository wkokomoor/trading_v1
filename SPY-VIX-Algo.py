import schwabdev
import numpy as np
import math
import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import os
import pandas_market_calendars as mcal
import pandas as pd

env = 3 # 1=Production, 2=Paper, 3=Backtesting

if env == 3:
    globalCurrentBalances = {'cashBalance': 1000, 'longMarketValue':0, 'liquidationValue': 1000}
    globalCurrentPositions = {} #{'UPRO':{'shares': 0, 'value': 0}, 'SPXU':{'shares': 0, 'value': 0}}
    globalTradeHistory = pd.DataFrame(columns=['Datetime', 'Equity', 'Qty', 'Price', 'Value'])
    globalSPYHistory = pd.DataFrame(columns=['Datetime', 'Equity', 'Qty', 'Price', 'Value'])

def getCurrentBalances(client):
    if env == 3: return globalCurrentBalances

    account=dict(client.account_details_all().json()[0])
    currentBalances = account["securitiesAccount"]["currentBalances"]

    return currentBalances

def getPositions(client):
    if env == 3: return globalCurrentPositions

    linked_accounts = client.account_linked().json()
    account_hash = linked_accounts[0].get('hashValue') # this will get the first linked account
    current_positions=dict(client.account_details(account_hash, fields="positions").json())["securitiesAccount"]["positions"]
    positions = {}
    for position in current_positions:
        symbol = position["instrument"]["symbol"]
        positions[symbol] = {}
        positions[symbol]["shares"] = position["longQuantity"]
        positions[symbol]["value"] = position["marketValue"]
    return positions

def getQuotes(client, symbols_list, endDate=""):
    quotes = {}
    if env == 3:
        trade_epoch = datetime.datetime(endDate.year, endDate.month, endDate.day, 15, 0, 0, 0, tzinfo=datetime.timezone.utc).timestamp()*1000
        for symbol in symbols_list:
            quotes[symbol] = {}
            quotes[symbol]['extended'] = {}
            
            price_history_df = pd.DataFrame(client.price_history(
                symbol,
                periodType="day",
                period=1,
                frequencyType="minute",
                frequency=5,
                startDate="",
                endDate=endDate).json()["candles"])
            
            # Ensure the DataFrame is not empty and the trade_epoch exists
            if not price_history_df.empty and not price_history_df.loc[price_history_df["datetime"] == trade_epoch].empty:
                quotes[symbol]['extended']['askPrice'] = price_history_df.loc[price_history_df["datetime"] == trade_epoch]["open"].item()
            else:
                # Handle cases where price data might be missing for a symbol at a specific time
                quotes[symbol]['extended']['askPrice'] = None # Or some other default/error indicator
                print(f"Warning: Price data for {symbol} at {endDate} 15:00 UTC not found.")

    else:
        raw_quotes = client.quotes(symbols_list).json()
        # The structure of raw_quotes needs to be transformed to match the expected structure:
        # quotes[symbol]['extended']['askPrice']
        # This depends on the actual structure of raw_quotes.
        # Assuming raw_quotes is like: {'SYMBOL': {'quote': ..., 'extended': {'askPrice': ...}}}
        # Or if it's {'SYMBOL': {'askPrice': ...}} for direct market hours,
        # and extended hours data is in a specific field.
        # For now, let's assume a structure that might align or would need adjustment.
        for symbol in symbols_list:
            quotes[symbol] = {}
            quotes[symbol]['extended'] = {}
            if symbol in raw_quotes and 'extendedMarket' in raw_quotes[symbol] and raw_quotes[symbol]['extendedMarket'] is not None and 'askPrice' in raw_quotes[symbol]['extendedMarket']:
                 quotes[symbol]['extended']['askPrice'] = raw_quotes[symbol]['extendedMarket']['askPrice']
            elif symbol in raw_quotes and 'askPrice' in raw_quotes[symbol]: # Fallback to regular market ask price if extended is not available
                 quotes[symbol]['extended']['askPrice'] = raw_quotes[symbol]['askPrice']
            else:
                 quotes[symbol]['extended']['askPrice'] = None # Or handle error
                 print(f"Warning: Quote data for {symbol} not found or incomplete.")
    return quotes

def placeOrders(client,orders,endDate):
    for equity in orders:
        orderType = orders[equity]['type']
        orderQty = orders[equity]['qty']
        orderPrice = orders[equity]['askPrice']
        orderValue = orderQty * orderPrice
        if orderType == 'SELL':
            globalCurrentPositions[equity]['shares'] -= orderQty
            if globalCurrentPositions[equity]['shares'] <= 0: del globalCurrentPositions[equity]
            globalCurrentBalances['cashBalance'] += orderValue
            globalCurrentBalances['longMarketValue'] -= orderValue
            globalTradeHistory.loc[len(globalTradeHistory)] = [endDate,equity,-1*orderQty,orderPrice,orderValue]
        if orderType == 'BUY':
            globalCurrentPositions[equity] = {}
            globalCurrentPositions[equity]['shares'] += orderQty
            globalCurrentBalances['cashBalance'] -= orderValue
            globalCurrentBalances['longMarketValue'] += orderValue
            globalTradeHistory.loc[len(globalTradeHistory)] = [endDate,equity,-1*orderQty,orderPrice,orderValue]            

    return 

def getThresholds():
    thresholds = {}
    thresholds["VIX_HIGH"] = 20
    thresholds["VIX_LOW"] = 17
    thresholds["SPY_HIGH"] = 0.005
    thresholds["SPY_LOW"] = -0.005
    return thresholds

def getMarkers(client, end_date, prev_date, thresholds, volatility_symbol, benchmark_symbol):
    markers = {}

    # Get appropriate timestamps
    open_epoch = datetime.datetime(end_date.year, end_date.month, end_date.day, 14, 30, 0, 0, tzinfo=datetime.timezone.utc).timestamp()*1000
    open_epoch_prev = datetime.datetime(prev_date.year, prev_date.month, prev_date.day, 14, 30, 0, 0, tzinfo=datetime.timezone.utc).timestamp()*1000
    trade_epoch = datetime.datetime(end_date.year, end_date.month, end_date.day, 15, 0, 0, 0, tzinfo=datetime.timezone.utc).timestamp()*1000

    # Use volatility_symbol (e.g., $VIX)
    volatility_df = pd.DataFrame(client.price_history(
        volatility_symbol,
        periodType="day",
        period=1,
        frequencyType="minute",
        frequency=5,
        startDate="",
        endDate=end_date).json()["candles"])

    markers["VIX_OPEN"] = volatility_df.loc[volatility_df["datetime"] == trade_epoch]["open"].item() # Key remains "VIX_OPEN" for now as thresholds use it
    if markers["VIX_OPEN"] > thresholds["VIX_HIGH"]: markers["VOLITILE_MARKET"] = 1
    elif markers["VIX_OPEN"] < thresholds["VIX_LOW"]: markers["VOLITILE_MARKET"] = -1
    else: markers["VOLITILE_MARKET"] = 0

    # Use benchmark_symbol (e.g., SPY)
    benchmark_df = pd.DataFrame(client.price_history(
        benchmark_symbol,
        periodType="day",
        period=2, # Fetches data for end_date and the day before
        frequencyType="minute",
        frequency=5,
        startDate="", # Not needed if period is specified
        endDate=end_date).json()["candles"])

    markers["SPY_OPEN"] = benchmark_df.loc[benchmark_df["datetime"] == open_epoch]["open"].item() # Key remains "SPY_OPEN" for now
    markers["SPY_OPEN_PREV"] = benchmark_df.loc[benchmark_df["datetime"] == open_epoch_prev]["open"].item() # Key remains "SPY_OPEN_PREV"
    markers["SPY_CHANGE"] = (markers["SPY_OPEN"] - markers["SPY_OPEN_PREV"])/markers["SPY_OPEN_PREV"] # Key remains "SPY_CHANGE"

    if markers["SPY_CHANGE"] > thresholds["SPY_HIGH"]: markers["SPY_BOOM"] = 1
    elif markers["SPY_CHANGE"] < thresholds["SPY_LOW"]: markers["SPY_BOOM"] = -1
    else: markers["SPY_BOOM"] = 0

    if markers["VOLITILE_MARKET"]== 1:
        if markers["SPY_BOOM"] != -1:
            markers["BUY_SELL"] = -1
        else: markers["BUY_SELL"] = 0
    else:
        if markers["SPY_BOOM"] != 1:
            markers["BUY_SELL"] = 1
        else: markers["BUY_SELL"] = 0

    if env !=1: 
        price = markers['SPY_OPEN'] # This still uses 'SPY_OPEN' key, which is now populated by benchmark_symbol's open price
        qty = math.floor(1000/price) # Assuming 1000 is a fixed amount for this comparison
        value = price * qty
        # Log with the actual benchmark_symbol used
        globalSPYHistory.loc[len(globalSPYHistory)] = [datetime.datetime.fromtimestamp(open_epoch/1000), benchmark_symbol, qty, price, value]
    return markers

def rebalance(client, markers, long_etf_symbol, short_etf_symbol, endDate=""):
    # BUY/SELL says BUY:
    #   We're already LONG --> do nothing
    #   We're already SHORT:
    #       BUY the addition of SHORT, plus new LONGS
    #   We have nothing:
    #       New LONGS
    # BUY/SELL says SELL:
    #   We're already SHORT --> do nothing
    #   We're already Long:
    #       SELL the addition of LONG, plus new SHORTS
    #   We have nothing:
    #       New SHORTS
    # BUY/SELL says Neutral:
    #   Do nothing?

    currentBalances = getCurrentBalances(client)
    positions = getPositions(client)
    
    # Get quotes for the specified long and short ETFs
    quotes = getQuotes(client, [long_etf_symbol, short_etf_symbol], endDate)

    # Ensure that quotes for the required symbols are available
    if quotes[long_etf_symbol]['extended']['askPrice'] is None or \
       quotes[short_etf_symbol]['extended']['askPrice'] is None:
        print(f"Error: Missing quote data for {long_etf_symbol} or {short_etf_symbol} at {endDate}. Skipping rebalance.")
        return {} # Return empty orders if essential data is missing

    askPriceLong = quotes[long_etf_symbol]['extended']['askPrice']
    askPriceShort = quotes[short_etf_symbol]['extended']['askPrice']
    orders = {}

    if markers["BUY_SELL"] == 1: # Signal to buy long_etf_symbol (go long)
        if askPriceLong == 0: # Avoid division by zero
            print(f"Warning: askPrice for {long_etf_symbol} is 0 for {endDate}. Cannot calculate shares.")
            return {}
        shares = math.floor(currentBalances['liquidationValue'] / askPriceLong)
        if long_etf_symbol in positions: # Already long long_etf_symbol
            return {} # Do nothing
        elif short_etf_symbol in positions: # Currently short short_etf_symbol, need to sell short_etf_symbol and buy long_etf_symbol
            orders[short_etf_symbol] = {'type': 'SELL', 'qty': positions[short_etf_symbol]['shares'], 'askPrice': askPriceShort}
            orders[long_etf_symbol] = {'type': 'BUY', 'qty': shares, 'askPrice': askPriceLong}
        else: # No positions, buy long_etf_symbol
            orders[long_etf_symbol] = {'type': 'BUY', 'qty': shares, 'askPrice': askPriceLong}
    elif markers['BUY_SELL'] == -1: # Signal to buy short_etf_symbol (go short)
        if askPriceShort == 0: # Avoid division by zero
            print(f"Warning: askPrice for {short_etf_symbol} is 0 for {endDate}. Cannot calculate shares.")
            return {}
        shares = math.floor(currentBalances['liquidationValue'] / askPriceShort)
        if short_etf_symbol in positions: # Already short short_etf_symbol
            return {} # Do nothing
        elif long_etf_symbol in positions: # Currently long long_etf_symbol, need to sell long_etf_symbol and buy short_etf_symbol
            orders[long_etf_symbol] = {'type': 'SELL', 'qty': positions[long_etf_symbol]['shares'], 'askPrice': askPriceLong}
            orders[short_etf_symbol] = {'type': 'BUY', 'qty': shares, 'askPrice': askPriceShort}
        else: # No positions, buy short_etf_symbol
            orders[short_etf_symbol] = {'type': 'BUY', 'qty': shares, 'askPrice': askPriceShort}

    return orders

def main():
    # place app key and app secret in the .env file
    load_dotenv()  # load environment variables from .env file
    client = schwabdev.Client(os.getenv('app_key'), os.getenv('app_secret'), os.getenv('callback_url'), verbose=True)

    # Set Thresholds
    thresholds = getThresholds()

    # Create a calendar
    end_date = datetime.date.today()
    end_date = datetime.datetime(2025,5,13)
    start_date = end_date - relativedelta(months=1)
    nyse = mcal.get_calendar('NYSE')
    early = nyse.schedule(start_date, end_date)
    date_index = mcal.date_range(early,frequency='1D')

    if env == 3: #backtesting
        # Define the ETF pair for the strategy
        long_etf = "UPRO"
        short_etf = "SPXU"
        # Define market condition symbols
        volatility_index_symbol = "$VIX"
        market_benchmark_symbol = "SPY"
        for index, value in enumerate(date_index, start=1):
            endDate = date_index[index]
            prevDate = date_index[index-1]
            markers = getMarkers(client, endDate, prevDate, thresholds, volatility_index_symbol, market_benchmark_symbol)
            orders = rebalance(client, markers, long_etf, short_etf, endDate)
            placeOrders(client,orders,endDate)
            print(endDate)
            print(markers)
            print(orders)
            print(globalCurrentBalances)

    else: #production
        # Define the ETF pair for the strategy
        long_etf = "UPRO"
        short_etf = "SPXU"
        # Define market condition symbols
        volatility_index_symbol = "$VIX"
        market_benchmark_symbol = "SPY"
        endDate = date_index[-1]
        prevDate = date_index[-2]

        markers = getMarkers(client, endDate, prevDate, thresholds, volatility_index_symbol, market_benchmark_symbol)
        orders = rebalance(client, markers, long_etf, short_etf, endDate)
        placeOrders(client,orders,endDate)

#    if env==3: #Backtesting
#        print(globalTradeHistory)
#        print(globalSPYHistory)



if __name__ == '__main__':
    main() 