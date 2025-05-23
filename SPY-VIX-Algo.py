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

def getQuotes(client, endDate=""):
    if env == 3: 
        quotes = {}
        quotes['UPRO'] = {}
        quotes['UPRO']['extended'] = {}
        quotes['SPXU'] = {}
        quotes['SPXU']['extended'] = {}

        trade_epoch = datetime.datetime(endDate.year, endDate.month, endDate.day, 15, 0, 0, 0, tzinfo=datetime.timezone.utc).timestamp()*1000

        UPRO = pd.DataFrame(client.price_history(
            "UPRO", 
            periodType="day", 
            period=1, 
            frequencyType="minute", 
            frequency=5,
            startDate="",
            endDate=endDate).json()["candles"])
        quotes['UPRO']['extended']['askPrice'] = UPRO.loc[UPRO["datetime"] == trade_epoch]["open"].item()

        SPXU = pd.DataFrame(client.price_history(
            "SPXU", 
            periodType="day", 
            period=1, 
            frequencyType="minute", 
            frequency=5,
            startDate="",
            endDate=endDate).json()["candles"])
        quotes['SPXU']['extended']['askPrice'] = SPXU.loc[SPXU["datetime"] == trade_epoch]["open"].item()
    else:
        quotes = client.quotes(["UPRO", "SPXU"]).json()
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

def getMarkers(client,end_date,prev_date,thresholds):
    markers = {}

    # Get appropriate timestamps
    open_epoch = datetime.datetime(end_date.year, end_date.month, end_date.day, 14, 30, 0, 0, tzinfo=datetime.timezone.utc).timestamp()*1000
    open_epoch_prev = datetime.datetime(prev_date.year, prev_date.month, prev_date.day, 14, 30, 0, 0, tzinfo=datetime.timezone.utc).timestamp()*1000
    trade_epoch = datetime.datetime(end_date.year, end_date.month, end_date.day, 15, 0, 0, 0, tzinfo=datetime.timezone.utc).timestamp()*1000

    vix = pd.DataFrame(client.price_history(
        "$VIX", 
        periodType="day", 
        period=1, 
        frequencyType="minute", 
        frequency=5,
        startDate="",
        endDate=end_date).json()["candles"])

    markers["VIX_OPEN"] = vix.loc[vix["datetime"] == trade_epoch]["open"].item()
    if markers["VIX_OPEN"] > thresholds["VIX_HIGH"]: markers["VOLITILE_MARKET"] = 1
    elif markers["VIX_OPEN"] < thresholds["VIX_LOW"]: markers["VOLITILE_MARKET"] = -1
    else: markers["VOLITILE_MARKET"] = 0

    spy = pd.DataFrame(client.price_history(
        "SPY", 
        periodType="day", 
        period=2, 
        frequencyType="minute", 
        frequency=5,
        startDate="",
        endDate=end_date).json()["candles"])

    markers["SPY_OPEN"] = spy.loc[spy["datetime"] == open_epoch]["open"].item()
    markers["SPY_OPEN_PREV"] = spy.loc[spy["datetime"] == open_epoch_prev]["open"].item()
    markers["SPY_CHANGE"] = (markers["SPY_OPEN"] - markers["SPY_OPEN_PREV"])/markers["SPY_OPEN_PREV"]

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
        price = markers['SPY_OPEN']
        qty = math.floor(1000/price)
        value = price * qty
        globalSPYHistory.loc[len(globalSPYHistory)] = [datetime.datetime.fromtimestamp(open_epoch/1000),'SPY',qty,price,value]
    return markers

def rebalance(client,markers,endDate=""):
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
    quotes = getQuotes(client,endDate)
    askPriceUPRO = quotes['UPRO']['extended']['askPrice']
    askPriceSPXU = quotes['SPXU']['extended']['askPrice']
    orders = {}

    if markers["BUY_SELL"] == 1:
        shares = math.floor(currentBalances['liquidationValue']/askPriceUPRO)
        if 'UPRO' in positions:
            return
        elif 'SPXU' in positions:
            orders['SPXU'] = {'type': 'SELL', 'qty': positions['SPXU']['shares'], 'askPrice': askPriceSPXU}
            orders['UPRO'] = {'type': 'BUY', 'qty': shares, 'askPrice': askPriceUPRO}
        else:
            orders['UPRO'] = {'type': 'BUY', 'qty': shares, 'askPrice': askPriceUPRO}
    elif markers['BUY_SELL'] == -1:
        shares = math.floor(currentBalances['liquidationValue']/askPriceSPXU)
        if 'SPXU' in positions:
            return
        elif 'UPRO' in positions:
            orders['UPRO'] = {'type': 'SELL', 'qty': positions['UPRO']['shares'], 'askPrice': askPriceUPRO}
            orders['SPXU'] = {'type': 'BUY', 'qty': shares, 'askPrice': askPriceSPXU}
        else:
            orders['SPXU'] = {'type': 'BUY', 'qty': shares, 'askPrice': askPriceSPXU}

    return  orders

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
        for index, value in enumerate(date_index, start=1):
            endDate = date_index[index]
            prevDate = date_index[index-1]
            markers = getMarkers(client,endDate,prevDate,thresholds)
            orders = rebalance(client,markers,endDate)
            placeOrders(client,orders,endDate)
            print(endDate)
            print(markers)
            print(orders)
            print(globalCurrentBalances)

    else: #production
        endDate = date_index[-1]
        prevDate = date_index[-2]

        markers = getMarkers(client,endDate,prevDate,thresholds)
        orders = rebalance(client,markers,endDate)
        placeOrders(client,orders,endDate)

#    if env==3: #Backtesting
#        print(globalTradeHistory)
#        print(globalSPYHistory)



if __name__ == '__main__':
    main() 