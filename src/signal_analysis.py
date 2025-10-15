import yfinance as yf
import pandas as pd
import numpy as np

"""
def get_stock_returns(ticker, start="2025-10-03"):
    # Force consistent structure
    data = yf.download(ticker, start=start, auto_adjust=False, group_by='column')
    
    # If yfinance returns multi-level columns, flatten them
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)
    
    data["Return"] = data["Adj Close"].pct_change()
    return data

def merge_sentiment_with_returns(df_sentiment, ticker):
    start = df_sentiment["publishedAt"].min()[:10]
    df_price = get_stock_returns(ticker, start=start)
    df_price["date"] = df_price.index.date
    df_sentiment = df_sentiment.assign(date=pd.to_datetime(df_sentiment["publishedAt"]).dt.date).drop(columns="publishedAt")
    
    merged = pd.merge(df_sentiment, df_price, on="date", how="inner")
    return merged
"""
def get_stock_returns(ticker, start_date, end_date=None):
    """
    Fetch historical stock data and compute daily returns.
    """
    data = yf.download(ticker, start=str(start_date), end=str(end_date), auto_adjust=False, group_by='column')

    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)

    if "Adj Close" not in data.columns and "Close" in data.columns:
        data["Adj Close"] = data["Close"]

    data["Return"] = data["Adj Close"].pct_change()
    data["date"] = data.index.date
    return data
"""
def merge_sentiment_with_returns(df_sentiment, ticker, start_date, end_date=None):
    df_price = get_stock_returns(ticker, start_date, end_date)

    if "publishedAt" in df_sentiment.columns:
        df_sentiment = (
            df_sentiment
            .assign(date=pd.to_datetime(df_sentiment["publishedAt"]).dt.date)
            .drop(columns="publishedAt")
        )


    merged = pd.merge(df_sentiment, df_price, on="date", how="inner")
    return merged
"""
def merge_sentiment_with_returns(df_sentiment, ticker, start_date, end_date=None):
    df_price = get_stock_returns(ticker, start_date, end_date)

    # Ensure consistent date format in both dataframes
    df_sentiment["date"] = pd.to_datetime(df_sentiment["date"]).dt.normalize()
    df_price["date"] = pd.to_datetime(df_price["date"]).dt.normalize()

    merged = pd.merge(df_price, df_sentiment, on="date", how="inner")

    # Sort by date for clarity
    merged = merged.sort_values("date").reset_index(drop=True)
    return merged

def normalize_sentiment(df):
    if "date" not in df.columns and "publishedAt" in df.columns:
        df["date"] = pd.to_datetime(df["publishedAt"], errors="coerce").dt.date
    mapping = {"positive": 1, "neutral": 0, "negative": -1}
    df["sentiment_score"] = df["sentiment"].str.lower().map(mapping)
    df_daily = df.groupby("date", as_index=False).agg(
        sentiment_score=("sentiment_score", "mean"),
        confidence=("confidence", "mean")
    )
    return df_daily