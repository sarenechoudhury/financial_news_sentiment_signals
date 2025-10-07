import requests
import pandas as pd
from datetime import datetime, timedelta, date
import os
from io import StringIO
from dotenv import load_dotenv
load_dotenv()

NEWS_API_KEY = os.getenv("NEWS_API_KEY")

def fetch_news(query, start_date, end_date=None, language="en", page_size=100):
    """
    Fetch news articles between two dates using the NewsAPI.
    """
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    url = "https://newsapi.org/v2/everything"
    params = {
        "q": query,
        "from": start_date,
        "to": end_date,
        "language": language,
        "sortBy": "publishedAt",
        "pageSize": page_size,
        "apiKey": NEWS_API_KEY
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    articles = data.get("articles", [])
    if not articles:
        print(f"No news articles found for '{query}' between {start_date} and {end_date}.")
        return pd.DataFrame()

    df = pd.DataFrame(articles)
    df = df.rename(columns={"publishedAt": "date"})
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df

def _fetch_gdelt_chunk(query, start_date, end_date, maxrecords=250):
    """
    Fetch a single small GDELT time window (internal helper).
    """
    try:
        url = (
            "https://api.gdeltproject.org/api/v2/doc/doc?"
            f"query={query}&mode=artlist&maxrecords={maxrecords}&format=csv&"
            f"STARTDATETIME={start_date.strftime('%Y%m%d000000')}&"
            f"ENDDATETIME={end_date.strftime('%Y%m%d235959')}"
        )

        response = requests.get(url, timeout=20)
        response.raise_for_status()
        df = pd.read_csv(StringIO(response.text))
        df.columns = [c.strip() for c in df.columns]

        # Handle GDELT error messages
        if "Invalid query" in df.columns[0]:
            print(f"⚠️ Invalid GDELT range: {start_date}–{end_date}")
            return pd.DataFrame()

        # Identify columns dynamically
        title_col = next((c for c in ["DocumentTitle", "Title", "DocTitle"] if c in df.columns), None)
        date_col = next((c for c in ["Date", "DATE", "DATEADDED", "PublishDate"] if c in df.columns), None)
        tone_col = next((c for c in ["DocumentTone", "Tone", "ToneAvg"] if c in df.columns), None)
        url_col = next((c for c in ["DocumentIdentifier", "URL", "Link"] if c in df.columns), None)
        source_col = next((c for c in ["SourceCommonName", "Source", "Domain"] if c in df.columns), None)

        if not title_col or not date_col:
            return pd.DataFrame()

        keep_cols = [c for c in [title_col, url_col, tone_col, source_col, date_col] if c]
        df = df[keep_cols]

        rename_map = {
            title_col: "title",
            url_col: "url",
            tone_col: "sentiment",
            source_col: "source",
            date_col: "publishedAt"
        }
        df.rename(columns=rename_map, inplace=True)

        if "sentiment" in df.columns:
            df["sentiment"] = df["sentiment"].astype(float) / 100.0
        else:
            df["sentiment"] = 0.0

        df["publishedAt"] = pd.to_datetime(df["publishedAt"], errors="coerce").dt.date
        return df.dropna(subset=["title", "publishedAt"])

    except Exception as e:
        print(f"⚠️ GDELT chunk fetch failed: {e}")
        return pd.DataFrame()

    
def fetch_news_gdelt(query, start_date, end_date, maxrecords=250):
    """
    Fetch GDELT news data in chunks of 90 days to avoid API limits.
    """
    GDELT_MIN_DATE = date(2015, 1, 1)
    MAX_RANGE_DAYS = 20
    start_date = max(start_date, GDELT_MIN_DATE)

    all_dfs = []
    current = start_date

    while current < end_date:
        chunk_end = min(current + timedelta(days=MAX_RANGE_DAYS), end_date)
        print(f"🗂 Fetching GDELT chunk {current} → {chunk_end}")
        df_chunk = _fetch_gdelt_chunk(query, current, chunk_end, maxrecords)
        if not df_chunk.empty:
            all_dfs.append(df_chunk)
        current = chunk_end  # ✅ progress forward, no recursion

    if all_dfs:
        df = pd.concat(all_dfs, ignore_index=True)
        print(f"✅ Combined {len(df)} articles from GDELT for '{query}' ({start_date} → {end_date})")
        return df
    else:
        print("⚠️ No valid GDELT data found.")
        return pd.DataFrame()  # ✅ not None




""" 
def fetch_news_gdelt_gkg(query, start_date, end_date):

    try:
        url = (
            "https://api.gdeltproject.org/api/v2/doc/timeseries?"
            f"query={query}&mode=ToneChart&format=csv&"
            f"STARTDATETIME={start_date.strftime('%Y%m%d000000')}&"
            f"ENDDATETIME={end_date.strftime('%Y%m%d235959')}"
        )

        response = requests.get(url, timeout=30)
        response.raise_for_status()

        df = pd.read_csv(StringIO(response.text))
        df.columns = [c.strip() for c in df.columns]

        if "Date" not in df.columns or "Tone" not in df.columns:
            print("⚠️ No valid tone data found in GDELT response.")
            print("Columns:", df.columns.tolist())
            return pd.DataFrame()

        df.rename(columns={"Date": "publishedAt", "Tone": "sentiment"}, inplace=True)
        df["publishedAt"] = pd.to_datetime(df["publishedAt"], errors="coerce").dt.date
        df["sentiment"] = df["sentiment"].astype(float) / 100.0
        df["title"] = f"GDELT Tone for {query}"

        print(f"✅ Retrieved {len(df)} daily tone records for '{query}' ({start_date} → {end_date})")
        return df[["title", "sentiment", "publishedAt"]].dropna()

    except Exception as e:
        print(f"⚠️ GDELT GKG fetch failed: {e}")
        return pd.DataFrame()

    
def fetch_news_auto(query, start_date, end_date):

    days = (end_date - start_date).days
    print(f"📅 Date range selected: {days} days")

    if days > 30:
        print("🗂 Trying GDELT Doc API (recent historical)...")
        df = fetch_news_gdelt(query, start_date, end_date)
        if df.empty:
            print("📊 Falling back to GDELT GKG endpoint (deep historical)...")
            df = fetch_news_gdelt_gkg(query, start_date, end_date)
        return df
    else:
        print("📰 Using NewsAPI for recent data...")
        return fetch_news(query, start_date=start_date, end_date=end_date)

"""

def fetch_news_auto(query, start_date, end_date):
    """
    Automatically choose NewsAPI (recent) or GDELT (historical)
    based on the date range, with NewsAPI fallback for empty GDELT results.
    """
    days = (end_date - start_date).days
    print(f"📅 Date range selected: {days} days")

    if days > 30:
        print("🗂 Using GDELT Doc API for historical news data...")
        df = _fetch_gdelt_chunk(query, start_date, end_date)

        # ✅ Fallback protection
        if df is None or df.empty:
            print("⚠️ No valid GDELT data found. Falling back to NewsAPI.")
            df = fetch_news(query, start_date=start_date, end_date=end_date)
        else:
            print(f"✅ Retrieved {len(df)} historical articles from GDELT.")

        return df

    else:
        print("📰 Using NewsAPI for recent data...")
        df = fetch_news(query, start_date=start_date, end_date=end_date)
        print(f"✅ Retrieved {len(df)} recent articles from NewsAPI.")
        return df
