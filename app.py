import os, sys
import pandas as pd
import matplotlib.pyplot as plt
import gradio as gr
from datetime import datetime, date
import io
import tempfile

# --- Import project modules ---
from src.news_fetcher import fetch_news_auto
from src.sentiment_model import analyze_sentiment
from src.signal_analysis import merge_sentiment_with_returns, normalize_sentiment

def normalize_date(d):
    """Handle datetime, date, float (timestamp), or string inputs safely."""
    if isinstance(d, (datetime, date)):
        return d.date() if isinstance(d, datetime) else d
    elif isinstance(d, (float, int)):
        return datetime.fromtimestamp(d).date()
    elif isinstance(d, str):
        try:
            return datetime.strptime(d.split("T")[0], "%Y-%m-%d").date()
        except ValueError:
            return None
    else:
        return None

def run_analysis(ticker, start_date, end_date):
    start_date = normalize_date(start_date)
    end_date = normalize_date(end_date)

    if not start_date or not end_date:
        return "Please select both start and end dates in the correct format.", None, pd.DataFrame()

    if start_date > end_date:
        return "Start date must be before end date.", None, pd.DataFrame()
    
    # 1️⃣ Fetch news
    df_news = fetch_news_auto(ticker, start_date=start_date, end_date=end_date)
    if df_news.empty:
        return "No news found in this date range.", None, pd.DataFrame()

    # 2️⃣ Analyze sentiment (FinBERT)
    df_news = analyze_sentiment(df_news)
    print("After sentiment analysis:")
    print(df_news.head())
    print(df_news.columns)

    # 3️⃣ Normalize & aggregate daily sentiment
    df_daily = normalize_sentiment(df_news)

    # 4️⃣ Merge with returns
    merged = merge_sentiment_with_returns(df_daily, ticker, start_date, end_date)
    if merged.empty:
        return "No overlapping trading days found.", None, pd.DataFrame()
    print(merged[["date", "sentiment_score", "Return"]].head())
    print(merged[["sentiment_score", "Return"]].describe())
    corr = merged["sentiment_score"].corr(merged["Return"])
    status_msg = f"Correlation between sentiment and return: {corr:.2f}"


    # 5️⃣ Plot Sentiment vs Returns
    fig, ax1 = plt.subplots(figsize=(8, 5))
    ax2 = ax1.twinx()

    ax1.plot(merged["date"], merged["Return"], color="blue", label="Stock Return")
    ax2.plot(merged["date"], merged["sentiment_score"], color="orange", label="Sentiment")

    ax1.set_xlabel("Date")
    ax1.set_ylabel("Daily Return", color="blue")
    ax2.set_ylabel("Sentiment Score", color="orange")
    plt.title(f"{ticker}: Sentiment vs Returns")
    fig.tight_layout()


    return status_msg, fig, merged


# --- Gradio UI ---
with gr.Blocks(theme="soft") as demo:
    gr.Markdown("# Financial News Sentiment Dashboard")
    gr.Markdown("Analyze sentiment vs market returns using FinBERT and Yahoo Finance data.")

    with gr.Row():
        ticker = gr.Textbox(label="Stock Ticker (e.g. AAPL, TSLA, NVDA)", value="AAPL")
    with gr.Row():
        start_date = gr.DateTime(label="Start Date", include_time=False)
        end_date = gr.DateTime(label="End Date", include_time=False)


    run_btn = gr.Button("Run Analysis")

    output_text = gr.Textbox(label="Status", interactive=False)
    output_plot = gr.Plot(label="Sentiment vs Returns")
    output_table = gr.Dataframe(label="Merged Dataset")
    download_btn = gr.DownloadButton(label="Download CSV")

    """
    def prepare_csv(merged):
        if merged is None or merged.empty:
            return None
        return merged.to_csv(index=False)
    
    def prepare_csv(df):
        if df is None or df.empty:
            return None
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        return csv_buffer
    """

    def prepare_csv(df):
        if df is None or df.empty:
            return None

        # Create a temporary file
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        df.to_csv(tmp.name, index=False)
        return tmp.name

    run_btn.click(
        fn=run_analysis,
        inputs=[ticker, start_date, end_date],
        outputs=[output_text, output_plot, output_table],
    )

    output_table.change(fn=prepare_csv, inputs=output_table, outputs=download_btn)

if __name__ == "__main__":
    demo.launch()

