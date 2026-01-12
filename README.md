# Financial News Sentiment Signals

**Extract sentiment from financial news → Generate trading/investment signals**

A practical pipeline for turning financial news headlines and articles into quantifiable sentiment scores and trading signals.

Current status: **Work in progress / Experimental**

## 🎯 Project Goals

- Collect real-time / historical financial news
- Perform sentiment analysis using modern NLP techniques
- Create actionable sentiment-based signals for stocks, sectors or market indices
- Provide visualization & backtesting capabilities
- Eventually support multiple models & comparison (VADER → FinBERT → LLM-based)

## 📁 Repository Structure
financial_news_sentiment_signals/
├── notebooks/              # Exploratory data analysis & model experiments
│   ├── 01_data_collection.ipynb
│   ├── 02_sentiment_analysis.ipynb
│   └── 03_signal_generation_backtest.ipynb
├── src/                    # Production-ready modular code
│   ├── data/               # News scrapers & APIs connectors
│   ├── sentiment/          # Sentiment models & preprocessors
│   ├── signals/            # Signal logic & position sizing
│   └── utils/              # Logging, config, helpers
├── app.py                  # Gradio / Streamlit demo interface (choose your flavor)
├── requirements.txt        # Core dependencies
├── .gitignore
└── README.md

## 🚀 Quick Start (Demo)

```bash
# 1. Clone the repo
git clone https://github.com/sarenechoudhury/financial_news_sentiment_signals.git
cd financial_news_sentiment_signals

# 2. Create & activate virtual environment (recommended)
python -m venv venv
source venv/bin/activate    # Linux/macOS
venv\Scripts\activate       # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Launch the web demo
streamlit run app.py        # or
python app.py               # depending on your implementation
