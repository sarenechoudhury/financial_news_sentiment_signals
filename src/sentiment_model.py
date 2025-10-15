from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline

def load_finbert():
    tokenizer = AutoTokenizer.from_pretrained("yiyanghkust/finbert-tone")
    model = AutoModelForSequenceClassification.from_pretrained("yiyanghkust/finbert-tone")
    return pipeline(
        "sentiment-analysis",
        model=model,
        tokenizer=tokenizer,
        truncation=True,      # already good
        max_length=512,       # ✅ add this
        padding=True          # ✅ optional, safer for batch mode
    )

def analyze_sentiment(df, text_col="title"):
    nlp = load_finbert()
    texts = df[text_col].fillna("").tolist()
    preds = nlp(texts, batch_size=8, truncation=True)

    df["sentiment"] = [p["label"] for p in preds]
    df["confidence"] = [p["score"] for p in preds]

    sentiment_map = {"positive": 1, "neutral": 0, "negative": -1}
    df["sentiment_score"] = df.apply(
        lambda x: sentiment_map.get(x["sentiment"].lower(), 0) * x["confidence"], axis=1
    )
    return df
