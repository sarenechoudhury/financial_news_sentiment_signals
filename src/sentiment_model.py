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
    #results = df[text_col].apply(lambda x: nlp(x)[0] if isinstance(x, str) else {"label": "neutral", "score": 0})
    texts = df[text_col].fillna("").tolist()
    preds = nlp(texts, batch_size=8, truncation=True)
    df["sentiment"] = [p["label"] for p in preds]
    df["confidence"] = [p["score"] for p in preds]
    #df["sentiment"] = results.apply(lambda x: x["label"])
    #df["confidence"] = results.apply(lambda x: x["score"])
    return df
