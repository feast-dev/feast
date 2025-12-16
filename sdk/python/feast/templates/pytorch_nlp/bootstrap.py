import pathlib
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from feast.file_utils import replace_str_in_file


def create_sentiment_data(num_samples: int = 1000) -> pd.DataFrame:
    """Generate sentiment analysis dataset using BERTweet classifier predictions."""

    # Diverse realistic text samples from various domains
    sample_texts = [
        # Social media / tweets style
        "Having an amazing day at the beach with friends!",
        "Traffic is horrible today, going to be late for everything",
        "Just finished my morning coffee, time to start work",
        "This weather is perfect for a weekend getaway",
        "Frustrated with this constant construction noise",
        "The sunset tonight is absolutely breathtaking",
        "Finally got tickets to the concert I wanted!",
        "My phone battery died right when I needed it most",
        "Loving the new album that just dropped today",
        "Can't believe how long this line is taking",

        # Product reviews / opinions
        "This phone has incredible battery life and camera quality",
        "The delivery was late and packaging was damaged",
        "Pretty standard laptop, does what it's supposed to do",
        "Amazing customer service, resolved my issue quickly",
        "The quality is terrible for the price, disappointed",
        "Works fine, good value for money, as described",
        "Best purchase I've made this year, highly recommend",
        "Returned this item, didn't work as advertised",
        "Decent product but could be better for the cost",
        "Exceeded my expectations, will buy again",

        # General experiences
        "Learning something new always makes me happy",
        "Dealing with technical issues is draining my energy",
        "The meeting went okay, covered the basic topics",
        "Excited about the weekend plans with family",
        "Another day of debugging code, the struggle continues",
        "Really enjoying this book I started reading",
        "The restaurant service was disappointing tonight",
        "Nothing special planned, just a quiet evening",
        "Great presentation today, audience was engaged",
        "Feeling overwhelmed with all these deadlines",

        # News / current events style
        "The new policy changes will benefit small businesses",
        "This decision could have negative environmental impact",
        "The research findings are interesting but inconclusive",
        "Economic indicators suggest stable growth ahead",
        "Mixed reactions to the announcement yesterday",
        "The data shows promising results across demographics",
        "Public opinion remains divided on this issue",
        "Significant improvements in the healthcare system",
        "Concerns raised about the new regulations",
        "Standard quarterly results meeting projections"
    ]

    # Try to use BERTweet sentiment classifier, fallback to rule-based if not available
    try:
        from transformers import pipeline
        print("   🤖 Loading BERTweet sentiment classifier...")

        # Use BERTweet model specifically trained for Twitter sentiment
        sentiment_classifier = pipeline(
            "sentiment-analysis",
            model="finiteautomata/bertweet-base-sentiment-analysis",
            return_all_scores=True
        )
        use_real_classifier = True
        print("   ✅ BERTweet sentiment classifier loaded successfully")

    except ImportError:
        print("   ⚠️  Transformers not available, using rule-based sentiment")
        print("   💡 For real classifier: pip install transformers torch")
        use_real_classifier = False
    except Exception as e:
        print(f"   ⚠️  Could not load BERTweet ({e}), using rule-based sentiment")
        use_real_classifier = False

    # Generate data
    data = []
    # Use current time and generate data within the last 30 days
    now = datetime.now()
    start_date = now - timedelta(days=30)

    # Extend sample texts by cycling through them to reach num_samples
    all_texts = (sample_texts * (num_samples // len(sample_texts) + 1))[:num_samples]

    for i, base_text in enumerate(all_texts):
        # Add some realistic variations to make texts more diverse
        text = base_text

        # Occasionally add emphasis or emoji
        if random.random() < 0.15:
            text = text + "!"
        elif random.random() < 0.1:
            text = text + "..."
        elif random.random() < 0.08:
            if any(word in text.lower() for word in ["amazing", "love", "great", "best", "happy", "excited"]):
                text = text + " 😊"
        elif random.random() < 0.08:
            if any(word in text.lower() for word in ["terrible", "disappointed", "frustrated", "horrible"]):
                text = text + " 😞"

        # Get sentiment from real classifier or fallback
        if use_real_classifier:
            try:
                predictions = sentiment_classifier(text)[0]

                # Find highest confidence prediction
                best_pred = max(predictions, key=lambda x: x['score'])
                sentiment_label = best_pred['label'].upper()  # BERTweet returns 'POS', 'NEG', 'NEU'
                sentiment_score = best_pred['score']

                # Map BERTweet labels to our format
                label_map = {"POS": "positive", "NEG": "negative", "NEU": "neutral"}
                sentiment_label = label_map.get(sentiment_label, sentiment_label.lower())

            except Exception as e:
                print(f"   ⚠️  Classifier error for text {i}: {e}")
                # Fallback to simple rule-based
                sentiment_label, sentiment_score = _rule_based_sentiment(text)
        else:
            # Rule-based fallback
            sentiment_label, sentiment_score = _rule_based_sentiment(text)

        # Generate engineered features
        text_length = len(text)
        word_count = len(text.split())
        exclamation_count = text.count('!')
        caps_ratio = sum(1 for c in text if c.isupper()) / len(text) if text else 0
        emoji_count = sum(1 for c in text if ord(c) > 127)  # Simple emoji detection

        # Random timestamp within the past 30 days
        days_offset = random.randint(0, 30)
        hours_offset = random.randint(0, 23)
        minutes_offset = random.randint(0, 59)
        event_timestamp = start_date + timedelta(days=days_offset, hours=hours_offset, minutes=minutes_offset)

        data.append({
            'text_id': f'text_{i:04d}',
            'user_id': f'user_{random.randint(1, 100):03d}',
            'text_content': text,
            'sentiment_label': sentiment_label,
            'sentiment_score': round(sentiment_score, 3),
            'text_length': text_length,
            'word_count': word_count,
            'exclamation_count': exclamation_count,
            'caps_ratio': round(caps_ratio, 3),
            'emoji_count': emoji_count,
            'event_timestamp': pd.Timestamp(event_timestamp, tz='UTC'),
            'created': pd.Timestamp.now(tz='UTC').round('ms')
        })

    df = pd.DataFrame(data)

    # Calculate user-level aggregations
    user_stats = df.groupby('user_id').agg({
        'sentiment_score': 'mean',
        'text_id': 'count',
        'text_length': 'mean'
    }).rename(columns={
        'sentiment_score': 'user_avg_sentiment',
        'text_id': 'user_text_count',
        'text_length': 'user_avg_text_length'
    }).round(3).reset_index()

    # Merge user stats back to main dataframe
    df = df.merge(user_stats, on='user_id', how='left')

    return df


def _rule_based_sentiment(text: str) -> tuple[str, float]:
    """Fallback rule-based sentiment analysis when BERTweet is not available."""
    text_lower = text.lower()

    positive_words = ["amazing", "love", "great", "excellent", "wonderful", "perfect",
                     "outstanding", "fantastic", "best", "happy", "good", "awesome",
                     "incredible", "beautiful", "excited", "enjoying"]
    negative_words = ["terrible", "horrible", "awful", "worst", "bad", "disappointed",
                     "frustrated", "angry", "sad", "broken", "failed", "poor",
                     "draining", "overwhelming", "disappointing"]

    positive_count = sum(1 for word in positive_words if word in text_lower)
    negative_count = sum(1 for word in negative_words if word in text_lower)

    if positive_count > negative_count:
        return "positive", random.uniform(0.6, 0.9)
    elif negative_count > positive_count:
        return "negative", random.uniform(0.6, 0.9)
    else:
        return "neutral", random.uniform(0.5, 0.7)


def bootstrap():
    """Bootstrap the pytorch_nlp template with sample data."""
    repo_path = pathlib.Path(__file__).parent.absolute() / "feature_repo"
    raw_project_name = pathlib.Path(__file__).parent.absolute().name

    # Sanitize project name for SQLite compatibility (no hyphens allowed)
    project_name = raw_project_name.replace('-', '_')
    if project_name != raw_project_name:
        print(f"   ℹ️  Project name sanitized: '{raw_project_name}' → '{project_name}'")
        print("   💡 SQLite table names cannot contain hyphens")

    data_path = repo_path / "data"
    data_path.mkdir(exist_ok=True)

    print("🎭 Setting up sentiment analysis data for PyTorch NLP demonstration...")

    parquet_file = data_path / "sentiment_data.parquet"

    # Generate sentiment data
    print("   📝 Generating synthetic sentiment analysis dataset...")
    df = create_sentiment_data(num_samples=1000)

    # Save to parquet
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_file)

    print(f"   ✅ Created sentiment dataset with {len(df)} samples")
    print(f"   📊 Sentiment distribution:")
    sentiment_counts = df['sentiment_label'].value_counts()
    for sentiment, count in sentiment_counts.items():
        print(f"      - {sentiment.capitalize()}: {count} samples")

    # Replace template placeholders
    example_py_file = repo_path / "example_repo.py"
    replace_str_in_file(example_py_file, "%PROJECT_NAME%", str(project_name))

    test_workflow_file = repo_path / "test_workflow.py"
    replace_str_in_file(test_workflow_file, "%PROJECT_NAME%", str(project_name))

    print("🚀 PyTorch NLP template initialized successfully!")

    print("\n🎯 To get started:")
    print(f"  1. cd {project_name}")
    print("  2. pip install -r requirements.txt")
    print(f"  3. cd feature_repo")
    print("  4. feast apply")
    print("  5. feast materialize")
    print("  6. python test_workflow.py")
    print("\n💡 This template demonstrates:")
    print("  - Text feature engineering with Feast")
    print("  - PyTorch + Hugging Face transformers integration")
    print("  - Sentiment analysis with pre-trained models")
    print("  - Online and offline feature serving")


if __name__ == "__main__":
    bootstrap()