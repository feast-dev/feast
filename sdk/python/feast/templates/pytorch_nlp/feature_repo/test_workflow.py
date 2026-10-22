"""
PyTorch NLP Sentiment Analysis - Complete Test Workflow

This script demonstrates the full lifecycle of a sentiment analysis project using Feast:
1. Feature store setup and deployment
2. Historical feature retrieval for model training
3. Online feature serving for real-time inference
4. Integration with PyTorch and Hugging Face models
5. Performance evaluation and monitoring
"""

import subprocess
from datetime import datetime, timedelta

import pandas as pd

from feast import FeatureStore


def run_demo():
    """Run the complete PyTorch NLP sentiment analysis demo."""
    print("🎭 PyTorch NLP Sentiment Analysis Demo")
    print("=====================================")

    store = FeatureStore(repo_path=".")

    # 1. Deploy feature definitions
    print("\n🚀 Step 1: Deploy feature definitions")
    print("--- Run feast apply ---")
    subprocess.run(["feast", "apply"])

    # 2. Materialize features to online store
    print("\n💾 Step 2: Materialize features to online store")
    print("--- Load features into online store ---")
    store.materialize_incremental(end_date=datetime.now())

    # 3. Demonstrate historical feature retrieval for training
    print("\n📚 Step 3: Historical features for model training")
    training_data = fetch_historical_features_for_training(store)

    # 4. Simulate model training (conceptual)
    print("\n🏋️ Step 4: Simulate model training")
    simulate_model_training(training_data)

    # 5. Online feature serving for real-time inference
    print("\n⚡ Step 5: Real-time inference with online features")
    test_online_inference(store)

    # 6. Demonstrate on-demand feature views
    print("\n🔮 Step 6: On-demand sentiment prediction")
    test_on_demand_sentiment_prediction(store)

    # 7. Feature service usage
    print("\n🎯 Step 7: Feature services for model versioning")
    test_feature_services(store)

    # 8. Performance evaluation
    print("\n📊 Step 8: Model performance evaluation")
    evaluate_model_performance(store)

    print("\n✨ Demo completed successfully!")
    print("\n📖 Next steps:")
    print("  - Modify the sentiment data in data/sentiment_data.parquet")
    print("  - Experiment with different models in example_repo.py")
    print("  - Add more feature engineering transformations")
    print("  - Deploy to production with cloud providers (AWS, GCP, etc.)")


def fetch_historical_features_for_training(store: FeatureStore) -> pd.DataFrame:
    """Fetch historical features for model training with point-in-time correctness."""
    # Create entity DataFrame for training
    # In practice, this would come from your ML pipeline or data warehouse
    entity_df = pd.DataFrame.from_dict(
        {
            "text_id": [
                "text_0001",
                "text_0002",
                "text_0003",
                "text_0004",
                "text_0005",
                "text_0010",
                "text_0015",
                "text_0020",
                "text_0025",
                "text_0030",
            ],
            "user_id": [
                "user_001",
                "user_002",
                "user_001",
                "user_003",
                "user_002",
                "user_001",
                "user_004",
                "user_003",
                "user_005",
                "user_001",
            ],
            "event_timestamp": [
                datetime(2023, 6, 15, 10, 0, 0),
                datetime(2023, 6, 15, 11, 30, 0),
                datetime(2023, 6, 15, 14, 15, 0),
                datetime(2023, 6, 16, 9, 45, 0),
                datetime(2023, 6, 16, 13, 20, 0),
                datetime(2023, 6, 17, 8, 30, 0),
                datetime(2023, 6, 17, 16, 45, 0),
                datetime(2023, 6, 18, 12, 10, 0),
                datetime(2023, 6, 18, 15, 30, 0),
                datetime(2023, 6, 19, 11, 0, 0),
            ],
        }
    )

    # Fetch historical features using the training feature service
    print("   📊 Retrieving training dataset with point-in-time correctness...")
    training_df = store.get_historical_features(
        entity_df=entity_df,
        features=[
            "text_features:text_content",
            "text_features:sentiment_label",
            "text_features:sentiment_score",
            "text_features:text_length",
            "text_features:word_count",
            "text_features:exclamation_count",
            "text_features:caps_ratio",
            "text_features:emoji_count",
            "user_stats:user_avg_sentiment",
            "user_stats:user_text_count",
        ],
    ).to_df()

    print(f"   ✅ Retrieved {len(training_df)} training samples")
    print("   📋 Sample training data:")
    print(
        training_df[
            ["text_content", "sentiment_label", "text_length", "word_count"]
        ].head(3)
    )

    return training_df


def simulate_model_training(training_data: pd.DataFrame):
    """Simulate model training process (conceptual implementation)."""
    print("   🧠 Training sentiment analysis model...")

    # In a real implementation, you would:
    # 1. Split data into train/validation/test
    # 2. Tokenize text using transformers tokenizer
    # 3. Fine-tune a pre-trained model (BERT, RoBERTa, etc.)
    # 4. Evaluate performance metrics
    # 5. Save the trained model

    print(f"   📊 Training data shape: {training_data.shape}")

    if not training_data.empty:
        # Simple statistics as a proxy for training
        sentiment_dist = training_data["sentiment_label"].value_counts()
        avg_text_length = training_data["text_length"].mean()

        print("   📈 Sentiment distribution:")
        for sentiment, count in sentiment_dist.items():
            print(
                f"      {sentiment}: {count} samples ({count / len(training_data) * 100:.1f}%)"
            )

        print(f"   📏 Average text length: {avg_text_length:.1f} characters")
        print("   ✅ Model training simulation completed!")
    else:
        print("   ⚠️  No training data available")


def test_online_inference(store: FeatureStore):
    """Test online feature serving for real-time inference."""
    print("   ⚡ Testing real-time feature serving...")

    # Entity rows for online inference
    entity_rows = [
        {"text_id": "text_0001", "user_id": "user_001"},
        {"text_id": "text_0002", "user_id": "user_002"},
        {"text_id": "text_0005", "user_id": "user_002"},
    ]

    # Fetch online features
    online_features = store.get_online_features(
        features=[
            "text_features:text_content",
            "text_features:text_length",
            "text_features:word_count",
            "user_stats:user_avg_sentiment",
        ],
        entity_rows=entity_rows,
    ).to_dict()

    print("   📊 Retrieved online features:")
    for key, values in online_features.items():
        if key in ["text_content"]:
            # Truncate long text for display
            display_values = [
                str(v)[:50] + "..." if len(str(v)) > 50 else str(v) for v in values
            ]
            print(f"      {key}: {display_values}")
        else:
            print(f"      {key}: {values}")


def test_on_demand_sentiment_prediction(store: FeatureStore):
    """Test on-demand feature views for real-time sentiment prediction."""
    print("   🔮 Testing on-demand sentiment prediction...")

    # Request data for on-demand features
    entity_rows = [
        {
            "input_text": "I love this product! It's absolutely amazing and works perfectly!",
            "model_name": "cardiffnlp/twitter-roberta-base-sentiment-latest",
        },
        {
            "input_text": "This is terrible quality. Completely disappointed with the purchase.",
            "model_name": "cardiffnlp/twitter-roberta-base-sentiment-latest",
        },
        {
            "input_text": "The product is okay. Nothing special but it does work as expected.",
            "model_name": "cardiffnlp/twitter-roberta-base-sentiment-latest",
        },
    ]

    try:
        # Get on-demand predictions
        predictions = store.get_online_features(
            features=[
                "sentiment_prediction:predicted_sentiment",
                "sentiment_prediction:sentiment_confidence",
                "sentiment_prediction:positive_prob",
                "sentiment_prediction:negative_prob",
                "sentiment_prediction:neutral_prob",
            ],
            entity_rows=entity_rows,
        ).to_dict()

        print("   🎯 Prediction results:")
        for i in range(len(entity_rows)):
            text = entity_rows[i]["input_text"][:60] + "..."
            sentiment = predictions["predicted_sentiment"][i]
            confidence = predictions["sentiment_confidence"][i]
            print(f"      Text: {text}")
            print(f"      Predicted: {sentiment} (confidence: {confidence:.3f})")
            print(
                f"      Probabilities: P={predictions['positive_prob'][i]:.3f}, "
                f"N={predictions['negative_prob'][i]:.3f}, "
                f"Neu={predictions['neutral_prob'][i]:.3f}"
            )
            print()

    except Exception as e:
        print(f"   ⚠️  On-demand prediction failed: {e}")
        print(
            "   💡 This is expected if PyTorch/transformers dependencies are not installed"
        )
        print("   📦 Install with: pip install torch transformers")


def test_feature_services(store: FeatureStore):
    """Test different feature services for model versioning."""
    print("   🎯 Testing feature services...")

    entity_rows = [{"text_id": "text_0001", "user_id": "user_001"}]

    # Test basic sentiment analysis service (v1)
    print("   📦 Testing sentiment_analysis_v1 feature service...")
    try:
        features_v1 = store.get_online_features(
            features=store.get_feature_service("sentiment_analysis_v1"),
            entity_rows=entity_rows,
        ).to_dict()
        print(f"   ✅ Retrieved {len(features_v1)} feature types for v1")
    except Exception as e:
        print(f"   ⚠️  Feature service v1 failed: {e}")

    # Test advanced sentiment analysis service (v2)
    print("   📦 Testing sentiment_analysis_v2 feature service...")
    try:
        features_v2 = store.get_online_features(
            features=store.get_feature_service("sentiment_analysis_v2"),
            entity_rows=entity_rows,
        ).to_dict()
        print(f"   ✅ Retrieved {len(features_v2)} feature types for v2")
    except Exception as e:
        print(f"   ⚠️  Feature service v2 failed: {e}")


def evaluate_model_performance(store: FeatureStore):
    """Evaluate model performance using historical features."""
    print("   📊 Evaluating model performance...")

    try:
        # Get a sample of historical data for evaluation
        entity_df = pd.DataFrame(
            {
                "text_id": [f"text_{i:04d}" for i in range(1, 21)],
                "user_id": [f"user_{(i % 5) + 1:03d}" for i in range(1, 21)],
                "event_timestamp": [
                    datetime.now() - timedelta(hours=i) for i in range(20)
                ],
            }
        )

        # Fetch features and labels
        eval_df = store.get_historical_features(
            entity_df=entity_df,
            features=[
                "text_features:text_content",
                "text_features:sentiment_label",
                "text_features:sentiment_score",
            ],
        ).to_df()

        if not eval_df.empty and "sentiment_label" in eval_df.columns:
            # Calculate basic performance metrics
            sentiment_dist = eval_df["sentiment_label"].value_counts()
            avg_score = (
                eval_df["sentiment_score"].mean()
                if "sentiment_score" in eval_df.columns
                else 0
            )

            print("   📈 Performance summary:")
            print(f"      Evaluation samples: {len(eval_df)}")
            print(f"      Average sentiment score: {avg_score:.3f}")
            print("      Class distribution:")
            for sentiment, count in sentiment_dist.items():
                print(
                    f"        {sentiment}: {count} ({count / len(eval_df) * 100:.1f}%)"
                )

            # In a real implementation, you would:
            # 1. Compare predicted vs actual labels
            # 2. Calculate accuracy, precision, recall, F1-score
            # 3. Generate confusion matrix
            # 4. Analyze error cases
            # 5. Monitor model drift over time

        else:
            print("   ⚠️  No evaluation data available")

    except Exception as e:
        print(f"   ⚠️  Evaluation failed: {e}")


if __name__ == "__main__":
    run_demo()
