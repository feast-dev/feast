"""
Streaming Integration Example

This example shows how tiled transformations integrate with Feast's
StreamFeatureView for real-time feature engineering.
"""

from datetime import datetime, timedelta
from typing import List, Optional

# Note: This example shows the intended integration pattern
# In a real Feast installation, you would use actual Feast imports


class MockStreamFeatureView:
    """Mock StreamFeatureView for demonstration."""
    
    def __init__(self, name: str, feature_transformation=None, source=None, 
                 entities: List[str] = None, **kwargs):
        self.name = name
        self.feature_transformation = feature_transformation
        self.source = source
        self.entities = entities or []
        
    def __repr__(self):
        return f"StreamFeatureView(name='{self.name}', transformation={type(self.feature_transformation).__name__})"


class MockKafkaSource:
    """Mock Kafka source for demonstration."""
    
    def __init__(self, topic: str, bootstrap_servers: str):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        
    def __repr__(self):
        return f"KafkaSource(topic='{self.topic}')"


def pandas_tiled_transformation(tile_size, overlap=None, **kwargs):
    """Mock tiled transformation decorator."""
    def decorator(func):
        func.tile_size = tile_size
        func.overlap = overlap or timedelta(seconds=0)
        return func
    return decorator


# Define tiled transformation for customer transaction features
@pandas_tiled_transformation(
    tile_size=timedelta(minutes=30),  # 30-minute tiles for real-time processing
    overlap=timedelta(minutes=2),     # 2-minute overlap for continuity
    max_tiles_in_memory=8,           # Keep 4 hours of tiles in memory
    aggregation_functions=[
        # Real-time aggregations within each tile
        lambda df: df.groupby('customer_id').agg({
            'transaction_amount': ['sum', 'mean', 'count', 'std'],
            'merchant_risk_score': 'mean',
            'timestamp': ['min', 'max'],
            'is_anomaly': 'sum'
        }).reset_index()
    ],
    chaining_functions=[
        # Chain fraud detection features across tiles
        lambda prev_df, curr_df: chain_fraud_features(prev_df, curr_df)
    ]
)
def real_time_fraud_features(df):
    """
    Real-time fraud detection features using tiled processing.
    
    This transformation processes streaming transaction data in 30-minute tiles
    to compute fraud detection features efficiently.
    """
    import pandas as pd
    
    df = df.copy()
    
    # Real-time feature engineering
    df['hour_of_day'] = df['timestamp'].dt.hour
    df['is_weekend'] = df['timestamp'].dt.dayofweek.isin([5, 6]).astype(int)
    df['is_night'] = df['hour_of_day'].isin([22, 23, 0, 1, 2, 3, 4, 5]).astype(int)
    
    # Transaction velocity features (within tile)
    df = df.sort_values(['customer_id', 'timestamp'])
    df['time_since_last_tx'] = df.groupby('customer_id')['timestamp'].diff().dt.total_seconds()
    df['tx_velocity'] = 1 / (df['time_since_last_tx'] / 60 + 1)  # transactions per minute
    
    # Amount-based features
    customer_stats = df.groupby('customer_id')['transaction_amount'].agg(['mean', 'std']).reset_index()
    df = df.merge(customer_stats, on='customer_id', how='left', suffixes=('', '_customer_avg'))
    
    # Anomaly detection (simplified)
    df['amount_z_score'] = abs((df['transaction_amount'] - df['mean']) / (df['std'] + 1e-6))
    df['is_anomaly'] = (df['amount_z_score'] > 2.5).astype(int)
    
    # Merchant risk scoring
    merchant_risk = {
        'gambling': 0.8, 'adult': 0.7, 'alcohol': 0.6,
        'grocery': 0.1, 'restaurant': 0.2, 'gas': 0.15,
        'retail': 0.3, 'healthcare': 0.2, 'education': 0.1
    }
    df['merchant_risk_score'] = df['merchant_category'].map(merchant_risk).fillna(0.5)
    
    # Composite fraud score
    df['fraud_score'] = (
        df['is_anomaly'] * 0.4 +
        df['merchant_risk_score'] * 0.3 +
        df['is_night'] * 0.2 +
        (df['tx_velocity'] > df['tx_velocity'].quantile(0.95)).astype(int) * 0.1
    )
    
    return df


def chain_fraud_features(prev_df, curr_df):
    """Chain fraud detection features across tiles."""
    if prev_df.empty:
        return curr_df
    
    curr_df = curr_df.copy()
    
    # Add cross-tile fraud patterns (simplified example)
    curr_df['session_continued'] = 1
    
    # In a real implementation, you might:
    # - Track spending patterns across tiles
    # - Maintain customer behavior baselines
    # - Chain velocity calculations
    # - Update fraud model states
    
    return curr_df


def create_streaming_feature_views():
    """Create example StreamFeatureViews with tiled transformations."""
    
    # Kafka source for real-time transaction stream
    transaction_stream = MockKafkaSource(
        topic="transactions",
        bootstrap_servers="localhost:9092"
    )
    
    # Real-time fraud detection features
    fraud_detection_fv = MockStreamFeatureView(
        name="real_time_fraud_features",
        entities=["customer_id"],
        feature_transformation=real_time_fraud_features,
        source=transaction_stream,
        description="Real-time fraud detection features using tiled processing"
    )
    
    # Customer behavior aggregations with different tile size
    @pandas_tiled_transformation(
        tile_size=timedelta(hours=1),  # Larger tiles for behavior patterns
        aggregation_functions=[
            lambda df: df.groupby('customer_id').agg({
                'transaction_amount': ['sum', 'count', 'mean'],
                'merchant_category': lambda x: x.nunique(),  # diversity
                'fraud_score': 'mean'
            }).reset_index()
        ]
    )
    def hourly_behavior_features(df):
        """Hourly customer behavior aggregations."""
        import pandas as pd
        
        df = df.copy()
        df['spending_pattern'] = pd.cut(df['transaction_amount'], 
                                      bins=[0, 50, 200, 1000, float('inf')], 
                                      labels=['low', 'medium', 'high', 'very_high'])
        return df
    
    behavior_fv = MockStreamFeatureView(
        name="hourly_behavior_features", 
        entities=["customer_id"],
        feature_transformation=hourly_behavior_features,
        source=transaction_stream,
        description="Hourly customer behavior patterns"
    )
    
    return [fraud_detection_fv, behavior_fv]


def demonstrate_streaming_pipeline():
    """Demonstrate a complete streaming feature pipeline with tiling."""
    
    print("=== Feast Tiled Streaming Features Integration ===\n")
    
    # Create feature views
    feature_views = create_streaming_feature_views()
    
    print("Created StreamFeatureViews with tiled transformations:")
    for fv in feature_views:
        print(f"  - {fv}")
        if hasattr(fv.feature_transformation, 'tile_size'):
            print(f"    Tile size: {fv.feature_transformation.tile_size}")
            print(f"    Overlap: {fv.feature_transformation.overlap}")
    
    print(f"\n=== Key Benefits of Tiled Architecture ===")
    print("1. Memory Efficiency: Process data in manageable time-based chunks")
    print("2. Low Latency: Incremental processing reduces computation time")
    print("3. Fault Tolerance: Tile-based checkpointing enables recovery")
    print("4. Scalability: Independent tile processing allows parallelization")
    print("5. Late Data Handling: Overlap windows handle out-of-order events")
    
    print(f"\n=== Integration with Feast Components ===")
    print("• StreamFeatureView: Defines tiled transformation logic")
    print("• StreamProcessor: Executes tiled transformations on streaming data")
    print("• Online Store: Receives processed features from tiles")
    print("• Feature Server: Serves real-time features to applications")
    
    print(f"\n=== Example Usage in Production ===")
    print("""
# Define tiled transformation
@pandas_tiled_transformation(
    tile_size=timedelta(minutes=30),
    overlap=timedelta(minutes=2)
)
def fraud_features(df):
    return compute_fraud_features(df)

# Create StreamFeatureView
fraud_fv = StreamFeatureView(
    name="fraud_detection",
    feature_transformation=fraud_features,
    source=kafka_source,
    entities=["customer_id"]
)

# Register with Feast
fs.apply([fraud_fv])

# Start streaming ingestion
processor = get_stream_processor(config, fs, fraud_fv)
processor.ingest_stream_feature_view()
    """)


if __name__ == "__main__":
    demonstrate_streaming_pipeline()