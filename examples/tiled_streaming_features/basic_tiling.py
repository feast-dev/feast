"""
Basic Tiled Transformation Example

This example shows how to use tiled transformations for processing
streaming transaction data in time-based chunks.
"""

import pandas as pd
from datetime import datetime, timedelta

# Note: In a real Feast installation, you would import like this:
# from feast.transformation import pandas_tiled_transformation

# For this example, we'll define a simplified version
def pandas_tiled_transformation(tile_size, overlap=None, max_tiles_in_memory=10, aggregation_functions=None):
    """Simplified decorator for demonstration purposes."""
    def decorator(func):
        func.tile_size = tile_size  
        func.overlap = overlap or timedelta(seconds=0)
        func.aggregation_functions = aggregation_functions or []
        
        def transform(data, timestamp_column='timestamp'):
            """Simplified tiled transformation logic."""
            # In the real implementation, this would partition data into tiles
            # and apply the transformation to each tile
            result = func(data)
            
            # Apply aggregation functions
            for agg_func in func.aggregation_functions:
                result = agg_func(result)
            
            return result
        
        func.transform = transform
        return func
    
    return decorator


# Define a tiled transformation for transaction features
@pandas_tiled_transformation(
    tile_size=timedelta(hours=1),  # Process data in 1-hour tiles
    overlap=timedelta(minutes=5),  # 5-minute overlap for continuity
    max_tiles_in_memory=5,  # Keep max 5 tiles in memory
    aggregation_functions=[
        # Aggregate by customer within each tile
        lambda df: df.groupby('customer_id').agg({
            'amount': ['sum', 'mean', 'count'],
            'timestamp': 'max'
        }).reset_index()
    ]
)
def hourly_transaction_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform transaction data within each hour tile.
    
    Args:
        df: DataFrame with columns: customer_id, amount, timestamp, merchant_category
    
    Returns:
        DataFrame with derived features
    """
    # Basic transformations within the tile
    df = df.copy()
    
    # Calculate rolling statistics (within tile)
    df = df.sort_values(['customer_id', 'timestamp'])
    df['rolling_avg_3'] = df.groupby('customer_id')['amount'].rolling(window=3, min_periods=1).mean().reset_index(drop=True)
    df['rolling_sum_2'] = df.groupby('customer_id')['amount'].rolling(window=2, min_periods=1).sum().reset_index(drop=True)
    
    # Calculate cumulative features (within tile)
    df['cumulative_amount'] = df.groupby('customer_id')['amount'].cumsum()
    df['transaction_count'] = df.groupby('customer_id').cumcount() + 1
    
    # Merchant category features
    df['high_risk_merchant'] = df['merchant_category'].isin(['gambling', 'adult', 'alcohol']).astype(int)
    
    return df


def main():
    """Example usage of the tiled transformation."""
    
    # Create sample streaming transaction data
    base_time = datetime(2024, 1, 1, 10, 0, 0)
    
    transactions = []
    for i in range(50):
        transactions.append({
            'timestamp': base_time + timedelta(minutes=i*3),  # Every 3 minutes
            'customer_id': f'customer_{i % 5}',  # 5 different customers
            'amount': 50 + (i % 7) * 20,  # Varying amounts
            'merchant_category': ['grocery', 'restaurant', 'gas', 'retail'][i % 4]
        })
    
    df = pd.DataFrame(transactions)
    print(f"Created {len(df)} sample transactions")
    print(f"Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    
    # Apply the tiled transformation
    print("\nApplying tiled transformation...")
    result = hourly_transaction_features.transform(df, timestamp_column='timestamp')
    
    print(f"\nResult shape: {result.shape}")
    print("\nResult columns:", list(result.columns))
    print("\nResult sample:")
    print(result.head(10))
    
    # Show the effect of tiling configuration
    print(f"\nTile configuration:")
    print(f"- Tile size: {hourly_transaction_features.tile_size}")
    print(f"- Overlap: {hourly_transaction_features.overlap}")
    print(f"- Aggregation functions: {len(hourly_transaction_features.aggregation_functions)}")


if __name__ == "__main__":
    main()