"""
Advanced Tiled Transformation Example

This example demonstrates advanced tiling features including:
- Chaining functions for cross-tile continuity
- Complex aggregation patterns
- Late data handling
- Memory management
"""

import pandas as pd
from datetime import datetime, timedelta
import random


def pandas_tiled_transformation(tile_size, overlap=None, max_tiles_in_memory=10, 
                              aggregation_functions=None, chaining_functions=None):
    """Simplified decorator for demonstration purposes."""
    def decorator(func):
        func.tile_size = tile_size  
        func.overlap = overlap or timedelta(seconds=0)
        func.aggregation_functions = aggregation_functions or []
        func.chaining_functions = chaining_functions or []
        
        def transform(data, timestamp_column='timestamp'):
            """Simplified tiled transformation with chaining logic."""
            # Simulate tile processing
            tiles = []
            
            # Sort data by timestamp
            data_sorted = data.sort_values(timestamp_column)
            
            # Create mock tiles (in real implementation, this would be time-based)
            tile_size_minutes = int(tile_size.total_seconds() / 60)
            unique_hours = data_sorted[timestamp_column].dt.floor(f'{tile_size_minutes}min').unique()
            
            for hour in unique_hours:
                tile_data = data_sorted[
                    data_sorted[timestamp_column].dt.floor(f'{tile_size_minutes}min') == hour
                ].copy()
                
                if not tile_data.empty:
                    # Apply base transformation
                    tile_result = func(tile_data)
                    
                    # Apply aggregation functions
                    for agg_func in func.aggregation_functions:
                        tile_result = agg_func(tile_result)
                    
                    tiles.append(tile_result)
            
            # Chain tiles if chaining functions are provided
            if func.chaining_functions and len(tiles) > 1:
                chained_result = tiles[0]
                for i in range(1, len(tiles)):
                    for chain_func in func.chaining_functions:
                        chained_result = chain_func(chained_result, tiles[i])
                return chained_result
            elif tiles:
                return pd.concat(tiles, ignore_index=True)
            else:
                return pd.DataFrame()
        
        func.transform = transform
        return func
    
    return decorator


# Advanced tiled transformation with chaining
@pandas_tiled_transformation(
    tile_size=timedelta(hours=1),
    overlap=timedelta(minutes=10),
    max_tiles_in_memory=3,
    aggregation_functions=[
        # Complex multi-level aggregation
        lambda df: df.groupby(['customer_id', 'merchant_category']).agg({
            'amount': ['sum', 'mean', 'std', 'count'],
            'timestamp': ['min', 'max'],
            'high_value_tx': 'sum'
        }).reset_index(),
    ],
    chaining_functions=[
        # Chain running totals across tiles
        lambda prev_df, curr_df: chain_running_totals(prev_df, curr_df),
        # Chain session continuity
        lambda prev_df, curr_df: chain_session_features(prev_df, curr_df)
    ]
)
def advanced_customer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Advanced transformation with complex feature engineering.
    
    Args:
        df: DataFrame with transaction data
        
    Returns:
        DataFrame with advanced derived features
    """
    df = df.copy()
    
    # Feature engineering within tile
    df['hour_of_day'] = df['timestamp'].dt.hour
    df['day_of_week'] = df['timestamp'].dt.dayofweek
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    
    # Transaction patterns
    df['high_value_tx'] = (df['amount'] > df['amount'].quantile(0.8)).astype(int)
    df['velocity'] = df.groupby('customer_id')['timestamp'].diff().dt.total_seconds() / 60  # minutes
    
    # Rolling features within tile (with minimum periods for robustness)
    df = df.sort_values(['customer_id', 'timestamp'])
    df['rolling_velocity_avg'] = df.groupby('customer_id')['velocity'].rolling(
        window=3, min_periods=1
    ).mean().reset_index(drop=True)
    
    # Customer behavior segmentation within tile
    customer_stats = df.groupby('customer_id').agg({
        'amount': ['mean', 'std'],
        'velocity': 'mean'
    }).reset_index()
    customer_stats.columns = ['customer_id', 'avg_amount', 'std_amount', 'avg_velocity']
    customer_stats['customer_segment'] = 'regular'
    customer_stats.loc[customer_stats['avg_amount'] > customer_stats['avg_amount'].quantile(0.8), 'customer_segment'] = 'high_value'
    customer_stats.loc[customer_stats['avg_velocity'] < 10, 'customer_segment'] = 'slow_spender'
    
    # Merge back to main dataframe
    df = df.merge(customer_stats[['customer_id', 'customer_segment']], on='customer_id', how='left')
    
    return df


def chain_running_totals(prev_df: pd.DataFrame, curr_df: pd.DataFrame) -> pd.DataFrame:
    """Chain running totals across tiles."""
    if prev_df.empty:
        return curr_df
    
    curr_df = curr_df.copy()
    
    # Get the last running total for each customer from previous tile
    if 'amount' in prev_df.columns and 'sum' in prev_df.columns:
        prev_totals = prev_df.groupby('customer_id')['amount']['sum'].last().to_dict()
        
        # Add to current tile's totals
        for customer_id in curr_df['customer_id'].unique():
            if customer_id in prev_totals:
                mask = curr_df['customer_id'] == customer_id
                if 'amount' in curr_df.columns and 'sum' in curr_df.columns:
                    curr_df.loc[mask, ('amount', 'sum')] += prev_totals[customer_id]
    
    return curr_df


def chain_session_features(prev_df: pd.DataFrame, curr_df: pd.DataFrame) -> pd.DataFrame:
    """Chain session-based features across tiles."""
    if prev_df.empty:
        return curr_df
    
    curr_df = curr_df.copy()
    
    # Add session continuity indicator (simplified)
    curr_df['session_continued'] = 1  # Would be more complex in real implementation
    
    return curr_df


def simulate_late_arriving_data(df: pd.DataFrame, late_probability: float = 0.1) -> pd.DataFrame:
    """Simulate late-arriving data by randomly delaying some records."""
    df = df.copy()
    
    # Randomly select some records to be "late"
    late_mask = pd.Series([random.random() < late_probability for _ in range(len(df))], index=df.index)
    
    # Add delay to late records
    df.loc[late_mask, 'timestamp'] = df.loc[late_mask, 'timestamp'] + timedelta(minutes=random.randint(5, 30))
    
    # Re-sort by original order but keep the delayed timestamps
    return df


def main():
    """Demonstrate advanced tiling features."""
    
    # Create more complex sample data
    base_time = datetime(2024, 1, 1, 10, 0, 0)
    
    transactions = []
    customers = [f'customer_{i}' for i in range(8)]
    merchant_categories = ['grocery', 'restaurant', 'gas', 'retail', 'entertainment', 'healthcare']
    
    for i in range(120):
        # More realistic transaction patterns
        customer = random.choice(customers)
        category = random.choice(merchant_categories)
        
        # Vary amounts based on category
        category_multipliers = {
            'grocery': 1.0, 'restaurant': 1.5, 'gas': 0.8, 
            'retail': 2.0, 'entertainment': 1.8, 'healthcare': 3.0
        }
        base_amount = random.uniform(20, 200) * category_multipliers[category]
        
        transactions.append({
            'timestamp': base_time + timedelta(minutes=i*5 + random.randint(-2, 2)),
            'customer_id': customer,
            'amount': round(base_amount, 2),
            'merchant_category': category
        })
    
    df = pd.DataFrame(transactions)
    
    # Simulate late-arriving data
    df = simulate_late_arriving_data(df, late_probability=0.15)
    
    print(f"Created {len(df)} sample transactions with realistic patterns")
    print(f"Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"Customers: {df['customer_id'].nunique()}")
    print(f"Categories: {df['merchant_category'].nunique()}")
    
    # Apply the advanced tiled transformation
    print("\nApplying advanced tiled transformation...")
    result = advanced_customer_features.transform(df, timestamp_column='timestamp')
    
    print(f"\nResult shape: {result.shape}")
    print(f"Result columns: {list(result.columns)}")
    
    if not result.empty:
        print("\nSample of advanced features:")
        display_cols = []
        for col in ['customer_id', 'amount', 'customer_segment', 'high_value_tx']:
            if col in result.columns:
                display_cols.append(col)
            elif isinstance(result.columns, pd.MultiIndex):
                # Handle MultiIndex columns from aggregation
                matching_cols = [c for c in result.columns if col in str(c)]
                display_cols.extend(matching_cols[:2])  # Take first 2 matching
        
        if display_cols:
            print(result[display_cols].head(10))
    
    # Show tiling configuration
    print(f"\nAdvanced tiling configuration:")
    print(f"- Tile size: {advanced_customer_features.tile_size}")
    print(f"- Overlap: {advanced_customer_features.overlap}")
    print(f"- Aggregation functions: {len(advanced_customer_features.aggregation_functions)}")
    print(f"- Chaining functions: {len(advanced_customer_features.chaining_functions)}")


if __name__ == "__main__":
    main()