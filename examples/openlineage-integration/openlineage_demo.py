#!/usr/bin/env python
# Copyright 2026 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Feast OpenLineage Native Integration Demo

This demo shows how Feast's native OpenLineage integration works.
When OpenLineage is enabled in feature_store.yaml, lineage events
are emitted automatically - no code changes required!

Usage:
    python openlineage_demo.py --url http://localhost:5000
"""

import argparse
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from feast import FeatureStore
from feast import Entity, FeatureService, FeatureView, FileSource, Field
from feast.types import Float32, Int64

def create_feature_store_yaml(url: str) -> str:
    """Create a feature_store.yaml with OpenLineage configuration."""
    return f"""project: openlineage_demo
registry: data/registry.db
provider: local
online_store:
  type: sqlite
  path: data/online_store.db

openlineage:
  enabled: true
  transport_type: http
  transport_url: {url}
  transport_endpoint: api/v1/lineage
  namespace: feast
  emit_on_apply: true
  emit_on_materialize: true
"""


def run_demo(url: str):
    """Run the OpenLineage integration demo."""
    print("Feast OpenLineage Native Integration Demo")

    # Create temporary directory for the demo
    with tempfile.TemporaryDirectory() as tmpdir:
        repo_path = Path(tmpdir)
        data_dir = repo_path / "data"
        data_dir.mkdir(exist_ok=True)

        # Create feature_store.yaml with OpenLineage configuration
        feature_store_yaml = create_feature_store_yaml(url)
        (repo_path / "feature_store.yaml").write_text(feature_store_yaml)

        print(f"Created demo repository at: {repo_path}")
        print(f"feature_store.yaml:")
        print("-" * 50)
        print(feature_store_yaml)
        print("-" * 50)

        try:
            import openlineage.client
        except ImportError:
            print("OpenLineage client not installed.")
            print("Install with: pip install openlineage-python")
            raise ImportError("OpenLineage client not installed")

        fs = FeatureStore(repo_path=str(repo_path))
        driver_stats_df = pd.DataFrame(
            {
                "driver_id": [1001, 1002, 1003, 1001, 1002, 1003],
                "conv_rate": [0.85, 0.72, 0.91, 0.87, 0.75, 0.89],
                "acc_rate": [0.95, 0.88, 0.97, 0.94, 0.90, 0.96],
                "avg_daily_trips": [12, 8, 15, 14, 9, 16],
                "event_timestamp": pd.to_datetime(
                    [
                        "2024-01-01 10:00:00",
                        "2024-01-01 10:00:00",
                        "2024-01-01 10:00:00",
                        "2024-01-02 10:00:00",
                        "2024-01-02 10:00:00",
                        "2024-01-02 10:00:00",
                    ],
                    utc=True,
                ),
                "created": pd.to_datetime(["2024-01-01"] * 6, utc=True),
            }
        )

        parquet_path = data_dir / "driver_stats.parquet"
        driver_stats_df.to_parquet(str(parquet_path))

        driver = Entity(name="driver_id", description="Driver identifier")

        driver_stats_source = FileSource(
            name="driver_stats_source",
            path=str(parquet_path),
            timestamp_field="event_timestamp",
            created_timestamp_column="created",
            description="Driver statistics from data warehouse",
        )

        driver_hourly_stats_view = FeatureView(
            name="driver_hourly_stats",
            entities=[driver],
            ttl=timedelta(days=1),
            schema=[
                Field(name="conv_rate", dtype=Float32, description="Conversion rate"),
                Field(name="acc_rate", dtype=Float32, description="Acceptance rate"),
                Field(
                    name="avg_daily_trips", dtype=Int64, description="Average daily trips"
                ),
            ],
            source=driver_stats_source,
            description="Hourly driver performance statistics",
            tags={"team": "driver_performance", "priority": "high"},
        )

        driver_stats_service = FeatureService(
            name="driver_stats_service",
            features=[driver_hourly_stats_view],
            description="Driver statistics for real-time scoring",
            tags={"use_case": "scoring"},
        )

        try:
            fs.apply(
                [driver, driver_stats_source, driver_hourly_stats_view, driver_stats_service]
            )
            print("Applied entities, feature views, and feature services")
            print("OpenLineage events emitted automatically:")
            print("     - feast_feature_views_openlineage_demo (DataSources → FeatureViews)")
            print("     - feature_service_driver_stats_service (FeatureViews → FeatureService)")
        except Exception as e:
            print(f"Apply failed: {e}")

        start_date = datetime(
            2024, 1, 1, tzinfo=driver_stats_df["event_timestamp"].dt.tz
        )
        end_date = datetime(2024, 1, 3, tzinfo=driver_stats_df["event_timestamp"].dt.tz)
        fs.materialize(start_date=start_date, end_date=end_date)

        # Retrieve online features (no OpenLineage events emitted for retrieval)
        online_features = fs.get_online_features(
            features=["driver_hourly_stats:conv_rate", "driver_hourly_stats:acc_rate"],
            entity_rows=[{"driver_id": 1001}, {"driver_id": 1002}],
        )
        print(f"Retrieved online features: {online_features.to_dict()}")
        print(
            """
The native OpenLineage integration works automatically when configured.

Lineage Graph Created:
  DataSources + Entities → feast_feature_views_{project} → FeatureViews
  FeatureViews → feature_service_{name} → FeatureServices

Key benefits:
  - No code changes required
  - Just add 'openlineage' section to feature_store.yaml
  - All operations emit lineage events automatically
  - Feature metadata (tags, descriptions) included in lineage
  - Non-blocking and fail-safe (won't break Feast operations)

View your lineage at: http://localhost:3000
"""
        )


def main():
    parser = argparse.ArgumentParser(
        description="Feast OpenLineage Native Integration Demo",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
    # Start Marquez first:
    docker run -p 5000:5000 -p 3000:3000 marquezproject/marquez

    # Run the demo:
    python openlineage_demo.py --url http://localhost:5000

    # View lineage at http://localhost:3000
""",
    )
    parser.add_argument(
        "--url",
        required=True,
        help="OpenLineage HTTP URL (e.g., http://localhost:5000)",
    )

    args = parser.parse_args()

    run_demo(args.url)


if __name__ == "__main__":
    main()
