"""
Generates sample data, applies the Feast registry, and materializes features
into the online store so the agent demo is ready to run.

Usage:
    cd examples/agent_feature_store
    python setup_data.py
"""

import os
import sys

import numpy as np
import pandas as pd

REPO_DIR = os.path.join(os.path.dirname(__file__), "feature_repo")
DATA_DIR = os.path.join(REPO_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

EMBEDDING_DIM = 384
NOW = pd.Timestamp.now()


def generate_customer_profiles() -> pd.DataFrame:
    customers = [
        {
            "customer_id": "C1001",
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "plan_tier": "enterprise",
            "account_age_days": 730,
            "total_spend": 24500.00,
            "open_tickets": 1,
            "satisfaction_score": 4.5,
        },
        {
            "customer_id": "C1002",
            "name": "Bob Smith",
            "email": "bob@example.com",
            "plan_tier": "pro",
            "account_age_days": 365,
            "total_spend": 8400.00,
            "open_tickets": 3,
            "satisfaction_score": 3.2,
        },
        {
            "customer_id": "C1003",
            "name": "Carol Lee",
            "email": "carol@example.com",
            "plan_tier": "starter",
            "account_age_days": 90,
            "total_spend": 990.00,
            "open_tickets": 0,
            "satisfaction_score": 4.8,
        },
    ]
    df = pd.DataFrame(customers)
    df["event_timestamp"] = NOW
    return df


def generate_knowledge_base() -> pd.DataFrame:
    articles = [
        {
            "doc_id": 1,
            "title": "How to reset your password",
            "content": (
                "To reset your password, go to Settings > Security > Reset Password. "
                "Enter your current password, then choose a new one that is at least "
                "12 characters long. Click Save. If you forgot your current password, "
                "click 'Forgot Password' on the login page to receive a reset link "
                "via email."
            ),
            "category": "account",
        },
        {
            "doc_id": 2,
            "title": "Upgrading your subscription plan",
            "content": (
                "You can upgrade your plan from Starter to Pro or Enterprise at any "
                "time. Navigate to Billing > Plans and select the plan you want. "
                "The price difference is prorated for the current billing cycle. "
                "Enterprise plans include priority support, custom integrations, "
                "and a dedicated account manager."
            ),
            "category": "billing",
        },
        {
            "doc_id": 3,
            "title": "Setting up API access",
            "content": (
                "To generate an API key, go to Settings > Developer > API Keys and "
                "click 'Create New Key'. Choose the appropriate scopes for your use "
                "case. API keys are tied to your account and inherit your permissions. "
                "Rate limits are 1000 requests/minute for Pro and 5000 for Enterprise."
            ),
            "category": "developer",
        },
        {
            "doc_id": 4,
            "title": "Understanding your invoice",
            "content": (
                "Invoices are generated on the first of each month and sent to the "
                "billing email on file. Each invoice includes a breakdown of base plan "
                "charges, overage fees, and any credits applied. You can download past "
                "invoices from Billing > Invoices."
            ),
            "category": "billing",
        },
        {
            "doc_id": 5,
            "title": "Configuring single sign-on (SSO)",
            "content": (
                "SSO is available on Enterprise plans. To configure SSO, go to "
                "Settings > Security > SSO and provide your Identity Provider (IdP) "
                "metadata URL. We support SAML 2.0 and OIDC. Once configured, all "
                "team members will authenticate through your IdP."
            ),
            "category": "account",
        },
        {
            "doc_id": 6,
            "title": "Contacting support",
            "content": (
                "You can reach our support team via the in-app chat widget, by "
                "emailing support@example.com, or by opening a ticket at "
                "https://support.example.com. Enterprise customers have access to "
                "a dedicated Slack channel and a named account manager with a "
                "guaranteed 1-hour response time."
            ),
            "category": "support",
        },
    ]

    np.random.seed(42)
    df = pd.DataFrame(articles)
    df["vector"] = [
        np.random.randn(EMBEDDING_DIM).astype(np.float32).tolist()
        for _ in range(len(df))
    ]
    df["event_timestamp"] = NOW
    return df


def main():
    print("Generating customer profile data...")
    customers_df = generate_customer_profiles()
    customers_path = os.path.join(DATA_DIR, "customer_profiles.parquet")
    customers_df.to_parquet(customers_path, index=False)
    print(f"  Saved {len(customers_df)} customer profiles to {customers_path}")

    print("Generating knowledge-base data...")
    kb_df = generate_knowledge_base()
    kb_path = os.path.join(DATA_DIR, "knowledge_base.parquet")
    kb_df.to_parquet(kb_path, index=False)
    print(f"  Saved {len(kb_df)} knowledge-base articles to {kb_path}")

    print("Generating empty agent memory scaffold...")
    memory_df = pd.DataFrame(
        {
            "customer_id": pd.Series(dtype="str"),
            "last_topic": pd.Series(dtype="str"),
            "last_resolution": pd.Series(dtype="str"),
            "interaction_count": pd.Series(dtype="int64"),
            "preferences": pd.Series(dtype="str"),
            "open_issue": pd.Series(dtype="str"),
            "event_timestamp": pd.Series(dtype="datetime64[ns]"),
        }
    )
    memory_path = os.path.join(DATA_DIR, "agent_memory.parquet")
    memory_df.to_parquet(memory_path, index=False)
    print(f"  Saved empty memory scaffold to {memory_path}")

    print("Applying Feast registry...")
    sys.path.insert(0, REPO_DIR)
    from feast import FeatureStore
    from features import (
        agent_memory,
        customer,
        customer_profile,
        document,
        knowledge_base,
    )

    store = FeatureStore(repo_path=REPO_DIR)
    store.apply([customer, document, customer_profile, knowledge_base, agent_memory])

    print("Materializing customer profiles to the online store...")
    store.write_to_online_store(feature_view_name="customer_profile", df=customers_df)
    print("  Done.")

    print("Materializing knowledge-base to the online store...")
    store.write_to_online_store(feature_view_name="knowledge_base", df=kb_df)
    print("  Done.")

    print("\nSetup complete! Start the feature server with:")
    print("  cd feature_repo && feast serve --host 0.0.0.0 --port 6566")


if __name__ == "__main__":
    main()
