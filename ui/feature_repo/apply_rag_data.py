import pandas as pd
import numpy as np
from datetime import datetime, timedelta

now = datetime.now()
embeddings = []
for i in range(10):
    embeddings.append({
        'document_id': f'doc_{i}',
        'embedding': np.random.rand(768).astype(np.float32),
        'event_timestamp': now - timedelta(days=i),
        'created_timestamp': now - timedelta(days=i, hours=1)
    })
df_embeddings = pd.DataFrame(embeddings)
df_embeddings.to_parquet('data/document_embeddings.parquet', index=False)

metadata = []
for i in range(10):
    metadata.append({
        'document_id': f'doc_{i}',
        'title': f'Document {i}',
        'content': f'This is the content of document {i}',
        'source': 'web',
        'author': f'author_{i}',
        'publish_date': (now - timedelta(days=i*30)).strftime('%Y-%m-%d'),
        'event_timestamp': now - timedelta(days=i),
        'created_timestamp': now - timedelta(days=i, hours=1)
    })
df_metadata = pd.DataFrame(metadata)
df_metadata.to_parquet('data/document_metadata.parquet', index=False)

print('Created RAG data files successfully!')
