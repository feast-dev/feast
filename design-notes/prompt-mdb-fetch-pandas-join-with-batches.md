Enhance MongoDBOfflineStoreNative.get_historical_features to support chunked execution for large entity_df, while preserving the existing fetch + pandas PIT join logic.

Goals
	•	Prevent memory blowups for large entity_df
	•	Reuse the current implementation as much as possible
	•	Keep the code clean and idiomatic to Feast

⸻

Requirements

1. Add chunking based on entity_df size
	•	Introduce a constant:
``` python
CHUNK_SIZE = 5000  # make configurable configurable
```
	•	If len(entity_df) <= CHUNK_SIZE:
	•	Run the existing _run() logic unchanged
	•	Else:
	•	Split entity_df into chunks of size CHUNK_SIZE

⸻

2. Extract existing logic into reusable function
Refactor the current _run() implementation into a helper:
``` python
def _run_single(entity_subset_df: pd.DataFrame) -> pd.DataFrame:
    ...
```
This function should:
	•	Perform:
	•	entity_id serialization
	•	MongoDB fetch ($in query)
	•	pandas normalization
	•	per-feature-view merge_asof
	•	Return a pandas DataFrame (not Arrow)
3. Implement chunked execution
In _run():
``` python
if len(entity_df) <= CHUNK_SIZE:
    df = _run_single(entity_df)
else:
    dfs = []
    for chunk in chunk_dataframe(entity_df, CHUNK_SIZE):
        dfs.append(_run_single(chunk))
    df = pd.concat(dfs, ignore_index=True)
```
4. Implement chunk helper
Add:
```
def chunk_dataframe(df: pd.DataFrame, size: int):
    for i in range(0, len(df), size):
        yield df.iloc[i:i+size]
```
5. Preserve ordering
	•	Ensure final DataFrame preserves original row order
	•	Use a _row_idx column if necessary
6. Handle edge cases
Ensure chunked version correctly handles:
	•	Empty MongoDB results
	•	Missing feature_views
	•	Missing features inside documents
	•	TTL filtering (already implemented in pandas)

⸻

7. Return Arrow table
Final _run() must still return:
```
pyarrow.Table.from_pandas(df, preserve_index=False)
```
Constraints
	•	Do NOT reintroduce $lookup
	•	Do NOT use temp collections
	•	Do NOT duplicate large blocks of logic
	•	Keep code readable and maintainable

⸻

Optional (nice-to-have)
	•	Add logging or debug print:
	•	number of chunks processed
	•	rows per chunk

⸻

Outcome
	•	Small workloads behave exactly as before
	•	Large workloads are processed safely in chunks
	•	Performance remains close to Ibis for moderate sizes
	•	Memory usage is bounded

⸻

🧠 Why this design is the right one

This keeps your system:

✅ Fast
	•	still uses vectorized joins

✅ Scalable
	•	bounded memory

✅ Clean
	•	no duplication
	•	no branching chaos

