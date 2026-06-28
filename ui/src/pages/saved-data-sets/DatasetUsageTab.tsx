import React from "react";
import {
  EuiPanel,
  EuiTitle,
  EuiText,
  EuiSpacer,
  EuiCodeBlock,
  EuiHorizontalRule,
  EuiFlexGroup,
  EuiFlexItem,
  EuiCallOut,
  EuiLoadingSpinner,
  EuiIcon,
} from "@elastic/eui";
import { useParams } from "react-router-dom";
import useLoadDataset from "./useLoadDataset";

const DatasetUsageTab = () => {
  const { datasetName } = useParams();

  if (!datasetName) {
    throw new Error("Unable to get dataset name.");
  }

  const { isLoading, isSuccess, data } = useLoadDataset(datasetName);

  if (isLoading) {
    return <EuiLoadingSpinner size="l" />;
  }

  if (!isSuccess || !data) {
    return (
      <EuiCallOut title="Dataset not found" color="warning" iconType="alert">
        <p>Could not load dataset details.</p>
      </EuiCallOut>
    );
  }

  const name = data.spec?.name || datasetName;

  const loadCode = `from feast import FeatureStore

store = FeatureStore(repo_path=".")

# Load the saved dataset
dataset = store.get_saved_dataset("${name}")

# Convert to pandas DataFrame
df = dataset.to_df()
print(f"Loaded {len(df)} rows, {len(df.columns)} columns")
print(df.head())`;

  const loadArrowCode = `# Load as PyArrow Table (more efficient for large datasets)
dataset = store.get_saved_dataset("${name}")
table = dataset.to_arrow()
print(f"Schema: {table.schema}")
print(f"Rows: {table.num_rows}")`;

  const torchCode = `import torch
from torch.utils.data import TensorDataset, DataLoader

dataset = store.get_saved_dataset("${name}")
df = dataset.to_df()

# Select your feature columns and target
feature_cols = df.select_dtypes(include=["number"]).columns.tolist()
X = torch.tensor(df[feature_cols].values, dtype=torch.float32)

# Create DataLoader
torch_dataset = TensorDataset(X)
loader = DataLoader(torch_dataset, batch_size=32, shuffle=True)

for batch in loader:
    # Your training loop here
    pass`;

  const registerCode = `from feast import FeatureStore
from feast.infra.offline_stores.file_source import SavedDatasetFileStorage

store = FeatureStore(repo_path=".")

# Create from a historical retrieval job
entity_df = ...  # Your entity DataFrame with timestamps
job = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "feature_view:feature_1",
        "feature_view:feature_2",
    ],
)

# Persist and register
dataset = store.create_saved_dataset(
    from_=job,
    name="${name}",
    storage=SavedDatasetFileStorage(path="data/${name}.parquet"),
    tags={"team": "ml", "version": "1"},
)`;

  const validationCode = `from feast.dqm.profilers.ge_profiler import GEProfiler

dataset = store.get_saved_dataset("${name}")

# Create a validation reference
profiler = GEProfiler()
ref = dataset.as_reference(name="${name}_ref", profiler=profiler)

# Apply the reference to the registry
store.apply(ref)

# Later: validate logged features against this reference
store.validate_logged_features(
    source=feature_service,
    start=start_time,
    end=end_time,
    reference=ref,
)`;

  return (
    <EuiFlexGroup direction="column" gutterSize="l">
      {/* Load Dataset */}
      <EuiFlexItem>
        <EuiPanel hasBorder>
          <EuiFlexGroup alignItems="center" gutterSize="s" responsive={false}>
            <EuiFlexItem grow={false}>
              <EuiIcon type="download" color="primary" />
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiTitle size="xs">
                <h3>Load Dataset</h3>
              </EuiTitle>
            </EuiFlexItem>
          </EuiFlexGroup>
          <EuiText size="s" color="subdued">
            <p>
              Retrieve this dataset as a pandas DataFrame or PyArrow Table for
              analysis or training.
            </p>
          </EuiText>
          <EuiSpacer size="s" />
          <EuiCodeBlock
            language="python"
            isCopyable
            paddingSize="m"
            fontSize="s"
          >
            {loadCode}
          </EuiCodeBlock>
          <EuiSpacer size="m" />
          <EuiCodeBlock
            language="python"
            isCopyable
            paddingSize="m"
            fontSize="s"
          >
            {loadArrowCode}
          </EuiCodeBlock>
        </EuiPanel>
      </EuiFlexItem>

      {/* Training Integration */}
      <EuiFlexItem>
        <EuiPanel hasBorder>
          <EuiFlexGroup alignItems="center" gutterSize="s" responsive={false}>
            <EuiFlexItem grow={false}>
              <EuiIcon type="compute" color="primary" />
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiTitle size="xs">
                <h3>Use for Training (PyTorch)</h3>
              </EuiTitle>
            </EuiFlexItem>
          </EuiFlexGroup>
          <EuiText size="s" color="subdued">
            <p>Convert the dataset into PyTorch tensors for model training.</p>
          </EuiText>
          <EuiSpacer size="s" />
          <EuiCodeBlock
            language="python"
            isCopyable
            paddingSize="m"
            fontSize="s"
          >
            {torchCode}
          </EuiCodeBlock>
        </EuiPanel>
      </EuiFlexItem>

      {/* Register via SDK */}
      <EuiFlexItem>
        <EuiPanel hasBorder>
          <EuiFlexGroup alignItems="center" gutterSize="s" responsive={false}>
            <EuiFlexItem grow={false}>
              <EuiIcon type="plusInCircle" color="primary" />
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiTitle size="xs">
                <h3>Register via SDK</h3>
              </EuiTitle>
            </EuiFlexItem>
          </EuiFlexGroup>
          <EuiText size="s" color="subdued">
            <p>
              Create this dataset programmatically from a historical feature
              retrieval job.
            </p>
          </EuiText>
          <EuiSpacer size="s" />
          <EuiCodeBlock
            language="python"
            isCopyable
            paddingSize="m"
            fontSize="s"
          >
            {registerCode}
          </EuiCodeBlock>
        </EuiPanel>
      </EuiFlexItem>

      {/* Validation */}
      <EuiFlexItem>
        <EuiPanel hasBorder>
          <EuiFlexGroup alignItems="center" gutterSize="s" responsive={false}>
            <EuiFlexItem grow={false}>
              <EuiIcon type="check" color="primary" />
            </EuiFlexItem>
            <EuiFlexItem>
              <EuiTitle size="xs">
                <h3>Data Validation</h3>
              </EuiTitle>
            </EuiFlexItem>
          </EuiFlexGroup>
          <EuiText size="s" color="subdued">
            <p>
              Use this dataset as a reference for validating feature quality and
              detecting drift.
            </p>
          </EuiText>
          <EuiSpacer size="s" />
          <EuiCodeBlock
            language="python"
            isCopyable
            paddingSize="m"
            fontSize="s"
          >
            {validationCode}
          </EuiCodeBlock>
        </EuiPanel>
      </EuiFlexItem>
    </EuiFlexGroup>
  );
};

export default DatasetUsageTab;
