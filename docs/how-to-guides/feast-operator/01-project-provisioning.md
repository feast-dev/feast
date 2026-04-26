# Guide 1 — Project Provisioning

The operator needs a Feast feature repository (a directory containing `feature_store.yaml`
and Python feature-view definitions) to work from. `spec.feastProjectDir` controls how that
directory is created inside the pods. Exactly one of `git` or `init` must be set.

---

## Option A — Clone from a Git repository (`feastProjectDir.git`)

The operator runs an init container that clones the repository before the Feast processes
start. Use this for production: your feature definitions live in version control and the
operator tracks a specific commit or branch.

### Minimal example

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: my-feature-store
spec:
  feastProject: credit_scoring
  feastProjectDir:
    git:
      url: https://github.com/my-org/feast-feature-repo
      ref: main          # branch, tag, or commit SHA
```

### Pinning to a specific commit (recommended for production)

```yaml
feastProjectDir:
  git:
    url: https://github.com/my-org/feast-feature-repo
    ref: 598a270        # immutable SHA — no surprise changes on pod restart
```

### Monorepo: feature repo in a subdirectory

When the Feast feature repository lives inside a larger monorepo, use `featureRepoPath`
to point at the subdirectory (relative path, no leading `/`):

```yaml
feastProjectDir:
  git:
    url: https://github.com/my-org/data-platform
    ref: e959053
    featureRepoPath: ml/feast/feature_repo   # relative to repo root
```

### Private repositories — token authentication

Create a Kubernetes Secret containing the token:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: git-token
stringData:
  TOKEN: <your-personal-access-token>
```

Reference the Secret from `envFrom` and rewrite the remote URL via `configs`:

```yaml
feastProjectDir:
  git:
    url: https://github.com/my-org/private-repo
    configs:
      # Replaces the HTTPS URL with one that includes the token
      'url."https://api:${TOKEN}@github.com/".insteadOf': 'https://github.com/'
    envFrom:
      - secretRef:
          name: git-token
```

### Disabling TLS verification (not recommended for production)

```yaml
feastProjectDir:
  git:
    url: https://internal-git.corp/feast-repo
    configs:
      http.sslVerify: 'false'
```

### Full `git` field reference

| Field | Type | Description |
|-------|------|-------------|
| `url` | string | Repository URL (HTTPS or SSH) |
| `ref` | string | Branch, tag, or commit SHA. Defaults to the remote HEAD |
| `featureRepoPath` | string | Relative path within the repo to the feature repository directory. Default: `feature_repo` |
| `configs` | map[string]string | Key-value pairs passed to `git -c` before clone |
| `env` | EnvVar[] | Environment variables for the git init container |
| `envFrom` | EnvFromSource[] | Sources (Secrets, ConfigMaps) for init container environment |

---

## Option B — Scaffold a new project (`feastProjectDir.init`)

The operator runs `feast init` on first startup to create a minimal feature repository.
Use this for development, demos, and CI environments where you do not yet have a feature
repo to point at.

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: feast-dev
spec:
  feastProject: sample_project
  feastProjectDir:
    init: {}           # defaults: template=local, minimal=false
```

### Templates

`feast init` supports store-specific templates. Set `template` to generate a scaffold that
matches your chosen online/offline store:

```yaml
feastProjectDir:
  init:
    template: spark      # scaffolds Spark-compatible feature_store.yaml
```

Available templates (validated by the CRD):
`local` · `gcp` · `aws` · `snowflake` · `spark` · `postgres` · `hbase` · `cassandra` ·
`hazelcast` · `couchbase` · `clickhouse`

### Minimal scaffold

`minimal: true` skips example feature-view files and creates only the bare
`feature_store.yaml`:

```yaml
feastProjectDir:
  init:
    minimal: true
```

### Full `init` field reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `template` | string | `local` | Template name for `feast init --template` |
| `minimal` | bool | `false` | Pass `--minimal` to `feast init` |

---

## `feast apply` on startup

By default, when the init container completes (git clone or `feast init`), the operator runs
`feast apply` before starting the servers. This registers all feature definitions with the
registry.

To **skip** `feast apply` on pod start (e.g. you manage registry updates separately):

```yaml
services:
  disableInitContainers: true    # skip both clone/init AND feast apply
```

Or to keep the init container but skip the apply step:

```yaml
services:
  runFeastApplyOnInit: false
```

---

## When `feastProjectDir` is omitted

If neither `git` nor `init` is set, the operator mounts an empty directory. In this case
you must supply a `feature_store.yaml` through another mechanism (e.g. a ConfigMap volume
mount via `services.volumes` + `volumeMounts`).

---

## See also

- [API reference — `FeastProjectDir`](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/docs/api/markdown/ref.md#feastprojectdir)
- [Sample: public git repo](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_git.yaml)
- [Sample: private git repo with token](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_git_token.yaml)
- [Sample: monorepo with featureRepoPath](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_git_repopath.yaml)
- [Sample: feast init](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_init.yaml)
