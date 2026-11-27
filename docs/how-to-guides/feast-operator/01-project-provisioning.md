# Guide 1 — Project Provisioning

The operator needs a Feast feature repository (a directory containing `feature_store.yaml`
and Python feature-view definitions) to work from. `spec.feastProjectDir` controls how that
directory is created inside the pods. When `feastProjectDir` is specified, exactly one of
`git`, `init`, or `packaged` must be set.

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
| ------- | ------ | ------------- |
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

## Option C — Use a repository packaged in an image (`feastProjectDir.packaged`)

Use `packaged` when the feature repository is built into a feature-server image. This is
useful in air-gapped environments and in release workflows where feature definitions and
their Python dependencies are promoted together as an immutable image.

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: packaged-feature-store
spec:
  feastProject: credit_scoring
  feastProjectDir:
    packaged:
      image: registry.example.com/feature-server@sha256:0123456789abcdef
      featureRepoPath: /opt/feast/feature_repo
```

The repository can be added to a Feast feature-server image with a Dockerfile such as:

```dockerfile
FROM quay.io/feastdev/feature-server:latest
COPY feature_repo/ /opt/feast/feature_repo/
```

`featureRepoPath` must be a canonical absolute, non-root path: do not use `.`, `..`,
repeated separators, or a trailing separator. Put it outside operator-mounted locations
such as `/feast-data`; a volume mounted there would hide files baked into the image. When
init containers are enabled, the packaged path also must not equal, contain, or be
contained by the staged repository path.

With init containers enabled (the default), each Pod starts in this order:

1. `feast-init` replaces the operator-managed staged repository with a fresh copy of the
   repository from `packaged.featureRepoPath`. With the default storage configuration,
   for example, it copies `/opt/feast/feature_repo` from the image to
   `/feast-data/<feastProject>/feature_repo`.
2. In the staged copy only, `feast-init` replaces `feature_store.yaml` (if exists in the
   baked image) with the configuration generated from the FeatureStore resource. The file
   baked into the image is not modified. The Python feature definitions come from the
   packaged repository, while the FeatureStore resource remains authoritative for runtime
   configuration.
3. When `services.runFeastApplyOnInit` is omitted or `true` (the default), `feast-apply`
   runs `feast apply` from the staged repository using the packaged image. Setting it to
   `false` skips only this step; repository staging still occurs.
4. The Feast service containers start with the staged repository as their working
   directory.

The repository baked into the image is therefore the source artifact, while the staged
repository is the runtime copy used by `feast apply` and the Feast services.

For a baked repository whose own `feature_store.yaml` must remain authoritative, disable
init containers:

```yaml
services:
  disableInitContainers: true
```

In that mode, Feast service containers use `featureRepoPath` directly and neither staging
nor `feast apply` runs during pod initialization. The Operator does not update the registry,
so `feast apply` must be handled separately—for example, by CI/CD or a separately managed
Kubernetes Job or CronJob—whenever the packaged feature definitions change.

The packaged `image` is optional. When set, it is the default for repository initialization,
`feast apply`, and Feast services. `services.initImage` takes precedence for the
`feast-init` and `feast-apply` init containers, while an explicit image on an individual
service takes precedence for that service. When the packaged image is omitted, the operator
uses `RELATED_IMAGE_FEATURE_SERVER` or its built-in feature-server image fallback.

### Full `packaged` field reference

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `featureRepoPath` | string | yes | Canonical absolute, non-root path to the feature repository in the image; it must not overlap the staged repository path |
| `image` | string | no | Image containing the repository; defaults to the operator feature-server image |

---

## `feast apply` on startup

By default, when repository initialization completes (git clone, `feast init`, or packaged
repository staging), the operator runs
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

If `feastProjectDir` is not set, the operator defaults to `feastProjectDir.init: {}` and
creates a local template repository.

---

## See also

- [API reference — `FeastProjectDir`](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/docs/api/markdown/ref.md#feastprojectdir)
- [Sample: public git repo](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_git.yaml)
- [Sample: private git repo with token](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_git_token.yaml)
- [Sample: monorepo with featureRepoPath](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_git_repopath.yaml)
- [Sample: feast init](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_init.yaml)
- [Sample: packaged feature repository](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_packaged.yaml)
