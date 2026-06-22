# Chronon Demo

This example shows Feast reading Chronon-backed features in two modes:

- Offline historical retrieval from a Chronon materialization exported as Parquet.
- Online retrieval from a live Chronon quickstart service.

## Offline Only

```bash
uv run python examples/chronon/run_demo.py --offline-only
```

## Live Chronon Service

Install `sbt` and a JDK. On macOS with Homebrew, this is enough:

```bash
brew install sbt openjdk@11
export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home
```

Build Chronon at the commit used by Feast's Chronon integration workflow:

```bash
git clone https://github.com/airbnb/chronon.git /tmp/chronon
git -C /tmp/chronon checkout 6c0b8de9f0301521baf61a46ff3083c566fb4052  # pragma: allowlist secret

cd /tmp/chronon/quickstart/mongo-online-impl
sbt assembly

cd /tmp/chronon
sbt "project service" assembly
```

Start the local quickstart stack and Chronon service:

```bash
CHRONON_REPO=/tmp/chronon \
JAVA_BIN="${JAVA_HOME}/bin/java" \
infra/scripts/chronon/start-local-chronon-service.sh
```

If port `9000` is already in use, choose another port and pass the same URL to the demo:

```bash
CHRONON_REPO=/tmp/chronon \
CHRONON_SERVICE_PORT=19000 \
JAVA_BIN="${JAVA_HOME}/bin/java" \
infra/scripts/chronon/start-local-chronon-service.sh

CHRONON_SERVICE_URL=http://127.0.0.1:19000 \
uv run python examples/chronon/run_demo.py
```

Stop the local stack when done:

```bash
infra/scripts/chronon/stop-local-chronon-service.sh
```
