---
title: "Building AI Agents with Feast: Feature Stores as Context and Memory"
description: "How Feast's MCP integration turns your feature store into a governed context and memory layer for AI agents, bridging the gap between experimental agents and production-ready systems."
date: 2026-04-09
authors: ["Nikhil Kathole"]
---

<div class="hero-image">
  <img src="/images/blog/feast-mcp-agent.png" alt="AI Agents powered by Feast Feature Store" loading="lazy">
</div>

AI agents are moving from demos to production. They handle customer support, orchestrate complex workflows, and make real-time decisions that affect business outcomes. But there is a gap between a working prototype and a production system: agents need reliable, low-latency access to structured data, they need to remember what happened in prior interactions, and all of this access needs to be governed.

This is where feature stores enter the picture. In this post, we show how **Feast** -- an open-source feature store -- can serve as both the **context provider** and the **persistent memory layer** for AI agents, using the **Model Context Protocol (MCP)**.

## The Problem: Agents Need Context, Memory, and Governance

A standalone LLM knows nothing about your customers, your products, or your internal processes. To make good decisions, agents need **tools** that give them access to real data:

- **Who is this user?** Their plan tier, account age, purchase history, satisfaction score.
- **What do we know about this topic?** Relevant documentation, knowledge-base articles, FAQs.
- **What happened before?** What did this agent discuss with this customer last time? What was left unresolved?

That last point is critical and often overlooked. Most agent demos are stateless -- every conversation starts from scratch. But real support interactions build on prior context: *"I called about this yesterday"*, *"you said you'd escalate"*, *"I prefer email over chat."* An agent without memory cannot handle these.

Without a proper data layer, teams end up writing ad-hoc database queries, hardcoding API calls, stuffing memory into Redis with no governance, or giving agents raw database access. This creates fragile, ungoverned, and hard-to-audit agent systems.

## Feature Stores Solve This -- Including Memory

Feature stores were built to solve exactly this class of problem -- albeit originally for traditional ML. They provide:

1. **Low-latency online serving** of pre-computed features.
2. **Versioned, governed access** to data with RBAC and audit trails.
3. **Consistency** between training/offline and serving/online environments.
4. **A single abstraction** over diverse data sources (databases, data warehouses, streaming systems, and vector stores -- including Milvus, Elasticsearch, Qdrant, PGVector, and FAISS).
5. **Entity-keyed read/write** -- the same mechanism that serves features can also store and retrieve agent memory, keyed by customer ID, session ID, or any entity.

With Feast's MCP support, these capabilities are exposed as **tools that AI agents can discover and call dynamically** -- and critically, agents can **write back** to the feature store, turning it into a governed memory layer.

## Feast + MCP: Turning a Feature Store into an Agent Tool

The [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) is an open standard that lets AI applications discover and interact with external tools through a unified interface. Feast's feature server can now expose its endpoints as MCP tools with a simple configuration change:

```yaml
feature_server:
  type: mcp
  enabled: true
  mcp_enabled: true
  mcp_transport: http
  mcp_server_name: "feast-feature-store"
  mcp_server_version: "1.0.0"
```

Once enabled, any MCP-compatible agent -- whether built with LangChain, CrewAI, AutoGen, or a custom framework -- can connect to `http://your-feast-server/mcp` and discover available tools like `get-online-features` for entity-based retrieval, `retrieve-online-documents` for vector similarity search, and `write-to-online-store` for persisting agent state.

## A Concrete Example: Customer-Support Agent with Memory

To make this tangible, let's walk through a customer-support agent that uses Feast for structured feature retrieval, document search, and persistent memory.

> **Note on the implementation:** This example builds the agent loop from scratch using the OpenAI tool-calling API -- no framework required. We chose this approach to keep dependencies minimal and make every Feast interaction visible. In production, you would typically use a framework like LangChain/LangGraph, CrewAI, or AutoGen. Because Feast exposes a standard MCP endpoint, any of these frameworks can auto-discover the tools with zero custom code (see [Connecting Your Agent Framework](#connecting-your-agent-framework) below).

### The Setup

We define three feature views in Feast:

**Customer profiles** -- structured data served from the online store:

```python
customer_profile = FeatureView(
    name="customer_profile",
    entities=[customer],
    schema=[
        Field(name="name", dtype=String),
        Field(name="email", dtype=String),
        Field(name="plan_tier", dtype=String),
        Field(name="account_age_days", dtype=Int64),
        Field(name="total_spend", dtype=Float64),
        Field(name="open_tickets", dtype=Int64),
        Field(name="satisfaction_score", dtype=Float64),
    ],
    source=customer_profile_source,
    ttl=timedelta(days=1),
)
```

**Knowledge base** -- support articles stored as vector embeddings (Feast supports multiple vector backends including Milvus, Elasticsearch, Qdrant, PGVector, and FAISS -- this example uses Milvus):

```python
knowledge_base = FeatureView(
    name="knowledge_base",
    entities=[document],
    schema=[
        Field(
            name="vector", dtype=Array(Float32),
            vector_index=True,
            vector_search_metric="COSINE",
        ),
        Field(name="title", dtype=String),
        Field(name="content", dtype=String),
        Field(name="category", dtype=String),
    ],
    source=knowledge_base_source,
    ttl=timedelta(days=7),
)
```

**Agent memory** -- per-customer interaction state written back by the agent:

```python
agent_memory = FeatureView(
    name="agent_memory",
    entities=[customer],
    schema=[
        Field(name="last_topic", dtype=String),
        Field(name="last_resolution", dtype=String),
        Field(name="interaction_count", dtype=Int64),
        Field(name="preferences", dtype=String),
        Field(name="open_issue", dtype=String),
    ],
    ttl=timedelta(days=30),
)
```

This is the key insight: Feast is not just providing context to the agent -- it is also **storing the agent's memory**. The `agent_memory` feature view is entity-keyed by customer ID, TTL-managed (30-day expiration), schema-typed, and governed by the same RBAC as every other feature. The agent reads prior interactions via `recall_memory` and writes new ones via `save_memory`, using the same online store infrastructure.

### The Agent Loop

The agent uses OpenAI's tool-calling API. Each round, the LLM sees the conversation history plus the available tools, and decides what to do:

```python
for round in range(MAX_ROUNDS):
    response = call_llm(messages, tools=[
        lookup_customer, search_knowledge_base,
        recall_memory, save_memory,
    ])

    if response.finish_reason == "stop":
        return response.content

    for tool_call in response.tool_calls:
        result = execute_tool(tool_call)  # reads/writes Feast
        messages.append(tool_result(result))
```

A typical multi-turn flow looks like:

1. **Round 1**: Agent calls `recall_memory("C1001")` -- checks for prior interactions. Calls `lookup_customer("C1001")` -- gets profile data. Calls `search_knowledge_base("SSO setup")` -- finds the relevant article.
2. **Round 2**: Has enough context. Generates a personalised response. Calls `save_memory("C1001", topic="SSO setup", resolution="Walked through SSO config steps")`.
3. **Round 3**: Done -- returns the answer.

When C1001 comes back later and says *"I'm following up on my SSO question"*, the agent calls `recall_memory` and immediately knows what was discussed -- no re-explanation needed.

For a simpler question like *"What's my current plan?"*, the agent only calls `lookup_customer` -- skipping the knowledge base entirely. The LLM makes these routing decisions based on the question, not hardcoded logic.

<div class="content-image">
  <img src="/images/blog/feast-mcp-agent-workflow.png" alt="Feast MCP Agent Workflow — agent loop with context retrieval, vector search, and memory persistence through Feast" loading="lazy">
</div>

The agent doesn't need to know *where* the data lives or *which* vector database is behind the scenes. It calls Feast through a standard protocol, and Feast handles the routing to the right store -- whether that's Milvus, Elasticsearch, Qdrant, PGVector, or FAISS. Swapping the vector backend is a configuration change, not a code change. The same protocol handles both reads and writes -- context retrieval and memory persistence use the same governed infrastructure.

### The Response

Instead of a generic answer, the agent produces something like:

> *"Hi Alice! Since you're on our Enterprise plan, SSO is available for your team. Go to Settings > Security > SSO and enter your Identity Provider metadata URL. We support SAML 2.0 and OIDC. Once configured, all team members will authenticate through your IdP. As an Enterprise customer, you also have a dedicated Slack channel and account manager if you need hands-on help."*

The personalisation (mentioning the Enterprise plan, dedicated Slack channel) comes directly from the Feast features. And if Alice calls back next week, the agent already knows what was discussed.

## Why This Matters for Production

### Unified Data Access: Structured Features + Vector Search + Memory

Real-world agents need more than document retrieval. They need access to multiple data types through a single governed interface:

| Data Type | Example | Feast Capability |
|---|---|---|
| Structured features | Account tier, spend history | `get_online_features` |
| Vector embeddings | Support article search | `retrieve_online_documents_v2` |
| Agent memory | Last interaction topic, open issues | `get_online_features` + `write_to_online_store` |
| Streaming features | Real-time click counts | Push sources with `write_to_online_store` |
| Pre-computed predictions | Churn probability | Served alongside other features |

Feast unifies all of these behind a single API that agents can both read from and write to.

### Context Memory: Why Feast Beats Ad-Hoc Solutions

Many teams use Redis, in-memory dicts, or custom databases for agent memory. Feast provides a better foundation:

| Concern | Ad-hoc Memory | Feast Memory |
|---|---|---|
| **Governance** | No RBAC, no audit trail | Same RBAC and permissions as all features |
| **TTL management** | Manual expiration logic | Declarative TTL on the feature view |
| **Entity-keying** | Custom key design | Native entity model (customer_id, session_id, etc.) |
| **Observability** | Custom logging | Integrated with OpenTelemetry and MLflow traces |
| **Offline analysis** | Separate export pipeline | Memory is just another feature -- queryable offline |
| **Schema evolution** | Unstructured blobs | Typed schema with versioning |

Because agent memory is stored as a standard Feast feature view, it inherits all the infrastructure that already exists for serving ML features: monitoring, access control, TTL management, and offline queryability. There is no separate system to operate.

### Governance: Who Can Access What

In production, you do not want every agent to access every feature. Feast provides:

- **RBAC**: Role-based access control with OIDC integration.
- **Feature-level permissions**: Control which feature views each service account can read or write.
- **Audit trails**: Track which agent accessed which features and when.

This is especially important for memory: you want governance over what agents remember and who can read those memories.

### Production Platform Architecture

Deploying agents in production requires more than just the agent code. A well-architected platform wraps agents in enterprise infrastructure without requiring changes to the agent itself. Feast fits naturally into this layered approach:

<div class="content-image">
  <img src="/images/blog/feast-mcp-agent-workflow-prod.png" alt="Production platform architecture — Feast MCP Server behind MCP Gateway with observability, guardrails, and lifecycle management" loading="lazy">
</div>

- **MCP Gateway**: Feast sits behind an Envoy-based MCP Gateway as one of many tool servers. The gateway provides identity-based tool filtering -- an agent's JWT claims determine whether it can call Feast at all, and which features it can access.
- **Sandboxed Execution**: Feast runs as a standard Kubernetes service, benefiting from sandboxed container isolation that keeps agent workloads separated.
- **Observability**: Feast feature-retrieval and memory-write calls flow through the platform's OpenTelemetry pipeline, appearing in MLflow traces alongside LLM calls and tool invocations.
- **Agent Lifecycle Management (Kagenti)**: An operator like Kagenti can discover Feast as a tool server via AgentCard CRDs and inject tracing and governance without code changes.

The principle is straightforward: the agent is yours, the platform provides the guardrails, and Feast provides the data and memory.

## Connecting Your Agent Framework

Since Feast exposes a standard MCP endpoint, integration is framework-agnostic:

**LangChain / LangGraph:**
```python
from langchain_mcp_adapters.client import MultiServerMCPClient

async with MultiServerMCPClient(
    {"feast": {"url": "http://feast-server:6566/mcp", "transport": "streamable_http"}}
) as client:
    tools = client.get_tools()
    # Use tools in your LangGraph agent
```

**Claude Desktop / Cursor:**
```json
{
  "mcpServers": {
    "feast": {
      "url": "http://feast-server:6566/mcp",
      "transport": "streamable_http"
    }
  }
}
```

**Direct REST API:**
```python
import requests

features = requests.post("http://feast-server:6566/get-online-features", json={
    "features": ["customer_profile:plan_tier", "customer_profile:satisfaction_score"],
    "entities": {"customer_id": ["C1001"]},
}).json()
```

## Try It Yourself

We have published a complete working example in the Feast repository. A single script handles setup, server startup, and the demo:

```bash
git clone https://github.com/feast-dev/feast.git
cd feast/examples/agent_feature_store

./run_demo.sh                          # demo mode (no API key needed)
OPENAI_API_KEY=sk-... ./run_demo.sh    # live LLM tool-calling
```

The script installs dependencies, generates sample data (customer profiles, knowledge-base articles, and the agent memory scaffold), starts the Feast MCP server, runs the agent, and tears everything down on exit.

To run with a real LLM, set the API key and (optionally) the base URL and model:

```bash
# OpenAI
export OPENAI_API_KEY="sk-..."  # pragma: allowlist secret
./run_demo.sh

# Ollama (free, local -- no API key needed)
ollama pull llama3.1:8b
export OPENAI_API_KEY="ollama"  # pragma: allowlist secret
export OPENAI_BASE_URL="http://localhost:11434/v1"
export LLM_MODEL="llama3.1:8b"
./run_demo.sh

# Any OpenAI-compatible provider (Azure, vLLM, LiteLLM, etc.)
export OPENAI_API_KEY="your-key"  # pragma: allowlist secret
export OPENAI_BASE_URL="https://your-endpoint/v1"
export LLM_MODEL="your-model"
./run_demo.sh
```

The agent demonstrates memory continuity: when the same customer returns, the agent recalls what was discussed previously.

See the full example on [GitHub](https://github.com/feast-dev/feast/tree/master/examples/agent_feature_store).

## What's Next

The intersection of feature stores and agentic AI is just getting started. Here is what we are working on:

- **Richer MCP tools**: Exposing `retrieve_online_documents_v2` as a first-class MCP tool for native vector search.
- **Memory patterns**: Expanding the memory model to support session-scoped memory, hierarchical summarisation, and cross-agent shared memory.
- **Platform integration**: First-class support in MCP Gateway tool catalogs and agent lifecycle operators.
- **Streaming features for agents**: Real-time feature updates from Kafka/Flink that agents can subscribe to.

## Join the Conversation

We would love to hear how you are using (or plan to use) Feast in your agent workflows. Reach out on [Slack](https://slack.feast.dev/) or [GitHub](https://github.com/feast-dev/feast) -- and give the example a try!
