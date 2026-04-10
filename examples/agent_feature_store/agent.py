"""
Customer-support AI agent powered by Feast features and memory via MCP.

The LLM decides which tools to call, when to call them, and what to do
with the results.  Feast acts as both the **context provider** (read) and
the **memory store** (write).

All Feast interactions use the **Model Context Protocol (MCP)**: the agent
connects to the Feast MCP server, discovers available tools dynamically,
and invokes them through the standard protocol -- exactly how production
frameworks like LangChain, LlamaIndex, and CrewAI integrate with MCP
tool servers.

The agent has three Feast-backed read tools:

    - lookup_customer:        Retrieve customer profile features.
    - search_knowledge_base:  Retrieve support articles.
    - recall_memory:          Read past interaction context for this customer.

Memory is automatically saved after every agent turn (framework-style
checkpointing), not as an explicit LLM tool call.  This mirrors how
production frameworks like LangGraph, CrewAI, and AutoGen handle
persistence -- as infrastructure, not an LLM decision.

Memory is entity-keyed (per customer), TTL-managed, versioned, and governed
by the same RBAC as every other feature -- unlike an ad-hoc Redis cache or
an in-process dict.

Prerequisites:
    1. Run `python setup_data.py` to populate sample data.
    2. Start the Feast server: `cd feature_repo && feast serve --host 0.0.0.0 --port 6566 --workers 1`
    3. Set OPENAI_API_KEY (required for tool-calling).

Usage:
    python agent.py
"""

import asyncio
import json
import os
import sys
from typing import Any

import requests

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

FEAST_SERVER = os.getenv("FEAST_SERVER_URL", "http://localhost:6566")
FEAST_MCP_URL = os.getenv("FEAST_MCP_URL", f"{FEAST_SERVER}/mcp")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")
MAX_TOOL_ROUNDS = 5

# Module-level MCP session (initialised in main)
_mcp_session: ClientSession | None = None
# Maps Feast MCP tool names discovered at startup
_feast_tools: dict[str, str] = {}


async def _call_feast_tool(tool_name: str, arguments: dict) -> dict:
    """Call a Feast MCP tool and parse the JSON response."""
    assert _mcp_session is not None, "MCP session not initialised"
    mcp_tool = _feast_tools.get(tool_name)
    if not mcp_tool:
        raise ValueError(
            f"Feast MCP tool '{tool_name}' not found. "
            f"Available: {list(_feast_tools.keys())}"
        )
    result = await _mcp_session.call_tool(mcp_tool, arguments)
    text = result.content[0].text if result.content else "{}"
    return json.loads(text)


async def _discover_feast_tools() -> dict[str, str]:
    """List MCP tools and build a lookup mapping logical names to MCP names."""
    assert _mcp_session is not None
    tools_result = await _mcp_session.list_tools()
    tool_map: dict[str, str] = {}
    for tool in tools_result.tools:
        name = tool.name
        if "get_online_features" in name:
            tool_map["get_online_features"] = name
        elif "retrieve_online_documents" in name:
            tool_map["retrieve_online_documents"] = name
        elif "write_to_online_store" in name:
            tool_map["write_to_online_store"] = name
    return tool_map


# ---------------------------------------------------------------------------
# Tools: each wraps a Feast MCP call
# ---------------------------------------------------------------------------
# These tool specs are domain-specific wrappers for the customer-support demo.
# With a framework (LangChain, LlamaIndex), you can skip these entirely --
# the LLM calls Feast's generic MCP tools (get_online_features, etc.) directly.

TOOLS_SPEC = [
    {
        "type": "function",
        "function": {
            "name": "lookup_customer",
            "description": (
                "Look up a customer's profile from the feature store. Returns "
                "name, email, plan tier, account age, total spend, open support "
                "tickets, and satisfaction score."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_id": {
                        "type": "string",
                        "description": "The customer ID, e.g. 'C1001'",
                    }
                },
                "required": ["customer_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_knowledge_base",
            "description": (
                "Search the support knowledge base for articles relevant to "
                "the user's question. Returns article titles, content, and "
                "categories. Use this when you need product documentation or "
                "how-to information."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The search query describing what the user needs help with",
                    }
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "recall_memory",
            "description": (
                "Recall past interaction context for a customer from the memory "
                "store. Returns the last topic discussed, how it was resolved, "
                "interaction count, stated preferences, and any open issue. "
                "Call this at the start of a conversation to personalise your "
                "response based on history."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_id": {
                        "type": "string",
                        "description": "The customer ID to recall memory for",
                    }
                },
                "required": ["customer_id"],
            },
        },
    },
]


def _parse_online_features(data: dict) -> dict[str, Any]:
    """Parse the MCP to_dict() response into a flat dict of first-row values."""
    features = {}
    for key, values in data.items():
        val = values[0] if isinstance(values, list) and values else values
        if val is not None:
            features[key] = val
    return features


async def tool_lookup_customer(customer_id: str) -> dict[str, Any]:
    """Fetch customer profile features from Feast via MCP."""
    data = await _call_feast_tool(
        "get_online_features",
        {
            "features": [
                "customer_profile:name",
                "customer_profile:email",
                "customer_profile:plan_tier",
                "customer_profile:account_age_days",
                "customer_profile:total_spend",
                "customer_profile:open_tickets",
                "customer_profile:satisfaction_score",
            ],
            "entities": {"customer_id": [customer_id]},
        },
    )
    return _parse_online_features(data)


async def tool_search_knowledge_base(query: str) -> list[dict[str, Any]]:
    """Search knowledge-base articles from Feast via MCP."""
    data = await _call_feast_tool(
        "retrieve_online_documents",
        {
            "features": [
                "knowledge_base:title",
                "knowledge_base:content",
                "knowledge_base:category",
            ],
            "query_string": query,
            "top_k": 3,
            "api_version": 2,
        },
    )
    first_col = next(iter(data.values()), [])
    num_docs = len(first_col) if isinstance(first_col, list) else 0
    docs = []
    for doc_idx in range(num_docs):
        doc = {}
        for key, values in data.items():
            if isinstance(values, list) and doc_idx < len(values):
                doc[key] = values[doc_idx]
        if doc.get("title"):
            docs.append(doc)
    return docs


async def tool_recall_memory(customer_id: str) -> dict[str, Any]:
    """Read the agent's memory for a customer from Feast via MCP."""
    data = await _call_feast_tool(
        "get_online_features",
        {
            "features": [
                "agent_memory:last_topic",
                "agent_memory:last_resolution",
                "agent_memory:interaction_count",
                "agent_memory:preferences",
                "agent_memory:open_issue",
            ],
            "entities": {"customer_id": [customer_id]},
        },
    )
    memory = _parse_online_features(data)
    has_memory = any(v is not None for k, v in memory.items() if k != "customer_id")
    if not has_memory:
        return {"status": "no_previous_interactions", "customer_id": customer_id}
    return memory


async def tool_save_memory(
    customer_id: str,
    topic: str,
    resolution: str,
    open_issue: str = "",
    preferences: str = "",
) -> dict[str, str]:
    """Write interaction memory back to Feast via MCP."""
    from datetime import datetime, timezone

    existing = await tool_recall_memory(customer_id)
    prev_count = existing.get("interaction_count", 0)
    prev_preferences = existing.get("preferences", "")
    prev_open_issue = existing.get("open_issue", "")

    now = datetime.now(timezone.utc).isoformat()
    await _call_feast_tool(
        "write_to_online_store",
        {
            "feature_view_name": "agent_memory",
            "df": {
                "customer_id": [customer_id],
                "last_topic": [topic],
                "last_resolution": [resolution],
                "interaction_count": [prev_count + 1],
                "preferences": [preferences or prev_preferences],
                "open_issue": [open_issue or prev_open_issue],
                "event_timestamp": [now],
            },
            "allow_registry_cache": True,
        },
    )
    return {"status": "saved", "customer_id": customer_id, "topic": topic}


TOOL_REGISTRY: dict[str, Any] = {
    "lookup_customer": lambda args: tool_lookup_customer(
        args.get("customer_id") or next(iter(args.values()))
    ),
    "search_knowledge_base": lambda args: tool_search_knowledge_base(
        args.get("query") or args.get("search_query") or next(iter(args.values()))
    ),
    "recall_memory": lambda args: tool_recall_memory(
        args.get("customer_id") or next(iter(args.values()))
    ),
}


# ---------------------------------------------------------------------------
# Agent loop
# ---------------------------------------------------------------------------

# Demo-specific prompt: replace with your own domain instructions and tool
# names when adapting this example to a different use case.
SYSTEM_PROMPT = """\
You are a customer-support agent. You MUST follow these steps in order:

1. ALWAYS call BOTH recall_memory AND lookup_customer in your first round.
   Call them together in the same round. You MUST call lookup_customer even if
   recall_memory returns no history -- you need the customer's name and plan.
2. If the question is about a product feature (SSO, API, invoices, passwords,
   upgrades, etc.), also call search_knowledge_base with a short keyword.
3. Once you have the tool results, write a helpful, personalised answer.
   Use the customer's name and plan tier. Enterprise customers get full access;
   starter/pro customers may need to upgrade for certain features.

Memory is saved automatically -- do NOT try to save it yourself.

Rules:
- Never call the same tool twice with the same arguments.
- After you have tool results, WRITE your answer immediately. Do not call more tools.
"""


def _call_llm(messages: list, use_tools: bool = True) -> dict:
    """Make a single LLM API call, returning the parsed choice dict."""
    url = f"{OPENAI_BASE_URL}/chat/completions"
    payload: dict[str, Any] = {
        "model": LLM_MODEL,
        "messages": messages,
        "temperature": 0.3,
    }
    if use_tools:
        payload["tools"] = TOOLS_SPEC
    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        },
        json=payload,
    )
    if not resp.ok:
        print(f"\n  ERROR {resp.status_code} from {url}")
        print(f"  Response: {resp.text[:300]}")
        print(
            "\n  Hint: set OPENAI_BASE_URL to an OpenAI-compatible endpoint.\n"
            "  Examples:\n"
            "    OpenAI:  export OPENAI_BASE_URL=https://api.openai.com/v1\n"
            "    Ollama:  export OPENAI_BASE_URL=http://localhost:11434/v1\n"
        )
        resp.raise_for_status()
    return resp.json()["choices"][0]


async def run_agent(customer_id: str, user_message: str) -> str:
    """
    Agentic loop: the LLM decides which tools to call (if any), executes
    them, feeds results back, and repeats until it produces a final answer.
    Memory is auto-saved after every turn -- framework-style checkpointing,
    not an LLM decision.
    """
    if not OPENAI_API_KEY:
        response = await _run_agent_demo_mode(customer_id, user_message)
    else:
        response = await _run_agent_llm(customer_id, user_message)

    await _auto_save_memory(customer_id, user_message, response)
    return response


async def _auto_save_memory(customer_id: str, user_message: str, response: str) -> None:
    """
    Framework-style memory checkpoint: automatically persist interaction
    context after every agent turn.

    Production agent frameworks (LangGraph checkpointers, CrewAI memory,
    AutoGen teachable agents) all treat memory as infrastructure -- the
    framework saves state after each step, rather than relying on the LLM
    to decide when to persist.  This ensures consistent, reliable memory
    regardless of LLM behaviour.
    """
    topic = _extract_topic(user_message)
    resolution = response[:120] if response else "Answered query"
    try:
        await tool_save_memory(
            customer_id=customer_id,
            topic=topic,
            resolution=resolution,
        )
        print(f'  [Checkpoint] Memory saved: topic="{topic}"')
    except Exception as e:
        print(f"  [Checkpoint] Failed to save memory: {e}")


async def _run_agent_llm(customer_id: str, user_message: str) -> str:
    """Core LLM agent loop. Returns the response text."""
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": f"[Customer ID: {customer_id}]\n\n{user_message}",
        },
    ]

    seen_calls: set[str] = set()
    collected_context: dict[str, Any] = {}

    for round_num in range(1, MAX_TOOL_ROUNDS + 1):
        choice = _call_llm(messages)
        assistant_msg = choice["message"]
        messages.append(assistant_msg)

        content = assistant_msg.get("content") or ""
        if choice["finish_reason"] == "stop":
            if content:
                print(f"  ✓ Agent finished after {round_num} round(s)")
                return content
            break  # empty stop -- fall through to forced response

        tool_calls = assistant_msg.get("tool_calls", [])
        if not tool_calls:
            if content:
                return content
            break

        tool_names = [tc["function"]["name"] for tc in tool_calls]
        print(f"  🔧 Round {round_num}: LLM chose tool(s): {', '.join(tool_names)}")

        for tc in tool_calls:
            fn_name = tc["function"]["name"]
            fn_args = json.loads(tc["function"]["arguments"])
            call_key = f"{fn_name}:{json.dumps(fn_args, sort_keys=True)}"

            if call_key in seen_calls:
                print(f"  [Round {round_num}] Skipping duplicate: {fn_name}({fn_args})")
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tc["id"],
                        "content": json.dumps(
                            {
                                "note": "Already called. Use the results you have and respond."
                            }
                        ),
                    }
                )
                continue

            seen_calls.add(call_key)
            print(f"  [Round {round_num}] ➜ {fn_name}({fn_args})")

            handler = TOOL_REGISTRY.get(fn_name)
            if handler:
                result = await handler(fn_args)
                result_str = json.dumps(result, default=str)
                collected_context[fn_name] = result
            else:
                result_str = json.dumps({"error": f"Unknown tool: {fn_name}"})

            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "content": result_str,
                }
            )

    # If the LLM never produced a response, force one last call without tools
    print("  ⏳ Forcing final response...")
    messages.append(
        {
            "role": "user",
            "content": "You have all the information. Write your answer to the customer now.",
        }
    )
    choice = _call_llm(messages, use_tools=False)
    content = choice["message"].get("content") or ""
    if content:
        return content

    # Last resort: build a response from collected tool results
    return _fallback_response(collected_context, customer_id, user_message)


def _fallback_response(context: dict, customer_id: str, user_message: str) -> str:
    """Build a basic response from collected tool results when the LLM fails."""
    profile = context.get("lookup_customer", {})
    name = profile.get("name", customer_id)
    plan = profile.get("plan_tier", "your")
    memory = context.get("recall_memory", {})

    parts = []
    if memory and memory.get("last_topic"):
        parts.append(
            f'Welcome back, {name}! I see we last discussed "{memory["last_topic"]}".'
        )
    else:
        parts.append(f"Hi {name}!")

    parts.append(
        f"You're on the {plan} plan. I've noted your question about "
        f'"{user_message[:50]}" and will follow up.'
    )
    return " ".join(parts)


async def _run_agent_demo_mode(customer_id: str, user_message: str) -> str:
    """
    Demo mode: simulates the agentic tool-calling flow and generates a
    personalised response that shows how Feast context shapes the answer.
    """
    print("  [Demo mode] Simulating agent reasoning\n")

    # ── Round 1: recall memory ──────────────────────────────────────────
    print(f"  Round 1 | recall_memory(customer_id={customer_id})")
    memory = await tool_recall_memory(customer_id)
    has_memory = memory.get("status") != "no_previous_interactions"
    if has_memory:
        print(f"          -> Previous topic: {memory.get('last_topic')}")
        print(f"          -> Open issue: {memory.get('open_issue') or 'none'}")
        print(f"          -> Interaction count: {memory.get('interaction_count')}")
    else:
        print("          -> No prior interactions found")

    # ── Round 1: lookup customer ────────────────────────────────────────
    print(f"\n  Round 1 | lookup_customer(customer_id={customer_id})")
    profile = await tool_lookup_customer(customer_id)
    name = profile.get("name", "Customer")
    plan = profile.get("plan_tier", "unknown")
    spend = profile.get("total_spend", 0)
    tickets = profile.get("open_tickets", 0)
    print(
        f"          -> {name} | {plan} plan | ${spend:,.0f} spend | {tickets} open tickets"
    )

    # ── Round 1: search knowledge base (if question needs docs) ─────────
    needs_kb = any(
        kw in user_message.lower()
        for kw in [
            "how",
            "what",
            "set up",
            "configure",
            "reset",
            "help",
            "sso",
            "api",
            "invoice",
            "upgrade",
            "password",
        ]
    )
    kb_article = None
    if needs_kb:
        print(f'\n  Round 1 | search_knowledge_base(query="{user_message[:50]}...")')
        docs = await tool_search_knowledge_base(user_message)
        if docs:
            kb_article = _pick_best_article(user_message, docs)
            print(f'          -> Best match: "{kb_article.get("title")}"')

    # ── Round 2: generate response (simulated LLM reasoning) ────────────
    print("\n  Round 2 | Generating personalised response...")
    response = _build_demo_response(
        name=name,
        plan=plan,
        profile=profile,
        memory=memory if has_memory else None,
        kb_article=kb_article,
        user_message=user_message,
    )

    return response


def _pick_best_article(query: str, docs: list) -> dict:
    """Simple keyword matching to select the most relevant article."""
    query_lower = query.lower()
    keywords_to_category = {
        "sso": "Configuring single sign-on",
        "single sign": "Configuring single sign-on",
        "password": "How to reset your password",  # pragma: allowlist secret
        "reset": "How to reset your password",
        "invoice": "Understanding your invoice",
        "billing": "Understanding your invoice",
        "upgrade": "Upgrading your subscription",
        "plan": "Upgrading your subscription",
        "api": "Setting up API access",
        "rate limit": "Setting up API access",
        "support": "Contacting support",
        "contact": "Contacting support",
    }
    for keyword, title_prefix in keywords_to_category.items():
        if keyword in query_lower:
            for doc in docs:
                if doc.get("title", "").startswith(title_prefix):
                    return doc
    return docs[0]


def _extract_topic(message: str) -> str:
    """Extract a short topic label from the user message."""
    topic_map = {
        "sso": "SSO setup",
        "invoice": "Invoice help",
        "upgrade": "Plan upgrade",
        "api": "API access",
        "password": "Password reset",  # pragma: allowlist secret
        "reset": "Password reset",
    }
    lower = message.lower()
    for keyword, topic in topic_map.items():
        if keyword in lower:
            return topic
    return message[:40]


def _build_demo_response(
    name: str,
    plan: str,
    profile: dict,
    memory: dict | None,
    kb_article: dict | None,
    user_message: str,
) -> str:
    """Build a realistic personalised response based on Feast context."""
    parts = []

    # Acknowledge returning customer if we have memory
    if memory and memory.get("last_topic"):
        parts.append(
            f"Welcome back, {name}! I can see from our records that we last "
            f'discussed "{memory["last_topic"]}".'
        )
        if memory.get("open_issue"):
            parts.append(
                f"I also notice you have an open issue: {memory['open_issue']}. "
                "Let me know if you'd like to follow up on that."
            )
    else:
        parts.append(f"Hi {name}!")

    # Role-based response logic
    lower = user_message.lower()

    if "sso" in lower:
        if plan == "enterprise":
            parts.append(
                "Since you're on our Enterprise plan, SSO is available for your "
                "team. Go to Settings > Security > SSO and enter your Identity "
                "Provider metadata URL. We support SAML 2.0 and OIDC. Once "
                "configured, all team members will authenticate through your IdP."
            )
            parts.append(
                "As an Enterprise customer, you also have a dedicated Slack "
                "channel and account manager if you need hands-on help."
            )
        elif plan == "pro":
            parts.append(
                "SSO is only available on our Enterprise plan. You're currently "
                "on the Pro plan. Would you like to learn about upgrading? The "
                "Enterprise plan includes SSO, priority support, and a dedicated "
                "account manager."
            )
        else:
            parts.append(
                "SSO is an Enterprise-only feature. You're currently on the "
                f"Starter plan (${profile.get('total_spend', 0):,.0f} total spend). "
                "You'd need to upgrade to Enterprise to access SSO. I can walk "
                "you through the upgrade options if you're interested."
            )

    elif "invoice" in lower:
        parts.append(
            "Invoices are generated on the first of each month and sent to "
            f"{profile.get('email', 'your billing email')}."
        )
        if plan == "enterprise":
            parts.append(
                "As an Enterprise customer, your invoice includes base plan "
                "charges, any overage fees, and applied credits. You can also "
                "reach your dedicated account manager for a detailed breakdown."
            )
        else:
            parts.append(
                "You can download past invoices from Billing > Invoices. Each "
                "invoice shows base charges and any overages."
            )
        if profile.get("open_tickets", 0) > 0:
            parts.append(
                f"I also see you have {profile['open_tickets']} open support "
                "ticket(s) -- let me know if any are billing-related."
            )

    elif "upgrade" in lower or "api" in lower:
        if plan == "starter":
            parts.append(
                "Great question! You're on the Starter plan. Upgrading to Pro "
                "gives you API access with 1,000 requests/minute. Enterprise "
                "gets you 5,000 req/min plus priority support. The price "
                "difference is prorated for your current billing cycle."
            )
        elif plan == "pro":
            parts.append(
                "You're on the Pro plan with 1,000 API requests/minute. "
                "Upgrading to Enterprise would give you 5,000 req/min, SSO, "
                f"and a dedicated account manager. Given your ${profile.get('total_spend', 0):,.0f} "
                "total spend, I can check if there are any loyalty discounts available."
            )
        else:
            parts.append(
                "You're already on our Enterprise plan with the highest rate "
                "limits (5,000 req/min). If you need even higher throughput, "
                "I can connect you with your account manager to discuss custom limits."
            )

    elif memory and memory.get("last_topic"):
        parts.append(
            f"Yes, I have the full context from our previous conversation about "
            f'"{memory["last_topic"]}". '
            f"We've now had {memory.get('interaction_count', 1)} interaction(s). "
            "How can I help you today?"
        )

    else:
        parts.append("How can I help you today?")

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Demo queries
# ---------------------------------------------------------------------------

DEMO_QUERIES = [
    # Scene 1: Enterprise customer asks about SSO -- should get full access instructions
    ("C1001", "How do I set up SSO for my team?"),
    # Scene 2: Starter customer asks the SAME question -- should be told it's Enterprise-only
    ("C1003", "How do I set up SSO for my team?"),
    # Scene 3: Pro customer asks about invoices -- response uses their email/ticket context
    ("C1002", "I need help understanding my last invoice."),
    # Scene 4: C1001 returns -- agent should recall the SSO conversation from Scene 1
    ("C1001", "I'm back about my SSO question from earlier."),
]


async def main():
    global _mcp_session, _feast_tools

    print("=" * 65)
    print("  Feast-Powered AI Agent Demo: Context + Memory via MCP")
    print("=" * 65)
    print()
    print("  This demo shows two key capabilities:")
    print("  1. ROLE-BASED RESPONSES: Same question, different answer per plan tier")
    print("  2. PERSISTENT MEMORY:    Agent recalls prior conversations via Feast")
    print()
    print("  Tools: recall_memory | lookup_customer | search_knowledge_base")
    print("  Memory: auto-saved after each turn (framework-style checkpoint)")
    print(f"  Protocol: MCP ({FEAST_MCP_URL})")
    print()

    try:
        resp = requests.get(f"{FEAST_SERVER}/health")
        resp.raise_for_status()
        print(f"Feast server: healthy at {FEAST_SERVER}")
    except (requests.ConnectionError, requests.HTTPError) as exc:
        print(f"ERROR: Cannot reach Feast server at {FEAST_SERVER} ({exc})")
        print(
            "Start it with: cd feature_repo && feast serve --host 0.0.0.0 --port 6566 --workers 1"
        )
        sys.exit(1)

    async with streamablehttp_client(FEAST_MCP_URL) as (read_stream, write_stream, _):
        async with ClientSession(read_stream, write_stream) as session:
            _mcp_session = session
            await session.initialize()

            _feast_tools = await _discover_feast_tools()
            print(f"MCP tools discovered: {', '.join(_feast_tools.values())}")

            if not OPENAI_API_KEY:
                print(
                    "OPENAI_API_KEY not set -- running in demo mode (simulated reasoning)\n"
                )
            else:
                print(f"Using LLM: {LLM_MODEL} via {OPENAI_BASE_URL}\n")

            scene_labels = [
                "Scene 1: Enterprise customer (C1001) asks about SSO",
                "Scene 2: Starter customer (C1003) asks the SAME SSO question",
                "Scene 3: Pro customer (C1002) asks about invoices",
                "Scene 4: C1001 returns -- does the agent remember Scene 1?",
            ]

            for i, (customer_id, query) in enumerate(DEMO_QUERIES):
                label = scene_labels[i] if i < len(scene_labels) else ""
                print(f"\n{'=' * 65}")
                print(f"  {label}")
                print(f'  Customer: {customer_id}  |  Query: "{query}"')
                print(f"{'=' * 65}")

                response = await run_agent(customer_id, query)

                print(f"\n  {'─' * 61}")
                print("  Agent Response:")
                print(f"  {'─' * 61}")
                for line in response.split("\n"):
                    print(f"  {line}")
                print()


if __name__ == "__main__":
    asyncio.run(main())
