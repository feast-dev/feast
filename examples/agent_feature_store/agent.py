"""
Customer-support AI agent powered by Feast features and memory via MCP.

The LLM decides which tools to call, when to call them, and what to do
with the results.  Feast acts as both the **context provider** (read) and
the **memory store** (write).

The agent has four Feast-backed tools:

  READ tools:
    - lookup_customer:        Retrieve customer profile features.
    - search_knowledge_base:  Retrieve support articles.
    - recall_memory:          Read past interaction context for this customer.

  WRITE tool:
    - save_memory:            Persist notes about this interaction back to Feast.

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

import os
import sys
from typing import Any

import requests

FEAST_SERVER = os.getenv("FEAST_SERVER_URL", "http://localhost:6566")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")
MAX_TOOL_ROUNDS = 5

# ---------------------------------------------------------------------------
# Tools: each wraps a Feast REST call
# ---------------------------------------------------------------------------

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
    {
        "type": "function",
        "function": {
            "name": "save_memory",
            "description": (
                "Save a note about this interaction to the memory store so "
                "future conversations can reference it. Use this after resolving "
                "a question to record what was discussed and any commitments made."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_id": {
                        "type": "string",
                        "description": "The customer ID",
                    },
                    "topic": {
                        "type": "string",
                        "description": "Brief label for the topic discussed, e.g. 'SSO setup'",
                    },
                    "resolution": {
                        "type": "string",
                        "description": "How the issue was resolved or what was communicated",
                    },
                    "open_issue": {
                        "type": "string",
                        "description": "Any unresolved follow-up, or empty string if fully resolved",
                    },
                    "preferences": {
                        "type": "string",
                        "description": "Any stated customer preferences to remember",
                    },
                },
                "required": ["customer_id", "topic", "resolution"],
            },
        },
    },
]


def tool_lookup_customer(customer_id: str) -> dict[str, Any]:
    """Fetch customer profile features from Feast."""
    payload = {
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
    }
    resp = requests.post(f"{FEAST_SERVER}/get-online-features", json=payload)
    resp.raise_for_status()
    data = resp.json()
    results = data.get("results", [])
    feature_names = data.get("metadata", {}).get("feature_names", [])

    features = {}
    for i, name in enumerate(feature_names):
        values = results[i].get("values", [])
        features[name] = values[0] if values else None
    return features


def tool_search_knowledge_base(query: str) -> list[dict[str, Any]]:
    """Search knowledge-base articles from Feast using keyword similarity."""
    payload = {
        "features": [
            "knowledge_base:title",
            "knowledge_base:content",
            "knowledge_base:category",
        ],
        "query_string": query,
        "top_k": 3,
        "api_version": 2,
    }
    resp = requests.post(f"{FEAST_SERVER}/retrieve-online-documents", json=payload)
    resp.raise_for_status()
    data = resp.json()
    results = data.get("results", [])
    feature_names = data.get("metadata", {}).get("feature_names", [])

    num_docs = len(results[0]["values"]) if results else 0
    docs = []
    for doc_idx in range(num_docs):
        doc = {}
        for feat_idx, name in enumerate(feature_names):
            doc[name] = results[feat_idx]["values"][doc_idx]
        if doc.get("title"):
            docs.append(doc)
    return docs


def tool_recall_memory(customer_id: str) -> dict[str, Any]:
    """Read the agent's memory for a customer from Feast."""
    payload = {
        "features": [
            "agent_memory:last_topic",
            "agent_memory:last_resolution",
            "agent_memory:interaction_count",
            "agent_memory:preferences",
            "agent_memory:open_issue",
        ],
        "entities": {"customer_id": [customer_id]},
    }
    resp = requests.post(f"{FEAST_SERVER}/get-online-features", json=payload)
    resp.raise_for_status()
    data = resp.json()
    results = data.get("results", [])
    feature_names = data.get("metadata", {}).get("feature_names", [])

    memory = {}
    for i, name in enumerate(feature_names):
        values = results[i].get("values", [])
        memory[name] = values[0] if values else None

    has_memory = any(v is not None for k, v in memory.items() if k != "customer_id")
    if not has_memory:
        return {"status": "no_previous_interactions", "customer_id": customer_id}
    return memory


def tool_save_memory(
    customer_id: str,
    topic: str,
    resolution: str,
    open_issue: str = "",
    preferences: str = "",
) -> dict[str, str]:
    """Write interaction memory back to Feast via the REST API."""
    from datetime import datetime, timezone

    existing = tool_recall_memory(customer_id)
    prev_count = existing.get("interaction_count") or 0

    now = datetime.now(timezone.utc).isoformat()
    payload = {
        "feature_view_name": "agent_memory",
        "df": {
            "customer_id": [customer_id],
            "last_topic": [topic],
            "last_resolution": [resolution],
            "interaction_count": [prev_count + 1],
            "preferences": [preferences],
            "open_issue": [open_issue],
            "event_timestamp": [now],
        },
        "allow_registry_cache": True,
    }
    resp = requests.post(f"{FEAST_SERVER}/write-to-online-store", json=payload)
    resp.raise_for_status()
    return {"status": "saved", "customer_id": customer_id, "topic": topic}


TOOL_REGISTRY = {
    "lookup_customer": lambda args: tool_lookup_customer(args["customer_id"]),
    "search_knowledge_base": lambda args: tool_search_knowledge_base(args["query"]),
    "recall_memory": lambda args: tool_recall_memory(args["customer_id"]),
    "save_memory": lambda args: tool_save_memory(
        customer_id=args["customer_id"],
        topic=args["topic"],
        resolution=args["resolution"],
        open_issue=args.get("open_issue", ""),
        preferences=args.get("preferences", ""),
    ),
}


# ---------------------------------------------------------------------------
# Agent loop
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are a customer-support agent. Follow these steps EXACTLY:

Step 1: Call recall_memory and lookup_customer in your FIRST round (call both at once).
Step 2: If needed, call search_knowledge_base.
Step 3: Write your answer to the customer. Personalise it using their name and plan tier.
Step 4: Call save_memory with a short topic and resolution.

IMPORTANT:
- Never call the same tool twice with the same arguments.
- Call at most 2-3 tools per round.
- After you have the tool results, WRITE your answer. Do not call more tools.
- Tailor your answer to the customer's plan: enterprise customers get full access,
  pro/starter customers may need to upgrade for certain features.
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


def run_agent(customer_id: str, user_message: str) -> str:
    """
    Agentic loop: the LLM decides which tools to call (if any), executes
    them, feeds results back, and repeats until it produces a final answer.
    Memory is saved as a fallback only if the LLM didn't call save_memory.
    """
    if not OPENAI_API_KEY:
        return _run_agent_demo_mode(customer_id, user_message)

    response, memory_saved = _run_agent_llm(customer_id, user_message)

    if not memory_saved:
        _ensure_memory_saved(customer_id, user_message, response)

    return response


def _ensure_memory_saved(customer_id: str, user_message: str, response: str) -> None:
    """Fallback: save memory only when the LLM didn't call save_memory itself."""
    topic = _extract_topic(user_message)
    resolution = response[:120] if response else "Answered query"
    try:
        tool_save_memory(
            customer_id=customer_id,
            topic=topic,
            resolution=resolution,
        )
        print(f'  [Auto] Memory saved: topic="{topic}"')
    except Exception as e:
        print(f"  [Auto] Failed to save memory: {e}")


def _run_agent_llm(customer_id: str, user_message: str) -> tuple[str, bool]:
    """Core LLM agent loop. Returns (response_text, memory_was_saved)."""
    import json

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": f"[Customer ID: {customer_id}]\n\n{user_message}",
        },
    ]

    seen_calls: set[str] = set()
    collected_context: dict[str, Any] = {}
    memory_saved = False

    for round_num in range(1, MAX_TOOL_ROUNDS + 1):
        choice = _call_llm(messages)
        assistant_msg = choice["message"]
        messages.append(assistant_msg)

        content = assistant_msg.get("content") or ""
        if choice["finish_reason"] == "stop":
            if content:
                print(f"  ✓ Agent finished after {round_num} round(s)")
                return content, memory_saved
            break  # empty stop -- fall through to forced response

        tool_calls = assistant_msg.get("tool_calls", [])
        if not tool_calls:
            if content:
                return content, memory_saved
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
                result = handler(fn_args)
                result_str = json.dumps(result, default=str)
                collected_context[fn_name] = result
                if fn_name == "save_memory":
                    memory_saved = True
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
        return content, memory_saved

    # Last resort: build a response from collected tool results
    return _fallback_response(
        collected_context, customer_id, user_message
    ), memory_saved


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


def _run_agent_demo_mode(customer_id: str, user_message: str) -> str:
    """
    Demo mode: simulates the agentic tool-calling flow and generates a
    personalised response that shows how Feast context shapes the answer.
    """
    print("  [Demo mode] Simulating agent reasoning\n")

    # ── Round 1: recall memory ──────────────────────────────────────────
    print(f"  Round 1 | recall_memory(customer_id={customer_id})")
    memory = tool_recall_memory(customer_id)
    has_memory = memory.get("status") != "no_previous_interactions"
    if has_memory:
        print(f"          -> Previous topic: {memory.get('last_topic')}")
        print(f"          -> Open issue: {memory.get('open_issue') or 'none'}")
        print(f"          -> Interaction count: {memory.get('interaction_count')}")
    else:
        print("          -> No prior interactions found")

    # ── Round 1: lookup customer ────────────────────────────────────────
    print(f"\n  Round 1 | lookup_customer(customer_id={customer_id})")
    profile = tool_lookup_customer(customer_id)
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
        docs = tool_search_knowledge_base(user_message)
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

    # ── Round 2: save memory ────────────────────────────────────────────
    topic = _extract_topic(user_message)
    resolution = f"Answered based on {plan} plan context"
    if kb_article:
        resolution += f" and KB article '{kb_article.get('title')}'"
    print(f'\n  Round 2 | save_memory(customer_id={customer_id}, topic="{topic}")')
    tool_save_memory(
        customer_id=customer_id,
        topic=topic,
        resolution=resolution,
    )
    print("          -> Memory saved for future conversations")

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


def main():
    print("=" * 65)
    print("  Feast-Powered AI Agent Demo: Context + Memory")
    print("=" * 65)
    print()
    print("  This demo shows two key capabilities:")
    print("  1. ROLE-BASED RESPONSES: Same question, different answer per plan tier")
    print("  2. PERSISTENT MEMORY:    Agent recalls prior conversations via Feast")
    print()
    print(
        "  Tools: recall_memory | lookup_customer | search_knowledge_base | save_memory"
    )
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

    if not OPENAI_API_KEY:
        print("OPENAI_API_KEY not set -- running in demo mode (simulated reasoning)\n")
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

        response = run_agent(customer_id, query)

        print(f"\n  {'─' * 61}")
        print("  Agent Response:")
        print(f"  {'─' * 61}")
        for line in response.split("\n"):
            print(f"  {line}")
        print()


if __name__ == "__main__":
    main()
