"""
═══════════════════════════════════════════════════════════════════════════
API COST ENGINE — Real-Time Pricing & Usage Tracker
═══════════════════════════════════════════════════════════════════════════
Tracks actual API usage across Gemini, OpenAI & Anthropic, calculates
costs with current per-token pricing, generates comparison reports & Excel exports.

Updated: March 2026
═══════════════════════════════════════════════════════════════════════════
"""

import io
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

# ═══════════════════════════════════════════════════════════════════════════
# MODEL PRICING REGISTRY — Current as of March 2026
# All prices are USD per 1 Million tokens
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class ModelPricing:
    """Pricing info for a single AI model."""
    model_id: str
    display_name: str
    provider: str               # "Google", "OpenAI", or "Anthropic"
    input_per_m: float          # $ per 1M input tokens
    output_per_m: float         # $ per 1M output tokens
    cached_per_m: Optional[float] = None  # $ per 1M cached input tokens
    context_window: int = 128_000
    free_tier: bool = False
    icon: str = ""
    category: str = ""          # e.g., "Flagship", "Mini", "Reasoning"


# ── Google Gemini Models ──
GEMINI_PRICING: Dict[str, ModelPricing] = {
    "gemini-2.5-pro": ModelPricing(
        "gemini-2.5-pro", "Gemini 2.5 Pro", "Google",
        input_per_m=1.25, output_per_m=10.00,
        context_window=2_000_000, free_tier=True,
        icon="🧠", category="Flagship"
    ),
    "gemini-2.5-flash": ModelPricing(
        "gemini-2.5-flash", "Gemini 2.5 Flash", "Google",
        input_per_m=0.30, output_per_m=2.50,
        context_window=1_000_000, free_tier=True,
        icon="✨", category="Fast"
    ),
    "gemini-2.0-pro-exp-02-05": ModelPricing(
        "gemini-2.0-pro-exp-02-05", "Gemini 2.0 Pro (Exp)", "Google",
        input_per_m=1.25, output_per_m=5.00,
        context_window=2_000_000, free_tier=True,
        icon="🔬", category="Experimental"
    ),
    "gemini-2.0-flash": ModelPricing(
        "gemini-2.0-flash", "Gemini 2.0 Flash", "Google",
        input_per_m=0.10, output_per_m=0.40,
        context_window=1_000_000, free_tier=True,
        icon="💨", category="Fast"
    ),
    "gemini-pro-latest": ModelPricing(
        "gemini-pro-latest", "Gemini 1.5 Pro", "Google",
        input_per_m=1.25, output_per_m=5.00,
        context_window=2_000_000, free_tier=True,
        icon="📚", category="Legacy"
    ),
    "gemini-flash-latest": ModelPricing(
        "gemini-flash-latest", "Gemini 1.5 Flash", "Google",
        input_per_m=0.075, output_per_m=0.30,
        context_window=1_000_000, free_tier=True,
        icon="📜", category="Legacy"
    ),
    "gemini-flash-lite-latest": ModelPricing(
        "gemini-flash-lite-latest", "Gemini 1.5 Flash-8B", "Google",
        input_per_m=0.0375, output_per_m=0.15,
        context_window=1_000_000, free_tier=True,
        icon="🚀", category="Budget"
    ),
}

# ── OpenAI Models ──
OPENAI_PRICING: Dict[str, ModelPricing] = {
    # GPT-4.1 Family (1M context)
    "gpt-4.1": ModelPricing(
        "gpt-4.1", "GPT-4.1", "OpenAI",
        input_per_m=3.00, output_per_m=12.00, cached_per_m=0.75,
        context_window=1_000_000, icon="🧠", category="Flagship"
    ),
    "gpt-4.1-mini": ModelPricing(
        "gpt-4.1-mini", "GPT-4.1 Mini", "OpenAI",
        input_per_m=0.80, output_per_m=3.20, cached_per_m=0.20,
        context_window=1_000_000, icon="⚡", category="Mini"
    ),
    "gpt-4.1-nano": ModelPricing(
        "gpt-4.1-nano", "GPT-4.1 Nano", "OpenAI",
        input_per_m=0.20, output_per_m=0.80, cached_per_m=0.05,
        context_window=1_000_000, icon="🚀", category="Nano"
    ),
    # GPT-5 Family
    "gpt-5.4": ModelPricing(
        "gpt-5.4", "GPT-5.4", "OpenAI",
        input_per_m=2.50, output_per_m=15.00, cached_per_m=0.25,
        context_window=1_000_000, icon="👑", category="Flagship"
    ),
    "gpt-5.4-pro": ModelPricing(
        "gpt-5.4-pro", "GPT-5.4 Pro", "OpenAI",
        input_per_m=5.00, output_per_m=30.00, cached_per_m=0.50,
        context_window=1_000_000, icon="💎", category="Pro"
    ),
    "gpt-5.3-chat-latest": ModelPricing(
        "gpt-5.3-chat-latest", "GPT-5.3 Instant", "OpenAI",
        input_per_m=1.75, output_per_m=14.00, cached_per_m=0.175,
        context_window=128_000, icon="🌐", category="Chat"
    ),
    "gpt-5.3-codex": ModelPricing(
        "gpt-5.3-codex", "GPT-5.3 Codex", "OpenAI",
        input_per_m=1.75, output_per_m=14.00, cached_per_m=0.175,
        context_window=400_000, icon="💻", category="Codex"
    ),
    "gpt-5-mini": ModelPricing(
        "gpt-5-mini", "GPT-5 Mini", "OpenAI",
        input_per_m=0.25, output_per_m=2.00, cached_per_m=0.025,
        context_window=1_000_000, icon="🌟", category="Mini"
    ),
    "gpt-5-nano": ModelPricing(
        "gpt-5-nano", "GPT-5 Nano", "OpenAI",
        input_per_m=0.20, output_per_m=1.25, cached_per_m=0.02,
        context_window=1_000_000, icon="💫", category="Nano"
    ),
    # GPT-4o Family
    "gpt-4o": ModelPricing(
        "gpt-4o", "GPT-4o", "OpenAI",
        input_per_m=2.50, output_per_m=10.00, cached_per_m=0.625,
        context_window=128_000, icon="✨", category="Multimodal"
    ),
    "gpt-4o-mini": ModelPricing(
        "gpt-4o-mini", "GPT-4o Mini", "OpenAI",
        input_per_m=0.15, output_per_m=0.60, cached_per_m=0.0375,
        context_window=128_000, icon="💨", category="Budget"
    ),
    # GPT-4 Turbo
    "gpt-4-turbo": ModelPricing(
        "gpt-4-turbo", "GPT-4 Turbo", "OpenAI",
        input_per_m=10.00, output_per_m=30.00,
        context_window=128_000, icon="🏛️", category="Legacy"
    ),
    # O-Series Reasoning
    "o3": ModelPricing(
        "o3", "o3", "OpenAI",
        input_per_m=2.00, output_per_m=8.00, cached_per_m=0.50,
        context_window=200_000, icon="🔬", category="Reasoning"
    ),
    "o3-mini": ModelPricing(
        "o3-mini", "o3-mini", "OpenAI",
        input_per_m=1.10, output_per_m=4.40, cached_per_m=0.275,
        context_window=200_000, icon="🧪", category="Reasoning"
    ),
    "o4-mini": ModelPricing(
        "o4-mini", "o4-mini", "OpenAI",
        input_per_m=1.10, output_per_m=4.40, cached_per_m=0.275,
        context_window=200_000, icon="🔮", category="Reasoning"
    ),
    "o1": ModelPricing(
        "o1", "o1", "OpenAI",
        input_per_m=15.00, output_per_m=60.00, cached_per_m=3.75,
        context_window=200_000, icon="🧩", category="Reasoning"
    ),
    "o1-mini": ModelPricing(
        "o1-mini", "o1-mini", "OpenAI",
        input_per_m=1.10, output_per_m=4.40, cached_per_m=0.275,
        context_window=128_000, icon="📐", category="Reasoning"
    ),
    # GPT-3.5
    "gpt-3.5-turbo": ModelPricing(
        "gpt-3.5-turbo", "GPT-3.5 Turbo", "OpenAI",
        input_per_m=0.50, output_per_m=1.50,
        context_window=16_385, icon="💰", category="Budget"
    ),
}

# ── Anthropic Claude Models ──
CLAUDE_PRICING: Dict[str, ModelPricing] = {
    "claude-sonnet-4-6": ModelPricing(
        "claude-sonnet-4-6", "Claude Sonnet 4.6", "Anthropic",
        input_per_m=3.00, output_per_m=15.00,
        context_window=1_000_000, icon="🟣", category="Flagship"
    ),
    "claude-opus-4-6": ModelPricing(
        "claude-opus-4-6", "Claude Opus 4.6", "Anthropic",
        input_per_m=15.00, output_per_m=75.00,
        context_window=200_000, icon="💎", category="Premium"
    ),
    "claude-sonnet-4-5": ModelPricing(
        "claude-sonnet-4-5", "Claude Sonnet 4.5", "Anthropic",
        input_per_m=3.00, output_per_m=15.00,
        context_window=200_000, icon="🔮", category="Previous Gen"
    ),
    "claude-haiku-4-5": ModelPricing(
        "claude-haiku-4-5", "Claude Haiku 4.5", "Anthropic",
        input_per_m=0.80, output_per_m=4.00,
        context_window=200_000, icon="⚡", category="Fast"
    ),
}

# Combined registry for lookups
ALL_PRICING: Dict[str, ModelPricing] = {**GEMINI_PRICING, **OPENAI_PRICING, **CLAUDE_PRICING}


# ═══════════════════════════════════════════════════════════════════════════
# USAGE TRACKER — Records every API call
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class UsageRecord:
    """Single API call record."""
    timestamp: str
    provider: str
    model: str
    display_name: str
    input_tokens: int
    output_tokens: int
    cached_tokens: int
    input_cost: float
    output_cost: float
    total_cost: float
    prompt_type: str  # "preset" or "custom"
    question_preview: str  # first 80 chars of the question


def _init_usage_log():
    """Initialize the usage log in session state."""
    if "api_usage_log" not in st.session_state:
        st.session_state.api_usage_log = []


def log_api_usage(
    provider: str,
    model_id: str,
    input_tokens: int,
    output_tokens: int,
    cached_tokens: int = 0,
    prompt_type: str = "custom",
    question: str = "",
):
    """Log an API call with cost calculation. Call after every successful API response."""
    _init_usage_log()

    pricing = ALL_PRICING.get(model_id)
    if pricing:
        # Standard input tokens are (total - cached)
        standard_input = max(0, input_tokens - cached_tokens)
        
        # Calculate costs
        standard_cost = (standard_input / 1_000_000) * pricing.input_per_m
        
        # Apply cached discount if applicable
        cache_rate = pricing.cached_per_m if pricing.cached_per_m is not None else pricing.input_per_m
        cached_cost = (cached_tokens / 1_000_000) * cache_rate
        
        input_cost = standard_cost + cached_cost
        output_cost = (output_tokens / 1_000_000) * pricing.output_per_m
        display_name = pricing.display_name
    else:
        # Fallback for unknown models — estimate at average rate
        input_cost = (input_tokens / 1_000_000) * 1.00
        output_cost = (output_tokens / 1_000_000) * 5.00
        display_name = model_id

    record = UsageRecord(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        provider=provider,
        model=model_id,
        display_name=display_name,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cached_tokens=cached_tokens,
        input_cost=input_cost,
        output_cost=output_cost,
        total_cost=input_cost + output_cost,
        prompt_type=prompt_type,
        question_preview=question[:80] + ("..." if len(question) > 80 else ""),
    )
    st.session_state.api_usage_log.append(record)
    return record


def get_usage_summary() -> Dict[str, Any]:
    """Get aggregated usage statistics for the current session."""
    _init_usage_log()
    log: List[UsageRecord] = st.session_state.api_usage_log

    if not log:
        return {
            "total_calls": 0, "total_input_tokens": 0, "total_output_tokens": 0,
            "total_cost": 0.0, "by_provider": {}, "by_model": {},
        }

    total_input = sum(r.input_tokens for r in log)
    total_output = sum(r.output_tokens for r in log)
    total_cached = sum(r.cached_tokens for r in log)
    total_cost = sum(r.total_cost for r in log)

    by_provider: Dict[str, Dict] = {}
    for r in log:
        p = by_provider.setdefault(r.provider, {"calls": 0, "input_tokens": 0, "output_tokens": 0, "cost": 0.0})
        p["calls"] += 1
        p["input_tokens"] += r.input_tokens
        p["output_tokens"] += r.output_tokens
        p["cost"] += r.total_cost

    by_model: Dict[str, Dict] = {}
    for r in log:
        m = by_model.setdefault(r.display_name, {"calls": 0, "input_tokens": 0, "output_tokens": 0, "cost": 0.0})
        m["calls"] += 1
        m["input_tokens"] += r.input_tokens
        m["output_tokens"] += r.output_tokens
        m["cost"] += r.total_cost

    return {
        "total_calls": len(log),
        "total_input_tokens": total_input,
        "total_output_tokens": total_output,
        "total_cost": total_cost,
        "by_provider": by_provider,
        "by_model": by_model,
    }


# ═══════════════════════════════════════════════════════════════════════════
# COST CALCULATOR — Estimate costs before making API calls
# ═══════════════════════════════════════════════════════════════════════════

def estimate_cost(model_id: str, input_tokens: int, output_tokens: int = 2000) -> Dict:
    """Estimate cost for a given model and token count."""
    pricing = ALL_PRICING.get(model_id)
    if not pricing:
        return {"error": f"Unknown model: {model_id}"}

    input_cost = (input_tokens / 1_000_000) * pricing.input_per_m
    output_cost = (output_tokens / 1_000_000) * pricing.output_per_m
    fits = input_tokens < pricing.context_window

    return {
        "model": pricing.display_name,
        "provider": pricing.provider,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "input_cost": input_cost,
        "output_cost": output_cost,
        "total_cost": input_cost + output_cost,
        "fits_context": fits,
        "context_window": pricing.context_window,
        "free_tier": pricing.free_tier,
    }


def compare_all_models(input_tokens: int, output_tokens: int = 2000) -> pd.DataFrame:
    """Compare cost across all models for given token counts."""
    rows = []
    for model_id, pricing in ALL_PRICING.items():
        input_cost = (input_tokens / 1_000_000) * pricing.input_per_m
        output_cost = (output_tokens / 1_000_000) * pricing.output_per_m
        total = input_cost + output_cost
        fits = "✅" if input_tokens < pricing.context_window else "❌"
        free = "🆓 Free" if pricing.free_tier else "💳 Paid"

        rows.append({
            "Provider": pricing.provider,
            "Model": f"{pricing.icon} {pricing.display_name}",
            "Category": pricing.category,
            "Input $/1M": f"${pricing.input_per_m:.3f}",
            "Output $/1M": f"${pricing.output_per_m:.2f}",
            "Est. Cost": f"${total:.4f}" if total < 0.01 else f"${total:.3f}" if total < 1 else f"${total:.2f}",
            "Cost (raw)": total,
            "Fits Context": fits,
            "Context": f"{pricing.context_window:,}",
            "Free Tier": free,
            "10 Calls": f"${total * 10:.3f}",
            "100 Calls": f"${total * 100:.2f}",
            "Monthly (30/day)": f"${total * 900:.2f}",
        })

    df = pd.DataFrame(rows)
    df = df.sort_values("Cost (raw)")
    return df


def estimate_excel_cost(excel_data: Dict[str, pd.DataFrame]) -> int:
    """Estimate token count for full Excel data."""
    total_chars = 0
    for sheet_name, df in excel_data.items():
        try:
            csv_str = df.to_csv(index=False, max_rows=None)
            total_chars += len(csv_str) + len(f"\n=== {sheet_name} ===\n")
        except Exception:
            total_chars += 1000  # fallback estimate
    return total_chars // 4  # ~4 chars per token


# ═══════════════════════════════════════════════════════════════════════════
# EXCEL REPORT GENERATOR
# ═══════════════════════════════════════════════════════════════════════════

def generate_cost_report(excel_data: Optional[Dict] = None) -> bytes:
    """Generate a downloadable Excel report with usage, pricing, and projections."""
    _init_usage_log()
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        # Sheet 1: Session Usage Log
        log = st.session_state.api_usage_log
        if log:
            usage_rows = [{
                "Timestamp": r.timestamp,
                "Provider": r.provider,
                "Model": r.display_name,
                "Input Tokens": r.input_tokens,
                "Output Tokens": r.output_tokens,
                "Input Cost ($)": round(r.input_cost, 6),
                "Output Cost ($)": round(r.output_cost, 6),
                "Total Cost ($)": round(r.total_cost, 6),
                "Type": r.prompt_type,
                "Question": r.question_preview,
            } for r in log]
            pd.DataFrame(usage_rows).to_excel(writer, sheet_name="Usage Log", index=False)
        else:
            pd.DataFrame({"Info": ["No API calls recorded in this session"]}).to_excel(
                writer, sheet_name="Usage Log", index=False)

        # Sheet 2: Model Pricing Reference
        pricing_rows = []
        for model_id, p in ALL_PRICING.items():
            pricing_rows.append({
                "Provider": p.provider,
                "Model ID": p.model_id,
                "Display Name": p.display_name,
                "Category": p.category,
                "Input ($/1M tokens)": p.input_per_m,
                "Output ($/1M tokens)": p.output_per_m,
                "Cached ($/1M tokens)": p.cached_per_m or "N/A",
                "Context Window": p.context_window,
                "Free Tier": "Yes" if p.free_tier else "No",
            })
        pd.DataFrame(pricing_rows).to_excel(writer, sheet_name="Model Pricing", index=False)

        # Sheet 3: Cost Comparison (if Excel data available)
        if excel_data:
            est_tokens = estimate_excel_cost(excel_data)
            comparison_df = compare_all_models(est_tokens)
            comparison_df.to_excel(writer, sheet_name="Cost Comparison", index=False)

            # Sheet 4: Monthly Projections
            proj_rows = []
            for model_id, p in sorted(ALL_PRICING.items(), key=lambda x: x[1].input_per_m):
                for calls_per_day in [5, 10, 30, 50, 100]:
                    input_cost = (est_tokens / 1_000_000) * p.input_per_m
                    output_cost = (2000 / 1_000_000) * p.output_per_m
                    daily = (input_cost + output_cost) * calls_per_day
                    proj_rows.append({
                        "Provider": p.provider,
                        "Model": p.display_name,
                        "Calls/Day": calls_per_day,
                        "Daily Cost ($)": round(daily, 4),
                        "Monthly Cost ($)": round(daily * 30, 2),
                        "Yearly Cost ($)": round(daily * 365, 2),
                    })
            pd.DataFrame(proj_rows).to_excel(writer, sheet_name="Monthly Projections", index=False)

        # Sheet 5: Summary
        summary = get_usage_summary()
        summary_rows = [
            {"Metric": "Total API Calls", "Value": summary["total_calls"]},
            {"Metric": "Total Input Tokens", "Value": f"{summary['total_input_tokens']:,}"},
            {"Metric": "Total Output Tokens", "Value": f"{summary['total_output_tokens']:,}"},
            {"Metric": "Total Session Cost ($)", "Value": f"${summary['total_cost']:.4f}"},
            {"Metric": "Report Generated", "Value": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
        ]
        if excel_data:
            summary_rows.insert(0, {"Metric": "Excel Data Tokens (est.)", "Value": f"{estimate_excel_cost(excel_data):,}"})
        pd.DataFrame(summary_rows).to_excel(writer, sheet_name="Summary", index=False)

    return output.getvalue()


# ═══════════════════════════════════════════════════════════════════════════
# STREAMLIT COST TAB — Premium UI
# ═══════════════════════════════════════════════════════════════════════════

COST_TAB_CSS = """
<style>
.cost-hero {
    background: linear-gradient(135deg, rgba(99, 102, 241, 0.15), rgba(168, 85, 247, 0.08));
    border: 1px solid rgba(99, 102, 241, 0.2);
    border-radius: 16px;
    padding: 24px;
    margin-bottom: 20px;
}
.cost-metric {
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 12px;
    padding: 16px;
    text-align: center;
}
.cost-metric .value {
    font-size: 1.8rem;
    font-weight: 800;
    background: linear-gradient(135deg, #818cf8, #a78bfa);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}
.cost-metric .label {
    font-size: 0.85rem;
    opacity: 0.65;
    margin-top: 4px;
}
.provider-badge {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 999px;
    font-size: 0.75rem;
    font-weight: 600;
}
.badge-google { background: rgba(52, 211, 153, 0.15); color: #34d399; }
.badge-openai { background: rgba(99, 102, 241, 0.15); color: #818cf8; }
.free-badge { background: rgba(52, 211, 153, 0.2); color: #34d399; padding: 2px 8px; border-radius: 6px; font-size: 0.7rem; }
</style>
"""


def _fmt_cost(cost: float) -> str:
    """Format cost with appropriate precision."""
    if cost == 0:
        return "$0.00"
    elif cost < 0.001:
        return f"${cost:.6f}"
    elif cost < 0.01:
        return f"${cost:.4f}"
    elif cost < 1:
        return f"${cost:.3f}"
    else:
        return f"${cost:.2f}"


def _fmt_tokens(tokens: int) -> str:
    """Format token count with K/M suffix."""
    if tokens >= 1_000_000:
        return f"{tokens / 1_000_000:.1f}M"
    elif tokens >= 1_000:
        return f"{tokens / 1_000:.1f}K"
    return str(tokens)


def render_cost_analysis_tab(excel_data: Optional[Dict[str, pd.DataFrame]] = None):
    """Render the full Cost Analysis tab in Streamlit."""
    _init_usage_log()
    st.markdown(COST_TAB_CSS, unsafe_allow_html=True)

    st.header("💰 AI API Cost Analysis")
    st.caption("Real-time usage tracking • Model comparison • Cost optimization")

    # ── Hero: Session Summary ──
    summary = get_usage_summary()

    st.markdown('<div class="cost-hero">', unsafe_allow_html=True)
    cols = st.columns(4)
    metrics = [
        ("🔢", str(summary["total_calls"]), "API Calls"),
        ("📥", _fmt_tokens(summary["total_input_tokens"]), "Input Tokens"),
        ("📤", _fmt_tokens(summary["total_output_tokens"]), "Output Tokens"),
        ("💰", _fmt_cost(summary["total_cost"]), "Session Cost"),
    ]
    for col, (icon, val, label) in zip(cols, metrics):
        with col:
            st.markdown(f"""<div class="cost-metric">
                <div class="value">{icon} {val}</div>
                <div class="label">{label}</div>
            </div>""", unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    # ── Main Tabs ──
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Usage Dashboard",
        "💡 Cost Estimator",
        "📈 Pricing Reference",
        "📥 Export Report",
    ])

    # ══════════════════════════════════════════════════════════════════════
    # TAB 1: Usage Dashboard
    # ══════════════════════════════════════════════════════════════════════
    with tab1:
        if summary["total_calls"] == 0:
            st.info("🔍 No API calls recorded yet. Use the **🤖 AI Chat**, **🧠 ChatGPT**, or **🟣 Claude AI** tabs to start analyzing, and your usage will appear here automatically.")
        else:
            # Provider breakdown
            st.subheader("Provider Breakdown")
            pcols = st.columns(len(summary["by_provider"]))
            provider_colors = {"Google": "#34d399", "OpenAI": "#818cf8", "Anthropic": "#a78bfa"}
            for col, (provider, data) in zip(pcols, summary["by_provider"].items()):
                with col:
                    color = provider_colors.get(provider, "#666")
                    st.markdown(f"""
                    <div style="background: {color}11; border: 1px solid {color}33; border-radius: 12px; padding: 16px;">
                        <div style="font-size: 1.1rem; font-weight: 700; color: {color};">{provider}</div>
                        <div style="margin-top: 8px;">
                            <strong>{data['calls']}</strong> calls •
                            <strong>{_fmt_tokens(data['input_tokens'])}</strong> in •
                            <strong>{_fmt_tokens(data['output_tokens'])}</strong> out
                        </div>
                        <div style="font-size: 1.3rem; font-weight: 800; margin-top: 6px;">{_fmt_cost(data['cost'])}</div>
                    </div>""", unsafe_allow_html=True)

            # Model breakdown
            if summary["by_model"]:
                st.subheader("Cost by Model")
                model_rows = []
                for model, data in sorted(summary["by_model"].items(), key=lambda x: x[1]["cost"], reverse=True):
                    model_rows.append({
                        "Model": model,
                        "Calls": data["calls"],
                        "Input Tokens": f"{data['input_tokens']:,}",
                        "Output Tokens": f"{data['output_tokens']:,}",
                        "Cost": _fmt_cost(data["cost"]),
                    })
                st.dataframe(pd.DataFrame(model_rows), width="stretch", hide_index=True)

            # Call history
            st.subheader("Call History")
            log = st.session_state.api_usage_log
            history_rows = [{
                "Time": r.timestamp.split(" ")[1],
                "Provider": r.provider,
                "Model": r.display_name,
                "In": f"{r.input_tokens:,}",
                "Out": f"{r.output_tokens:,}",
                "Cost": _fmt_cost(r.total_cost),
                "Type": r.prompt_type,
                "Question": r.question_preview,
            } for r in reversed(log)]
            st.dataframe(pd.DataFrame(history_rows), width="stretch", hide_index=True, height=400)

    # ══════════════════════════════════════════════════════════════════════
    # TAB 2: Cost Estimator
    # ══════════════════════════════════════════════════════════════════════
    with tab2:
        st.subheader("💡 Estimate Cost Before You Ask")

        if excel_data:
            est_tokens = estimate_excel_cost(excel_data)
            st.success(f"📊 Your Excel data is approximately **{est_tokens:,} tokens** ({est_tokens * 4:,} chars across all sheets)")
        else:
            st.warning("Upload an Excel file first to get accurate estimates.")
            est_tokens = 100_000  # default placeholder

        st.markdown("---")

        # Custom token input
        ecol1, ecol2 = st.columns(2)
        with ecol1:
            custom_input = st.number_input(
                "Input Tokens",
                value=est_tokens,
                min_value=100,
                max_value=10_000_000,
                step=10_000,
                key="cost_est_input",
            )
        with ecol2:
            custom_output = st.number_input(
                "Expected Output Tokens",
                value=2000,
                min_value=100,
                max_value=200_000,
                step=500,
                key="cost_est_output",
            )

        # Comparison table
        st.markdown("### Model Comparison")
        comparison_df = compare_all_models(custom_input, custom_output)

        # Filter controls
        fcol1, fcol2 = st.columns(2)
        with fcol1:
            provider_filter = st.multiselect("Filter by Provider", ["Google", "OpenAI", "Anthropic"], default=["Google", "OpenAI", "Anthropic"], key="cost_provider_filter")
        with fcol2:
            fit_only = st.checkbox("Only models that fit context", value=True, key="cost_fit_only")

        display_df = comparison_df[comparison_df["Provider"].isin(provider_filter)]
        if fit_only:
            display_df = display_df[display_df["Fits Context"] == "✅"]

        # Display minus the raw sort column
        st.dataframe(
            display_df.drop(columns=["Cost (raw)"]),
            width="stretch",
            hide_index=True,
            height=600,
        )

        # Cheapest recommendation
        if not display_df.empty:
            cheapest = display_df.iloc[0]
            st.markdown(f"""
            ### 🏆 Best Value
            **{cheapest['Model']}** ({cheapest['Provider']}) — **{cheapest['Est. Cost']}** per call
            | {cheapest['Free Tier']} | Context: {cheapest['Context']} tokens |
            Monthly at 30 calls/day: **{cheapest['Monthly (30/day)']}**
            """)

    # ══════════════════════════════════════════════════════════════════════
    # TAB 3: Pricing Reference
    # ══════════════════════════════════════════════════════════════════════
    with tab3:
        st.subheader("📈 Current Model Pricing (March 2026)")

        # Gemini section
        st.markdown("#### 🟢 Google Gemini — Free Tier Available")
        gemini_rows = []
        for mid, p in GEMINI_PRICING.items():
            gemini_rows.append({
                "Model": f"{p.icon} {p.display_name}",
                "Category": p.category,
                "Input ($/1M)": f"${p.input_per_m:.4f}",
                "Output ($/1M)": f"${p.output_per_m:.2f}",
                "Context": f"{p.context_window:,}",
                "Free Tier": "🆓 Yes" if p.free_tier else "❌ No",
            })
        st.dataframe(pd.DataFrame(gemini_rows), width="stretch", hide_index=True)

        st.markdown("---")

        # OpenAI section
        st.markdown("#### 🔵 OpenAI — Paid API")
        openai_rows = []
        for mid, p in OPENAI_PRICING.items():
            cached = f"${p.cached_per_m:.4f}" if p.cached_per_m else "—"
            openai_rows.append({
                "Model": f"{p.icon} {p.display_name}",
                "Category": p.category,
                "Input ($/1M)": f"${p.input_per_m:.3f}",
                "Output ($/1M)": f"${p.output_per_m:.2f}",
                "Cached ($/1M)": cached,
                "Context": f"{p.context_window:,}",
            })
        st.dataframe(pd.DataFrame(openai_rows), width="stretch", hide_index=True)

        st.markdown("---")
        st.markdown("---")

        # Claude section
        st.markdown("#### 🟣 Anthropic Claude — Code Execution")
        claude_rows = []
        for mid, p in CLAUDE_PRICING.items():
            claude_rows.append({
                "Model": f"{p.icon} {p.display_name}",
                "Category": p.category,
                "Input ($/1M)": f"${p.input_per_m:.2f}",
                "Output ($/1M)": f"${p.output_per_m:.2f}",
                "Context": f"{p.context_window:,}",
            })
        st.dataframe(pd.DataFrame(claude_rows), width="stretch", hide_index=True)

        st.markdown("---")
        st.caption("💡 **Tip:** Google Gemini models have a free tier. Anthropic Claude has native code execution. Prices updated March 2026.")

    # ══════════════════════════════════════════════════════════════════════
    # TAB 4: Export Report
    # ══════════════════════════════════════════════════════════════════════
    with tab4:
        st.subheader("📥 Download Cost Report")
        st.markdown("Generate a comprehensive Excel report with usage logs, pricing comparisons, and monthly projections.")

        if st.button("📊 Generate Cost Report", key="gen_cost_report", width="stretch", type="primary"):
            with st.spinner("Generating report..."):
                report_bytes = generate_cost_report(excel_data)
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                st.download_button(
                    label="⬇️ Download Excel Report",
                    data=report_bytes,
                    file_name=f"api_cost_report_{ts}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_cost_report",
                    width="stretch",
                )

        st.markdown("---")
        st.markdown("""
        **Report includes:**
        - 📋 **Usage Log** — Every API call with timestamps, tokens, and costs
        - 💲 **Model Pricing** — All 24+ models with current per-token rates
        - 📊 **Cost Comparison** — Side-by-side model costs for your data
        - 📈 **Monthly Projections** — Estimated costs at 5/10/30/50/100 calls/day
        - 📑 **Summary** — Session totals and key metrics
        """)
