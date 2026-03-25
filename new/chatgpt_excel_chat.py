"""
ChatGPT-Powered Excel Chat for ADF Analyzer Dashboard
======================================================
Smart tiered context management for cost-efficient Excel analysis.

DESIGN:
• 3-Tier Context: Core sheets always → keyword-matched sheets → filtered Activities
• Cascade filtering: Pipeline → DataFlow → Dataset → LinkedService
• OpenAI Chat Completions REST API with SSE streaming
• History trimming (default 8 turns) to control token growth
• Smart nano-routing: auto-routes simple queries to gpt-4.1-nano
• Per-message cost tracking via api_cost_engine
"""

# Standard Imports
import os
import re
import time
import json
import datetime
import hashlib
import requests
import pandas as pd
import streamlit as st
from pathlib import Path
from typing import Optional, Dict, List, Any, Tuple
from dataclasses import dataclass

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ════════════════════════════════════════════════════════════════════════
# MODEL CONFIGURATION — OpenAI ChatGPT Models
# ════════════════════════════════════════════════════════════════════════

@dataclass
class ChatGPTModelConfig:
    name: str
    display_name: str
    max_output_tokens: int
    context_window: int
    best_for: str
    icon: str

CHATGPT_MODELS = {
    # ── GPT-4.1 Family (1M context) ──
    "gpt-4.1": ChatGPTModelConfig(
        "gpt-4.1", "GPT-4.1",
        32768, 1_000_000, "Best coding & instructions, 1M context", "🧠"
    ),
    "gpt-4.1-mini": ChatGPTModelConfig(
        "gpt-4.1-mini", "GPT-4.1 Mini",
        16384, 1_000_000, "Fast & affordable, 1M context", "⚡"
    ),
    "gpt-4.1-nano": ChatGPTModelConfig(
        "gpt-4.1-nano", "GPT-4.1 Nano",
        16384, 1_000_000, "Smallest & fastest, 1M context", "🚀"
    ),
    # ── GPT-5 Family (Latest Frontier) ──
    "gpt-5.4": ChatGPTModelConfig(
        "gpt-5.4", "GPT-5.4",
        32768, 1_000_000, "Best intelligence at scale, agentic workflows", "👑"
    ),
    "gpt-5.4-pro": ChatGPTModelConfig(
        "gpt-5.4-pro", "GPT-5.4 Pro",
        32768, 1_000_000, "Smarter & more precise, pro-tier", "💎"
    ),
    "gpt-5-mini": ChatGPTModelConfig(
        "gpt-5-mini", "GPT-5 Mini",
        16384, 1_000_000, "Near-frontier, cost-efficient", "🌟"
    ),
    "gpt-5-nano": ChatGPTModelConfig(
        "gpt-5-nano", "GPT-5 Nano",
        16384, 1_000_000, "Fastest GPT-5, ultra-low cost", "💫"
    ),
    # ── GPT-5.3 Family (Your ChatGPT Default!) ──
    "gpt-5.3-chat-latest": ChatGPTModelConfig(
        "gpt-5.3-chat-latest", "GPT-5.3 Instant",
        16384, 128_000, "ChatGPT default — best everyday AI, reduced hallucination", "🌐"
    ),
    "gpt-5.3-codex": ChatGPTModelConfig(
        "gpt-5.3-codex", "GPT-5.3 Codex",
        128_000, 400_000, "Agentic coding, 400K context, 128K output", "💻"
    ),
    "gpt-5.3-codex-high": ChatGPTModelConfig(
        "gpt-5.3-codex-high", "GPT-5.3 Codex High",
        128_000, 400_000, "Deep analysis coding, max quality", "🖥️"
    ),
    # ── GPT-4o Family (128K context) ──
    "gpt-4o": ChatGPTModelConfig(
        "gpt-4o", "GPT-4o",
        16384, 128_000, "Multimodal flagship (128K)", "✨"
    ),
    "gpt-4o-mini": ChatGPTModelConfig(
        "gpt-4o-mini", "GPT-4o Mini",
        16384, 128_000, "Fast & cheap (128K)", "💨"
    ),
    "chatgpt-4o-latest": ChatGPTModelConfig(
        "chatgpt-4o-latest", "ChatGPT-4o Latest",
        16384, 128_000, "Latest ChatGPT optimized 4o", "🔄"
    ),
    # ── GPT-4 Turbo ──
    "gpt-4-turbo": ChatGPTModelConfig(
        "gpt-4-turbo", "GPT-4 Turbo",
        4096, 128_000, "Legacy flagship, vision support", "🏛️"
    ),
    # ── O-Series Reasoning Models ──
    "o3": ChatGPTModelConfig(
        "o3", "o3",
        100_000, 200_000, "Most powerful reasoning, complex analysis", "🔬"
    ),
    "o3-mini": ChatGPTModelConfig(
        "o3-mini", "o3-mini",
        65536, 200_000, "Fast reasoning, math & science", "🧪"
    ),
    "o4-mini": ChatGPTModelConfig(
        "o4-mini", "o4-mini",
        65536, 200_000, "Latest reasoning, efficient deep-research", "🔮"
    ),
    "o1": ChatGPTModelConfig(
        "o1", "o1",
        32768, 200_000, "Advanced reasoning (previous gen)", "🧩"
    ),
    "o1-mini": ChatGPTModelConfig(
        "o1-mini", "o1-mini",
        65536, 128_000, "Fast reasoning, math-focused", "📐"
    ),
    # ── GPT-3.5 (Budget) ──
    "gpt-3.5-turbo": ChatGPTModelConfig(
        "gpt-3.5-turbo", "GPT-3.5 Turbo",
        4096, 16_385, "Budget option (16K context)", "💰"
    ),
}

DEFAULT_CHATGPT_MODEL = "gpt-4.1"


# ════════════════════════════════════════════════════════════════════════
# OPENAI API KEY MANAGER
# ════════════════════════════════════════════════════════════════════════

class OpenAIKeyManager:
    """Manages OpenAI API keys."""

    def __init__(self):
        self.keys: List[str] = []
        self._load_keys()

    def _load_keys(self):
        for var in ["OPENAI_API_KEY", "OPENAI_API_KEY_2", "OPENAI_API_KEY_3"]:
            key = os.getenv(var, "").strip()
            if key and len(key) > 10 and key not in self.keys:
                self.keys.append(key)

        # User-entered key gets priority
        try:
            user_key = st.session_state.get('user_openai_api_key', '').strip()
            if user_key and user_key not in self.keys:
                self.keys.insert(0, user_key)
        except Exception:
            pass

    def get_key(self) -> Optional[str]:
        return self.keys[0] if self.keys else None

    @property
    def has_keys(self) -> bool:
        return len(self.keys) > 0

    @property
    def key_count(self) -> int:
        return len(self.keys)


# ════════════════════════════════════════════════════════════════════════
# OPENAI CHAT COMPLETIONS CLIENT
# ════════════════════════════════════════════════════════════════════════

class ChatGPTClient:
    """REST API client for OpenAI Chat Completions with SSE streaming."""

    BASE_URL = "https://api.openai.com/v1/chat/completions"

    def __init__(self, key_manager: OpenAIKeyManager, model: str = DEFAULT_CHATGPT_MODEL):
        self.key_manager = key_manager
        self.model = model

    def call_api(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.1,
        max_retries: int = 2,
        stream: bool = False,
    ) -> str:
        """Call OpenAI Chat Completions API."""
        api_key = self.key_manager.get_key()
        if not api_key:
            return "❌ No OpenAI API key configured."

        model_cfg = CHATGPT_MODELS.get(self.model, CHATGPT_MODELS[DEFAULT_CHATGPT_MODEL])

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        payload = {
            "model": self.model,
            "messages": messages,
        }

        # GPT-5.x and o-series models DON'T support temperature (only default=1)
        # Only set temperature for older models (gpt-4o, gpt-4-turbo, gpt-3.5)
        is_new_model = any(x in self.model for x in ["gpt-5", "o3", "o4", "o1"])
        if not is_new_model:
            payload["temperature"] = temperature

        # OpenAI newer models use max_completion_tokens instead of max_tokens
        if is_new_model:
            payload["max_completion_tokens"] = model_cfg.max_output_tokens
        else:
            payload["max_tokens"] = model_cfg.max_output_tokens

        if stream:
            payload["stream"] = True

        for attempt in range(max_retries):
            try:
                response = requests.post(
                    self.BASE_URL,
                    headers=headers,
                    json=payload,
                    timeout=180,
                    stream=stream,
                )

                if response.status_code == 200:
                    if stream:
                        return self._process_stream(response)
                    else:
                        data = response.json()
                        content = data["choices"][0]["message"]["content"]
                        
                        # Log usage for cost tracking
                        try:
                            from api_cost_engine import log_api_usage
                            usage = data.get("usage", {})
                            cached_tokens = usage.get("prompt_tokens_details", {}).get("cached_tokens", 0)
                            log_api_usage(
                                provider="OpenAI",
                                model_id=self.model,
                                input_tokens=usage.get("prompt_tokens", 0),
                                output_tokens=usage.get("completion_tokens", 0),
                                cached_tokens=cached_tokens,
                                prompt_type=st.session_state.get("_gpt_prompt_type", "custom"),
                                question=st.session_state.get("_gpt_current_question", ""),
                            )
                        except Exception:
                            pass  # Don't break chat if logging fails
                        
                        return content

                elif response.status_code == 429:
                    is_pro = "4.1" in self.model and "mini" not in self.model and "nano" not in self.model
                    wait = min(2 ** attempt * 5, 30)
                    if attempt < max_retries - 1:
                        time.sleep(wait)
                        continue
                    return (
                        f"❌ API Quota Exceeded (429 HTTP). "
                        f"You have hit the OpenAI rate limits. "
                        f"Please wait {wait} seconds before trying again."
                    )

                elif response.status_code == 401:
                    return "❌ Invalid OpenAI API key. Please check your key and try again."

                elif response.status_code == 400:
                    try:
                        error_msg = response.json().get("error", {}).get("message", "")
                    except Exception:
                        error_msg = response.text[:500]
                    if "too large" in error_msg.lower() or "token" in error_msg.lower():
                        return "❌ Context too large for this model. Try a model with a larger context window or use pipeline filters."
                    return f"❌ API Error: {error_msg[:500]}"

                elif response.status_code == 503:
                    if attempt < max_retries - 1:
                        time.sleep(3)
                        continue
                    return "❌ OpenAI servers are temporarily overloaded. Please try again in a moment."

                else:
                    return f"❌ HTTP {response.status_code}: {response.text[:500]}"

            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    continue
                return "❌ Request timed out. Try a simpler question or a faster model."
            except requests.exceptions.ConnectionError:
                return "❌ Network error. Please check your internet connection."
            except Exception as e:
                return f"❌ Unexpected error: {str(e)[:300]}"

        return "❌ All retries exhausted. Please wait and try again."

    def _process_stream(self, response) -> str:
        """Process SSE stream from OpenAI."""
        full_text = []
        for line in response.iter_lines():
            if not line:
                continue
            decoded = line.decode("utf-8")
            if decoded.startswith("data: "):
                data_str = decoded[6:]
                if data_str.strip() == "[DONE]":
                    break
                try:
                    chunk = json.loads(data_str)
                    delta = chunk.get("choices", [{}])[0].get("delta", {})
                    content = delta.get("content", "")
                    if content:
                        full_text.append(content)
                except json.JSONDecodeError:
                    continue
        return "".join(full_text)


# ════════════════════════════════════════════════════════════════════════
# TIERED SMART CONTEXT BUILDER — Gemini-style (cost-optimized)
# ════════════════════════════════════════════════════════════════════════

# Tier 1: Always included — these sheets are critical for any ADF question
GPT_TIER1_SHEETS = [
    "Summary", "Statistics", "Pipelines", "PipelineAnalysis",
    "DataLineage", "ImpactAnalysis", "DataFlowLineage",
]

# Tier 2: Keyword-matched supplementary sheets
GPT_TIER2_KEYWORDS = {
    "dataset": ["Datasets", "DatasetUsage"],
    "linked service": ["LinkedServices", "LinkedServiceUsage"],
    "dataflow": ["DataFlows", "DataFlowLineage", "DataFlowTransformations"],
    "trigger": ["Triggers", "TriggerDetails"],
    "orphan": ["OrphanedPipelines", "OrphanedDataFlows", "OrphanedDatasets",
               "OrphanedLinkedServices", "OrphanedTriggers"],
    "parameter": ["GlobalParameters"],
    "error": ["Errors"],
    "health": ["OrphanedPipelines", "OrphanedDataFlows", "OrphanedDatasets",
               "OrphanedLinkedServices", "Errors"],
    "lineage": ["DataFlowLineage", "DataFlows"],
    "execution": ["ActivityExecutionOrder"],
    "activity": [],  # Handled by Tier 3
}

# Tier 3: Activities keywords
GPT_ACTIVITIES_KEYWORDS = [
    "activit", "forEach", "ifCondition", "switch", "lookup", "copy",
    "stored proc", "script", "source table", "sink table", "execute",
    "depends", "nested", "depth", "sequence",
]

# Smart routing keywords
SIMPLE_QUERY_KEYWORDS = [
    "how many", "count", "list", "show", "what is", "which", "total",
    "overview", "summary", "name", "names", "version",
]

DEFAULT_MAX_GPT_HISTORY_TURNS = 8


def _extract_related_entities_gpt(excel_data: Dict[str, pd.DataFrame],
                                   pipeline_filter: List[str]) -> dict:
    """Cascade filter: from pipeline filter → extract DataFlows/Datasets/LinkedServices."""
    related = {"dataflows": set(), "datasets": set(), "linked_services": set()}
    if not pipeline_filter:
        return related
    if "Activities" in excel_data:
        acts = excel_data["Activities"]
        if isinstance(acts, pd.DataFrame) and "Pipeline" in acts.columns:
            filtered = acts[acts["Pipeline"].isin(pipeline_filter)]
            if "DataFlow" in filtered.columns:
                related["dataflows"].update(filtered["DataFlow"].dropna().unique())
            for col in ["SourceLinkedService", "SinkLinkedService"]:
                if col in filtered.columns:
                    related["linked_services"].update(filtered[col].dropna().unique())
            for col in ["SourceTable", "SinkTable"]:
                if col in filtered.columns:
                    related["datasets"].update(filtered[col].dropna().unique())
    for key in related:
        related[key].discard("")
    return related


def build_smart_gpt_context(
    excel_data: Dict[str, pd.DataFrame],
    question: str,
    model: str = DEFAULT_CHATGPT_MODEL,
    pipeline_filter: List[str] = None,
    dataflow_filter: List[str] = None,
    trigger_filter: List[str] = None,
) -> Tuple[str, int]:
    """
    3-Tier Smart Context Builder — sends only what's needed.
    Tier 1: Core sheets (always) with pipeline filtering
    Tier 2: Keyword-matched supplementary sheets with cascade filtering
    Tier 3: Activities — filtered/summarized unless high-context model
    Returns: (context_text, estimated_token_count)
    """
    parts = []
    q_lower = question.lower()

    # Filter banners
    if pipeline_filter:
        parts.append(f"⚠️ PIPELINE FILTER ACTIVE: {len(pipeline_filter)} pipeline(s): {', '.join(pipeline_filter[:20])}")
    if dataflow_filter:
        parts.append(f"⚠️ DATAFLOW FILTER ACTIVE: {len(dataflow_filter)} dataflow(s): {', '.join(dataflow_filter[:20])}")
    if trigger_filter:
        parts.append(f"⚠️ TRIGGER FILTER ACTIVE: {len(trigger_filter)} trigger(s): {', '.join(trigger_filter[:20])}")

    # ── Tier 1: Core sheets ──
    PIPELINE_FILTERABLE = {"PipelineAnalysis", "DataLineage", "DataFlowLineage", "ImpactAnalysis", "Pipelines"}
    tier1_parts = []
    for sheet_name in GPT_TIER1_SHEETS:
        if sheet_name not in excel_data:
            continue
        df = excel_data[sheet_name]
        if not isinstance(df, pd.DataFrame) or df.empty:
            continue
        tag = ""
        if pipeline_filter and sheet_name in PIPELINE_FILTERABLE and "Pipeline" in df.columns:
            df = df[df["Pipeline"].isin(pipeline_filter)]
            tag = f" [FILTERED: {len(pipeline_filter)} pipelines]"
            if df.empty:
                continue
        csv_data = df.to_csv(index=False)
        tier1_parts.append(
            f"\n### 📋 Sheet: {sheet_name}{tag} ({len(df)} rows × {len(df.columns)} cols)\n{csv_data}"
        )
    if tier1_parts:
        parts.append("══════════════════════════════════════════\n## CORE DATA (always included)\n══════════════════════════════════════════" + "\n".join(tier1_parts))

    # ── Tier 2: Keyword-matched ──
    matched_sheets = set()
    for keyword, sheet_names in GPT_TIER2_KEYWORDS.items():
        if keyword in q_lower:
            matched_sheets.update(sheet_names)
    # Remove already-included Tier1 and massive Activities
    matched_sheets -= set(GPT_TIER1_SHEETS)
    matched_sheets.discard("Activities")
    matched_sheets.discard("ActivityExecutionOrder")
    # Also match direct sheet name mentions
    for sheet_name in excel_data:
        if sheet_name.lower() in q_lower and sheet_name not in set(GPT_TIER1_SHEETS) | {"Activities"}:
            matched_sheets.add(sheet_name)

    cascaded = _extract_related_entities_gpt(excel_data, pipeline_filter) if pipeline_filter else None
    DATAFLOW_FILTERABLE = {"DataFlows", "DataFlowLineage", "DataFlowTransformations"}
    TRIGGER_FILTERABLE = {"Triggers", "TriggerDetails"}

    tier2_parts = []
    for sheet_name in sorted(matched_sheets):
        if sheet_name not in excel_data:
            continue
        df = excel_data[sheet_name]
        if not isinstance(df, pd.DataFrame) or df.empty:
            continue
        tag = ""
        # Apply cascade/explicit filters
        effective_df = set(dataflow_filter or [])
        if cascaded and cascaded.get("dataflows") and not dataflow_filter:
            effective_df.update(cascaded["dataflows"])
        if effective_df and sheet_name in DATAFLOW_FILTERABLE:
            for col in ["DataFlow", "Name", "DataFlowName"]:
                if col in df.columns:
                    df = df[df[col].isin(effective_df)]
                    tag = f" [CASCADED: {len(effective_df)} dataflow(s)]"
                    break
            if df.empty:
                continue
        if trigger_filter and sheet_name in TRIGGER_FILTERABLE:
            for col in ["Trigger", "Name", "TriggerName"]:
                if col in df.columns:
                    df = df[df[col].isin(trigger_filter)]
                    tag = f" [FILTERED: {len(trigger_filter)} trigger(s)]"
                    break
            if df.empty:
                continue
        csv_data = df.to_csv(index=False)
        tier2_parts.append(
            f"\n### 📋 Sheet: {sheet_name}{tag} ({len(df)} rows × {len(df.columns)} cols)\n{csv_data}"
        )
    if tier2_parts:
        parts.append("══════════════════════════════════════════\n## SUPPLEMENTARY DATA (matched to your question)\n══════════════════════════════════════════" + "\n".join(tier2_parts))

    # ── Tier 3: Activities — filtered/summarized ──
    needs_activities = any(kw in q_lower for kw in GPT_ACTIVITIES_KEYWORDS)
    if needs_activities and "Activities" in excel_data:
        act_df = excel_data["Activities"]
        if isinstance(act_df, pd.DataFrame) and not act_df.empty:
            model_cfg = CHATGPT_MODELS.get(model, CHATGPT_MODELS[DEFAULT_CHATGPT_MODEL])
            # Apply pipeline filter first
            if pipeline_filter and "Pipeline" in act_df.columns:
                act_df = act_df[act_df["Pipeline"].isin(pipeline_filter)]
            if not act_df.empty:
                # High-context models (1M+) → send full
                if model_cfg.context_window >= 1_000_000:
                    csv_data = act_df.to_csv(index=False)
                    parts.append(
                        f"══════════════════════════════════════════\n## ACTIVITY DATA\n══════════════════════════════════════════"
                        f"\n### 📋 Sheet: Activities ({len(act_df)} rows)\n{csv_data}"
                    )
                else:
                    # Summarize for shorter-context models
                    tier3_parts = []
                    if "ActivityType" in act_df.columns:
                        summary = act_df.groupby("ActivityType").size().reset_index(name="Count").sort_values("Count", ascending=False)
                        tier3_parts.append(f"### Activity Type Distribution\n{summary.to_csv(index=False)}")
                    if "Pipeline" in act_df.columns:
                        pl_counts = act_df.groupby("Pipeline").size().reset_index(name="ActivityCount").sort_values("ActivityCount", ascending=False).head(30)
                        tier3_parts.append(f"### Top 30 Pipelines by Activity Count\n{pl_counts.to_csv(index=False)}")
                    key_cols = [c for c in ["Pipeline", "Activity", "ActivityType", "SourceTable", "SinkTable"] if c in act_df.columns]
                    if key_cols:
                        sample = act_df[key_cols].head(100)
                        tier3_parts.append(f"### Activities Sample (first 100 of {len(act_df)})\n{sample.to_csv(index=False)}")
                    if tier3_parts:
                        parts.append("══════════════════════════════════════════\n## ACTIVITY DATA (summarized)\n══════════════════════════════════════════\n" + "\n".join(tier3_parts))

    full_context = "\n\n".join(parts)
    est_tokens = len(full_context) // 4
    return full_context, est_tokens


def build_chatgpt_system_prompt(excel_data: Dict[str, pd.DataFrame]) -> str:
    """Build enriched system prompt with full ADF domain knowledge."""

    sheet_catalog = []
    stats_lines = []
    for name, df in sorted(excel_data.items()):
        if isinstance(df, pd.DataFrame) and not df.empty:
            cols = ", ".join(list(df.columns)[:15])
            sheet_catalog.append(f"  📋 {name}: {len(df)} rows × {len(df.columns)} cols | Columns: [{cols}]")

    # Extract factory statistics
    if "Statistics" in excel_data and isinstance(excel_data["Statistics"], pd.DataFrame):
        for _, row in excel_data["Statistics"].iterrows():
            cat = str(row.get("Category", ""))
            typ = str(row.get("Type", ""))
            cnt = str(row.get("Count", ""))
            if cat and typ and cnt:
                stats_lines.append(f"  • {cat} > {typ}: {cnt}")

    if "Summary" in excel_data and isinstance(excel_data["Summary"], pd.DataFrame) and not excel_data["Summary"].empty:
        summary_df = excel_data["Summary"]
        for _, row in summary_df.iterrows():
            metric = str(row.get("Metric", row.get(summary_df.columns[0], "")))
            value = str(row.get("Value", row.get(summary_df.columns[1], "")))
            if metric and value and metric != "nan":
                stats_lines.append(f"  • {metric}: {value}")

    return f"""You are an expert Azure Data Factory (ADF) Analyst AI Assistant.
You have access to the COMPLETE output of an ADF ARM Template Analyzer tool.

CONVERSATIONAL MEMORY:
You are having a multi-turn conversation. You MUST remember and reference previous questions
and your own previous answers. If the user says "show more", "explain that", "what about the
previous one", or references something from earlier — look at the conversation history and
build upon it. Never say "I don't have access to previous messages" — you DO.

════════════════════════════════════════════════════════════
HOW THIS DATA WAS GENERATED (Core Analyzer Knowledge):
════════════════════════════════════════════════════════════
The data comes from the "UltimateEnterpriseADFAnalyzer v10.0" — a Python tool
that parses Azure Data Factory ARM Template JSON files.

ARM Template Structure:
 • The JSON has a "resources" array where each resource has:
   - type: "Microsoft.DataFactory/factories/pipelines|dataflows|datasets|linkedServices|triggers"
   - name: Resource name (may contain ARM expressions)
   - properties: Contains resource configuration (activities, typeProperties, etc.)
 • Pipelines contain an "activities" array — each activity has type, typeProperties, dependsOn
 • Activities can be nested (ForEach, IfCondition, Switch contain inner activities)
 • DataFlows have "sources", "sinks", and "transformations"

How the Analyzer Creates Each Sheet:
 • Summary/Statistics: Aggregated counts from all parsed resources
 • Pipelines: Basic pipeline info (name, folder, description, parameters)
 • PipelineAnalysis: DEEP analysis — complexity scoring, activity types, SQL detection
 • Activities: EVERY activity from ALL pipelines, including nested ones
 • DataLineage: Source→Sink mapping for every Copy/DataFlow/StoredProcedure activity
 • DataFlows: Parsed mapping dataflows with sources, sinks, transformations
 • ImpactAnalysis: Blast radius — how many resources are affected if one fails
 • Datasets: All dataset definitions with linked services, table names
 • LinkedServices: All connection definitions (SQL, ADLS, SFTP, REST, etc.)
 • Triggers: Schedule/tumbling/event triggers with pipeline associations
 • Orphaned*: Resources that exist but are never referenced

Cross-Reference Patterns:
 • Pipeline names: Pipelines, PipelineAnalysis, Activities, DataLineage, ImpactAnalysis
 • Linked services: LinkedServices, Datasets, Activities (Source/SinkLinkedService)
 • DataFlows: DataFlows, DataFlowLineage, Activities (ExecuteDataFlow type)
 • Table names: Activities (SourceTable/SinkTable), DataLineage, DataFlowLineage

════════════════════════════════════════════════════════════
ANTI-HALLUCINATION RULES:
════════════════════════════════════════════════════════════
1. ONLY report information that EXISTS in the provided data sheets
2. Do NOT infer, assume, or generate data that isn't in the sheets
3. If information is not available, say: "This data is not available in the analysis output"
4. Use EXACT names from the data (pipeline names, dataflow names, table names)
5. When counting items, count from the actual data — do not estimate
6. When listing items, list ALL of them — do not truncate

════════════════════════════════════════════════════════════
DATA SHEET CATALOG:
════════════════════════════════════════════════════════════
{chr(10).join(sheet_catalog)}

════════════════════════════════════════════════════════════
KEY FACTORY STATISTICS:
════════════════════════════════════════════════════════════
{chr(10).join(stats_lines[:50]) if stats_lines else '(Statistics not available)'}

════════════════════════════════════════════════════════════
RESPONSE FORMAT:
════════════════════════════════════════════════════════════
• Use Markdown formatting: tables, headers, bullet points
• Bold important names and numbers
• Include sheet references: "From [SheetName] sheet..."
• End with a brief "Data Sources" section listing which sheets were used
"""


# ════════════════════════════════════════════════════════════════════════
# PRESET QUESTIONS — Same as Gemini tab
# ════════════════════════════════════════════════════════════════════════

CHATGPT_PRESET_QUESTIONS = [
    ("📊 Factory Overview",
     "Give me a comprehensive overview of this Azure Data Factory. How many pipelines, dataflows, datasets, linked services, and triggers are there? What are the main folders and categories? Show counts from the Statistics sheet."),

    ("🔗 Pipeline Lineage",
     "Analyze the COMPLETE data lineage from the DataLineage sheet. Show all source → sink connections. What are the main source systems and target/destination systems? Group by pipeline and show the data flow path."),

    ("📈 Table Lineage",
     """You are an expert ADF Table Lineage Analyst for Tiger Analytics accelerators.

Input: Excel data from ADF Pipeline Analyzer. Focus on these sheets for extracting lineage:
- `Activities`: Contains granular activity details including `Pipeline`, `ActivityType`, `SourceTable`, `SinkTable`, `SourceLinkedService`, `SinkLinkedService`, `ValuesInfo` (for intermediate layers), `DataFlow` (for dataflow names).
- `DataFlows`: Contains `SinkTables` for data flows.
- `Datasets`: Contains `LinkedService` for datasets.
- `LinkedServices`: For linked service details.

Task: Extract comprehensive Table-Level Lineage.

1. **Trace Lineage Path**: For each pipeline, follow the data flow from source to target, identifying all intermediate steps and transformations.
2. **Extract Components**: For each stage in the lineage, identify the following based on the provided instructions:
   * **Pipeline Name**: From `Activities` sheet, `Pipeline` column.
   * **Target Table**: From `Activities` sheet, `SinkTable` column for the final data-producing activity (e.g., last execution stage). Also cross-reference with `DataFlows` sheet's `SinkTables` for the corresponding DataFlow.
   * **Target Linked Service Connection**: From `Datasets` sheet, find the `LinkedService` associated with the Dataset that the Target Table belongs to. If not directly available, use `SinkLinkedService` from the final activity in `Activities`.
   * **Transformation Layer (dataflow/SP)**: From `Activities` sheet, look at `DataFlow` column for 'ExecuteDataFlow' activities or `ActivityType` for 'StoredProcedure' activities, especially for the last execution stage.
   * **Intermediate layer (Stg table/ADLS)**: From `Activities` sheet, search for 'SetVariable' or 'Set Path' activities. Extract relevant patterns like "Inbound/GlobalSC/..." from the `ValuesInfo` column by concatenating SetContainer + SetPath values.
   * **Source Table**: From `Activities` sheet, `SourceTable` column, particularly for 'Copy Data', 'Data Flow Source', or 'Stored Procedure' activities at the initial stages.
   * **Source Linked Service**: From `Datasets` sheet, find the `LinkedService` associated with the Dataset that the Source Table belongs to. If not directly available, use `SourceLinkedService` from the initial activity in `Activities`.

3. **Output Format** (Markdown table):
   | Target Table | Target Linked Service Connection | Transformation Layer (dataflow/SP) | Intermediate layer(Stg table/ADLS) | Source Table | Source Linked Service | Pipeline Name |

4. **Example Output Path**:
   `dr.NHSC_EventMessageHeader -> LS_ASA -> Dataflow (df_EventMessageHeader) -> Inbound/GlobalSC/EventMessageHeader -> EDW.PRSTM.S6_SC_TM_EVENTMESSAGEHEADER_DM_V -> LS_SNOWFLAKE_PROD -> pl_EventMessageHeader`

Be precise, cite activity names and relevant sheet references. Process ALL pipelines in the provided data."""),

    ("🔍 Column Lineage",
     """You are an expert ADF Column Lineage Analyst for Tiger Analytics accelerators.

Input is Excel data from ADF Pipeline Analyzer: sheets with pipelines (`Pipelines`, `PipelineAnalysis`), datasets (`Datasets`, `DatasetUsage`), copy/data flow activities (`Activities`, `DataFlows`, `DataFlowLineage`, `DataLineage`), and parameters (`GlobalParameters`).

Task: Extract GRANULAR COLUMN-LEVEL LINEAGE.
1. Parse sources (datasets/linkedServices), transformations (mappingDataFlows: derived cols, joins, filters, aggregates; copyActivity projections), targets.
2. For each target column: Trace origin column(s) from source(s), list transformations (e.g., "UPPER(source.col1) AS target_colA"), data types, lineage path.
3. Detect gaps: Flag unmapped cols, inferred mappings (e.g., via param@dataset.schema), risks (drift, nulls).
4. Output:
   - Markdown table: | Target Dataset | Target Column | Source Dataset(s) | Source Column(s) | Transformations | Data Type | Confidence (0-100) |
   - Summary: Completeness score, critical paths, modernization recs (e.g., to Fabric/dbt).

Be precise, cite activity names and sheet references."""),

    ("⚠️ Orphaned Resources",
     "List ALL orphaned resources from every Orphaned* sheet: OrphanedPipelines, OrphanedDataFlows, OrphanedDatasets, OrphanedLinkedServices, OrphanedTriggers. Show the complete list with counts per category."),

    ("🏗️ Complex Pipelines",
     "Show ALL pipelines ranked by ComplexityScore from PipelineAnalysis. Include: Pipeline name, Folder, TotalActivities, Complexity, ComplexityScore, ImpactLevel, HasDataFlow, HasSQL, SourceSystems, TargetSystems."),

    ("💥 Impact Analysis",
     "From the ImpactAnalysis sheet, show ALL pipelines with CRITICAL or HIGH ImpactLevel. Include blast radius, upstream/downstream counts, connected triggers, and affected datasets."),

    ("🔄 DataFlow Details",
     "From DataFlows and DataFlowLineage sheets, show ALL dataflows with their source tables, sink tables, transformations, linked services. Which dataflows are most complex?"),

    ("🗓️ Trigger Schedule",
     "From the Triggers and TriggerDetails sheets, list ALL triggers with: name, type, state (Started/Stopped), frequency, schedule, and which pipelines they execute. Flag any issues."),

    ("🏥 Full Health Check",
     "Perform a COMPLETE health check using ALL sheets. Report: (1) Orphaned resource counts, (2) Pipelines with CRITICAL impact but no triggers, (3) Overly complex pipelines, (4) Unused datasets, (5) Misconfigured triggers, (6) Any errors from the Errors sheet."),
]


# ════════════════════════════════════════════════════════════════════════
# CHATGPT TAB CSS — Premium Glassmorphism (Same variables, different accent)
# ════════════════════════════════════════════════════════════════════════

CHATGPT_CSS = """
<style>
/* ChatGPT Tab Variables — Emerald/Teal accent */
:root {
    --gpt-primary: #10b981;
    --gpt-secondary: #059669;
    --gpt-accent: #34d399;
    --gpt-bg: rgba(16, 185, 129, 0.03);
    --gpt-bg-secondary: rgba(16, 185, 129, 0.06);
    --gpt-border: rgba(16, 185, 129, 0.12);
    --gpt-text: #e2e8f0;
    --gpt-muted: #94a3b8;
}
.gpt-chat-header {
    text-align: center;
    padding: 1.5rem;
    background: linear-gradient(135deg, rgba(16, 185, 129, 0.08), rgba(5, 150, 105, 0.04));
    border-radius: 16px;
    border: 1px solid var(--gpt-border);
    margin-bottom: 1.5rem;
}
.gpt-chat-header h2 {
    margin: 0 0 0.3rem 0;
    background: linear-gradient(135deg, #10b981, #34d399);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    font-weight: 700;
}
.gpt-chat-header p {
    color: var(--gpt-muted);
    font-size: 0.88rem;
    margin: 0;
}
.gpt-data-status {
    padding: 0.6rem 1.2rem;
    border-radius: 10px;
    font-size: 0.82rem;
    margin-bottom: 1rem;
}
.gpt-data-status.loaded {
    background: rgba(16, 185, 129, 0.08);
    border: 1px solid rgba(16, 185, 129, 0.2);
    color: #a7f3d0;
}
.gpt-metric-card {
    background: var(--gpt-bg-secondary);
    border: 1px solid var(--gpt-border);
    border-radius: 12px;
    padding: 0.8rem;
    text-align: center;
}
.gpt-metric-card .metric-value {
    font-size: 1.1rem;
    font-weight: 700;
    color: var(--gpt-accent);
}
.gpt-metric-card .metric-label {
    font-size: 0.72rem;
    color: var(--gpt-muted);
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-top: 0.2rem;
}
.gpt-control-bar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0.5rem 1rem;
    background: var(--gpt-bg-secondary);
    border: 1px solid var(--gpt-border);
    border-radius: 12px;
    margin-bottom: 0.8rem;
    font-size: 0.78rem;
    color: var(--gpt-muted);
}
.gpt-empty-state {
    text-align: center;
    padding: 3rem 2rem;
    color: var(--gpt-muted);
}
.gpt-empty-state .empty-icon {
    font-size: 3.5rem;
    margin-bottom: 1rem;
    opacity: 0.6;
}
.gpt-empty-state h3 { color: var(--gpt-text); font-weight: 600; margin: 0 0 0.5rem 0; }
.gpt-empty-state p { font-size: 0.88rem; max-width: 400px; margin: 0 auto; line-height: 1.5; }
</style>
"""


# ════════════════════════════════════════════════════════════════════════
# SESSION STATE INITIALIZATION
# ════════════════════════════════════════════════════════════════════════

def _is_simple_gpt_question(question: str) -> bool:
    """Detect if a question is simple enough for the cheaper Nano model."""
    q_lower = question.lower().strip()
    if len(q_lower) > 200:
        return False
    return any(kw in q_lower for kw in SIMPLE_QUERY_KEYWORDS)


def _trim_gpt_history(api_messages: List[Dict], max_turns: int = 8) -> List[Dict]:
    """Keep only the last N user-assistant turn pairs."""
    if not api_messages:
        return []
    max_msgs = max_turns * 2
    if len(api_messages) <= max_msgs:
        return api_messages
    return api_messages[-max_msgs:]


def initialize_chatgpt_session_state():
    """Initialize ChatGPT tab session state variables."""
    defaults = {
        "gpt_chat_history": [],
        "gpt_api_history": [],
        "gpt_model": DEFAULT_CHATGPT_MODEL,
        "gpt_chat_id": f"gpt_{int(time.time())}",
        "gpt_pending_question": None,
        "gpt_total_tokens_used": 0,
        "gpt_pipeline_filter": [],
        "gpt_dataflow_filter": [],
        "gpt_trigger_filter": [],
        # Session cost tracking
        "gpt_session_cost": 0.0,
        "gpt_session_calls": 0,
        # New: History trimming + smart routing
        "gpt_limit_history": True,
        "gpt_max_history_turns": DEFAULT_MAX_GPT_HISTORY_TURNS,
        "gpt_auto_nano": True,
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val


# ════════════════════════════════════════════════════════════════════════
# MAIN RENDER FUNCTION
# ════════════════════════════════════════════════════════════════════════

def render_chatgpt_chat_tab(excel_data: Dict[str, pd.DataFrame] = None):
    """
    Render the complete ChatGPT Chat tab with premium design.

    Args:
        excel_data: Dict of sheet_name → DataFrame from loaded Excel.
    """
    initialize_chatgpt_session_state()

    # Premium CSS
    st.markdown(CHATGPT_CSS, unsafe_allow_html=True)

    # ── Premium Header ──
    st.markdown("""
    <div class="gpt-chat-header">
        <h2>🧠 ChatGPT ADF Analyst</h2>
        <p>Ask anything about your Azure Data Factory — powered by OpenAI ChatGPT (Full Excel, No Truncation)</p>
    </div>
    """, unsafe_allow_html=True)

    # ── Check if data is available ──
    if not excel_data or len(excel_data) == 0:
        output_path = Path("output/adf_analysis_latest.xlsx")
        if not output_path.exists():
            output_path = Path("D:/armtemp/ADF_Analyzer_v10_Production/output/adf_analysis_latest.xlsx")

        if output_path.exists():
            st.info("📂 Found generated Excel file! Click below to load it.")
            if st.button("📥 Load Latest Excel for ChatGPT", type="primary", key="gpt_load_excel"):
                with st.spinner("📖 Reading Excel file..."):
                    try:
                        xf = pd.ExcelFile(str(output_path))
                        data = {}
                        for sn in xf.sheet_names:
                            try:
                                data[sn] = pd.read_excel(xf, sn)
                            except Exception:
                                continue
                        st.session_state.excel_data = data
                        st.session_state.data_loaded = True
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Failed to read Excel: {e}")
            return
        else:
            st.warning(
                "📊 **No Excel data loaded.** Please use:\n\n"
                "1. **⚙️ Generate Excel** tab → Generate analysis from ARM Template\n"
                "2. **📊 Upload & Analyze** tab → Upload existing Excel file"
            )
            return

    # ── Check API Key ──
    key_mgr = OpenAIKeyManager()
    if not key_mgr.has_keys:
        st.error("🔑 **No OpenAI API key configured.**")
        st.markdown("""
        **Get an OpenAI API key:**
        1. Go to [platform.openai.com](https://platform.openai.com/api-keys)
        2. Create a new key
        3. Paste it below 👇
        """)
        user_key = st.text_input(
            "Paste your OpenAI API Key:",
            type="password",
            key="gpt_tab_api_key_input",
        )
        if user_key:
            st.session_state.user_openai_api_key = user_key
            st.rerun()
        return

    # ── Calculate full context stats ──
    ctx = st.session_state.get("ai_context_builder")
    if ctx:
        total_sheets = ctx.sheet_count
        total_rows = sum(info["rows"] for info in ctx.sheet_info.values())
        est_total_tokens = ctx.total_tokens
    else:
        total_sheets = len(excel_data)
        total_rows = sum(len(df) for df in excel_data.values() if isinstance(df, pd.DataFrame))
        est_total_tokens = sum(len(df.to_csv(index=False)) // 4 for df in excel_data.values() if isinstance(df, pd.DataFrame))

    # ── Data Status Bar ──
    hash_str = f"<br><span style='font-family:monospace;opacity:0.6;font-size:0.8rem;'>Hash: {ctx.data_hash[:12]}</span>" if ctx else ""
    st.markdown(f"""
    <div class="gpt-data-status loaded">
        ✅ {total_sheets} sheets • {total_rows:,} rows • ~{est_total_tokens:,} tokens (full Excel — no truncation){hash_str}
    </div>
    """, unsafe_allow_html=True)

    # ── Metrics Row ──
    cols = st.columns(4)
    with cols[0]:
        st.markdown(f"""<div class="gpt-metric-card">
            <div class="metric-value">{total_sheets}</div>
            <div class="metric-label">Sheets</div>
        </div>""", unsafe_allow_html=True)
    with cols[1]:
        st.markdown(f"""<div class="gpt-metric-card">
            <div class="metric-value">{total_rows:,}</div>
            <div class="metric-label">Total Rows</div>
        </div>""", unsafe_allow_html=True)
    with cols[2]:
        model_cfg = CHATGPT_MODELS.get(st.session_state.gpt_model, CHATGPT_MODELS[DEFAULT_CHATGPT_MODEL])
        st.markdown(f"""<div class="gpt-metric-card">
            <div class="metric-value">{model_cfg.icon} {model_cfg.display_name}</div>
            <div class="metric-label">Model</div>
        </div>""", unsafe_allow_html=True)
    with cols[3]:
        st.markdown(f"""<div class="gpt-metric-card">
            <div class="metric-value">{key_mgr.key_count}</div>
            <div class="metric-label">API Keys</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("---")

    # ── Model Selection + Cost Toggles ──
    cc1, cc2 = st.columns([1, 1])
    with cc1:
        model_options = list(CHATGPT_MODELS.keys())
        current_idx = model_options.index(st.session_state.gpt_model) if st.session_state.gpt_model in model_options else 0
        selected_model = st.selectbox(
            "🧠 ChatGPT Model:",
            options=model_options,
            index=current_idx,
            format_func=lambda m: f"{CHATGPT_MODELS[m].icon} {CHATGPT_MODELS[m].display_name} — {CHATGPT_MODELS[m].best_for}",
            key="gpt_model_select",
        )
        st.session_state.gpt_model = selected_model
    with cc2:
        st.markdown("**💰 Cost Savings:**")
        st.session_state.gpt_limit_history = st.checkbox(
            f"📜 Limit history to {st.session_state.gpt_max_history_turns} turns",
            value=st.session_state.gpt_limit_history, key="gpt_hist_toggle")
        st.session_state.gpt_auto_nano = st.checkbox(
            "🚀 Auto-Nano for simple questions ($0.20/M vs $3/M)",
            value=st.session_state.gpt_auto_nano, key="gpt_nano_toggle")

    # ── Extract pipeline data for smart filter ──
    all_pipeline_names = []
    pipeline_df = None
    for sn in ["PipelineAnalysis", "Pipelines"]:
        if sn in excel_data and isinstance(excel_data[sn], pd.DataFrame):
            if "Pipeline" in excel_data[sn].columns:
                pipeline_df = excel_data[sn]
                all_pipeline_names = sorted(pipeline_df["Pipeline"].dropna().unique().tolist())
                break
    if not all_pipeline_names and "Activities" in excel_data and isinstance(excel_data["Activities"], pd.DataFrame):
        if "Pipeline" in excel_data["Activities"].columns:
            all_pipeline_names = sorted(excel_data["Activities"]["Pipeline"].dropna().unique().tolist())

    current_filter = st.session_state.get("gpt_pipeline_filter", [])
    filter_count = len(current_filter)
    filter_label = f"🎯 {filter_count} selected" if filter_count > 0 else "All"

    # ── Smart Pipeline Filter (3-Tab — same as Gemini) ──
    if all_pipeline_names:
        with st.expander(f"🔍 **Smart Pipeline Filter** — {len(all_pipeline_names)} pipelines ({filter_label})", expanded=False):
            gpt_ft1, gpt_ft2, gpt_ft3 = st.tabs(["📂 By Folder", "⚡ By Complexity", "📋 Manual Select"])

            # TAB 1: By Folder
            with gpt_ft1:
                if pipeline_df is not None and "Folder" in pipeline_df.columns:
                    folders = sorted(pipeline_df["Folder"].fillna("(Root / No Folder)").unique().tolist())
                    st.caption(f"**{len(folders)}** folders detected. Click any to select its pipelines.")
                    fc1, fc2 = st.columns(2)
                    for fi, folder in enumerate(folders):
                        col = fc1 if fi % 2 == 0 else fc2
                        with col:
                            mask = pipeline_df["Folder"].fillna("(Root / No Folder)") == folder
                            folder_pls = pipeline_df.loc[mask, "Pipeline"].tolist()
                            if st.button(f"📁 {folder} ({len(folder_pls)})", key=f"gpt_folder_{fi}", width="stretch"):
                                st.session_state.gpt_pipeline_multiselect = folder_pls
                                st.session_state.gpt_pipeline_filter = folder_pls
                                st.rerun()
                else:
                    st.info("No `Folder` column in PipelineAnalysis.")

            # TAB 2: By Complexity / Impact Level
            with gpt_ft2:
                if pipeline_df is not None and "Complexity" in pipeline_df.columns:
                    st.caption("Filter by complexity level.")
                    complexity_lvls = sorted(pipeline_df["Complexity"].dropna().unique().tolist())
                    cc1, cc2 = st.columns(2)
                    for ci, lvl in enumerate(complexity_lvls):
                        col = cc1 if ci % 2 == 0 else cc2
                        with col:
                            mask = pipeline_df["Complexity"] == lvl
                            lvl_pls = pipeline_df.loc[mask, "Pipeline"].tolist()
                            icon = {"Simple": "🟢", "Medium": "🟡", "Complex": "🟠", "Critical": "🔴"}.get(str(lvl), "⚪")
                            if st.button(f"{icon} {lvl} ({len(lvl_pls)})", key=f"gpt_cplx_{ci}", width="stretch"):
                                st.session_state.gpt_pipeline_multiselect = lvl_pls
                                st.session_state.gpt_pipeline_filter = lvl_pls
                                st.rerun()

                    if "ImpactLevel" in pipeline_df.columns:
                        st.markdown("---")
                        st.caption("Or filter by **Impact Level**:")
                        impact_lvls = sorted(pipeline_df["ImpactLevel"].dropna().unique().tolist())
                        ic1, ic2 = st.columns(2)
                        for ii, impact in enumerate(impact_lvls):
                            col = ic1 if ii % 2 == 0 else ic2
                            with col:
                                mask = pipeline_df["ImpactLevel"] == impact
                                imp_pls = pipeline_df.loc[mask, "Pipeline"].tolist()
                                icon = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢"}.get(str(impact).upper(), "⚪")
                                if st.button(f"{icon} {impact} ({len(imp_pls)})", key=f"gpt_impact_{ii}", width="stretch"):
                                    st.session_state.gpt_pipeline_multiselect = imp_pls
                                    st.session_state.gpt_pipeline_filter = imp_pls
                                    st.rerun()
                else:
                    st.info("No `Complexity` column in PipelineAnalysis.")

            # TAB 3: Manual Select
            with gpt_ft3:
                mc1, mc2 = st.columns(2)
                with mc1:
                    if st.button("📋 Select All", key="gpt_pf_all", width="stretch"):
                        st.session_state.gpt_pipeline_multiselect = all_pipeline_names
                        st.session_state.gpt_pipeline_filter = all_pipeline_names
                        st.rerun()
                with mc2:
                    if st.button("🧹 Clear All", key="gpt_pf_clr", width="stretch"):
                        st.session_state.gpt_pipeline_multiselect = []
                        st.session_state.gpt_pipeline_filter = []
                        st.rerun()
                if "gpt_pipeline_multiselect" not in st.session_state:
                    st.session_state.gpt_pipeline_multiselect = st.session_state.get("gpt_pipeline_filter", [])
                def _on_gpt_filter_change():
                    st.session_state.gpt_pipeline_filter = st.session_state.gpt_pipeline_multiselect
                st.multiselect("Pipelines:", all_pipeline_names, key="gpt_pipeline_multiselect",
                               placeholder="All Pipelines (no filter)", on_change=_on_gpt_filter_change)
                st.session_state.gpt_pipeline_filter = st.session_state.get("gpt_pipeline_multiselect", [])

            # Filter Status Bar with token reduction estimate
            current_sel = st.session_state.get("gpt_pipeline_filter", [])
            if current_sel:
                if "Activities" in excel_data and isinstance(excel_data["Activities"], pd.DataFrame) and "Pipeline" in excel_data["Activities"].columns:
                    total_act = len(excel_data["Activities"])
                    filtered_act = len(excel_data["Activities"][excel_data["Activities"]["Pipeline"].isin(current_sel)])
                    reduction_pct = int((1 - filtered_act / max(total_act, 1)) * 100)
                    st.success(f"🎯 **{len(current_sel)}** pipeline(s) • **{filtered_act}** activities (of {total_act}) • ~{reduction_pct}% token reduction")
                else:
                    st.info(f"🎯 **{len(current_sel)}** pipeline(s) selected.")
            else:
                st.caption("💡 No filter — ChatGPT processes **all** pipelines.")

    # ── Secondary Entity Filters (DataFlow + Trigger) side-by-side ──
    all_dataflow_names = []
    if "DataFlows" in excel_data and isinstance(excel_data["DataFlows"], pd.DataFrame):
        for col in ["Name", "DataFlow", "DataFlowName"]:
            if col in excel_data["DataFlows"].columns:
                all_dataflow_names = sorted(excel_data["DataFlows"][col].dropna().unique().tolist())
                break
    all_trigger_names = []
    if "Triggers" in excel_data and isinstance(excel_data["Triggers"], pd.DataFrame):
        for col in ["Name", "TriggerName", "Trigger"]:
            if col in excel_data["Triggers"].columns:
                all_trigger_names = sorted(excel_data["Triggers"][col].dropna().unique().tolist())
                break

    ef_col1, ef_col2 = st.columns(2)
    with ef_col1:
        if all_dataflow_names:
            if "gpt_dataflow_multiselect" not in st.session_state:
                st.session_state.gpt_dataflow_multiselect = st.session_state.get("gpt_dataflow_filter", [])
            def _on_gpt_df_change():
                st.session_state.gpt_dataflow_filter = st.session_state.gpt_dataflow_multiselect
            st.multiselect(f"🔄 DataFlow Filter ({len(all_dataflow_names)})", all_dataflow_names,
                           key="gpt_dataflow_multiselect", placeholder="All DataFlows",
                           on_change=_on_gpt_df_change)
            st.session_state.gpt_dataflow_filter = st.session_state.get("gpt_dataflow_multiselect", [])
    with ef_col2:
        if all_trigger_names:
            if "gpt_trigger_multiselect" not in st.session_state:
                st.session_state.gpt_trigger_multiselect = st.session_state.get("gpt_trigger_filter", [])
            def _on_gpt_tf_change():
                st.session_state.gpt_trigger_filter = st.session_state.gpt_trigger_multiselect
            st.multiselect(f"🗓️ Trigger Filter ({len(all_trigger_names)})", all_trigger_names,
                           key="gpt_trigger_multiselect", placeholder="All Triggers",
                           on_change=_on_gpt_tf_change)
            st.session_state.gpt_trigger_filter = st.session_state.get("gpt_trigger_multiselect", [])


    # Active filter badge
    _apf = st.session_state.get("gpt_pipeline_filter", [])
    _adf = st.session_state.get("gpt_dataflow_filter", [])
    _atf = st.session_state.get("gpt_trigger_filter", [])
    if _apf or _adf or _atf:
        badge_parts = []
        if _apf: badge_parts.append(f"🔹 {len(_apf)} pipelines")
        if _adf: badge_parts.append(f"🔹 {len(_adf)} dataflows")
        if _atf: badge_parts.append(f"🔹 {len(_atf)} triggers")
        st.markdown(f'<div style="background:rgba(129,140,248,0.08);border:1px solid rgba(129,140,248,0.2);border-radius:8px;padding:6px 12px;font-size:.82rem;color:#818cf8;margin-bottom:.5rem;">🎯 Active: {" • ".join(badge_parts)}</div>', unsafe_allow_html=True)

    # ── Preset Questions ──
    no_history = len(st.session_state.gpt_chat_history) == 0

    def _build_gpt_filter_text() -> str:
        """Build filter instruction text for ChatGPT from all active filters."""
        instructions = []
        pf = st.session_state.get("gpt_pipeline_filter", [])
        df_f = st.session_state.get("gpt_dataflow_filter", [])
        tf = st.session_state.get("gpt_trigger_filter", [])
        if pf and len(pf) <= 30:
            instructions.append(f"⚠️ PIPELINE FILTER: Focus ONLY on these {len(pf)} pipeline(s): {', '.join(pf)}.")
        if df_f and len(df_f) <= 30:
            instructions.append(f"⚠️ DATAFLOW FILTER: Focus ONLY on these {len(df_f)} dataflow(s): {', '.join(df_f)}.")
        if tf and len(tf) <= 30:
            instructions.append(f"⚠️ TRIGGER FILTER: Focus ONLY on these {len(tf)} trigger(s): {', '.join(tf)}.")
        return "\n".join(instructions)

    with st.expander("💡 **Quick Questions** — click any to ask ChatGPT", expanded=no_history):
        if _apf or _adf or _atf:
            parts = []
            if _apf: parts.append(f"{len(_apf)} pipeline(s)")
            if _adf: parts.append(f"{len(_adf)} dataflow(s)")
            if _atf: parts.append(f"{len(_atf)} trigger(s)")
            st.success(f"🎯 Filter active: {', '.join(parts)}")
        else:
            st.caption("💡 No filter — ChatGPT receives ALL Excel data.")

        col1, col2 = st.columns(2)
        for i, (label, question) in enumerate(CHATGPT_PRESET_QUESTIONS):
            col = col1 if i % 2 == 0 else col2
            with col:
                if st.button(label, key=f"gpt_preset_{i}", width="stretch"):
                    final_q = question
                    ft = _build_gpt_filter_text()
                    if ft:
                        final_q += f"\n\n{ft}"
                    st.session_state.gpt_pending_question = final_q
                    st.session_state._gpt_prompt_type = "preset"
                    st.session_state._gpt_current_question = question[:80]
                    st.rerun()

    # ── Custom Question Input ──
    with st.expander("✏️ **Custom Question** — write your own prompt", expanded=False):
        custom_text = st.text_area(
            "Your prompt:",
            value=st.session_state.get("gpt_custom_text", ""),
            height=150,
            key="gpt_custom_area",
            placeholder="Ask anything about your ADF factory...",
        )
        if st.button("🚀 **Send**", key="gpt_send_custom", width="stretch", type="primary", disabled=not custom_text.strip()):
            final_q = custom_text.strip()
            ft = _build_gpt_filter_text()
            if ft:
                final_q += f"\n\n{ft}"
            st.session_state.gpt_pending_question = final_q
            st.rerun()

    # ── Model Selection Guide ──
    with st.expander("🧠 **Model Guide: How to use OpenAI effectively?**", expanded=False):
        st.markdown("""
        To get the best results and save on costs, strictly follow this guide:

        | 🎯 Your Goal | 🏆 Recommended Model | ⏱️ Why? |
        |---|---|---|
        | **Simple Counts / Factory Overview** | **GPT-4.1 Nano / GPT-5 Nano** | Fast, extremely cheap (15x cheaper), and perfect for answering basic structural questions. |
        | **Deep Column Lineage / Logic Debugging** | **GPT-5.4 Pro / GPT-4o** | Unrivaled reasoning capabilities for tracing complex data flows and nested SQL queries. |
        | **Complex Coding Tasks** | **o4-mini / o-series** | Uses native reasoning tokens to solve complex multi-step logical problems before answering. |

        💡 **Pro-Tip on Filtering (Save 90% Costs):** 
        Unlike Gemini, OpenAI does not natively ingest 1-million tokens easily. You **MUST** use the Pipeline, Dataflow, and Trigger dropdowns at the top of this sidebar to filter down your data before asking questions. Trying to send the entire Excel file at once will result in a **413 Payload Too Large** error or massive API bills!
        """)

    # ── Chat Control Bar ──
    msg_count = len(st.session_state.gpt_chat_history)
    tokens_used = st.session_state.get("gpt_total_tokens_used", 0)
    if msg_count > 0:
        ctrl_col1, ctrl_col2 = st.columns([4, 1])
        with ctrl_col1:
            st.markdown(f"""
            <div class="gpt-control-bar">
                <span>💬 {msg_count} messages • ~{tokens_used:,} tokens used</span>
                <span style="font-family: monospace; font-size: 0.7rem; opacity: 0.6;">{st.session_state.gpt_chat_id[-8:]}</span>
            </div>
            """, unsafe_allow_html=True)
        with ctrl_col2:
            if st.button("➕ New Chat", key="gpt_new_chat", width="stretch", type="primary"):
                st.session_state.gpt_chat_id = f"gpt_{int(time.time())}"
                st.session_state.gpt_chat_history = []
                st.session_state.gpt_api_history = []
                st.session_state.gpt_total_tokens_used = 0
                st.rerun()

    # ── Chat History Display ──
    if msg_count == 0:
        st.markdown("""
        <div class="gpt-empty-state">
            <div class="empty-icon">🧠</div>
            <h3>Ready to Analyze</h3>
            <p>Click a Quick Question above or type your own below. ChatGPT receives your complete Excel — no data is truncated.</p>
        </div>
        """, unsafe_allow_html=True)
    else:
        for i, msg in enumerate(st.session_state.gpt_chat_history):
            icon = "🧑‍💻" if msg["role"] == "user" else "🧠"
            with st.chat_message(msg["role"], avatar=icon):
                st.markdown(msg["content"])
                if msg["role"] == "assistant":
                    st.download_button(
                        label="⬇️ Download Response",
                        data=msg["content"],
                        file_name=f"chatgpt_response_{i}.md",
                        mime="text/markdown",
                        key=f"dl_gpt_msg_{i}"
                    )

    # ── Chat Input & Processing ──
    user_input = st.chat_input(
        "Ask ChatGPT about your ADF analysis...",
        key="gpt_chat_input",
    )

    active_question = user_input or st.session_state.pop("gpt_pending_question", None)

    if active_question:
        # 1. Render user message
        with st.chat_message("user", avatar="🧑‍💻"):
            st.markdown(active_question)

        # 2. Process with ChatGPT
        with st.chat_message("assistant", avatar="🧠"):
            with st.status("🧠 Analyzing with ChatGPT...", expanded=True) as status:
                status.write("📂 Building smart tiered context...")

                pipeline_filter = st.session_state.get("gpt_pipeline_filter", []) or None
                dataflow_filter = st.session_state.get("gpt_dataflow_filter", []) or None
                trigger_filter = st.session_state.get("gpt_trigger_filter", []) or None

                # Smart routing: auto-route simple queries to Nano
                model = st.session_state.gpt_model
                if st.session_state.gpt_auto_nano and _is_simple_gpt_question(active_question):
                    if model not in ("gpt-4.1-nano", "gpt-5-nano"):
                        model = "gpt-4.1-nano"
                        status.write("🚀 Simple query detected → auto-routed to GPT-4.1 Nano (15x cheaper)")

                data_context, est_tokens = build_smart_gpt_context(
                    excel_data, active_question, model,
                    pipeline_filter, dataflow_filter, trigger_filter
                )

                active_parts = []
                if pipeline_filter: active_parts.append(f"{len(pipeline_filter)} pipeline(s)")
                if dataflow_filter: active_parts.append(f"{len(dataflow_filter)} dataflow(s)")
                if trigger_filter: active_parts.append(f"{len(trigger_filter)} trigger(s)")
                if active_parts:
                    status.write(f"🎯 Filter active: {', '.join(active_parts)}")

                model_cfg = CHATGPT_MODELS.get(model, CHATGPT_MODELS[DEFAULT_CHATGPT_MODEL])
                status.write(f"📡 Sending request (~{est_tokens:,} tokens) to {model_cfg.display_name}...")

                # ── PRE-FLIGHT: Context Window Check ──
                if est_tokens > model_cfg.context_window * 0.9:
                    capable_models = [
                        f"{cfg.icon} **{cfg.display_name}** (`{name}`) — {cfg.context_window:,} tokens"
                        for name, cfg in CHATGPT_MODELS.items()
                        if cfg.context_window >= est_tokens * 1.1
                    ]
                    suggestions = "\n".join(f"  - {m}" for m in capable_models[:5]) if capable_models else "  - None available — use Pipeline Filter to reduce data"
                    
                    status.update(label="⚠️ Context too large!", state="error")
                    response = (
                        f"⚠️ **Context Too Large for {model_cfg.display_name}**\n\n"
                        f"Your data is **~{est_tokens:,} tokens** but **{model_cfg.display_name}** "
                        f"only supports **{model_cfg.context_window:,} tokens**.\n\n"
                        f"### 💡 Solutions:\n\n"
                        f"**Option 1 — Switch to a larger model:**\n{suggestions}\n\n"
                        f"**Option 2 — Use Pipeline Filter** (above) to select specific pipelines."
                    )
                    st.markdown(response)
                    st.session_state.gpt_chat_history.append({"role": "user", "content": active_question})
                    st.session_state.gpt_chat_history.append({"role": "assistant", "content": response})
                    st.rerun()
                    return

                # Build messages
                system_prompt = build_chatgpt_system_prompt(excel_data)
                full_user_message = f"""Analyze the following ADF data and answer this question.
Use ONLY the data provided below.

══════════════════════════════════════════
QUESTION: {active_question}
══════════════════════════════════════════

{data_context}

══════════════════════════════════════════
REMINDER: At the end, mention which sheets you used to answer.
══════════════════════════════════════════
"""

                # Build conversation with history (trimmed)
                messages = [{"role": "system", "content": system_prompt}]

                api_history = st.session_state.get("gpt_api_history", [])
                if st.session_state.gpt_limit_history:
                    api_history = _trim_gpt_history(api_history, st.session_state.gpt_max_history_turns)
                messages.extend(api_history)

                messages.append({"role": "user", "content": full_user_message})

                # Call API
                client = ChatGPTClient(key_mgr, model=model)
                response = client.call_api(messages, temperature=0.1)

                status.update(label="✅ Analysis complete!", state="complete")

            # 3. Show response
            if response and not response.startswith("❌"):
                routed_tag = f" (auto-routed)" if model != st.session_state.gpt_model else ""
                footer = f"\n\n---\n*{model_cfg.icon} {model_cfg.display_name}{routed_tag} • 📊 ~{est_tokens:,} tokens*"
                final_response = response + footer
            else:
                final_response = response or "❌ No response received."

            st.markdown(final_response)
            
            # Per-message cost display
            try:
                from api_cost_engine import OPENAI_PRICING
                op_cfg = OPENAI_PRICING.get(model)
                if op_cfg:
                    in_t = est_tokens
                    out_t = len(response) // 4 if response else 0
                    msg_cost = (in_t / 1_000_000) * op_cfg.input_per_m + (out_t / 1_000_000) * op_cfg.output_per_m
                    st.session_state.gpt_session_cost = st.session_state.get("gpt_session_cost", 0.0) + msg_cost
                    st.session_state.gpt_session_calls = st.session_state.get("gpt_session_calls", 0) + 1
                    cost_str = f"${msg_cost:.6f}" if msg_cost < 0.01 else f"${msg_cost:.4f}"
                    session_total = st.session_state.gpt_session_cost
                    st.caption(f"🧠 {model_cfg.display_name} • ~{est_tokens:,} in • ~{out_t:,} out • 💲 {cost_str} • Session: ${session_total:.4f}")
            except Exception:
                pass

        # 4. Update history
        st.session_state.gpt_chat_history.append({"role": "user", "content": active_question})
        st.session_state.gpt_chat_history.append({"role": "assistant", "content": final_response})

        st.session_state.gpt_api_history.append({"role": "user", "content": active_question})
        st.session_state.gpt_api_history.append({"role": "assistant", "content": response})

        st.session_state.gpt_total_tokens_used += est_tokens

        try:
            from ai_excel_chat import ChatPersistenceManager
            pm = ChatPersistenceManager()
            pm.save_chat(
                chat_id=st.session_state.gpt_chat_id,
                chat_history=st.session_state.gpt_chat_history,
                api_history=st.session_state.gpt_api_history,
                tokens=st.session_state.gpt_total_tokens_used,
                model=model
            )
        except Exception as e:
            print(f"Failed to persist GPT chat: {e}")

        st.rerun()
