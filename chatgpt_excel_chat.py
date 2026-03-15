"""
ChatGPT-Powered Excel Chat for ADF Analyzer Dashboard
======================================================
Sends the COMPLETE processed Excel output to OpenAI ChatGPT API
in a single shot — no tiering, no truncation.

DESIGN:
• Send ALL Excel sheets as CSV in the system message
• OpenAI Chat Completions REST API with SSE streaming
• Same premium glassmorphism UI as the Gemini tab
• Optional pipeline filter to reduce context
• Async conversation with chat history
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
        16384, 1_000_000, "Smallest & fastest", "🚀"
    ),
    "gpt-4o": ChatGPTModelConfig(
        "gpt-4o", "GPT-4o",
        16384, 128_000, "Multimodal flagship (128K)", "✨"
    ),
    "gpt-4o-mini": ChatGPTModelConfig(
        "gpt-4o-mini", "GPT-4o Mini",
        16384, 128_000, "Fast & cheap (128K)", "💨"
    ),
    "o4-mini": ChatGPTModelConfig(
        "o4-mini", "o4-mini",
        65536, 200_000, "Advanced reasoning", "🔬"
    ),
}

DEFAULT_CHATGPT_MODEL = "gpt-4.1-mini"


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
            "temperature": temperature,
            "max_tokens": model_cfg.max_output_tokens,
        }

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
                        return data["choices"][0]["message"]["content"]

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
# FULL EXCEL CONTEXT BUILDER — Send Everything
# ════════════════════════════════════════════════════════════════════════

def build_full_excel_context(
    excel_data: Dict[str, pd.DataFrame],
    pipeline_filter: List[str] = None,
) -> Tuple[str, int]:
    """
    Build context from ALL Excel sheets as CSV.
    No tiering — everything goes in one shot.
    Optional pipeline filter to reduce Activities sheet size.

    Returns: (context_text, estimated_token_count)
    """
    parts = []
    total_rows = 0

    if pipeline_filter:
        parts.append(
            f"⚠️ PIPELINE FILTER ACTIVE: Only showing data for these {len(pipeline_filter)} pipeline(s): "
            + ", ".join(pipeline_filter)
        )

    for sheet_name, df in sorted(excel_data.items()):
        if not isinstance(df, pd.DataFrame) or df.empty:
            continue

        # Apply pipeline filter to sheets that have a Pipeline column
        filtered = False
        if pipeline_filter and "Pipeline" in df.columns:
            df = df[df["Pipeline"].isin(pipeline_filter)]
            filtered = True
            if df.empty:
                continue

        csv_data = df.to_csv(index=False)
        tag = f" [FILTERED: {len(pipeline_filter)} pipelines]" if filtered else ""
        parts.append(
            f"\n### 📋 Sheet: {sheet_name}{tag} "
            f"({len(df)} rows × {len(df.columns)} cols)\n{csv_data}"
        )
        total_rows += len(df)

    full_context = "\n\n".join(parts)
    est_tokens = len(full_context) // 4
    return full_context, est_tokens


def build_chatgpt_system_prompt(excel_data: Dict[str, pd.DataFrame]) -> str:
    """Build system prompt with sheet catalog for ChatGPT."""

    sheet_catalog = []
    for name, df in sorted(excel_data.items()):
        if isinstance(df, pd.DataFrame) and not df.empty:
            cols = ", ".join(list(df.columns)[:15])
            sheet_catalog.append(f"  📋 {name}: {len(df)} rows × {len(df.columns)} cols | Columns: [{cols}]")

    return f"""You are an expert Azure Data Factory (ADF) Analyst AI Assistant.
You have access to the COMPLETE output of an ADF ARM Template Analyzer tool.

CONVERSATIONAL MEMORY:
You are having a multi-turn conversation. You MUST remember and reference previous questions
and your own previous answers. If the user says "show more", "explain that", "what about the
previous one", or references something from earlier — look at the conversation history and
build upon it. Never say "I don't have access to previous messages" — you DO.

AVAILABLE DATA SHEETS:
{chr(10).join(sheet_catalog)}

CRITICAL RULES:
1. Answer ONLY using data from the sheets provided. Never invent data.
2. If data is not in the sheets, say "Data not available in the provided sheets."
3. Reference specific sheet names, column names, and row counts in your answers.
4. Use markdown tables, bullet points, and code blocks for clarity.
5. When asked about lineage, trace the full path: Source → Intermediate → Target.
6. Cross-reference between sheets (e.g., Activities.Pipeline → PipelineAnalysis.Pipeline).
7. If the user applies a pipeline filter, focus ONLY on those pipelines.

SHEET RELATIONSHIPS:
• Pipeline names: Pipelines, PipelineAnalysis, Activities, DataLineage, ImpactAnalysis
• Linked services: LinkedServices, Datasets, Activities (Source/SinkLinkedService)
• DataFlows: DataFlows, DataFlowLineage, DataFlowTransformations
• Orphaned resources: OrphanedPipelines, OrphanedDataFlows, OrphanedDatasets, OrphanedLinkedServices
"""


# ════════════════════════════════════════════════════════════════════════
# PRESET QUESTIONS — Same as Gemini tab
# ════════════════════════════════════════════════════════════════════════

CHATGPT_PRESET_QUESTIONS = [
    ("🏭 Factory Overview", "Give me a comprehensive overview of this Azure Data Factory. How many pipelines, dataflows, datasets, linked services, and triggers are there? What are the main folders and categories? Show counts from the Statistics sheet."),
    ("🔗 Table Lineage", """You are an expert ADF Table Lineage Analyst for Tiger Analytics accelerators.

Input: Excel data from ADF Pipeline Analyzer. Focus on these sheets for extracting lineage:

Activities: Contains granular activity details including Pipeline, ActivityType, SourceTable, SinkTable, SourceLinkedService, SinkLinkedService, ValuesInfo (for intermediate layers), DataFlow (for dataflow names).
DataFlows: Contains SinkTables for data flows.
Datasets: Contains LinkedService for datasets.
LinkedServices: For linked service details.
Task: Extract comprehensive Table-Level Lineage.

Trace Lineage Path: For each pipeline, follow the data flow from source to target, identifying all intermediate steps and transformations.

Extract Components: For each stage in the lineage, identify:
Pipeline Name, Target Table, Target Linked Service Connection, Transformation Layer (dataflow/SP), Intermediate layer (Stg table/ADLS), Source Table, Source Linked Service.

Output Format (Markdown table): | Target Table | Target Linked Service Connection | Transformation Layer (dataflow/SP) | Intermediate layer(Stg table/ADLS) | Source Table | Source Linked Service | Pipeline Name |

Be precise, cite activity names and relevant sheet references. Process ALL pipelines in the provided data."""),
    ("⚡ Complex Pipelines", "List the top 10 most complex pipelines based on complexity score. Show their activity counts, dataflow usage, SQL usage, and folder. Use the PipelineAnalysis sheet."),
    ("🔄 DataFlow Analysis", "Analyze all data flows. For each, show: name, source tables, sink tables, transformation types, and complexity. Use the DataFlows and DataFlowTransformations sheets."),
    ("👻 Orphaned Resources", "List ALL orphaned resources across all categories (pipelines, dataflows, datasets, linked services, triggers). Show counts and names. Explain the risk of each orphaned resource."),
    ("💥 Impact Analysis", "Show the top 10 highest-impact pipelines based on blast radius. What would break if each one fails? Use the ImpactAnalysis sheet."),
    ("🗓️ Trigger Schedule", "Show all triggers with their schedules, associated pipelines, and status. Are there any scheduling conflicts? Use the Triggers and TriggerDetails sheets."),
    ("🏥 Health Check", "Perform a comprehensive health check: orphaned resources, missing triggers, high-complexity pipelines, unused datasets, and potential issues. Provide recommendations."),
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

def initialize_chatgpt_session_state():
    """Initialize ChatGPT tab session state variables."""
    defaults = {
        "gpt_chat_id": f"gpt_{int(time.time())}",
        "gpt_chat_history": [],
        "gpt_api_history": [],
        "gpt_total_tokens_used": 0,
        "gpt_model": DEFAULT_CHATGPT_MODEL,
        "gpt_pipeline_filter": [],
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
    total_sheets = sum(1 for df in excel_data.values() if isinstance(df, pd.DataFrame) and not df.empty)
    total_rows = sum(len(df) for df in excel_data.values() if isinstance(df, pd.DataFrame))
    total_chars = sum(len(df.to_csv(index=False)) for df in excel_data.values() if isinstance(df, pd.DataFrame) and not df.empty)
    est_total_tokens = total_chars // 4

    # ── Data Status Bar ──
    st.markdown(f"""
    <div class="gpt-data-status loaded">
        ✅ {total_sheets} sheets • {total_rows:,} rows • ~{est_total_tokens:,} tokens (full Excel — no truncation)
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

    # ── Model Selection ──
    model_options = list(CHATGPT_MODELS.keys())
    model_labels = [f"{CHATGPT_MODELS[m].icon} {CHATGPT_MODELS[m].display_name} — {CHATGPT_MODELS[m].best_for}" for m in model_options]
    current_idx = model_options.index(st.session_state.gpt_model) if st.session_state.gpt_model in model_options else 0

    selected_model = st.selectbox(
        "🧠 ChatGPT Model:",
        options=model_options,
        index=current_idx,
        format_func=lambda m: f"{CHATGPT_MODELS[m].icon} {CHATGPT_MODELS[m].display_name} — {CHATGPT_MODELS[m].best_for}",
        key="gpt_model_select",
    )
    st.session_state.gpt_model = selected_model

    # ── Optional Pipeline Filter ──
    all_pipeline_names = []
    if "PipelineAnalysis" in excel_data and isinstance(excel_data["PipelineAnalysis"], pd.DataFrame):
        if "Pipeline" in excel_data["PipelineAnalysis"].columns:
            all_pipeline_names = sorted(excel_data["PipelineAnalysis"]["Pipeline"].dropna().unique().tolist())

    if all_pipeline_names:
        with st.expander(f"🔍 **Pipeline Filter** (optional) — {len(all_pipeline_names)} available", expanded=False):
            st.caption("💡 ChatGPT receives ALL data by default. Use this filter to focus on specific pipelines and reduce token usage.")

            fcol1, fcol2 = st.columns(2)
            with fcol1:
                if st.button("📋 Select All", key="gpt_filter_all", use_container_width=True):
                    st.session_state.gpt_pipeline_filter = all_pipeline_names
                    st.rerun()
            with fcol2:
                if st.button("🧹 Clear Filter", key="gpt_filter_clear", use_container_width=True):
                    st.session_state.gpt_pipeline_filter = []
                    st.rerun()

            selected = st.multiselect(
                "Select Pipelines:",
                options=all_pipeline_names,
                default=st.session_state.get("gpt_pipeline_filter", []),
                key="gpt_pipeline_multiselect",
                placeholder="All Pipelines (no filter — full Excel sent)",
            )
            st.session_state.gpt_pipeline_filter = selected

            if selected:
                st.success(f"🎯 {len(selected)} pipeline(s) selected — context will be filtered.")

    # ── Preset Questions ──
    no_history = len(st.session_state.gpt_chat_history) == 0
    active_filter = st.session_state.get("gpt_pipeline_filter", [])

    with st.expander("💡 **Quick Questions** — click any to ask ChatGPT", expanded=no_history):
        if active_filter:
            st.success(f"🎯 Filter active: {len(active_filter)} pipeline(s) selected")
        else:
            st.caption("💡 No filter — ChatGPT receives ALL Excel data.")

        col1, col2 = st.columns(2)
        for i, (label, question) in enumerate(CHATGPT_PRESET_QUESTIONS):
            col = col1 if i % 2 == 0 else col2
            with col:
                if st.button(label, key=f"gpt_preset_{i}", use_container_width=True):
                    final_q = question
                    if active_filter and len(active_filter) <= 20:
                        final_q += f"\n\n⚠️ IMPORTANT: Focus ONLY on these {len(active_filter)} pipeline(s): {', '.join(active_filter)}. Do not include data from other entities."
                    st.session_state.gpt_pending_question = final_q
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
        if st.button("🚀 **Send**", key="gpt_send_custom", use_container_width=True, type="primary", disabled=not custom_text.strip()):
            final_q = custom_text.strip()
            if active_filter and len(active_filter) <= 20:
                final_q += f"\n\n⚠️ IMPORTANT: Focus ONLY on these {len(active_filter)} pipeline(s): {', '.join(active_filter)}. Do not include data from other entities."
            st.session_state.gpt_pending_question = final_q
            st.rerun()

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
            if st.button("➕ New Chat", key="gpt_new_chat", use_container_width=True, type="primary"):
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
        for msg in st.session_state.gpt_chat_history:
            icon = "🧑‍💻" if msg["role"] == "user" else "🧠"
            with st.chat_message(msg["role"], avatar=icon):
                st.markdown(msg["content"])

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
                status.write("📂 Building full Excel context...")

                pipeline_filter = st.session_state.get("gpt_pipeline_filter", []) or None
                data_context, est_tokens = build_full_excel_context(excel_data, pipeline_filter)

                if pipeline_filter:
                    status.write(f"🎯 Filter active: {len(pipeline_filter)} pipeline(s)")

                model = st.session_state.gpt_model
                model_cfg = CHATGPT_MODELS.get(model, CHATGPT_MODELS[DEFAULT_CHATGPT_MODEL])
                status.write(f"📡 Sending request (~{est_tokens:,} tokens) to {model_cfg.display_name}...")

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

                # Build conversation with history
                messages = [{"role": "system", "content": system_prompt}]

                # Conversation memory — keep last 12 messages (6 full exchanges) for rich context
                api_history = st.session_state.get("gpt_api_history", [])
                if len(api_history) > 12:
                    api_history = api_history[-12:]
                messages.extend(api_history)

                # Add current question
                messages.append({"role": "user", "content": full_user_message})

                # Call API
                client = ChatGPTClient(key_mgr, model=model)
                response = client.call_api(messages, temperature=0.1)

                status.update(label="✅ Analysis complete!", state="complete")

            # 3. Show response
            if response and not response.startswith("❌"):
                footer = f"\n\n---\n*{model_cfg.icon} {model_cfg.display_name} • 📊 ~{est_tokens:,} tokens*"
                final_response = response + footer
            else:
                final_response = response or "❌ No response received."

            st.markdown(final_response)

        # 4. Update history
        st.session_state.gpt_chat_history.append({"role": "user", "content": active_question})
        st.session_state.gpt_chat_history.append({"role": "assistant", "content": final_response})

        # Update conversation memory — store full question for recall
        st.session_state.gpt_api_history.append({"role": "user", "content": active_question})
        st.session_state.gpt_api_history.append({"role": "assistant", "content": response})

        st.session_state.gpt_total_tokens_used += est_tokens

        st.rerun()
