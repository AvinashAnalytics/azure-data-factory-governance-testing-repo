"""
═══════════════════════════════════════════════════════════════════════════
Claude AI Excel Analyst — Production-Ready, Cost-Optimized
═══════════════════════════════════════════════════════════════════════════
Uses Anthropic Claude Files API + Code Execution (code_execution_20250825)
with PROMPT CACHING for up to 90% cost reduction.

COST OPTIMIZATIONS:
  • Prompt caching — system prompt + file ref cached → 0.1x token cost
  • Smart history trimming — default 8 turns to reduce input tokens
  • Auto-Haiku for simple questions — auto-detect and route to cheaper model
  • Real-time cost display per message

FEATURES:
  • Native Excel upload via Files API — no CSV conversion
  • Code execution with pandas, matplotlib, seaborn, scipy
  • Real chart/graph generation as PNG (rendered inline)
  • Generated file downloads (Excel, CSV, images)
  • All 3 filter types: Pipeline / DataFlow / Trigger
  • Exact same 10 presets as Gemini + 4 chart-only presets
  • Multi-turn conversation with history
  • Cost tracking via api_cost_engine

REQUIRES: pip install anthropic   |   ANTHROPIC_API_KEY in .env
═══════════════════════════════════════════════════════════════════════════
"""

import base64
import io
import json
import os
import re
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False


# ════════════════════════════════════════════════════════════════════════
# MODEL CONFIGURATION
# ════════════════════════════════════════════════════════════════════════

@dataclass
class ClaudeModelConfig:
    name: str
    display_name: str
    max_output_tokens: int
    context_window: int
    best_for: str
    icon: str
    input_per_m: float
    output_per_m: float
    cached_per_m: float  # cached input token rate (0.1x)

CLAUDE_MODELS = {
    "claude-sonnet-4-6": ClaudeModelConfig(
        "claude-sonnet-4-6", "Claude Sonnet 4.6",
        16384, 1_000_000,
        "Best balance — 1M context, fast, great for Excel",
        "🟣", 3.00, 15.00, 0.30,
    ),
    "claude-opus-4-6": ClaudeModelConfig(
        "claude-opus-4-6", "Claude Opus 4.6",
        32768, 200_000,
        "Most intelligent — deep reasoning, complex analysis",
        "💎", 15.00, 75.00, 1.50,
    ),
    "claude-sonnet-4-5": ClaudeModelConfig(
        "claude-sonnet-4-5", "Claude Sonnet 4.5",
        16384, 200_000,
        "Previous gen — reliable and proven",
        "🔮", 3.00, 15.00, 0.30,
    ),
    "claude-haiku-4-5": ClaudeModelConfig(
        "claude-haiku-4-5", "Claude Haiku 4.5",
        8192, 200_000,
        "⚡ Cheapest — simple questions, quick summaries",
        "⚡", 0.80, 4.00, 0.08,
    ),
}

DEFAULT_CLAUDE_MODEL = "claude-sonnet-4-6"
HAIKU_MODEL = "claude-haiku-4-5"

FILES_API_BETA = "files-api-2025-04-14"
CODE_EXECUTION_TOOL = {"type": "code_execution_20250825", "name": "code_execution"}

# Cost defaults
DEFAULT_MAX_HISTORY_TURNS = 8
SIMPLE_QUESTION_KEYWORDS = [
    "how many", "count", "list", "show", "what is", "which", "total",
    "overview", "summary", "name", "names", "version",
]


# ════════════════════════════════════════════════════════════════════════
# CLIENT WRAPPER
# ════════════════════════════════════════════════════════════════════════

class ClaudeClient:
    """Anthropic SDK wrapper with Files API + Code Execution + Prompt Caching."""

    def __init__(self, model: str = DEFAULT_CLAUDE_MODEL):
        self.model = model
        self.client = None
        self._init_client()

    def _init_client(self):
        from dotenv import load_dotenv
        load_dotenv()
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if api_key:
            self.client = anthropic.Anthropic(api_key=api_key)

    @property
    def is_ready(self) -> bool:
        return self.client is not None

    def upload_bytes(self, data: bytes, filename: str) -> Optional[str]:
        """Upload bytes to Claude Files API. Returns file_id."""
        if not self.client:
            return None
        try:
            suffix = Path(filename).suffix
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(data)
                tmp_path = tmp.name
            with open(tmp_path, "rb") as f:
                result = self.client.beta.files.upload(file=f, betas=[FILES_API_BETA])
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
            return result.id
        except Exception as e:
            st.error(f"❌ Upload failed: {e}")
            return None

    def send_message(
        self,
        messages: List[Dict],
        system_blocks: List[Dict],
        max_tokens: int = 16384,
        thinking_budget: int = 0,
        stream_callback = None
    ) -> Dict[str, Any]:
        """
        Send message with prompt caching and optional extended thinking streams.
        system_blocks: list of system content blocks with cache_control.
        """
        if not self.client:
            return {"error": "Client not initialized. Check ANTHROPIC_API_KEY."}
        try:
            kwargs = {
                "model": self.model,
                "max_tokens": max_tokens,
                "system": system_blocks,
                "messages": messages,
                "tools": [CODE_EXECUTION_TOOL],
                "betas": [FILES_API_BETA]
            }
            if thinking_budget > 0:
                kwargs["thinking"] = {"type": "enabled", "budget_tokens": thinking_budget}

            if stream_callback:
                with self.client.beta.messages.stream(**kwargs) as stream:
                    for event in stream:
                        if event.type == "content_block_delta":
                            if hasattr(event.delta, "type"):
                                if event.delta.type == "thinking_delta":
                                    stream_callback("thinking", event.delta.thinking)
                                elif event.delta.type == "text_delta":
                                    stream_callback("text", event.delta.text)
                    response = stream.get_final_message()
            else:
                response = self.client.beta.messages.create(**kwargs)
                
            return self._parse_response(response)
        except anthropic.RateLimitError as e:
            return {"error": f"❌ Rate limited. Wait and retry.\n{str(e)[:200]}"}
        except anthropic.AuthenticationError:
            return {"error": "❌ Invalid API key. Check ANTHROPIC_API_KEY."}
        except anthropic.BadRequestError as e:
            msg = str(e)
            if "too large" in msg.lower() or "token" in msg.lower():
                return {"error": "❌ Too large. Use pipeline filter or Claude Sonnet 4.6 (1M context)."}
            return {"error": f"❌ API Error: {msg[:500]}"}
        except Exception as e:
            return {"error": f"❌ Error: {str(e)[:500]}"}

    def _parse_response(self, response) -> Dict[str, Any]:
        """Parse full Claude response — text, code, stdout, files."""
        result = {
            "text": "",
            "code_blocks": [],
            "stdout_blocks": [],
            "stderr_blocks": [],
            "generated_files": [],
            "input_tokens": getattr(response.usage, "input_tokens", 0),
            "output_tokens": getattr(response.usage, "output_tokens", 0),
            "cache_creation_input_tokens": getattr(response.usage, "cache_creation_input_tokens", 0),
            "cache_read_input_tokens": getattr(response.usage, "cache_read_input_tokens", 0),
            "container_id": getattr(response, "container_id", None),
            "stop_reason": response.stop_reason,
        }
        for block in response.content:
            if block.type == "text":
                result["text"] += block.text
            elif block.type == "server_tool_use":
                if hasattr(block, "input") and isinstance(block.input, dict):
                    cmd = block.input.get("command", block.input.get("code", ""))
                    if cmd:
                        result["code_blocks"].append(cmd)
            elif block.type == "bash_code_execution_tool_result":
                c = block.content
                if hasattr(c, "type") and c.type == "bash_code_execution_result":
                    if hasattr(c, "stdout") and c.stdout:
                        result["stdout_blocks"].append(c.stdout)
                    if hasattr(c, "stderr") and c.stderr:
                        result["stderr_blocks"].append(c.stderr)
                    if hasattr(c, "content"):
                        for item in c.content:
                            if hasattr(item, "file_id"):
                                fname = getattr(item, "filename", "output")
                                result["generated_files"].append((item.file_id, fname))
            elif block.type == "code_execution_tool_result":
                c = getattr(block, "content", None)
                if c:
                    if hasattr(c, "stdout") and c.stdout:
                        result["stdout_blocks"].append(c.stdout)
                    if hasattr(c, "stderr") and c.stderr:
                        result["stderr_blocks"].append(c.stderr)
            elif block.type == "text_editor_code_execution_tool_result":
                c = getattr(block, "content", None)
                if c and hasattr(c, "text"):
                    result["stdout_blocks"].append(c.text)
        return result

    def download_file(self, file_id: str) -> Optional[Tuple[bytes, str]]:
        if not self.client:
            return None
        try:
            metadata = self.client.beta.files.retrieve_metadata(file_id, betas=[FILES_API_BETA])
            content = self.client.beta.files.download(file_id, betas=[FILES_API_BETA])
            return (content.read(), metadata.filename)
        except Exception:
            return None


# ════════════════════════════════════════════════════════════════════════
# SMART ROUTING — Auto-detect simple questions for cheaper Haiku
# ════════════════════════════════════════════════════════════════════════

def _is_simple_question(question: str) -> bool:
    """Detect if a question is simple enough for the cheaper Haiku model."""
    q_lower = question.lower().strip()
    if len(q_lower) > 200:
        return False
    for kw in SIMPLE_QUESTION_KEYWORDS:
        if q_lower.startswith(kw):
            return True
    if q_lower.endswith("?") and len(q_lower.split()) <= 12:
        return True
    return False


# ════════════════════════════════════════════════════════════════════════
# PRESET PROMPTS — EXACT SAME as Gemini AI Excel Chat + Chart Presets
# ════════════════════════════════════════════════════════════════════════

CLAUDE_PRESET_QUESTIONS = [
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

    # ── CHART PRESETS — Claude-Only (Code Execution) ──
    ("📊 Complexity Chart",
     """Create these charts using PipelineAnalysis sheet:
1. Horizontal bar chart: Top 15 pipelines by ComplexityScore, color-coded by ImpactLevel (CRITICAL=red, HIGH=orange, MEDIUM=yellow, LOW=green). Save as 'complexity_chart.png'.
2. Pie chart: Distribution of Complexity levels. Save as 'complexity_pie.png'.
Use dark background (plt.style.use('dark_background')), figsize=(12,6), dpi=150. Provide text summary."""),

    ("📈 Activity Distribution",
     """Create visualizations from Activities sheet:
1. Bar chart: Top 10 ActivityType by count. Save as 'activity_types.png'.
2. Stacked bar: Top 15 pipelines by activity count, breakdown by ActivityType. Save as 'pipeline_activities.png'.
Use dark background, attractive colors, proper labels. Print summary table."""),

    ("🕸️ Lineage Heatmap",
     """From DataLineage sheet, create a heatmap:
1. Find all unique (SourceSystem, TargetSystem) pairs with counts.
2. Create heatmap showing connection density. Save as 'lineage_heatmap.png'.
3. Print summary table of most-connected systems.
Use dark background, seaborn heatmap with 'viridis' colormap."""),

    ("🏥 Health Dashboard",
     """Create multi-chart health dashboard:
1. Pie chart of orphaned resources by type. Save as 'orphan_pie.png'.
2. Histogram of pipeline complexity distribution. Save as 'complexity_hist.png'.
3. Bar chart of pipelines by ImpactLevel. Save as 'impact_bar.png'.
4. Print numeric health score (0-100) based on orphan ratio, complexity, trigger coverage.
Use dark backgrounds, vibrant colors."""),
]


# ════════════════════════════════════════════════════════════════════════
# SYSTEM PROMPT BUILDER — with cache_control for prompt caching
# ════════════════════════════════════════════════════════════════════════

def build_system_blocks(
    excel_data: Dict[str, pd.DataFrame],
    enable_caching: bool = True,
) -> List[Dict]:
    """Build system content blocks with optional prompt caching."""
    sheet_catalog = []
    for name, df in sorted(excel_data.items()):
        if isinstance(df, pd.DataFrame) and not df.empty:
            cols = ", ".join(list(df.columns)[:15])
            sheet_catalog.append(f"  📋 {name}: {len(df)} rows × {len(df.columns)} cols | [{cols}]")

    prompt_text = f"""You are an expert Azure Data Factory (ADF) Analyst AI with CODE EXECUTION.
You have the COMPLETE ADF ARM Template Analyzer output as an Excel file in your sandbox.

AVAILABLE SHEETS:
{chr(10).join(sheet_catalog)}

INSTRUCTIONS:
1. ALWAYS use code execution to read/analyze data — never guess values.
2. Use pd.read_excel(filepath, sheet_name='XXX') to load sheets.
3. For charts: plt.style.use('dark_background'), figsize=(12,6), dpi=150.
   Save as PNG: plt.savefig('name.png', bbox_inches='tight', facecolor='#1a1b2e')
4. Show actual data, counts, statistics from code output.
5. Use markdown tables, bullets, code blocks in your text.
6. Cross-reference between sheets for complete analysis.
7. Apply pandas .isin() filtering when pipeline/dataflow/trigger filters are active.

CONVERSATIONAL MEMORY:
You have multi-turn context. When user says "show more" or references prior answers, build on them.

SHEET RELATIONSHIPS:
• Pipelines: Pipelines, PipelineAnalysis, Activities, DataLineage, ImpactAnalysis
• LinkedServices: LinkedServices, Datasets, Activities (Source/SinkLinkedService)
• DataFlows: DataFlows, DataFlowLineage, DataFlowTransformations
• Orphaned: OrphanedPipelines, OrphanedDataFlows, OrphanedDatasets, OrphanedLinkedServices
• Triggers: Triggers, TriggerDetails"""

    block = {"type": "text", "text": prompt_text}
    if enable_caching:
        block["cache_control"] = {"type": "ephemeral"}
    return [block]


# ════════════════════════════════════════════════════════════════════════
# CSS — Purple/Violet theme
# ════════════════════════════════════════════════════════════════════════

CLAUDE_CSS = """
<style>
:root {
    --cl-primary: #8b5cf6; --cl-secondary: #7c3aed; --cl-accent: #a78bfa;
    --cl-bg: rgba(139,92,246,0.03); --cl-bg2: rgba(139,92,246,0.06);
    --cl-border: rgba(139,92,246,0.12); --cl-text: #e2e8f0; --cl-muted: #94a3b8;
}
.cl-header {
    text-align:center; padding:1.5rem;
    background:linear-gradient(135deg,rgba(139,92,246,0.10),rgba(124,58,237,0.05));
    border-radius:16px; border:1px solid var(--cl-border); margin-bottom:1.2rem;
}
.cl-header h2 {
    margin:0 0 .3rem; background:linear-gradient(135deg,#8b5cf6,#a78bfa);
    -webkit-background-clip:text; -webkit-text-fill-color:transparent; font-weight:700;
}
.cl-header p { color:var(--cl-muted); font-size:.88rem; margin:0; }
.cl-metric {
    background:var(--cl-bg2); border:1px solid var(--cl-border);
    border-radius:12px; padding:.75rem; text-align:center;
}
.cl-metric .val { font-size:1.05rem; font-weight:700; color:var(--cl-accent); }
.cl-metric .lbl {
    font-size:.68rem; color:var(--cl-muted); text-transform:uppercase;
    letter-spacing:.06em; margin-top:.15rem;
}
.cl-code {
    background:rgba(0,0,0,0.35); border:1px solid rgba(139,92,246,0.2);
    border-radius:8px; padding:12px; font-family:'JetBrains Mono','Fira Code',monospace;
    font-size:.82rem; overflow-x:auto; white-space:pre-wrap; color:#a5f3fc;
    margin:8px 0; max-height:500px; overflow-y:auto;
}
.cl-empty {
    text-align:center; padding:3rem 2rem; color:var(--cl-muted);
}
.cl-empty .icon { font-size:3.5rem; margin-bottom:1rem; opacity:.6; }
.cl-empty h3 { color:var(--cl-text); font-weight:600; margin:0 0 .5rem; }
.cl-empty p { font-size:.88rem; max-width:420px; margin:0 auto; line-height:1.5; }
.cl-filter-badge {
    background:rgba(139,92,246,0.08); border:1px solid rgba(139,92,246,0.2);
    border-radius:8px; padding:6px 12px; font-size:.82rem; color:#a78bfa; margin-bottom:.5rem;
}
.cl-cost-bar {
    display:flex; align-items:center; justify-content:space-between;
    padding:.4rem .8rem; background:var(--cl-bg2); border:1px solid var(--cl-border);
    border-radius:10px; margin-bottom:.6rem; font-size:.76rem; color:var(--cl-muted);
}
.cl-savings { color:#34d399; font-weight:600; }
</style>
"""


# ════════════════════════════════════════════════════════════════════════
# SESSION STATE
# ════════════════════════════════════════════════════════════════════════

def initialize_claude_session_state():
    defaults = {
        "claude_chat_history": [],
        "claude_api_messages": [],
        "claude_model": DEFAULT_CLAUDE_MODEL,
        "claude_file_id": None,
        "claude_file_name": None,
        "claude_container_id": None,
        "claude_pending_question": None,
        "claude_pipeline_filter": [],
        "claude_dataflow_filter": [],
        "claude_trigger_filter": [],
        # Cost-saving toggles
        "claude_enable_caching": True,
        "claude_limit_history": True,
        "claude_auto_haiku": True,
        "claude_max_history": DEFAULT_MAX_HISTORY_TURNS,
        # Session cost tracking
        "claude_session_cost": 0.0,
        "claude_session_calls": 0,
        "claude_session_cached_tokens": 0,
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val


# ════════════════════════════════════════════════════════════════════════
# HISTORY TRIMMING — Keep last N turns to reduce cost
# ════════════════════════════════════════════════════════════════════════

def _trim_history(api_messages: List[Dict], max_turns: int = 8) -> List[Dict]:
    """Keep only the last N user-assistant turn pairs."""
    if not api_messages:
        return []
    # Each turn = 1 user + 1 assistant = 2 messages
    max_msgs = max_turns * 2
    if len(api_messages) <= max_msgs:
        return api_messages
    return api_messages[-max_msgs:]


# ════════════════════════════════════════════════════════════════════════
# FILTER BUILDER
# ════════════════════════════════════════════════════════════════════════

def _build_filter_text() -> str:
    parts = []
    pf = st.session_state.get("claude_pipeline_filter", [])
    df = st.session_state.get("claude_dataflow_filter", [])
    tf = st.session_state.get("claude_trigger_filter", [])
    if pf and len(pf) <= 30:
        names = ", ".join(f"'{p}'" for p in pf)
        parts.append(f"⚠️ PIPELINE FILTER: Only these {len(pf)}: [{names}]. Use df[df['Pipeline'].isin([{names}])].")
    if df and len(df) <= 30:
        names = ", ".join(f"'{d}'" for d in df)
        parts.append(f"⚠️ DATAFLOW FILTER: Only these {len(df)}: [{names}]. Filter DataFlows by name.")
    if tf and len(tf) <= 30:
        names = ", ".join(f"'{t}'" for t in tf)
        parts.append(f"⚠️ TRIGGER FILTER: Only these {len(tf)}: [{names}]. Filter Triggers by name.")
    return "\n\n".join(parts)


# ════════════════════════════════════════════════════════════════════════
# RESPONSE RENDERER
# ════════════════════════════════════════════════════════════════════════

def _render_response(result: Dict[str, Any], client: ClaudeClient = None):
    """Render text, code outputs, charts inline, and offer file downloads."""
    if result.get("text"):
        st.markdown(result["text"])

    for stdout in result.get("stdout_blocks", []):
        if stdout.strip():
            with st.expander("📟 **Code Output**", expanded=True):
                st.markdown(f'<div class="cl-code">{stdout}</div>', unsafe_allow_html=True)

    for stderr in result.get("stderr_blocks", []):
        c = stderr.strip()
        if c and not any(s in c.lower() for s in ["userwarning", "futurewarning", "deprecat"]):
            with st.expander("⚠️ Warning", expanded=False):
                st.code(c[:500], language="text")

    if not client:
        client = ClaudeClient()
    for file_id, filename in result.get("generated_files", []):
        is_img = filename.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp'))
        try:
            dl = client.download_file(file_id)
            if dl:
                fbytes, fname = dl
                if is_img:
                    st.image(fbytes, caption=f"📊 {fname}", width="stretch")
                    st.download_button(f"⬇️ {fname}", data=fbytes, file_name=fname,
                                       mime="image/png", key=f"dl_{file_id}")
                else:
                    mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if fname.endswith('.xlsx') else "application/octet-stream"
                    st.download_button(f"📥 Download {fname}", data=fbytes, file_name=fname,
                                       mime=mime, key=f"dl_{file_id}", type="primary")
        except Exception:
            st.success(f"📁 Generated: **{filename}**")


# ════════════════════════════════════════════════════════════════════════
# MAIN RENDER FUNCTION
# ════════════════════════════════════════════════════════════════════════

def render_claude_chat_tab(excel_data: Dict[str, pd.DataFrame] = None):
    """Render complete Claude AI Chat tab — production-ready."""
    initialize_claude_session_state()
    st.markdown(CLAUDE_CSS, unsafe_allow_html=True)

    if not HAS_ANTHROPIC:
        st.error("❌ `anthropic` package not installed.")
        st.code("pip install anthropic", language="bash")
        return

    client = ClaudeClient(model=st.session_state.claude_model)
    if not client.is_ready:
        st.warning("⚠️ Add `ANTHROPIC_API_KEY=sk-ant-...` to your `.env` file.")
        key_input = st.text_input("🔑 Or paste key:", type="password", key="cl_key_input")
        if key_input:
            os.environ["ANTHROPIC_API_KEY"] = key_input
            client = ClaudeClient(model=st.session_state.claude_model)
            if client.is_ready:
                st.success("✅ Key accepted!")
                st.rerun()
        return

    # ── Header ──
    st.markdown("""
    <div class="cl-header">
        <h2>🟣 Claude AI Excel Analyst</h2>
        <p>Code Execution • Real Charts • Prompt Caching (90% cheaper) • Native Excel</p>
    </div>
    """, unsafe_allow_html=True)

    if not excel_data or not isinstance(excel_data, dict) or len(excel_data) == 0:
        st.warning("⚠️ No data. Go to **📊 Upload & Analyze** tab first.")
        return

    ctx = st.session_state.get("ai_context_builder")
    if ctx:
        total_sheets = ctx.sheet_count
        total_rows = sum(info["rows"] for info in ctx.sheet_info.values())
        hash_label = "Hash"
        hash_val = f"<span style='font-family:monospace;'>{ctx.data_hash[:8]}</span>"
    else:
        total_sheets = len(excel_data)
        total_rows = sum(len(df) for df in excel_data.values() if isinstance(df, pd.DataFrame))
        hash_label = "Status"
        hash_val = "✅ Ready"

    # ── File Upload ──
    file_id = st.session_state.claude_file_id
    if not file_id:
        st.info("📤 Upload Excel to Claude's sandbox for native code execution analysis.")
        if st.button("🚀 Upload Excel to Claude", key="cl_upload", type="primary", width="stretch"):
            with st.spinner("Uploading..."):
                buf = io.BytesIO()
                with pd.ExcelWriter(buf, engine="openpyxl") as w:
                    for sn, df in excel_data.items():
                        if isinstance(df, pd.DataFrame) and not df.empty:
                            df.to_excel(w, sheet_name=sn[:31], index=False)
                fid = client.upload_bytes(buf.getvalue(), "adf_analysis.xlsx")
                if fid:
                    st.session_state.claude_file_id = fid
                    st.session_state.claude_file_name = "adf_analysis.xlsx"
                    st.success(f"✅ Uploaded! ID: {fid[:20]}...")
                    st.rerun()
                else:
                    st.error("❌ Upload failed.")
        return

    # ── Metrics Row ──
    model_cfg = CLAUDE_MODELS.get(st.session_state.claude_model, CLAUDE_MODELS[DEFAULT_CLAUDE_MODEL])
    mc = st.columns(6)
    session_cost = st.session_state.get("claude_session_cost", 0.0)
    cached_tok = st.session_state.get("claude_session_cached_tokens", 0)
    metrics = [
        (str(total_sheets), "Sheets"),
        (f"{total_rows:,}", "Rows"),
        (model_cfg.icon, model_cfg.display_name.split()[-1]),
        (hash_val, hash_label),
        (f"${session_cost:.4f}" if session_cost < 1 else f"${session_cost:.2f}", "Session Cost"),
        (f"{cached_tok:,}" if cached_tok else "0", "Cached Tokens"),
    ]
    for col, (v, l) in zip(mc, metrics):
        with col:
            st.markdown(f'<div class="cl-metric"><div class="val">{v}</div><div class="lbl">{l}</div></div>', unsafe_allow_html=True)

    st.markdown("---")

    # ── Controls Row: Model + Cost Toggles ──
    cc1, cc2 = st.columns([1, 1])
    with cc1:
        model_opts = list(CLAUDE_MODELS.keys())
        model_lbls = [f"{CLAUDE_MODELS[m].icon} {CLAUDE_MODELS[m].display_name} — {CLAUDE_MODELS[m].best_for}" for m in model_opts]
        idx = model_opts.index(st.session_state.claude_model) if st.session_state.claude_model in model_opts else 0
        sel = st.selectbox("🟣 Model:", options=model_opts,
                           format_func=lambda x: model_lbls[model_opts.index(x)],
                           index=idx, key="cl_model_sel")
        if sel != st.session_state.claude_model:
            st.session_state.claude_model = sel
            st.rerun()

    with cc2:
        st.markdown("**💰 Cost Savings:**")
        st.session_state.claude_enable_caching = st.checkbox(
            "🔄 Prompt Caching (90% cheaper on system prompt)", value=st.session_state.claude_enable_caching, key="cl_cache_toggle")
        st.session_state.claude_limit_history = st.checkbox(
            f"📜 Limit history to {st.session_state.claude_max_history} turns", value=st.session_state.claude_limit_history, key="cl_hist_toggle")
        st.session_state.claude_auto_haiku = st.checkbox(
            "⚡ Auto-Haiku for simple questions", value=st.session_state.claude_auto_haiku, key="cl_haiku_toggle")

    # ════════════════════════════════════════════════════════════════════
    # SMART PIPELINE FILTER (3-Tab — same as Gemini/ChatGPT)
    # ════════════════════════════════════════════════════════════════════

    all_pipelines = []
    pipeline_df = None
    for sn in ["PipelineAnalysis", "Pipelines"]:
        if sn in excel_data and isinstance(excel_data[sn], pd.DataFrame):
            if "Pipeline" in excel_data[sn].columns:
                pipeline_df = excel_data[sn]
                all_pipelines = sorted(pipeline_df["Pipeline"].dropna().unique().tolist())
                break
    if not all_pipelines and "Activities" in excel_data and isinstance(excel_data["Activities"], pd.DataFrame):
        if "Pipeline" in excel_data["Activities"].columns:
            all_pipelines = sorted(excel_data["Activities"]["Pipeline"].dropna().unique().tolist())

    cl_filter_count = len(st.session_state.get("claude_pipeline_filter", []))
    cl_filter_label = f"🎯 {cl_filter_count} selected" if cl_filter_count > 0 else "All"

    if all_pipelines:
        with st.expander(f"🔍 **Smart Pipeline Filter** — {len(all_pipelines)} pipelines ({cl_filter_label})", expanded=False):
            cl_ft1, cl_ft2, cl_ft3 = st.tabs(["📂 By Folder", "⚡ By Complexity", "📋 Manual Select"])

            # TAB 1: By Folder
            with cl_ft1:
                if pipeline_df is not None and "Folder" in pipeline_df.columns:
                    folders = sorted(pipeline_df["Folder"].fillna("(Root / No Folder)").unique().tolist())
                    st.caption(f"**{len(folders)}** folders. Click to select pipelines.")
                    fc1, fc2 = st.columns(2)
                    for fi, folder in enumerate(folders):
                        col = fc1 if fi % 2 == 0 else fc2
                        with col:
                            mask = pipeline_df["Folder"].fillna("(Root / No Folder)") == folder
                            folder_pls = pipeline_df.loc[mask, "Pipeline"].tolist()
                            if st.button(f"📁 {folder} ({len(folder_pls)})", key=f"cl_folder_{fi}", width="stretch"):
                                st.session_state.cl_pf_ms = folder_pls
                                st.session_state.claude_pipeline_filter = folder_pls
                                st.rerun()
                else:
                    st.info("No `Folder` column in PipelineAnalysis.")

            # TAB 2: By Complexity / Impact Level
            with cl_ft2:
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
                            if st.button(f"{icon} {lvl} ({len(lvl_pls)})", key=f"cl_cplx_{ci}", width="stretch"):
                                st.session_state.cl_pf_ms = lvl_pls
                                st.session_state.claude_pipeline_filter = lvl_pls
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
                                if st.button(f"{icon} {impact} ({len(imp_pls)})", key=f"cl_impact_{ii}", width="stretch"):
                                    st.session_state.cl_pf_ms = imp_pls
                                    st.session_state.claude_pipeline_filter = imp_pls
                                    st.rerun()
                else:
                    st.info("No `Complexity` column in PipelineAnalysis.")

            # TAB 3: Manual Select
            with cl_ft3:
                mc1, mc2 = st.columns(2)
                with mc1:
                    if st.button("📋 Select All", key="cl_pf_all", width="stretch"):
                        st.session_state.cl_pf_ms = all_pipelines
                        st.session_state.claude_pipeline_filter = all_pipelines
                        st.rerun()
                with mc2:
                    if st.button("🧹 Clear All", key="cl_pf_clr", width="stretch"):
                        st.session_state.cl_pf_ms = []
                        st.session_state.claude_pipeline_filter = []
                        st.rerun()
                if "cl_pf_ms" not in st.session_state:
                    st.session_state.cl_pf_ms = st.session_state.get("claude_pipeline_filter", [])
                def _pf_cb():
                    st.session_state.claude_pipeline_filter = st.session_state.cl_pf_ms
                st.multiselect("Pipelines:", all_pipelines, key="cl_pf_ms", placeholder="All Pipelines (no filter)", on_change=_pf_cb)
                st.session_state.claude_pipeline_filter = st.session_state.get("cl_pf_ms", [])

            # Filter Status Bar with token reduction estimate
            current_sel = st.session_state.get("claude_pipeline_filter", [])
            if current_sel:
                if "Activities" in excel_data and isinstance(excel_data["Activities"], pd.DataFrame) and "Pipeline" in excel_data["Activities"].columns:
                    total_act = len(excel_data["Activities"])
                    filtered_act = len(excel_data["Activities"][excel_data["Activities"]["Pipeline"].isin(current_sel)])
                    reduction_pct = int((1 - filtered_act / max(total_act, 1)) * 100)
                    st.success(f"🎯 **{len(current_sel)}** pipeline(s) • **{filtered_act}** activities (of {total_act}) • ~{reduction_pct}% token reduction")
                else:
                    st.info(f"🎯 **{len(current_sel)}** pipeline(s) selected.")
            else:
                st.caption("💡 No filter — Claude processes **all** pipelines via code execution.")

    # ── Secondary Entity Filters (DataFlow + Trigger) side-by-side ──
    all_dataflows, all_triggers = [], []
    if "DataFlows" in excel_data and isinstance(excel_data["DataFlows"], pd.DataFrame):
        for c in ["Name", "DataFlow", "DataFlowName"]:
            if c in excel_data["DataFlows"].columns:
                all_dataflows = sorted(excel_data["DataFlows"][c].dropna().unique().tolist())
                break
    if "Triggers" in excel_data and isinstance(excel_data["Triggers"], pd.DataFrame):
        for c in ["Name", "TriggerName", "Trigger"]:
            if c in excel_data["Triggers"].columns:
                all_triggers = sorted(excel_data["Triggers"][c].dropna().unique().tolist())
                break

    ef_c1, ef_c2 = st.columns(2)
    with ef_c1:
        if all_dataflows:
            if "cl_df_ms" not in st.session_state:
                st.session_state.cl_df_ms = st.session_state.get("claude_dataflow_filter", [])
            def _df_cb():
                st.session_state.claude_dataflow_filter = st.session_state.cl_df_ms
            st.multiselect(f"🔄 DataFlow Filter ({len(all_dataflows)})", all_dataflows,
                           key="cl_df_ms", placeholder="All DataFlows", on_change=_df_cb)
            st.session_state.claude_dataflow_filter = st.session_state.get("cl_df_ms", [])
    with ef_c2:
        if all_triggers:
            if "cl_tf_ms" not in st.session_state:
                st.session_state.cl_tf_ms = st.session_state.get("claude_trigger_filter", [])
            def _tf_cb():
                st.session_state.claude_trigger_filter = st.session_state.cl_tf_ms
            st.multiselect(f"🗓️ Trigger Filter ({len(all_triggers)})", all_triggers,
                           key="cl_tf_ms", placeholder="All Triggers", on_change=_tf_cb)
            st.session_state.claude_trigger_filter = st.session_state.get("cl_tf_ms", [])

    # Active filter badge
    apf = st.session_state.get("claude_pipeline_filter", [])
    adf = st.session_state.get("claude_dataflow_filter", [])
    atf = st.session_state.get("claude_trigger_filter", [])
    if apf or adf or atf:
        parts = []
        if apf: parts.append(f"{len(apf)} pipelines")
        if adf: parts.append(f"{len(adf)} dataflows")
        if atf: parts.append(f"{len(atf)} triggers")
        st.markdown(f'<div class="cl-filter-badge">🎯 Filters: {" • ".join(parts)}</div>', unsafe_allow_html=True)

    # ════════════════════════════════════════════════════════════════════
    # PRESETS — Analysis + Charts
    # ════════════════════════════════════════════════════════════════════

    no_hist = len(st.session_state.claude_chat_history) == 0
    with st.expander("🎯 **Analysis Presets** (same as Gemini/ChatGPT)", expanded=no_hist):
        c1, c2 = st.columns(2)
        for i, (lbl, q) in enumerate(CLAUDE_PRESET_QUESTIONS[:10]):
            col = c1 if i % 2 == 0 else c2
            with col:
                if st.button(lbl, key=f"cl_p_{i}", width="stretch"):
                    fq = q
                    ft = _build_filter_text()
                    if ft: fq += f"\n\n{ft}"
                    st.session_state.claude_pending_question = fq
                    st.session_state._claude_prompt_type = "preset"
                    st.rerun()

    with st.expander("📊 **Chart Presets** (Claude code execution only)", expanded=False):
        st.caption("🎨 These generate REAL charts — only possible with Claude's code sandbox.")
        c1, c2 = st.columns(2)
        for i, (lbl, q) in enumerate(CLAUDE_PRESET_QUESTIONS[10:]):
            col = c1 if i % 2 == 0 else c2
            with col:
                if st.button(lbl, key=f"cl_c_{i}", width="stretch"):
                    fq = q
                    ft = _build_filter_text()
                    if ft: fq += f"\n\n{ft}"
                    st.session_state.claude_pending_question = fq
                    st.session_state._claude_prompt_type = "preset"
                    st.rerun()

    # ── Custom Question ──
    with st.expander("✏️ **Custom Question**", expanded=False):
        cq = st.text_area("Ask anything:", placeholder="e.g. Show a chart of activity types per pipeline",
                          height=100, key="cl_custom")
        if st.button("🚀 Ask Claude", key="cl_send", type="primary", width="stretch"):
            if cq.strip():
                fq = cq.strip()
                ft = _build_filter_text()
                if ft: fq += f"\n\n{ft}"
                st.session_state.claude_pending_question = fq
                st.session_state._claude_prompt_type = "custom"
                st.rerun()

    # ── Chat Controls ──
    bc1, bc2 = st.columns(2)
    with bc1:
        if st.button("🗑️ New Chat", key="cl_clear", width="stretch"):
            st.session_state.claude_chat_history = []
            st.session_state.claude_api_messages = []
            st.session_state.claude_container_id = None
            st.rerun()
    with bc2:
        if st.button("🔄 Re-upload File", key="cl_reupload", width="stretch"):
            st.session_state.claude_file_id = None
            st.session_state.claude_file_name = None
            st.session_state.claude_container_id = None
            st.rerun()

    st.markdown("---")

    # ── Model Selection Guide ──
    with st.expander("🧠 **Model Guide: How to use Claude effectively?**", expanded=False):
        st.markdown("""
        Claude uses a unique **Code Execution Container** to analyze your Excel files securely. Follow these best practices:

        | 🎯 Your Goal | 🏆 Recommended Model | ⏱️ Why? |
        |---|---|---|
        | **Basic Excel Analysis / Counts** | **Claude 3.5 Haiku** | Blazing fast and incredibly cheap. Can write quick Python scripts to count rows or find missing values. |
        | **Deep Architectural Review** | **Claude 3.7 / 4.6 Sonnet** | The undisputed king of massive multi-file coding and reasoning tasks. |
        | **Complex Math / Financial Logic** | **Claude 3 Opus** | Highly articulate and strong at understanding nuanced business logic. |

        💡 **Pro-Tip on Extended Thinking (The 90% Context Hack):** 
        We automatically pin your Excel file to Anthropic's **Prompt Cache** (saving you 90% on input tokens). Want the best possible analysis? Check the **"💭 Enable Extended Thinking"** box in the sidebar. Claude will use those savings to "think out loud" inside a scratchpad before executing any Python code, virtually eliminating hallucinations!
        """)

    # ── Chat History ──
    for i, entry in enumerate(st.session_state.claude_chat_history):
        if entry["role"] == "user":
            with st.chat_message("user"):
                d = entry["content"]
                if len(d) > 300:
                    d = d[:200] + "\n\n*... (full prompt sent)*"
                st.markdown(d)
        else:
            with st.chat_message("assistant", avatar="🟣"):
                c = entry["content"]
                dl_text = ""
                if isinstance(c, dict):
                    _render_response(c, client)
                    dl_text = c.get("text", "")
                else:
                    st.markdown(c)
                    dl_text = str(c)
                
                if dl_text:
                    st.download_button(
                        label="⬇️ Download Response",
                        data=dl_text,
                        file_name=f"claude_response_{i}.md",
                        mime="text/markdown",
                        key=f"dl_claude_msg_{i}"
                    )

    # Empty state
    if not st.session_state.claude_chat_history and not st.session_state.claude_pending_question:
        st.markdown("""
        <div class="cl-empty">
            <div class="icon">🟣</div>
            <h3>Claude AI Ready</h3>
            <p>Select a preset or write a custom question. Claude will execute Python code to analyze data, create charts, and generate files.</p>
        </div>
        """, unsafe_allow_html=True)

    # ════════════════════════════════════════════════════════════════════
    # PROCESS PENDING QUESTION
    # ════════════════════════════════════════════════════════════════════

    active_q = st.session_state.pop("claude_pending_question", None)
    if not active_q:
        return

    with st.chat_message("user"):
        dq = active_q if len(active_q) <= 300 else active_q[:200] + "\n\n*... (full prompt)*"
        st.markdown(dq)

    # Determine model — auto-route to Haiku for simple questions
    use_model = st.session_state.claude_model
    auto_routed = False
    if st.session_state.claude_auto_haiku and _is_simple_question(active_q):
        use_model = HAIKU_MODEL
        auto_routed = True

    use_cfg = CLAUDE_MODELS.get(use_model, CLAUDE_MODELS[DEFAULT_CLAUDE_MODEL])

    # Build user content with file ref + cache_control
    user_content = [{"type": "text", "text": active_q}]
    file_ref = {"type": "container_upload", "file_id": file_id}
    if st.session_state.claude_enable_caching:
        file_ref["cache_control"] = {"type": "ephemeral"}
    user_content.append(file_ref)

    # History trimming
    api_messages = list(st.session_state.claude_api_messages)
    if st.session_state.claude_limit_history:
        api_messages = _trim_history(api_messages, st.session_state.claude_max_history)
    api_messages.append({"role": "user", "content": user_content})

    # System blocks with caching
    system_blocks = build_system_blocks(excel_data, enable_caching=st.session_state.claude_enable_caching)

    # Send
    with st.chat_message("assistant", avatar="🟣"):
        
        # Streaming Setup
        think_expander = None
        think_placeholder = None
        text_placeholder = st.empty()
        
        thinking_enabled = st.session_state.get("claude_enable_thinking", False)
        thinking_budget = 4096 if thinking_enabled else 0
        
        if thinking_enabled:
            think_expander = st.expander("💭 Claude's Thoughts", expanded=True)
            think_placeholder = think_expander.empty()

        think_text = ""
        final_text = ""
        
        def stream_ui_cb(block_type, content):
            nonlocal think_text, final_text
            if block_type == "thinking" and think_placeholder:
                think_text += content
                think_placeholder.markdown(think_text + "▌")
            elif block_type == "text":
                final_text += content
                text_placeholder.markdown(final_text + "▌")

        with st.status("🟣 Claude analyzing...", expanded=not thinking_enabled) as status:
            st.write(f"🧠 Model: {use_cfg.display_name}" + (" *(auto-routed for simple Q)*" if auto_routed else ""))
            st.write("⚙️ Code execution enabled...")
            if st.session_state.claude_enable_caching:
                st.write("🔄 Prompt caching active — cached tokens cost 90% less")
            if thinking_enabled:
                st.write("💭 Extended Thinking stream active.")

            api_client = ClaudeClient(model=use_model)
            result = api_client.send_message(
                messages=api_messages,
                system_blocks=system_blocks,
                max_tokens=use_cfg.max_output_tokens,
                thinking_budget=thinking_budget,
                stream_callback=stream_ui_cb
            )

            if "error" in result:
                status.update(label="❌ Error", state="error")

                st.error(result["error"])
                resp = result["error"]
            else:
                cc = len(result.get("code_blocks", []))
                fc = len(result.get("generated_files", []))
                lbl = f"✅ Done ({cc} code runs"
                if fc: lbl += f", {fc} files"
                lbl += ")"
                status.update(label=lbl, state="complete")

        # Clean up streaming cursors
        if think_placeholder and think_text:
            think_placeholder.markdown(think_text)
            
        if "error" not in result:
            if text_placeholder: 
                text_placeholder.empty()
            _render_response(result, api_client)
            resp = result

            # Cost calculation
            in_tok = result.get("input_tokens", 0)
            out_tok = result.get("output_tokens", 0)
            cache_create = result.get("cache_creation_input_tokens", 0)
            cache_read = result.get("cache_read_input_tokens", 0)

            # Actual cost with caching breakdown
            regular_in = in_tok - cache_create - cache_read
            cost_regular = max(0, regular_in) / 1_000_000 * use_cfg.input_per_m
            cost_cache_create = cache_create / 1_000_000 * use_cfg.input_per_m * 1.25  # 1.25x for cache writes
            cost_cache_read = cache_read / 1_000_000 * use_cfg.cached_per_m  # 0.1x
            cost_output = out_tok / 1_000_000 * use_cfg.output_per_m
            actual_cost = cost_regular + cost_cache_create + cost_cache_read + cost_output

            # What it would have cost without caching
            no_cache_cost = in_tok / 1_000_000 * use_cfg.input_per_m + cost_output
            savings = max(0, no_cache_cost - actual_cost)

            # Update session totals
            st.session_state.claude_session_cost += actual_cost
            st.session_state.claude_session_calls += 1
            st.session_state.claude_session_cached_tokens += cache_read

            # Display cost bar
            cost_text = f"🟣 {use_cfg.display_name} • 📥 {in_tok:,} in • 📤 {out_tok:,} out • 💲 ${actual_cost:.4f}"
            if cache_read > 0:
                cost_text += f' • <span class="cl-savings">💾 {cache_read:,} cached (saved ${savings:.4f})</span>'
            if auto_routed:
                cost_text += " • ⚡ Auto-Haiku"
            st.markdown(f'<div class="cl-cost-bar">{cost_text}</div>', unsafe_allow_html=True)

            # Log to cost engine
            try:
                from api_cost_engine import log_api_usage
                log_api_usage(
                    provider="Anthropic",
                    model_id=use_model,
                    input_tokens=in_tok,
                    output_tokens=out_tok,
                    cached_tokens=cache_read,
                    prompt_type=st.session_state.get("_claude_prompt_type", "custom"),
                    question=active_q[:80],
                )
            except Exception:
                pass

    # Save history
    st.session_state.claude_chat_history.append({"role": "user", "content": active_q})
    st.session_state.claude_chat_history.append({"role": "assistant", "content": resp})

    st.session_state.claude_api_messages.append({"role": "user", "content": user_content})
    if isinstance(resp, dict):
        st.session_state.claude_api_messages.append({"role": "assistant", "content": resp.get("text", "")})
    else:
        st.session_state.claude_api_messages.append({"role": "assistant", "content": resp})

    try:
        from ai_excel_chat import ChatPersistenceManager
        pm = ChatPersistenceManager()
        pm.save_chat(
            chat_id=st.session_state.claude_chat_id,
            chat_history=st.session_state.claude_chat_history,
            api_history=st.session_state.claude_api_messages,
            tokens=int(st.session_state.get("claude_session_cost", 0) * 1_000_000),
            model=use_model
        )
    except Exception as e:
        print(f"Failed to persist Claude chat: {e}")
        atxt = resp.get("text", "")
        for s in resp.get("stdout_blocks", []):
            atxt += f"\n\nCode output:\n{s}"
        st.session_state.claude_api_messages.append({"role": "assistant", "content": atxt})
    else:
        st.session_state.claude_api_messages.append({"role": "assistant", "content": str(resp)})

    if isinstance(resp, dict) and resp.get("container_id"):
        st.session_state.claude_container_id = resp["container_id"]

    st.rerun()
