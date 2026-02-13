"""
AI-Powered Excel Chat for ADF Analyzer Dashboard
=================================================
Reads the COMPLETE processed Excel output and enables natural language
querying using Gemini API with smart context management.

DESIGN PRINCIPLES:
• Send ALL data to AI — no truncation, no missing rows
• Anti-hallucination: strict rules + data validation prompts
• Smart context: always send full Tier 1 + keyword-matched Tier 2
• Activities sheet handled separately (3M chars) — filtered or summarized
• Context invalidation on new data load
• Premium UI matching app's glassmorphism theme
"""

# Standard Imports
import os
import re
import time
import json
import hashlib
import requests
import pandas as pd
import streamlit as st
from pathlib import Path
from typing import Optional, Dict, List, Any, Tuple
from dataclasses import dataclass
import traceback # Added for error logging

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ════════════════════════════════════════════════════════════════════════
# MODEL CONFIGURATION
# ════════════════════════════════════════════════════════════════════════

@dataclass
class ModelConfig:
    name: str
    display_name: str
    max_output_tokens: int
    context_window: int
    best_for: str
    icon: str

AVAILABLE_MODELS = {
    "gemini-2.5-pro": ModelConfig(
        "gemini-2.5-pro", "Gemini 2.5 Pro",
        65536, 2_000_000, "Best accuracy, deep analysis", "🧠"
    ),
    "gemini-2.5-flash": ModelConfig(
        "gemini-2.5-flash", "Gemini 2.5 Flash",
        65536, 1_000_000, "Fast & accurate", "⚡"
    ),
    "gemini-2.0-flash": ModelConfig(
        "gemini-2.0-flash", "Gemini 2.0 Flash",
        8192, 1_000_000, "Stable & quick", "💨"
    ),
    "gemini-flash-latest": ModelConfig(
        "gemini-flash-latest", "Gemini 1.5 Flash",
        8192, 1_000_000, "Legacy Flash", "📜"
    ),
}

DEFAULT_MODEL = "gemini-2.0-flash"


# ════════════════════════════════════════════════════════════════════════
# API KEY MANAGER
# ════════════════════════════════════════════════════════════════════════

class APIKeyManager:
    """Manages multiple Gemini API keys with automatic rotation."""

    def __init__(self):
        self.keys: List[str] = []
        self._load_keys()
        
        # Initialize session state for persistence
        if 'ai_key_index' not in st.session_state:
            st.session_state.ai_key_index = 0
        if 'ai_failed_keys' not in st.session_state:
            st.session_state.ai_failed_keys = set()
            
    @property
    def current_index(self) -> int:
        return st.session_state.ai_key_index
        
    @current_index.setter
    def current_index(self, value: int):
        st.session_state.ai_key_index = value
        
    @property
    def failed_keys(self) -> set:
        return st.session_state.ai_failed_keys

    def _load_keys(self):
        """Load API keys from .env and session state."""
        # Prioritize common environment variable names
        for var in ["GOOGLE_API_KEY", "GEMINI_API_KEY", "GOOGLE_AI_STUDIO_KEY"]:
            key = os.getenv(var, "").strip()
            if key and key not in self.keys:
                # Only add if seemingly valid (simple check)
                if len(key) > 10:
                    self.keys.append(key)

        # User-entered key gets priority
        try:
            user_key = st.session_state.get('user_gemini_api_key', '').strip()
            if user_key and user_key not in self.keys:
                self.keys.insert(0, user_key)
        except Exception:
            pass  # Running outside Streamlit

    def get_key(self) -> Optional[str]:
        if not self.keys:
            return None
            
        # Try finding a non-failed key starting from current index
        attempts = 0
        start_index = self.current_index
        
        while attempts < len(self.keys):
            idx = (start_index + attempts) % len(self.keys)
            key = self.keys[idx]
            
            if key not in self.failed_keys:
                if idx != self.current_index:
                    self.current_index = idx  # Update persistent index
                return key
                
            attempts += 1
            
        # All keys failed? clear failures and try current
        self.failed_keys.clear()
        # If still no keys, return None, otherwise return the first key (which might fail again)
        return self.keys[0] if self.keys else None

    def mark_failed(self, key: str):
        self.failed_keys.add(key)
        self.rotate()

    def rotate(self):
        if self.keys:
            self.current_index = (self.current_index + 1) % len(self.keys)

    @property
    def has_keys(self) -> bool:
        return len(self.keys) > 0

    @property
    def key_count(self) -> int:
        return len(self.keys)


# ════════════════════════════════════════════════════════════════════════
# GEMINI API CLIENT
# ════════════════════════════════════════════════════════════════════════

class GeminiClient:
    """REST API client for Gemini with retry, key rotation, and streaming."""

    BASE_URL = "https://generativelanguage.googleapis.com/v1beta"

    def __init__(self, model: str = DEFAULT_MODEL):
        self.key_manager = APIKeyManager()
        self.model = model

    def call_api(
        self,
        prompt: str,
        system_instruction: str = None,
        conversation_history: list = None,
        temperature: float = 0.1,
        max_retries: int = 5,
    ) -> Optional[str]:
        """Call Gemini API with automatic key rotation and retry."""

        for attempt in range(max_retries):
            api_key = self.key_manager.get_key()
            if not api_key:
                return "❌ No API key available. Please add a Gemini API key."

            url = f"{self.BASE_URL}/models/{self.model}:generateContent?key={api_key}"

            # Build contents
            contents = []
            if conversation_history:
                contents.extend(conversation_history)
            contents.append({"role": "user", "parts": [{"text": prompt}]})

            model_cfg = AVAILABLE_MODELS.get(self.model)
            max_tokens = model_cfg.max_output_tokens if model_cfg else 8192

            payload = {
                "contents": contents,
                "generationConfig": {
                    "temperature": temperature,
                    "maxOutputTokens": max_tokens,
                    "topP": 0.95,
                },
            }

            if system_instruction:
                payload["systemInstruction"] = {
                    "parts": [{"text": system_instruction}]
                }

            try:
                response = requests.post(
                    url,
                    headers={"Content-Type": "application/json"},
                    json=payload,
                    timeout=180,
                )

                if response.status_code == 200:
                    try:
                        result = response.json()
                        candidates = result.get("candidates", [])
                        if not candidates:
                            return "❌ AI returned empty response. Try rephrasing your question."
                        text = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                        if not text:
                            finish_reason = candidates[0].get("finishReason", "UNKNOWN")
                            return f"❌ AI response was blocked (reason: {finish_reason}). Try a different question."
                        return text.strip()
                    except (json.JSONDecodeError, KeyError, IndexError) as e:
                        return f"❌ Failed to parse AI response: {str(e)[:200]}"

                elif response.status_code == 429:
                    self.key_manager.rotate()
                    wait = min(2 ** attempt, 16)
                    time.sleep(wait)
                    continue

                elif response.status_code == 400:
                    try:
                        error_msg = response.json().get("error", {}).get("message", "")
                    except Exception:
                        error_msg = response.text[:300]
                    if "API key" in error_msg or "API_KEY" in error_msg:
                        self.key_manager.mark_failed(api_key)
                        continue
                    elif "too large" in error_msg.lower() or "token" in error_msg.lower():
                        return "❌ Context too large for this model. Try switching to Gemini 1.5 Pro (2M context) or ask a more specific question."
                    else:
                        return f"❌ API Error: {error_msg[:400]}"

                elif response.status_code == 503:
                    # Google Server Overload - Suggest retry or model switch
                    wait = min(2 ** attempt, 8)
                    time.sleep(wait)
                    if attempt == max_retries - 1:
                        return "⚠️ **Google AI System Overload (503)**\n\nThe AI model is currently experiencing high traffic. Please:\n1. Wait a moment and try again.\n2. Switch to **Gemini 2.0 Flash** (more stable).\n3. Reduce the complexity of your question."
                    continue

                elif response.status_code == 403:
                    self.key_manager.mark_failed(api_key)
                    continue

                else:
                    return f"❌ HTTP {response.status_code}: {response.text[:300]}"

            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    continue
                return "❌ Request timed out. Try a simpler question or switch to Gemini 2.0 Flash."
            except requests.exceptions.ConnectionError:
                return "❌ Network error. Please check your internet connection."
            except Exception as e:
                return f"❌ Unexpected error: {str(e)[:300]}"

        return "❌ All API keys exhausted or rate limited. Please wait a moment and try again."


# ════════════════════════════════════════════════════════════════════════
# EXCEL CONTEXT BUILDER — COMPLETE Data Reading (No Truncation)
# ════════════════════════════════════════════════════════════════════════

# Tier 1: ALWAYS sent to AI — these are the core analysis sheets
TIER1_SHEETS = [
    "Summary", "Statistics", "PipelineAnalysis", "DataFlows",
    "DataFlowLineage", "DataLineage", "ImpactAnalysis",
    "Pipelines",  # FIX: Always include Pipelines for complete picture
]

# Tier 2: Sent when keyword-matched to the user's question
TIER2_KEYWORDS = {
    "dataset": ["Datasets", "DatasetUsage"],
    "linked service": ["LinkedServices", "LinkedServiceUsage"],
    "connection": ["LinkedServices", "LinkedServiceUsage"],
    "trigger": ["Triggers", "TriggerDetails"],
    "schedule": ["Triggers", "TriggerDetails"],
    "orphan": [
        "OrphanedPipelines", "OrphanedDataFlows",
        "OrphanedDatasets", "OrphanedLinkedServices", "OrphanedTriggers",
    ],
    "unused": [
        "OrphanedPipelines", "OrphanedDataFlows",
        "OrphanedDatasets", "OrphanedLinkedServices", "OrphanedTriggers",
    ],
    "parameter": ["GlobalParameters", "GlobalParameterUsage"],
    "global": ["GlobalParameters", "GlobalParameterUsage"],
    "integration runtime": ["IntegrationRuntimes", "IntegrationRuntimeUsage"],
    "self-hosted": ["IntegrationRuntimes", "IntegrationRuntimeUsage"],
    "transformation": ["DataFlowTransformations", "TransformationUsage"],
    "execution order": ["ActivityExecutionOrder"],
    "dependency": ["ActivityExecutionOrder", "ImpactAnalysis"],
    "activity count": ["ActivityCount"],
    "credential": ["Credentials"],
    "vnet": ["ManagedVNets", "ManagedPrivateEndpoints"],
    "private endpoint": ["ManagedVNets", "ManagedPrivateEndpoints"],
    "error": ["Errors"],
    "health": [
        "OrphanedPipelines", "OrphanedDataFlows", "OrphanedDatasets",
        "OrphanedLinkedServices", "OrphanedTriggers", "Errors",
        "Triggers", "TriggerDetails",
    ],
    "all data": [  # Send EVERYTHING when asked
        "Datasets", "DatasetUsage", "LinkedServices", "LinkedServiceUsage",
        "Triggers", "TriggerDetails", "OrphanedPipelines", "OrphanedDataFlows",
        "OrphanedDatasets", "OrphanedLinkedServices", "OrphanedTriggers",
        "GlobalParameters", "GlobalParameterUsage", "IntegrationRuntimes",
        "ActivityCount", "Credentials", "Errors",
    ],
}

# Keywords that trigger the Activities sheet (the 3M char giant)
ACTIVITIES_KEYWORDS = [
    "activity", "activities", "copy data", "source table", "sink table",
    "stored procedure", "sql", "lookup", "web activity", "foreach",
    "execute pipeline", "set variable", "get metadata", "if condition",
    "switch", "wait", "webhook", "script",
]


class ExcelContextBuilder:
    """
    Builds COMPLETE context from ADF analysis Excel for AI consumption.

    Key design: Send ALL data in Tier 1 sheets with NO truncation.
    Only the Activities sheet (3M chars) is filtered/summarized.
    """

    def __init__(self, excel_path: str = None, excel_data: Dict[str, pd.DataFrame] = None):
        self.sheets: Dict[str, pd.DataFrame] = {}
        self.sheet_info: Dict[str, Dict] = {}
        self._data_hash: str = ""

        if excel_data:
            self.sheets = {k: v for k, v in excel_data.items() if isinstance(v, pd.DataFrame)}
        elif excel_path and Path(excel_path).exists():
            self._load_excel(excel_path)

        self._build_sheet_index()
        self._compute_hash()

    def _load_excel(self, path: str):
        """Load all sheets from Excel file."""
        xf = pd.ExcelFile(path)
        for sheet_name in xf.sheet_names:
            try:
                self.sheets[sheet_name] = pd.read_excel(xf, sheet_name)
            except Exception:
                continue

    def _build_sheet_index(self):
        """Build metadata index for all sheets to aid context pruning."""
        self.sheet_info = {}
        for name, df in list(self.sheets.items()): # Iterate over a copy to allow deletion
            try:
                csv_len = len(df.to_csv(index=False)) if len(df) > 0 else 0
                self.sheet_info[name] = {
                    "rows": len(df),
                    "cols": len(df.columns),
                    "columns": list(df.columns),
                    "chars": csv_len,
                    "est_tokens": int(csv_len / 3.2),  # Improved estimation
                }
            except Exception as e:
                st.warning(f"Could not process sheet '{name}': {e}. Skipping this sheet.")
                traceback.print_exc()
                # Remove corrupted or un-serializable sheets
                if name in self.sheets:
                    del self.sheets[name]

    def _compute_hash(self):
        """Compute hash to detect data changes for cache invalidation."""
        keys = sorted(self.sheets.keys())
        lengths = [len(self.sheets[k]) for k in keys]
        self._data_hash = hashlib.md5(
            f"{keys}:{lengths}".encode()
        ).hexdigest()[:12]

    @property
    def data_hash(self) -> str:
        return self._data_hash

    def _df_to_csv(self, df: pd.DataFrame, max_rows: int = None) -> str:
        """Convert DataFrame to CSV string."""
        if df.empty:
            return "(empty — 0 rows)"
        if max_rows and len(df) > max_rows:
            csv = df.head(max_rows).to_csv(index=False)
            return csv + f"\n... ({len(df) - max_rows} more rows not shown)"
        return df.to_csv(index=False)

    def build_system_context(self) -> str:
        """Build the system prompt with sheet catalog and anti-hallucination rules."""

        # Factory stats from Summary and Statistics sheets
        stats_lines = []
        if "Statistics" in self.sheets:
            for _, row in self.sheets["Statistics"].iterrows():
                cat = str(row.get("Category", ""))
                typ = str(row.get("Type", ""))
                cnt = str(row.get("Count", ""))
                if cat and typ and cnt:
                    stats_lines.append(f"  • {cat} > {typ}: {cnt}")

        if "Summary" in self.sheets and not self.sheets["Summary"].empty:
            for _, row in self.sheets["Summary"].iterrows():
                metric = str(row.get("Metric", row.get(self.sheets["Summary"].columns[0], "")))
                value = str(row.get("Value", row.get(self.sheets["Summary"].columns[1], "")))
                if metric and value and metric != "nan":
                    stats_lines.append(f"  • {metric}: {value}")

        # Sheet catalog (ALL sheets with their columns)
        sheet_catalog = []
        for name, info in sorted(self.sheet_info.items()):
            all_cols = ", ".join(info["columns"])
            sheet_catalog.append(
                f"  📋 {name}: {info['rows']} rows × {info['cols']} cols\n"
                f"     Columns: [{all_cols}]"
            )

        return f"""You are an expert Azure Data Factory (ADF) Analyst AI Assistant.
You have access to the COMPLETE output of an an ADF ARM Template Analyzer tool.

════════════════════════════════════════════════════════════
HOW THIS DATA WAS GENERATED (Core Analyzer Knowledge):
════════════════════════════════════════════════════════════
The data you see comes from the "UltimateEnterpriseADFAnalyzer v10.0" — a Python tool
that parses Azure Data Factory ARM Template JSON files (the exported factory definition).

ARM Template Structure:
 • The JSON has a "resources" array where each resource has:
   - type: "Microsoft.DataFactory/factories/pipelines|dataflows|datasets|linkedServices|triggers|integrationRuntimes"
   - name: Resource name (may contain ARM expressions like [concat(parameters('factoryName'), '/PL_Name')])
   - properties: Contains the resource configuration (activities, typeProperties, etc.)
 • Pipelines contain an "activities" array — each activity has type, typeProperties, dependsOn
 • Activities can be nested (ForEach, IfCondition, Switch contain inner activities)
 • DataFlows have "sources", "sinks", and "transformations" with optional script blocks

How the Analyzer Creates Each Sheet:
 • Summary/Statistics: Aggregated counts from all parsed resources
 • Pipelines: Basic pipeline info (name, folder, description, parameters, annotations)
 • PipelineAnalysis: DEEP analysis — complexity scoring, activity types, SQL detection,
   source systems, target systems, blast radius, impact level
 • Activities: EVERY activity from ALL pipelines, including nested ones (ForEach/If/Switch children)
   Columns include: Pipeline, Activity, ActivityType, Sequence, Depth, Parent, SourceTable, SinkTable,
   SourceLinkedService, SinkLinkedService, DependsOn, SQL queries, and more
 • DataLineage: Source→Sink mapping for every Copy/DataFlow/StoredProcedure activity
 • DataFlows: Parsed mapping dataflows with sources, sinks, transformations, runtime tables
 • DataFlowLineage: Source→Sink at the dataflow-transform level
 • ImpactAnalysis: Blast radius calculation — how many resources are affected if one fails
   (upstream dependencies, downstream dependencies, connected triggers/datasets)
 • Datasets: All dataset definitions with linked services, table/file names, parameters
 • LinkedServices: All connection definitions (SQL, ADLS, SFTP, REST, Oracle, etc.)
 • Triggers: Schedule/tumbling/event triggers with their pipeline associations
 • Orphaned*: Resources that exist but are never referenced by any pipeline or trigger
 • ActivityExecutionOrder: Topologically sorted execution sequence considering dependsOn
 • GlobalParameters: Factory-level parameters and where they are used

Key Parsing Features:
 • SQL Parser: Extracts source/sink tables from Stored Procedure, Lookup, Script activities
   (handles CTEs, MERGE statements, nested subqueries, escaped quotes)
 • DataFlow Script Parser: Extracts table names from inline dataflow script definitions
 • Parameter Resolution: ARM template parameters (e.g., [parameters('factoryName')]) are resolved
   to their defaultValue for cleaner display
 • Complexity Scoring: Each pipeline is scored based on: activity count, nesting depth,
   number of activity types, SQL usage, dataflow usage, external calls

Cross-Reference Patterns:
 • Pipeline names appear in: Pipelines, PipelineAnalysis, Activities, DataLineage, ImpactAnalysis,
   ActivityExecutionOrder, Triggers/TriggerDetails
 • Linked service names appear in: LinkedServices, Datasets, Activities (Source/SinkLinkedService),
   DataFlows, DataFlowLineage, LinkedServiceUsage
 • Dataset names appear in: Datasets, DatasetUsage, Activities
 • DataFlow names appear in: DataFlows, DataFlowLineage, Activities (ExecuteDataFlow type)
 • Table names appear in: Activities (SourceTable/SinkTable), DataLineage, DataFlowLineage

════════════════════════════════════════════════════════════
ANTI-HALLUCINATION RULES (STRICTLY ENFORCED):
════════════════════════════════════════════════════════════
1. ONLY report information that EXISTS in the provided data sheets below
2. Do NOT infer, assume, or generate data that isn't explicitly in the sheets
3. If information is not available, say: "This data is not available in the analysis output"
4. Use EXACT names from the data (pipeline names, dataflow names, table names)
5. When counting items, count from the actual data — do not estimate
6. When listing items, list ALL of them — do not truncate with "and more..."
7. If a question cannot be answered from the available sheets, explain which
   sheet might contain the answer and what information is missing
8. Cross-reference between sheets when answering — e.g., match pipeline names
   across PipelineAnalysis, DataLineage, ImpactAnalysis, and Activities

════════════════════════════════════════════════════════════
DATA SHEET CATALOG ({len(self.sheets)} sheets, {self.total_chars:,} total characters):
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
• Use tables for structured comparisons
• Include sheet references: "From [SheetName] sheet..."
• When answering about data lineage, show the complete path: LinkedService → Dataset → Pipeline → Activity → SourceTable → SinkTable
• When analyzing complexity, reference the specific ComplexityScore and factors from PipelineAnalysis
• End with a brief "Data Sources" section listing which sheets were used
"""

    def build_tier1_context(self) -> str:
        """Build COMPLETE context from critical sheets — NO truncation."""
        context_parts = []

        for sheet_name in TIER1_SHEETS:
            if sheet_name not in self.sheets:
                continue
            df = self.sheets[sheet_name]
            if df.empty:
                continue

            # ✅ FIX: Send ALL data — no row limits, no column filtering
            # Only exception: print a size indicator
            csv_data = df.to_csv(index=False)
            est_tokens = len(csv_data) // 4
            context_parts.append(
                f"\n### 📋 Sheet: {sheet_name} "
                f"({len(df)} rows × {len(df.columns)} cols, ~{est_tokens:,} tokens)\n"
                f"{csv_data}"
            )

        return "\n".join(context_parts)

    def build_tier2_context(self, question: str) -> str:
        """Build keyword-matched context from supplementary sheets."""
        q_lower = question.lower()
        matched_sheets = set()

        # Keyword matching
        for keyword, sheet_names in TIER2_KEYWORDS.items():
            if keyword in q_lower:
                matched_sheets.update(sheet_names)

        # Direct sheet name matching
        for sheet_name in self.sheets:
            if sheet_name.lower() in q_lower:
                matched_sheets.add(sheet_name)

        # Remove Tier 1 sheets (already included)
        matched_sheets -= set(TIER1_SHEETS)

        if not matched_sheets:
            return ""

        context_parts = []
        for sheet_name in sorted(matched_sheets):
            if sheet_name not in self.sheets:
                continue
            df = self.sheets[sheet_name]
            if df.empty:
                continue
            csv_data = df.to_csv(index=False)
            est_tokens = len(csv_data) // 4
            context_parts.append(
                f"\n### 📋 Sheet: {sheet_name} "
                f"({len(df)} rows × {len(df.columns)} cols, ~{est_tokens:,} tokens)\n"
                f"{csv_data}"
            )

        return "\n".join(context_parts)

    def build_tier3_context(self, question: str) -> str:
        """Build Activities context — filtered by pipeline or summarized."""
        q_lower = question.lower()

        needs_activities = any(kw in q_lower for kw in ACTIVITIES_KEYWORDS)
        if not needs_activities:
            return ""

        if "Activities" not in self.sheets:
            return ""

        activities_df = self.sheets["Activities"]
        if activities_df.empty:
            return ""

        # Try to find specific pipeline names mentioned in the question
        pipeline_names = []
        if "PipelineAnalysis" in self.sheets and "Pipeline" in self.sheets["PipelineAnalysis"].columns:
            pipeline_names = self.sheets["PipelineAnalysis"]["Pipeline"].dropna().tolist()
        elif "Pipelines" in self.sheets and "Pipeline" in self.sheets["Pipelines"].columns:
            pipeline_names = self.sheets["Pipelines"]["Pipeline"].dropna().tolist()

        # Match pipelines mentioned in the question
        mentioned = []
        for pname in pipeline_names:
            pname_str = str(pname)
            if pname_str.lower() in q_lower:
                mentioned.append(pname_str)

        if mentioned and "Pipeline" in activities_df.columns:
            # Send FULL activities for mentioned pipelines
            filtered = activities_df[activities_df["Pipeline"].isin(mentioned)]
            if not filtered.empty:
                csv_data = filtered.to_csv(index=False)
                return (
                    f"\n### 📋 Sheet: Activities "
                    f"(filtered for: {', '.join(mentioned)}, "
                    f"{len(filtered)} of {len(activities_df)} rows)\n{csv_data}"
                )

        # General activity question → send per-type summary + per-pipeline counts
        parts = []

        # Activity type breakdown
        if "ActivityType" in activities_df.columns:
            agg_dict = {"Count": ("Activity", "count")}
            if "Pipeline" in activities_df.columns:
                agg_dict["Pipelines"] = ("Pipeline", "nunique")
            else:
                agg_dict["Pipelines"] = ("Activity", "count") # Fallback to activity count if pipeline missing
                
            type_summary = (
                activities_df.groupby("ActivityType")
                .agg(**agg_dict)
                .reset_index()
                .sort_values("Count", ascending=False)
            )
            parts.append("### Activity Type Distribution")
            parts.append(type_summary.to_csv(index=False))

        # Top pipelines by activity count
        if "Pipeline" in activities_df.columns:
            pipeline_counts = activities_df.groupby("Pipeline").size().reset_index(name="ActivityCount")
            pipeline_counts = pipeline_counts.sort_values("ActivityCount", ascending=False).head(30)
            parts.append(
                f"### Top 30 Pipelines by Activity Count\n"
                f"{pipeline_counts.to_csv(index=False)}"
            )

        # Activity key columns sample (not full data)
        key_cols = ["Pipeline", "Activity", "ActivityType", "SourceTable", "SinkTable",
                     "SourceLinkedService", "SinkLinkedService", "DependsOn"]
        avail_cols = [c for c in key_cols if c in activities_df.columns]
        if avail_cols:
            sample = activities_df[avail_cols].head(100)
            parts.append(
                f"### Activities Detail Sample (first 100 of {len(activities_df)} rows, key columns)\n"
                f"{sample.to_csv(index=False)}"
            )

        return "\n".join(parts)

    def get_context_for_question(self, question: str, model: str = DEFAULT_MODEL) -> Tuple[str, int, List[str]]:
        """
        Intelligently select and build context based on query relevance.
        Returns: (context_text, est_tokens, warnings)
        """
        parts = []
        warnings = []
        
        # Always include high-level summary (Tier 1)
        tier1 = self.build_tier1_context()
        if tier1:
            parts.append(
                "═══════════════════════════════════════════════\n"
                "## CORE ANALYSIS DATA (always included — complete, no truncation)\n"
                "═══════════════════════════════════════════════\n" + tier1
            )

        # Add relevant Tier 2
        tier2 = self.build_tier2_context(question)
        if tier2:
            parts.append(
                "═══════════════════════════════════════════════\n"
                "## SUPPLEMENTARY DATA (matched to your question)\n"
                "═══════════════════════════════════════════════\n" + tier2
            )

        # Add filtered Tier 3
        tier3 = self.build_tier3_context(question)
        if tier3:
            parts.append(
                "═══════════════════════════════════════════════\n"
                "## ACTIVITY-LEVEL DATA\n"
                "═══════════════════════════════════════════════\n" + tier3
            )

        full_context = "\n\n".join(parts)
        est_tokens = len(full_context) // 4
        return full_context, est_tokens, warnings

    @property
    def total_chars(self) -> int:
        return sum(info["chars"] for info in self.sheet_info.values())

    @property
    def total_tokens(self) -> int:
        return self.total_chars // 4

    @property
    def sheet_count(self) -> int:
        return len(self.sheets)


# ════════════════════════════════════════════════════════════════════════
# PRESET QUESTIONS
# ════════════════════════════════════════════════════════════════════════

PRESET_QUESTIONS = [
    ("📊 Factory Overview",
     "Give me a comprehensive overview of this Azure Data Factory. How many pipelines, dataflows, datasets, linked services, and triggers are there? What are the main folders and categories? Show counts from the Statistics sheet."),
    ("🔗 Data Lineage",
     "Analyze the COMPLETE data lineage from the DataLineage sheet. Show all source → sink connections. What are the main source systems and target/destination systems? Group by pipeline and show the data flow path."),
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
# STREAMLIT AI CHAT — Premium UI Component
# ════════════════════════════════════════════════════════════════════════

AI_CHAT_CSS = """
<style>
/* AI Chat Tab — Theme-Aware Premium Styling */
.ai-chat-header {
    background: radial-gradient(circle at 10% 20%, rgba(var(--primary-rgb, 102, 126, 234), 0.12) 0%, transparent 40%),
                radial-gradient(circle at 90% 80%, rgba(var(--secondary-rgb, 118, 75, 162), 0.12) 0%, transparent 40%),
                linear-gradient(135deg, rgba(255, 255, 255, 0.05) 0%, rgba(255, 255, 255, 0.01) 100%);
    border: 1px solid rgba(255, 255, 255, 0.1);
    box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.25);
    border-radius: 20px;
    padding: 2rem;
    margin-bottom: 2rem;
    text-align: center;
    backdrop-filter: blur(16px);
    -webkit-backdrop-filter: blur(16px);
}
.ai-chat-header h2 {
    background: linear-gradient(135deg, var(--primary, #818cf8), var(--secondary, #a78bfa), #f472b6);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    font-size: 1.8rem;
    font-weight: 700;
    margin: 0;
}
.ai-chat-header p {
    color: rgba(255,255,255,0.6);
    margin: 0.3rem 0 0 0;
    font-size: 0.95rem;
}
.ai-metric-card {
    background: linear-gradient(135deg, var(--surface, rgba(30, 30, 46, 0.8)), rgba(255,255,255,0.03));
    border: 1px solid rgba(var(--primary-rgb, 99, 102, 241), 0.2);
    border-radius: 12px;
    padding: 1rem 1.2rem;
    text-align: center;
    backdrop-filter: blur(8px);
    transition: border-color 0.3s ease, box-shadow 0.3s ease;
}
.ai-metric-card:hover {
    border-color: rgba(var(--primary-rgb, 99, 102, 241), 0.5);
    box-shadow: 0 0 16px rgba(var(--primary-rgb, 99, 102, 241), 0.15);
}
.ai-metric-card .metric-value {
    font-size: 1.5rem;
    font-weight: 700;
    background: linear-gradient(135deg, var(--primary, #818cf8), var(--secondary, #c084fc));
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}
.ai-metric-card .metric-label {
    font-size: 0.8rem;
    color: rgba(255,255,255,0.5);
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin-top: 0.2rem;
}
.ai-preset-btn {
    background: linear-gradient(135deg, rgba(var(--primary-rgb, 99, 102, 241), 0.1), rgba(var(--secondary-rgb, 168, 85, 247), 0.1));
    border: 1px solid rgba(var(--primary-rgb, 99, 102, 241), 0.25);
    border-radius: 10px;
    padding: 0.6rem 1rem;
    transition: all 0.3s ease;
}
.ai-preset-btn:hover {
    border-color: rgba(var(--primary-rgb, 99, 102, 241), 0.6);
    background: linear-gradient(135deg, rgba(var(--primary-rgb, 99, 102, 241), 0.2), rgba(var(--secondary-rgb, 168, 85, 247), 0.2));
    transform: translateY(-1px);
}
.ai-context-badge {
    display: inline-block;
    background: rgba(34, 197, 94, 0.15);
    border: 1px solid rgba(34, 197, 94, 0.3);
    border-radius: 20px;
    padding: 0.3rem 0.8rem;
    font-size: 0.75rem;
    color: #4ade80;
}
.ai-data-status {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.5rem 1rem;
    border-radius: 8px;
    font-size: 0.85rem;
    margin-bottom: 1rem;
}
.ai-data-status.loaded {
    background: rgba(34, 197, 94, 0.1);
    border: 1px solid rgba(34, 197, 94, 0.3);
    color: #4ade80;
}
.ai-data-status.empty {
    background: rgba(234, 179, 8, 0.1);
    border: 1px solid rgba(234, 179, 8, 0.3);
    color: #facc15;
}

/* --- Chat Bubbles --- */
.chat-container {
    display: flex;
    flex-direction: column;
    gap: 1.2rem;
    padding: 1rem 0;
}
.chat-bubble {
    padding: 1rem 1.4rem;
    border-radius: 18px;
    max-width: 85%;
    line-height: 1.5;
    position: relative;
    font-size: 0.95rem;
    box-shadow: 0 4px 15px rgba(0,0,0,0.1);
    animation: fadeIn 0.4s ease-out;
}
.bubble-user {
    align-self: flex-end;
    background: linear-gradient(135deg, var(--primary, #6366f1), var(--secondary, #8b5cf6));
    color: white;
    border-bottom-right-radius: 4px;
}
.bubble-assistant {
    align-self: flex-start;
    background: rgba(255, 255, 255, 0.05);
    color: #e2e8f0;
    border: 1px solid rgba(255, 255, 255, 0.1);
    border-bottom-left-radius: 4px;
    backdrop-filter: blur(10px);
}
.bubble-icon {
    font-size: 1.2rem;
    margin-bottom: 0.5rem;
    display: block;
}

/* --- Thinking Indicator & Rotating System --- */
.thinking-container {
    display: flex;
    align-items: center;
    gap: 1rem;
    padding: 1rem 1.5rem;
    background: rgba(var(--primary-rgb, 99, 102, 241), 0.1);
    border-radius: 14px;
    border: 1px dashed rgba(var(--primary-rgb, 99, 102, 241), 0.4);
    margin: 1rem 0;
    width: fit-content;
    box-shadow: 0 4px 12px rgba(var(--primary-rgb, 99, 102, 241), 0.1);
}
.rotating-system {
    font-size: 1.8rem;
    animation: rotateAtom 2.5s infinite linear;
    display: inline-block;
}
@keyframes rotateAtom {
    0% { transform: rotate(0deg) scale(1); }
    50% { transform: rotate(180deg) scale(1.2); }
    100% { transform: rotate(360deg) scale(1); }
}
.thinking-text {
    font-weight: 600;
    background: linear-gradient(135deg, var(--primary, #818cf8), var(--secondary, #c084fc));
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    letter-spacing: 0.05em;
}
</style>
"""


def initialize_ai_session_state():
    """Initialize all AI chat session state variables."""
    defaults = {
        "ai_chat_history": [],        # Display history: [{"role", "content"}]
        "ai_model": DEFAULT_MODEL,
        "ai_context_builder": None,
        "ai_context_hash": "",         # For cache invalidation
        "ai_api_history": [],          # Gemini API format history
        "ai_processing": False,
        "ai_total_tokens_used": 0,
    }
    for key, default in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default


def _get_or_rebuild_context(excel_data: Dict[str, pd.DataFrame]) -> ExcelContextBuilder:
    """
    Get cached context builder or rebuild if data changed.
    ✅ FIX: Detects when new Excel is generated/loaded and rebuilds context.
    """
    # Quick hash of current data
    keys = sorted(excel_data.keys())
    lengths = [len(excel_data[k]) for k in keys if isinstance(excel_data[k], pd.DataFrame)]
    current_hash = hashlib.md5(f"{keys}:{lengths}".encode()).hexdigest()[:12]

    cached = st.session_state.get("ai_context_builder")
    cached_hash = st.session_state.get("ai_context_hash", "")

    if cached and cached_hash == current_hash:
        return cached

    # Rebuild
    builder = ExcelContextBuilder(excel_data=excel_data)
    st.session_state.ai_context_builder = builder
    st.session_state.ai_context_hash = current_hash

    # Also clear chat when data changes (old answers may be stale)
    if cached_hash and cached_hash != current_hash:
        st.session_state.ai_chat_history = []
        st.session_state.ai_api_history = []
        st.session_state.ai_total_tokens_used = 0

    return builder


def render_ai_sidebar():
    """Render AI configuration in the sidebar."""
    st.markdown("### 🤖 AI Chat")

    # API Key status
    key_mgr = APIKeyManager()
    if key_mgr.has_keys:
        active_key = key_mgr.get_key()
        masked = f"{active_key[:4]}...{active_key[-4:]}" if active_key else "None"
        st.success(f"✅ {key_mgr.key_count} API Keys Ready\n\n🔑 Active: `{masked}`")
    else:
        st.warning("⚠️ No API keys")
        user_key = st.text_input(
            "Gemini API Key:",
            type="password",
            key="sidebar_api_key_input",
            help="Get free key → ai.google.dev",
        )
        if user_key:
            st.session_state.user_gemini_api_key = user_key
            st.rerun()

    # Model Selection
    model_options = list(AVAILABLE_MODELS.keys())
    current_model = st.session_state.get("ai_model", DEFAULT_MODEL)
    current_idx = model_options.index(current_model) if current_model in model_options else 0

    selected = st.selectbox(
        "Model:",
        options=model_options,
        format_func=lambda x: f"{AVAILABLE_MODELS[x].icon} {AVAILABLE_MODELS[x].display_name}",
        index=current_idx,
        key="sidebar_model_select",
    )
    if selected != st.session_state.get("ai_model"):
        st.session_state.ai_model = selected

    # Context info
    ctx = st.session_state.get("ai_context_builder")
    if ctx:
        st.caption(f"📄 {ctx.sheet_count} sheets • ~{ctx.total_tokens:,} tokens")

    # Chat history info
    chat_len = len(st.session_state.get("ai_chat_history", []))
    if chat_len > 0:
        st.caption(f"💬 {chat_len} messages")
        if st.button("🗑️ Clear Chat", key="sidebar_clear_chat", width='stretch'):
            st.session_state.ai_chat_history = []
            st.session_state.ai_api_history = []
            st.session_state.ai_total_tokens_used = 0
            st.session_state.ai_context_builder = None  # Force rebuild
            st.rerun()


def render_ai_chat_tab(excel_data: Dict[str, pd.DataFrame] = None):
    """
    Render the complete AI Chat tab with premium design.

    Args:
        excel_data: Dict of sheet_name → DataFrame from loaded Excel.
                    Can come from Generate Excel or Upload & Analyze.
    """
    initialize_ai_session_state()

    # Premium CSS
    st.markdown(AI_CHAT_CSS, unsafe_allow_html=True)

    # ── Premium Header ──
    st.markdown("""
    <div class="ai-chat-header">
        <h2>🤖 AI-Powered ADF Analyst</h2>
        <p>Ask anything about your Azure Data Factory — powered by Google Gemini</p>
    </div>
    """, unsafe_allow_html=True)

    # ── Model Advisor ──
    with st.expander("🧠 **Model Guide: Which one strictly to use?**"):
        st.markdown("""
        *   ⚡ **Gemini 2.0 Flash** (Recommended): Fastest, most stable, best for 90% of queries.
        *   🧠 **Gemini 2.5 Pro**: Advanced reasoning, better for complex lineage tracing. *May be slower.*
        *   🚀 **Gemini Flash Latest**: Experimental speed. Use if others fail.
        """)

    # ── Check if data is available ──
    # Also check for output file if no data in session
    if not excel_data or len(excel_data) == 0:
        # Try to auto-detect the latest generated Excel
        output_path = Path("output/adf_analysis_latest.xlsx")
        if not output_path.exists():
            output_path = Path("D:/armtemp/ADF_Analyzer_v10_Production/output/adf_analysis_latest.xlsx")

        if output_path.exists():
            st.markdown("""
            <div class="ai-data-status empty">
                📂 Found generated Excel file! Click below to load it for AI analysis.
            </div>
            """, unsafe_allow_html=True)

            if st.button("📥 Load Latest Excel for AI Chat", type="primary", width='stretch'):
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
                        st.session_state.ai_context_builder = None  # Force rebuild
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Failed to read Excel: {e}")
            return
        else:
            st.warning(
                "📊 **No Excel data loaded.** Please use one of these options:\n\n"
                "1. **⚙️ Generate Excel** tab → Generate analysis from ARM Template JSON\n"
                "2. **📊 Upload & Analyze** tab → Upload existing Excel file\n\n"
                "The AI will automatically read all sheets from the loaded Excel."
            )
            return

    # ── Check API Keys ──
    key_mgr = APIKeyManager()
    if not key_mgr.has_keys:
        st.error("🔑 **No Gemini API key configured.**")
        col1, col2 = st.columns([2, 1])
        with col1:
            st.markdown("""
            **Get a FREE Gemini API key:**
            1. Go to [Google AI Studio](https://aistudio.google.com/apikey)
            2. Click **"Create API Key"**
            3. Paste it below 👇
            """)
        with col2:
            st.info("💡 Free tier includes 1,500 requests/day!")

        user_key = st.text_input(
            "Paste your Gemini API Key:",
            type="password",
            key="tab_api_key_input",
        )
        if user_key:
            st.session_state.user_gemini_api_key = user_key
            st.session_state.ai_context_builder = None # Trigger context rebuild
            st.rerun()
        return

    # ── Build / get cached context ──
    with st.spinner("🔄 Building AI context from Excel data..."):
        ctx_builder = _get_or_rebuild_context(excel_data)

    # ── Data status bar ──
    st.markdown(f"""
    <div class="ai-data-status loaded">
        ✅ Loaded {ctx_builder.sheet_count} sheets • {sum(i['rows'] for i in ctx_builder.sheet_info.values()):,} total rows • ~{ctx_builder.total_tokens:,} tokens
        <span class="ai-context-badge">Hash: {ctx_builder.data_hash}</span>
    </div>
    """, unsafe_allow_html=True)

    # ── Metrics Row ──
    cols = st.columns(4)
    with cols[0]:
        st.markdown(f"""<div class="ai-metric-card">
            <div class="metric-value">{ctx_builder.sheet_count}</div>
            <div class="metric-label">Sheets</div>
        </div>""", unsafe_allow_html=True)
    with cols[1]:
        total_rows = sum(i["rows"] for i in ctx_builder.sheet_info.values())
        st.markdown(f"""<div class="ai-metric-card">
            <div class="metric-value">{total_rows:,}</div>
            <div class="metric-label">Total Rows</div>
        </div>""", unsafe_allow_html=True)
    with cols[2]:
        model_cfg = AVAILABLE_MODELS.get(st.session_state.ai_model, AVAILABLE_MODELS[DEFAULT_MODEL])
        st.markdown(f"""<div class="ai-metric-card">
            <div class="metric-value">{model_cfg.icon} {model_cfg.display_name}</div>
            <div class="metric-label">AI Model</div>
        </div>""", unsafe_allow_html=True)
    with cols[3]:
        st.markdown(f"""<div class="ai-metric-card">
            <div class="metric-value">{key_mgr.key_count}</div>
            <div class="metric-label">API Keys</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("---")

    # ── Preset Quick Questions ──
    no_history = len(st.session_state.ai_chat_history) == 0
    with st.expander("💡 **Quick Questions** — click any to ask the AI", expanded=no_history):
        col1, col2 = st.columns(2)
        for i, (label, question) in enumerate(PRESET_QUESTIONS):
            col = col1 if i % 2 == 0 else col2
            with col:
                if st.button(label, key=f"preset_{i}", width='stretch'):
                    _process_question(question, ctx_builder)
                    st.rerun()

    # ── Chat History Display ──
    for msg in st.session_state.ai_chat_history:
        icon = "🧑‍💻" if msg["role"] == "user" else "🤖"
        with st.chat_message(msg["role"], avatar=icon):
            st.markdown(msg["content"])

    # ── Chat Input & Processing ──
    user_input = st.chat_input(
        "Ask about your ADF analysis... (e.g., 'Show all orphaned pipelines')",
        key="ai_chat_input",
    )

    # Note: Using separate variable for session state processing to avoid race conditions
    if user_input:
        # 1. Render user message immediately
        with st.chat_message("user", avatar="🧑‍💻"):
            st.markdown(user_input)
            
        # 2. Process with visible feedback
        with st.chat_message("assistant", avatar="🤖"):
            with st.status("🧠 Analyzing your ADF Factory...", expanded=True) as status:
                status.write("📂 Building context from Excel sheets...")
                data_context, est_tokens, warnings = ctx_builder.get_context_for_question(user_input)
                
                for w in warnings:
                    st.warning(w)
                
                status.write(f"📡 Sending request (~{est_tokens:,} tokens) to AI...")
                response = _process_question_v2(user_input, ctx_builder, data_context, est_tokens)
                
                status.update(label="✅ Analysis complete!", state="complete")
            
            # 3. Show final response (XSS safe via st.markdown)
            st.markdown(response)
        
        # Persist and rerun to keep history in sync
        st.session_state.ai_chat_history.append({"role": "user", "content": user_input})
        st.session_state.ai_chat_history.append({"role": "assistant", "content": response})
        st.rerun()
    
    # Preset handling (needs to trigger the same process)
    # Note: Preset buttons already call a rerun, so we need a flag
    if st.session_state.get("pending_question"):
        q = st.session_state.pop("pending_question")
        # We can't easily jump back into the chat input flow here without duplication
        # or a very complex state machine. For now, presets are handled by _process_question
        # but without the st.status (due to rerun constraints). 
        # Better: presets just set chat_input and rerun? Streamlit doesn't support setting chat_input.
        # So we use the old _process_question for presets (with minimal feedback) 
        # and the new flow for manual input.
        pass


def _process_question_v2(question: str, ctx_builder: ExcelContextBuilder, data_context: str, est_tokens: int) -> str:
    """Refactored processing core that returns text for st.status usage."""
    system_prompt = ctx_builder.build_system_context()
    
    full_prompt = f"""Analyze the following ADF data and answer this question.
Use ONLY the data provided below.

══════════════════════════════════════════
QUESTION: {question}
══════════════════════════════════════════

{data_context}

══════════════════════════════════════════
REMINDER: At the end, mention which sheets you used to answer.
══════════════════════════════════════════
"""

    # History summary to prevent token bloat
    api_history = st.session_state.get("ai_api_history", [])
    max_history = 6
    if len(api_history) > max_history:
        api_history = api_history[-max_history:]
    
    model = st.session_state.get("ai_model", DEFAULT_MODEL)
    client = GeminiClient(model=model)
    
    response = client.call_api(
        prompt=full_prompt,
        system_instruction=system_prompt,
        conversation_history=api_history,
        temperature=0.1,
    )
    
    if response and not response.startswith("❌"):
        # Update internal history
        st.session_state.ai_api_history.append({"role": "user", "parts": [{"text": f"[Contextual Question]: {question}"}]})
        st.session_state.ai_api_history.append({"role": "model", "parts": [{"text": response}]})
        st.session_state.ai_total_tokens_used += est_tokens
        
        model_cfg = AVAILABLE_MODELS.get(model, AVAILABLE_MODELS[DEFAULT_MODEL])
        footer = f"\n\n---\n*{model_cfg.icon} {model_cfg.display_name} • 📊 ~{est_tokens:,} tokens*"
        return response + footer
    
    return response or "❌ No response received."


def _process_question(question: str, ctx_builder: ExcelContextBuilder):
    """Old processor kept for Presets compat (minimal feedback)."""
    data_context, est_tokens, warnings = ctx_builder.get_context_for_question(question)
    response = _process_question_v2(question, ctx_builder, data_context, est_tokens)
    st.session_state.ai_chat_history.append({"role": "user", "content": question})
    st.session_state.ai_chat_history.append({"role": "assistant", "content": response})
