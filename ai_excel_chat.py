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
import datetime
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
        8192, 1_000_000, "Fast, lightweight generation", "✨"
    ),
    "gemini-2.0-pro-exp-02-05": ModelConfig(
        "gemini-2.0-pro-exp-02-05", "Gemini 2.0 Pro (Exp)",
        65536, 2_000_000, "Advanced reasoning", "🔬"
    ),
    "gemini-2.0-flash": ModelConfig(
        "gemini-2.0-flash", "Gemini 2.0 Flash",
        8192, 1_000_000, "Stable & quick", "💨"
    ),
    "gemini-pro-latest": ModelConfig(
        "gemini-pro-latest", "Gemini 1.5 Pro",
        65536, 2_000_000, "Deep context (2M)", "📚"
    ),
    "gemini-flash-latest": ModelConfig(
        "gemini-flash-latest", "Gemini 1.5 Flash",
        8192, 1_000_000, "Legacy Flash", "📜"
    ),
    "gemini-flash-lite-latest": ModelConfig(
        "gemini-flash-lite-latest", "Gemini 1.5 Flash-8B",
        8192, 1_000_000, "High volume, low latency", "🚀"
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
        for var in ["GOOGLE_API_KEY", "GEMINI_API_KEY", "GEMINI_API_KEY_2", "GEMINI_API_KEY_3", "GOOGLE_AI_STUDIO_KEY"]:
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

            # Dynamic timeout: large prompts need more time
            prompt_len = len(json.dumps(payload))
            dynamic_timeout = max(180, min(prompt_len // 2000, 600))

            try:
                response = requests.post(
                    url,
                    headers={"Content-Type": "application/json"},
                    json=payload,
                    timeout=dynamic_timeout,
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
                    try:
                        error_msg = response.json().get("error", {}).get("message", "").lower()
                    except Exception:
                        error_msg = ""
                    
                    if "quota" in error_msg or "exceeded" in error_msg:
                        # All free-tier keys share the same quota — rotating is futile
                        is_pro = "pro" in str(model).lower()
                        if is_pro:
                            return f"❌ **API Quota Exceeded (429 HTTP)**. Gemini Pro's free tier is strictly limited to **32,000** Tokens-Per-Minute. Your query was likely too large for this tier. Please switch to **Gemini 2.5 Flash** (1-Million token limit) or use a paid key."
                        else:
                            return f"❌ **API Quota Exceeded (429 HTTP)**. You have hit the Google API free tier limits (such as 15 Requests-Per-Minute or daily limits). Please wait a minute before sending another request, or use a paid key."
                    
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

        return "❌ **All API keys exhausted or rate limited.**\n_Note: If using a free key on a 'Pro' model, the limit is strictly 32,000 tokens per minute. Try switching to a 'Flash' model (1-Million tokens/min) for large Lineage queries._"


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
    # Table Lineage — needs datasets + linked services for lookup
    "table lineage": ["Datasets", "DatasetUsage", "LinkedServices", "LinkedServiceUsage", "DataFlowTransformations"],
    "target linked service": ["Datasets", "LinkedServices"],
    "source linked service": ["Datasets", "LinkedServices"],
    "sinklinkedservice": ["Datasets", "LinkedServices"],
    "sourcelinkedservice": ["Datasets", "LinkedServices"],
    "intermediate layer": ["Datasets"],
    # Column Lineage — needs all dataflow and dataset sheets
    "column lineage": [
        "Datasets", "DatasetUsage", "LinkedServices", "DataFlowLineage",
        "DataLineage", "DataFlowTransformations",
    ],
    "granular column": [
        "Datasets", "DatasetUsage", "LinkedServices", "DataFlowLineage",
        "DataLineage", "DataFlowTransformations",
    ],
    # Preset-specific keywords for best results
    "factory overview": [
        "Datasets", "DatasetUsage", "LinkedServices", "LinkedServiceUsage",
        "Triggers", "TriggerDetails",
    ],
    "complex pipelines": ["Datasets", "LinkedServices"],
    "impact": ["Datasets", "LinkedServices", "Triggers"],
    "blast radius": ["Datasets", "LinkedServices", "Triggers"],
    "dataflow": ["DataFlowTransformations", "Datasets", "LinkedServices"],
    "health check": [
        "OrphanedPipelines", "OrphanedDataFlows", "OrphanedDatasets",
        "OrphanedLinkedServices", "OrphanedTriggers", "Errors",
        "Triggers", "TriggerDetails", "Datasets", "LinkedServices",
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
    # Lineage queries always need the Activities sheet
    "table lineage", "column lineage", "setvariable", "set path",
    "valuesinfo", "execution stage", "sinklinked", "sourcelinked",
    "intermediate layer", "transformation layer",
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

    # NOTE: _df_to_csv removed — dead code. CSV conversion done inline in build_tier*_context.

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

    def build_tier1_context(self, pipeline_filter: List[str] = None) -> str:
        """Build COMPLETE context from critical sheets — NO truncation.
        If pipeline_filter is provided, filter sheets that have a Pipeline column."""
        context_parts = []

        # Sheets that can be filtered by Pipeline column
        PIPELINE_FILTERABLE = {"PipelineAnalysis", "DataLineage", "DataFlowLineage", "ImpactAnalysis", "Pipelines"}

        for sheet_name in TIER1_SHEETS:
            if sheet_name not in self.sheets:
                continue
            df = self.sheets[sheet_name]
            if df.empty:
                continue

            # Apply pipeline filter if specified and sheet supports it
            if pipeline_filter and sheet_name in PIPELINE_FILTERABLE and "Pipeline" in df.columns:
                df = df[df["Pipeline"].isin(pipeline_filter)]
                if df.empty:
                    continue

            csv_data = df.to_csv(index=False)
            est_tokens = len(csv_data) // 4
            filter_tag = f" [FILTERED: {len(pipeline_filter)} pipelines]" if pipeline_filter and sheet_name in PIPELINE_FILTERABLE else ""
            context_parts.append(
                f"\n### 📋 Sheet: {sheet_name}{filter_tag} "
                f"({len(df)} rows × {len(df.columns)} cols, ~{est_tokens:,} tokens)\n"
                f"{csv_data}"
            )

        return "\n".join(context_parts)

    def _extract_related_entities(self, pipeline_filter: List[str]) -> dict:
        """
        Cascade filter: from a pipeline filter, extract all related entity names
        (DataFlows, Datasets, LinkedServices) used by those pipelines.
        This enables downstream filtering of Tier2 sheets that lack a Pipeline column.
        """
        related = {
            "dataflows": set(),
            "datasets": set(),
            "linked_services": set(),
        }

        if not pipeline_filter:
            return related

        # Extract from Activities sheet (richest source)
        if "Activities" in self.sheets:
            acts = self.sheets["Activities"]
            if "Pipeline" in acts.columns:
                filtered_acts = acts[acts["Pipeline"].isin(pipeline_filter)]

                # DataFlow names
                if "DataFlow" in filtered_acts.columns:
                    related["dataflows"].update(filtered_acts["DataFlow"].dropna().unique())

                # Source/Sink LinkedServices
                for col in ["SourceLinkedService", "SinkLinkedService"]:
                    if col in filtered_acts.columns:
                        related["linked_services"].update(filtered_acts[col].dropna().unique())

                # Source/Sink tables → derive Dataset names from Datasets sheet
                for col in ["SourceTable", "SinkTable"]:
                    if col in filtered_acts.columns:
                        related["datasets"].update(filtered_acts[col].dropna().unique())

        # Also extract from DataLineage (Tier1, already filtered)
        if "DataLineage" in self.sheets:
            dl = self.sheets["DataLineage"]
            if "Pipeline" in dl.columns:
                filtered_dl = dl[dl["Pipeline"].isin(pipeline_filter)]
                for col in ["SourceDataset", "SinkDataset", "Dataset"]:
                    if col in filtered_dl.columns:
                        related["datasets"].update(filtered_dl[col].dropna().unique())
                for col in ["SourceLinkedService", "SinkLinkedService", "LinkedService"]:
                    if col in filtered_dl.columns:
                        related["linked_services"].update(filtered_dl[col].dropna().unique())

        # Clean up empty strings
        for key in related:
            related[key].discard("")

        return related

    def build_tier2_context(self, question: str,
                            dataflow_filter: List[str] = None,
                            trigger_filter: List[str] = None,
                            cascaded_entities: dict = None) -> str:
        """Build keyword-matched context from supplementary sheets, with entity filtering."""
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

        # Sheets filterable by DataFlow name
        DATAFLOW_FILTERABLE = {"DataFlows", "DataFlowLineage", "DataFlowTransformations"}
        # Sheets filterable by Trigger name
        TRIGGER_FILTERABLE = {"Triggers", "TriggerDetails"}
        # Sheets filterable by cascaded Dataset/LinkedService names
        DATASET_FILTERABLE = {"Datasets", "DatasetUsage"}
        LINKEDSERVICE_FILTERABLE = {"LinkedServices", "LinkedServiceUsage"}

        # Merge explicit filters with cascaded entities
        effective_df_filter = set(dataflow_filter or [])
        effective_dataset_filter = set()
        effective_ls_filter = set()

        if cascaded_entities:
            if cascaded_entities.get("dataflows") and not dataflow_filter:
                effective_df_filter.update(cascaded_entities["dataflows"])
            effective_dataset_filter.update(cascaded_entities.get("datasets", set()))
            effective_ls_filter.update(cascaded_entities.get("linked_services", set()))

        context_parts = []
        for sheet_name in sorted(matched_sheets):
            if sheet_name not in self.sheets:
                continue
            df = self.sheets[sheet_name]
            if df.empty:
                continue

            filter_tag = ""
            # Apply DataFlow filter (explicit or cascaded)
            if effective_df_filter and sheet_name in DATAFLOW_FILTERABLE and "DataFlow" in df.columns:
                df = df[df["DataFlow"].isin(effective_df_filter)]
                filter_tag = f" [CASCADED: {len(effective_df_filter)} dataflow(s)]"
                if df.empty:
                    continue
            # Apply Trigger filter (explicit)
            if trigger_filter and sheet_name in TRIGGER_FILTERABLE and "Trigger" in df.columns:
                df = df[df["Trigger"].isin(trigger_filter)]
                filter_tag = f" [FILTERED: {len(trigger_filter)} trigger(s)]"
                if df.empty:
                    continue
            # Apply cascaded Dataset filter
            if effective_dataset_filter and sheet_name in DATASET_FILTERABLE:
                for dcol in ["Dataset", "DatasetName"]:
                    if dcol in df.columns:
                        df = df[df[dcol].isin(effective_dataset_filter)]
                        filter_tag = f" [CASCADED: {len(effective_dataset_filter)} related entities]"
                        break
                if df.empty:
                    continue
            # Apply cascaded LinkedService filter
            if effective_ls_filter and sheet_name in LINKEDSERVICE_FILTERABLE:
                for lcol in ["LinkedService", "LinkedServiceName"]:
                    if lcol in df.columns:
                        df = df[df[lcol].isin(effective_ls_filter)]
                        filter_tag = f" [CASCADED: {len(effective_ls_filter)} related services]"
                        break
                if df.empty:
                    continue

            csv_data = df.to_csv(index=False)
            est_tokens = len(csv_data) // 4
            context_parts.append(
                f"\n### 📋 Sheet: {sheet_name}{filter_tag} "
                f"({len(df)} rows × {len(df.columns)} cols, ~{est_tokens:,} tokens)\n"
                f"{csv_data}"
            )

        return "\n".join(context_parts)

    def build_tier3_context(self, question: str, model_name: str = DEFAULT_MODEL, pipeline_filter: List[str] = None) -> str:
        """Build Activities context — filtered by pipeline or summarized, unless high-context model."""
        q_lower = question.lower()

        needs_activities = any(kw in q_lower for kw in ACTIVITIES_KEYWORDS)
        if not needs_activities:
            return ""

        if "Activities" not in self.sheets:
            return ""

        activities_df = self.sheets["Activities"]
        if activities_df.empty:
            return ""

        # ── Apply explicit pipeline filter from UI ──
        if pipeline_filter and "Pipeline" in activities_df.columns:
            activities_df = activities_df[activities_df["Pipeline"].isin(pipeline_filter)]
            if activities_df.empty:
                return "\n### 📋 Sheet: Activities\n(No activities found for the selected pipelines)"
            csv_data = activities_df.to_csv(index=False)
            return (
                f"\n### 📋 Sheet: Activities "
                f"[FILTERED: {len(pipeline_filter)} pipelines] "
                f"({len(activities_df)} rows)\n{csv_data}"
            )

        # High-context models (1M+ tokens) can handle the full Activities sheet
        model_config = AVAILABLE_MODELS.get(model_name)
        if model_config and model_config.context_window >= 1_000_000:
            csv_data = activities_df.to_csv(index=False)
            return (
                f"\n### 📋 Sheet: Activities "
                f"(FULL DATA - {len(activities_df)} rows)\n{csv_data}"
            )

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

    def get_context_for_question(self, question: str, model: str = DEFAULT_MODEL,
                                  pipeline_filter: List[str] = None,
                                  dataflow_filter: List[str] = None,
                                  trigger_filter: List[str] = None) -> Tuple[str, int, List[str]]:
        """
        Intelligently select and build context based on query relevance.
        Returns: (context_text, est_tokens, warnings)
        """
        parts = []
        warnings = []

        # Filter banners
        if pipeline_filter:
            parts.append(
                f"⚠️ PIPELINE FILTER ACTIVE: Only showing data for these {len(pipeline_filter)} pipeline(s): "
                + ", ".join(pipeline_filter)
            )
        if dataflow_filter:
            parts.append(
                f"⚠️ DATAFLOW FILTER ACTIVE: Only showing data for these {len(dataflow_filter)} dataflow(s): "
                + ", ".join(dataflow_filter)
            )
        if trigger_filter:
            parts.append(
                f"⚠️ TRIGGER FILTER ACTIVE: Only showing data for these {len(trigger_filter)} trigger(s): "
                + ", ".join(trigger_filter)
            )
        
        # Always include high-level summary (Tier 1)
        tier1 = self.build_tier1_context(pipeline_filter=pipeline_filter)
        if tier1:
            parts.append(
                "═══════════════════════════════════════════════\n"
                "## CORE ANALYSIS DATA (always included — complete, no truncation)\n"
                "═══════════════════════════════════════════════\n" + tier1
            )

        # ── Cascading Entity Extraction ──
        # When pipeline filter is active, derive DataFlows/Datasets/LinkedServices
        # used by those pipelines → cascade-filter supplementary (Tier2) sheets
        cascaded = self._extract_related_entities(pipeline_filter) if pipeline_filter else None

        # Add relevant Tier 2 (with entity filtering + cascading)
        tier2 = self.build_tier2_context(
            question,
            dataflow_filter=dataflow_filter,
            trigger_filter=trigger_filter,
            cascaded_entities=cascaded,
        )
        if tier2:
            parts.append(
                "═══════════════════════════════════════════════\n"
                "## SUPPLEMENTARY DATA (matched to your question)\n"
                "═══════════════════════════════════════════════\n" + tier2
            )

        # Add filtered Tier 3
        tier3 = self.build_tier3_context(question, model, pipeline_filter=pipeline_filter)
        if tier3:
            parts.append(
                "═══════════════════════════════════════════════\n"
                "## ACTIVITY-LEVEL DATA\n"
                "═══════════════════════════════════════════════\n" + tier3
            )

        full_context = "\n\n".join(parts)
        # Include system prompt + history estimate for a more accurate token count
        system_len = len(self.build_system_context()) if hasattr(self, 'build_system_context') else 3000
        est_tokens = (len(full_context) + system_len) // 4
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

# ── Preset Filter Guide: tells users which filters help which preset ──
# Each entry: (recommended_filters, sheets_loaded_description, hint_text)
PRESET_FILTER_GUIDE = {
    "📊 Factory Overview": {
        "filters": [],
        "sheets": "Statistics, PipelineAnalysis, Datasets, LinkedServices, Triggers",
        "hint": "No filter needed — counts from entire factory.",
    },
    "🔗 Pipeline Lineage": {
        "filters": ["🔍 Pipeline"],
        "sheets": "DataLineage, PipelineAnalysis + Activities",
        "hint": "Use Pipeline filter to focus on specific pipeline lineage paths.",
    },
    "📈 Table Lineage": {
        "filters": ["🔍 Pipeline"],
        "sheets": "Activities ↔ DataFlows ↔ Datasets ↔ LinkedServices (cross-referenced)",
        "hint": "Use Pipeline filter to trace specific pipelines. Loads 4 sheets with cross-references.",
    },
    "🔍 Column Lineage": {
        "filters": ["🔍 Pipeline", "🔄 DataFlow"],
        "sheets": "DataFlowLineage ↔ DataFlows ↔ Datasets ↔ Activities (cross-referenced)",
        "hint": "Use Pipeline + DataFlow filters for focused column tracing.",
    },
    "⚠️ Orphaned Resources": {
        "filters": [],
        "sheets": "OrphanedPipelines, OrphanedDataFlows, OrphanedDatasets, OrphanedLinkedServices, OrphanedTriggers",
        "hint": "No filter needed — scans all orphaned sheets.",
    },
    "🏗️ Complex Pipelines": {
        "filters": ["🔍 Pipeline"],
        "sheets": "PipelineAnalysis ↔ Activities (cross-referenced for complexity scoring)",
        "hint": "Use Pipeline filter to analyze specific pipeline complexity.",
    },
    "💥 Impact Analysis": {
        "filters": ["🔍 Pipeline"],
        "sheets": "ImpactAnalysis ↔ Datasets ↔ Triggers (cross-referenced for blast radius)",
        "hint": "Use Pipeline filter to check impact of specific pipelines.",
    },
    "🔄 DataFlow Details": {
        "filters": ["🔄 DataFlow"],
        "sheets": "DataFlows ↔ DataFlowLineage ↔ DataFlowTransformations ↔ Datasets (cross-referenced)",
        "hint": "Use DataFlow filter to analyze specific dataflows. Pipeline filter has no effect here.",
    },
    "🗓️ Trigger Schedule": {
        "filters": ["🗓️ Trigger"],
        "sheets": "Triggers ↔ TriggerDetails (cross-referenced with pipeline associations)",
        "hint": "Use Trigger filter to analyze specific triggers. Pipeline filter has no effect here.",
    },
    "🏥 Full Health Check": {
        "filters": ["🔍 Pipeline"],
        "sheets": "All Orphaned sheets + ImpactAnalysis + Errors + Triggers + PipelineAnalysis",
        "hint": "Use Pipeline filter to health-check specific pipelines, or leave empty for full factory scan.",
    },
}



# ════════════════════════════════════════════════════════════════════════
# STREAMLIT AI CHAT — Premium UI Component
# ════════════════════════════════════════════════════════════════════════

AI_CHAT_CSS = """
<style>
/* ═══════════════════════════════════════════════════════════════════
   AI CHAT — HYPER-MODERN GLASSMORPHISM UI (Dark + Light theme)
   ═══════════════════════════════════════════════════════════════════ */

/* --- Theme-aware CSS variables --- */
:root {
    --ai-bg-primary: rgba(30, 30, 46, 0.6);
    --ai-bg-secondary: rgba(255, 255, 255, 0.03);
    --ai-text: #e2e8f0;
    --ai-muted: rgba(255, 255, 255, 0.45);
    --ai-border: rgba(255, 255, 255, 0.08);
    --ai-code-bg: rgba(0, 0, 0, 0.3);
    --ai-header-shadow: rgba(0, 0, 0, 0.3);
    --ai-msg-user-bg: linear-gradient(135deg, rgba(99, 102, 241, 0.15), rgba(168, 85, 247, 0.1));
    --ai-msg-asst-bg: linear-gradient(135deg, rgba(255, 255, 255, 0.04), rgba(255, 255, 255, 0.02));
}

/* Light / White theme overrides */
@media (prefers-color-scheme: light) {
    :root {
        --ai-bg-primary: rgba(255, 255, 255, 0.85);
        --ai-bg-secondary: rgba(0, 0, 0, 0.02);
        --ai-text: #1e293b;
        --ai-muted: rgba(0, 0, 0, 0.5);
        --ai-border: rgba(0, 0, 0, 0.1);
        --ai-code-bg: rgba(0, 0, 0, 0.06);
        --ai-header-shadow: rgba(0, 0, 0, 0.08);
        --ai-msg-user-bg: linear-gradient(135deg, rgba(99, 102, 241, 0.08), rgba(168, 85, 247, 0.05));
        --ai-msg-asst-bg: linear-gradient(135deg, rgba(0, 0, 0, 0.02), rgba(0, 0, 0, 0.01));
    }
}
/* Streamlit's own light theme detection */
[data-testid="stAppViewContainer"][data-theme="light"],
html[data-theme="light"],
.stApp[data-theme="light"] {
    --ai-bg-primary: rgba(255, 255, 255, 0.85);
    --ai-bg-secondary: rgba(0, 0, 0, 0.02);
    --ai-text: #1e293b;
    --ai-muted: rgba(0, 0, 0, 0.5);
    --ai-border: rgba(0, 0, 0, 0.1);
    --ai-code-bg: rgba(0, 0, 0, 0.06);
    --ai-header-shadow: rgba(0, 0, 0, 0.08);
    --ai-msg-user-bg: linear-gradient(135deg, rgba(99, 102, 241, 0.08), rgba(168, 85, 247, 0.05));
    --ai-msg-asst-bg: linear-gradient(135deg, rgba(0, 0, 0, 0.02), rgba(0, 0, 0, 0.01));
}

/* --- Header --- */
.ai-chat-header {
    background: radial-gradient(circle at 10% 20%, rgba(99, 102, 241, 0.15) 0%, transparent 40%),
                radial-gradient(circle at 90% 80%, rgba(168, 85, 247, 0.15) 0%, transparent 40%),
                var(--ai-bg-secondary);
    border: 1px solid var(--ai-border);
    box-shadow: 0 8px 32px var(--ai-header-shadow), inset 0 0 60px rgba(99, 102, 241, 0.05);
    border-radius: 24px;
    padding: 2rem 2.5rem;
    margin-bottom: 1.5rem;
    text-align: center;
    backdrop-filter: blur(20px);
    position: relative;
    overflow: hidden;
}
.ai-chat-header::before {
    content: '';
    position: absolute;
    top: -50%;
    left: -50%;
    width: 200%;
    height: 200%;
    background: conic-gradient(from 0deg at 50% 50%, transparent 0deg, rgba(99, 102, 241, 0.03) 90deg, transparent 180deg);
    animation: headerGlow 8s linear infinite;
}
@keyframes headerGlow {
    from { transform: rotate(0deg); }
    to { transform: rotate(360deg); }
}
.ai-chat-header h2 {
    background: linear-gradient(135deg, #6366f1, #8b5cf6, #ec4899, #6366f1);
    background-size: 300% 300%;
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    font-size: 1.9rem;
    font-weight: 800;
    margin: 0;
    position: relative;
    animation: gradientShift 4s ease infinite;
    letter-spacing: -0.02em;
}
@keyframes gradientShift {
    0%, 100% { background-position: 0% 50%; }
    50% { background-position: 100% 50%; }
}
.ai-chat-header p {
    color: var(--ai-muted);
    margin: 0.4rem 0 0 0;
    font-size: 0.9rem;
    position: relative;
    letter-spacing: 0.02em;
}

/* --- Metric Cards --- */
.ai-metric-card {
    background: var(--ai-bg-primary);
    border: 1px solid rgba(99, 102, 241, 0.15);
    border-radius: 16px;
    padding: 1.1rem 1.2rem;
    text-align: center;
    backdrop-filter: blur(12px);
    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    position: relative;
    overflow: hidden;
}
.ai-metric-card::after {
    content: '';
    position: absolute;
    inset: 0;
    border-radius: 16px;
    background: linear-gradient(135deg, rgba(99, 102, 241, 0.08), transparent);
    opacity: 0;
    transition: opacity 0.3s ease;
}
.ai-metric-card:hover {
    border-color: rgba(99, 102, 241, 0.4);
    box-shadow: 0 0 20px rgba(99, 102, 241, 0.12);
    transform: translateY(-2px);
}
.ai-metric-card:hover::after { opacity: 1; }
.ai-metric-card .metric-value {
    font-size: 1.45rem;
    font-weight: 700;
    background: linear-gradient(135deg, #6366f1, #a855f7);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    position: relative;
    z-index: 1;
}
.ai-metric-card .metric-label {
    font-size: 0.75rem;
    color: var(--ai-muted);
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-top: 0.25rem;
    font-weight: 600;
    position: relative;
    z-index: 1;
}

/* --- Data Status Badge --- */
.ai-context-badge {
    display: inline-block;
    background: rgba(34, 197, 94, 0.12);
    border: 1px solid rgba(34, 197, 94, 0.25);
    border-radius: 20px;
    padding: 0.25rem 0.75rem;
    font-size: 0.72rem;
    color: #16a34a;
    font-weight: 600;
    letter-spacing: 0.03em;
}
.ai-data-status {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.6rem 1.2rem;
    border-radius: 12px;
    font-size: 0.82rem;
    margin-bottom: 1rem;
    font-weight: 500;
}
.ai-data-status.loaded {
    background: rgba(34, 197, 94, 0.08);
    border: 1px solid rgba(34, 197, 94, 0.2);
    color: #16a34a;
}
.ai-data-status.empty {
    background: rgba(234, 179, 8, 0.08);
    border: 1px solid rgba(234, 179, 8, 0.2);
    color: #ca8a04;
}

/* ═══════════════════════════════════════════════════════════════════
   CHAT MESSAGES — Override Streamlit defaults
   ═══════════════════════════════════════════════════════════════════ */

/* User messages */
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-user"]) {
    background: var(--ai-msg-user-bg) !important;
    border: 1px solid rgba(99, 102, 241, 0.2) !important;
    border-radius: 18px 18px 6px 18px !important;
    padding: 1rem 1.2rem !important;
    margin: 0.4rem 0 !important;
    box-shadow: 0 4px 16px rgba(99, 102, 241, 0.08) !important;
    animation: fadeSlideIn 0.35s ease-out !important;
}

/* Assistant messages */
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-assistant"]) {
    background: var(--ai-msg-asst-bg) !important;
    border: 1px solid var(--ai-border) !important;
    border-radius: 18px 18px 18px 6px !important;
    padding: 1rem 1.2rem !important;
    margin: 0.4rem 0 !important;
    backdrop-filter: blur(8px) !important;
    box-shadow: 0 4px 16px var(--ai-header-shadow) !important;
    animation: fadeSlideIn 0.4s ease-out !important;
}

/* Avatar styling */
[data-testid="stChatMessage"] [data-testid*="chatAvatarIcon"] {
    font-size: 1.3rem !important;
}

/* Message text */
[data-testid="stChatMessage"] [data-testid="stMarkdownContainer"] p {
    font-size: 0.92rem !important;
    line-height: 1.65 !important;
    color: var(--ai-text) !important;
}

/* Code blocks in messages */
[data-testid="stChatMessage"] pre {
    background: var(--ai-code-bg) !important;
    border: 1px solid var(--ai-border) !important;
    border-radius: 10px !important;
}

/* Chat input styling */
[data-testid="stChatInput"] {
    border-radius: 16px !important;
    border: 1px solid rgba(99, 102, 241, 0.2) !important;
    background: var(--ai-bg-secondary) !important;
    backdrop-filter: blur(10px) !important;
}
[data-testid="stChatInput"]:focus-within {
    border-color: rgba(99, 102, 241, 0.5) !important;
    box-shadow: 0 0 20px rgba(99, 102, 241, 0.1) !important;
}

@keyframes fadeSlideIn {
    from { opacity: 0; transform: translateY(8px); }
    to { opacity: 1; transform: translateY(0); }
}

/* --- Thinking Indicator --- */
.thinking-container {
    display: flex;
    align-items: center;
    gap: 1rem;
    padding: 1rem 1.5rem;
    background: linear-gradient(135deg, rgba(99, 102, 241, 0.08), rgba(168, 85, 247, 0.05));
    border-radius: 16px;
    border: 1px dashed rgba(99, 102, 241, 0.3);
    margin: 0.8rem 0;
    width: fit-content;
    box-shadow: 0 4px 16px rgba(99, 102, 241, 0.08);
}
.rotating-system {
    font-size: 1.8rem;
    animation: rotateAtom 2.5s infinite linear;
    display: inline-block;
}
@keyframes rotateAtom {
    0% { transform: rotate(0deg) scale(1); }
    50% { transform: rotate(180deg) scale(1.15); }
    100% { transform: rotate(360deg) scale(1); }
}
.thinking-text {
    font-weight: 600;
    background: linear-gradient(135deg, #6366f1, #a855f7);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    letter-spacing: 0.04em;
}

/* ═══════════════════════════════════════════════════════════════════
   PRESET BUTTONS — Glassmorphism Quick Actions
   ═══════════════════════════════════════════════════════════════════ */

/* Quick Question expander */
[data-testid="stExpander"] details[open] summary {
    border-bottom: 1px solid rgba(99, 102, 241, 0.15);
    padding-bottom: 0.6rem;
    margin-bottom: 0.4rem;
}

/* --- Status indicator pulse --- */
.status-pulse {
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: #22c55e;
    margin-right: 6px;
    animation: pulse 2s ease-in-out infinite;
}
@keyframes pulse {
    0%, 100% { box-shadow: 0 0 0 0 rgba(34, 197, 94, 0.4); }
    50% { box-shadow: 0 0 0 6px rgba(34, 197, 94, 0); }
}

/* --- Chat control bar --- */
.chat-control-bar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0.5rem 1rem;
    background: var(--ai-bg-secondary);
    border: 1px solid var(--ai-border);
    border-radius: 12px;
    margin-bottom: 0.8rem;
    font-size: 0.78rem;
    color: var(--ai-muted);
}
.chat-control-bar .chat-id-badge {
    font-family: 'Courier New', monospace;
    font-size: 0.7rem;
    opacity: 0.6;
}

/* --- Empty state centered --- */
.empty-state {
    text-align: center;
    padding: 3rem 2rem;
    color: var(--ai-muted);
}
.empty-state .empty-icon {
    font-size: 3.5rem;
    margin-bottom: 1rem;
    opacity: 0.6;
    animation: float 3s ease-in-out infinite;
}
@keyframes float {
    0%, 100% { transform: translateY(0); }
    50% { transform: translateY(-8px); }
}
.empty-state h3 {
    color: var(--ai-text);
    font-weight: 600;
    margin: 0 0 0.5rem 0;
}
.empty-state p {
    font-size: 0.88rem;
    max-width: 400px;
    margin: 0 auto;
    line-height: 1.5;
}
</style>
"""



class ChatPersistenceManager:
    """Manages saving and loading chat history from local JSON files."""
    
    SAVE_DIR = Path("saved_chats")
    
    def __init__(self):
        self.SAVE_DIR.mkdir(parents=True, exist_ok=True)
        
    def save_chat(self, chat_id: str, chat_history: List[Dict], api_history: List[Dict], tokens: int, model: str):
        """Save a chat session to disk."""
        if not chat_history:
            return
            
        file_path = self.SAVE_DIR / f"{chat_id}.json"
        
        # Determine title from first user message (word-boundary truncation)
        title = "New Conversation"
        for msg in chat_history:
            if msg["role"] == "user":
                raw = msg["content"].replace("\n", " ").strip()
                if len(raw) > 45:
                    title = raw[:42].rsplit(" ", 1)[0] + "..."
                else:
                    title = raw
                break
        
        # Cap API history to prevent JSON bloat
        capped_api_history = api_history[-12:] if len(api_history) > 12 else api_history
        
        data = {
            "chat_id": chat_id,
            "title": title,
            "timestamp": datetime.datetime.now().isoformat(),
            "chat_history": chat_history,
            "api_history": capped_api_history,
            "total_tokens": tokens,
            "model": model
        }
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            
    def get_history(self) -> List[Dict]:
        """Get list of all saved chats with metadata."""
        history = []
        if not self.SAVE_DIR.exists():
            return []
            
        for file in self.SAVE_DIR.glob("*.json"):
            try:
                with open(file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    history.append({
                        "id": data["chat_id"],
                        "title": data["title"],
                        "timestamp": data["timestamp"],
                        "tokens": data.get("total_tokens", 0)
                    })
            except Exception:
                continue
        
        # Sort by timestamp descending
        return sorted(history, key=lambda x: x["timestamp"], reverse=True)
        
    def load_chat(self, chat_id: str) -> Optional[Dict]:
        """Load a specific chat session."""
        file_path = self.SAVE_DIR / f"{chat_id}.json"
        if not file_path.exists():
            return None
            
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
            
    def delete_chat(self, chat_id: str):
        """Delete a saved chat."""
        file_path = self.SAVE_DIR / f"{chat_id}.json"
        if file_path.exists():
            file_path.unlink()


def initialize_ai_session_state():
    """Initialize all AI chat session state variables."""
    first_init = "ai_chat_id" not in st.session_state
    
    if first_init:
        st.session_state.ai_chat_id = f"chat_{int(time.time())}"
        
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
    
    # Auto-load the most recent chat on first visit
    if first_init and len(st.session_state.ai_chat_history) == 0:
        try:
            pm = ChatPersistenceManager()
            history = pm.get_history()
            if history:
                latest = history[0]  # Already sorted by timestamp desc
                data = pm.load_chat(latest["id"])
                if data and data.get("chat_history"):
                    st.session_state.ai_chat_id = data["chat_id"]
                    st.session_state.ai_chat_history = data.get("chat_history", [])
                    st.session_state.ai_api_history = data.get("api_history", [])
                    st.session_state.ai_total_tokens_used = data.get("total_tokens", 0)
                    st.session_state.ai_model = data.get("model", DEFAULT_MODEL)
        except Exception:
            pass  # Graceful fallback to empty state


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
    initialize_ai_session_state()  # Defensive — ensure state exists
    st.markdown("### 🤖 AI Chat")

    # Initialize persistence
    pm = ChatPersistenceManager()

    # --- New Chat Button ---
    if st.button("➕ New Chat", use_container_width=True, type='primary'):
        st.session_state.ai_chat_id = f"chat_{int(time.time())}"
        st.session_state.ai_chat_history = []
        st.session_state.ai_api_history = []
        st.session_state.ai_total_tokens_used = 0
        st.rerun()

    st.markdown("---")

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

    # --- Chat History Management ---
    st.markdown("---")
    history = pm.get_history()
    current_chat_id = st.session_state.get("ai_chat_id", "")
    
    if history:
        st.markdown("#### 📜 Recent Chats")
        
        for chat in history[:10]:
            chat_id = chat["id"]
            title = chat["title"]
            is_current = (chat_id == current_chat_id)
            
            # Format relative timestamp
            try:
                ts = datetime.datetime.fromisoformat(chat["timestamp"])
                delta = datetime.datetime.now() - ts
                if delta.total_seconds() < 60:
                    time_label = "just now"
                elif delta.total_seconds() < 3600:
                    time_label = f"{int(delta.total_seconds() / 60)}m ago"
                elif delta.total_seconds() < 86400:
                    time_label = f"{int(delta.total_seconds() / 3600)}h ago"
                else:
                    time_label = f"{int(delta.days)}d ago"
            except Exception:
                time_label = ""
            
            # Layout: [Chat Button | Delete Button]
            col_chat, col_del = st.columns([5, 1])
            with col_chat:
                # Visual indicator for active chat
                prefix = "✨ " if is_current else "💬 "
                btn_type = "primary" if is_current else "secondary"
                btn_label = f"{prefix}{title}"
                if time_label:
                    btn_label += f"  •  {time_label}"
                
                if st.button(btn_label, key=f"hist_{chat_id}", use_container_width=True, type=btn_type):
                    if not is_current:
                        data = pm.load_chat(chat_id)
                        if data:
                            st.session_state.ai_chat_id = data["chat_id"]
                            st.session_state.ai_chat_history = data.get("chat_history", [])
                            st.session_state.ai_api_history = data.get("api_history", [])
                            st.session_state.ai_total_tokens_used = data.get("total_tokens", 0)
                            st.session_state.ai_model = data.get("model", st.session_state.ai_model)
                            st.rerun()
            
            with col_del:
                if st.button("🗑️", key=f"del_{chat_id}", help="Delete this chat"):
                    pm.delete_chat(chat_id)
                    if current_chat_id == chat_id:
                        st.session_state.ai_chat_id = f"chat_{int(time.time())}"
                        st.session_state.ai_chat_history = []
                        st.session_state.ai_api_history = []
                        st.session_state.ai_total_tokens_used = 0
                    st.rerun()
    else:
        st.caption("💬 No saved chats yet. Start a conversation!")
    
    # Context info
    ctx = st.session_state.get("ai_context_builder")
    if ctx:
        st.markdown("---")
        st.caption(f"📄 {ctx.sheet_count} sheets • ~{ctx.total_tokens:,} tokens")

    # Chat history info & Clear button
    chat_len = len(st.session_state.get("ai_chat_history", []))
    if chat_len > 0:
        st.caption(f"💬 {chat_len} messages in current chat")
        if st.button("🧹 Clear Current Messages", key="sidebar_clear_chat", use_container_width=True):
            st.session_state.ai_chat_history = []
            st.session_state.ai_api_history = []
            st.session_state.ai_total_tokens_used = 0
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

    # ── Smart Pipeline Filter ──
    all_pipeline_names = []
    pipeline_df = None
    if "PipelineAnalysis" in ctx_builder.sheets and "Pipeline" in ctx_builder.sheets["PipelineAnalysis"].columns:
        pipeline_df = ctx_builder.sheets["PipelineAnalysis"]
        all_pipeline_names = sorted(pipeline_df["Pipeline"].dropna().unique().tolist())
    elif "Pipelines" in ctx_builder.sheets and "Pipeline" in ctx_builder.sheets["Pipelines"].columns:
        pipeline_df = ctx_builder.sheets["Pipelines"]
        all_pipeline_names = sorted(pipeline_df["Pipeline"].dropna().unique().tolist())

    current_filter = st.session_state.get("ai_pipeline_filter", [])
    filter_count = len(current_filter)
    filter_label = f"🎯 {filter_count} selected" if filter_count > 0 else "All"

    if all_pipeline_names:
        with st.expander(f"🔍 **Smart Pipeline Filter** — {len(all_pipeline_names)} pipelines available ({filter_label})", expanded=False):

            # ── Quick-Filter Tabs ──
            filter_tab1, filter_tab2, filter_tab3 = st.tabs(["📂 By Folder", "⚡ By Complexity", "📋 Manual Select"])

            # ── TAB 1: By Folder ──
            with filter_tab1:
                if pipeline_df is not None and "Folder" in pipeline_df.columns:
                    folders = pipeline_df["Folder"].fillna("(Root / No Folder)").unique().tolist()
                    folders = sorted(folders)
                    st.caption(f"**{len(folders)}** folders detected. Click any folder to select all its pipelines.")
                    
                    # Folder buttons in a grid
                    fcol1, fcol2 = st.columns(2)
                    for fi, folder in enumerate(folders):
                        col = fcol1 if fi % 2 == 0 else fcol2
                        with col:
                            mask = pipeline_df["Folder"].fillna("(Root / No Folder)") == folder
                            folder_pipelines = pipeline_df.loc[mask, "Pipeline"].tolist()
                            count = len(folder_pipelines)
                            if st.button(f"📁 {folder} ({count})", key=f"folder_{fi}", use_container_width=True):
                                st.session_state.ai_pipeline_filter = folder_pipelines
                                st.rerun()
                else:
                    st.info("No `Folder` column found in PipelineAnalysis sheet.")

            # ── TAB 2: By Complexity ──
            with filter_tab2:
                if pipeline_df is not None and "Complexity" in pipeline_df.columns:
                    st.caption("Filter pipelines by complexity level.")
                    complexity_levels = sorted(pipeline_df["Complexity"].dropna().unique().tolist())
                    
                    ccol1, ccol2 = st.columns(2)
                    for ci, level in enumerate(complexity_levels):
                        col = ccol1 if ci % 2 == 0 else ccol2
                        with col:
                            mask = pipeline_df["Complexity"] == level
                            level_pipelines = pipeline_df.loc[mask, "Pipeline"].tolist()
                            count = len(level_pipelines)
                            icon = {"Simple": "🟢", "Medium": "🟡", "Complex": "🟠", "Critical": "🔴"}.get(str(level), "⚪")
                            if st.button(f"{icon} {level} ({count})", key=f"complexity_{ci}", use_container_width=True):
                                st.session_state.ai_pipeline_filter = level_pipelines
                                st.rerun()
                    
                    # Also show ImpactLevel if available
                    if "ImpactLevel" in pipeline_df.columns:
                        st.markdown("---")
                        st.caption("Or filter by **Impact Level**:")
                        impact_levels = sorted(pipeline_df["ImpactLevel"].dropna().unique().tolist())
                        icol1, icol2 = st.columns(2)
                        for ii, impact in enumerate(impact_levels):
                            col = icol1 if ii % 2 == 0 else icol2
                            with col:
                                mask = pipeline_df["ImpactLevel"] == impact
                                impact_pipelines = pipeline_df.loc[mask, "Pipeline"].tolist()
                                count = len(impact_pipelines)
                                icon = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢"}.get(str(impact).upper(), "⚪")
                                if st.button(f"{icon} {impact} ({count})", key=f"impact_{ii}", use_container_width=True):
                                    st.session_state.ai_pipeline_filter = impact_pipelines
                                    st.rerun()
                else:
                    st.info("No `Complexity` column found in PipelineAnalysis sheet.")

            # ── TAB 3: Manual Select ──
            with filter_tab3:
                mcol1, mcol2 = st.columns(2)
                with mcol1:
                    if st.button("📋 Select All", key="filter_select_all", use_container_width=True):
                        st.session_state.ai_pipeline_filter = all_pipeline_names
                        st.rerun()
                with mcol2:
                    if st.button("🧹 Clear All", key="filter_clear_all", use_container_width=True):
                        st.session_state.ai_pipeline_filter = []
                        st.rerun()
                
                selected_pipelines = st.multiselect(
                    "Select Pipelines:",
                    options=all_pipeline_names,
                    default=st.session_state.get("ai_pipeline_filter", []),
                    key="pipeline_multiselect",
                    placeholder="All Pipelines (no filter)",
                )
                st.session_state.ai_pipeline_filter = selected_pipelines

            # ── Filter Status Bar ──
            current_sel = st.session_state.get("ai_pipeline_filter", [])
            if current_sel:
                # Estimate token reduction
                if "Activities" in ctx_builder.sheets and "Pipeline" in ctx_builder.sheets["Activities"].columns:
                    total_act = len(ctx_builder.sheets["Activities"])
                    filtered_act = len(ctx_builder.sheets["Activities"][ctx_builder.sheets["Activities"]["Pipeline"].isin(current_sel)])
                    reduction_pct = int((1 - filtered_act / max(total_act, 1)) * 100)
                    est_filtered_tokens = (ctx_builder.total_chars - (ctx_builder.sheet_info.get("Activities", {}).get("chars", 0))) // 4 + (filtered_act * 300 // 4)
                    
                    recommended_model = "Any model" if est_filtered_tokens < 30000 else "Flash models" if est_filtered_tokens < 900000 else "Gemini 2.5 Pro"
                    
                    st.success(
                        f"🎯 **{len(current_sel)}** pipeline(s) selected • "
                        f"**{filtered_act}** activities (of {total_act}) • "
                        f"~{reduction_pct}% token reduction • "
                        f"Recommended: **{recommended_model}**"
                    )
                else:
                    st.info(f"🎯 **{len(current_sel)}** pipeline(s) selected.")
            else:
                st.caption("💡 No filter active — AI will process **all** pipelines. Use the tabs above to narrow focus.")

    # ── Secondary Entity Filters (DataFlow, Trigger, LinkedService) ──
    entity_filter_col1, entity_filter_col2 = st.columns(2)
    
    # DataFlow Filter
    with entity_filter_col1:
        all_dataflow_names = []
        if "DataFlows" in ctx_builder.sheets and "DataFlow" in ctx_builder.sheets["DataFlows"].columns:
            all_dataflow_names = sorted(ctx_builder.sheets["DataFlows"]["DataFlow"].dropna().unique().tolist())
        
        if all_dataflow_names:
            df_filter = st.multiselect(
                f"🔄 DataFlow Filter ({len(all_dataflow_names)})",
                options=all_dataflow_names,
                default=st.session_state.get("ai_dataflow_filter", []),
                key="dataflow_multiselect",
                placeholder="All DataFlows (no filter)",
            )
            st.session_state.ai_dataflow_filter = df_filter
    
    # Trigger Filter
    with entity_filter_col2:
        all_trigger_names = []
        if "Triggers" in ctx_builder.sheets and "Trigger" in ctx_builder.sheets["Triggers"].columns:
            all_trigger_names = sorted(ctx_builder.sheets["Triggers"]["Trigger"].dropna().unique().tolist())
        
        if all_trigger_names:
            trig_filter = st.multiselect(
                f"🗓️ Trigger Filter ({len(all_trigger_names)})",
                options=all_trigger_names,
                default=st.session_state.get("ai_trigger_filter", []),
                key="trigger_multiselect",
                placeholder="All Triggers (no filter)",
            )
            st.session_state.ai_trigger_filter = trig_filter

    # ── Preset Quick Questions (Context-Aware with Filter Guidance) ──
    no_history = len(st.session_state.ai_chat_history) == 0
    active_filter = st.session_state.get("ai_pipeline_filter", [])
    active_df_filter = st.session_state.get("ai_dataflow_filter", [])
    active_trig_filter = st.session_state.get("ai_trigger_filter", [])
    
    with st.expander("💡 **Quick Questions** — click any to ask the AI", expanded=no_history):
        # Show active filter status
        filter_status = []
        if active_filter:
            filter_status.append(f"🔍 **{len(active_filter)}** pipeline(s)")
        if active_df_filter:
            filter_status.append(f"🔄 **{len(active_df_filter)}** dataflow(s)")
        if active_trig_filter:
            filter_status.append(f"🗓️ **{len(active_trig_filter)}** trigger(s)")
        if filter_status:
            st.success(f"🎯 Active Filters: {' • '.join(filter_status)} — questions will auto-scope to these entities.")
        else:
            st.caption("💡 No filters active. Set filters above to narrow AI focus. See filter guide below for recommendations.")
        
        col1, col2 = st.columns(2)
        for i, (label, question) in enumerate(PRESET_QUESTIONS):
            col = col1 if i % 2 == 0 else col2
            with col:
                # Get filter guide for this preset
                guide = PRESET_FILTER_GUIDE.get(label, {})
                rec_filters = guide.get("filters", [])
                hint = guide.get("hint", "")
                
                # Build tooltip with filter recommendation
                tooltip = hint
                if rec_filters:
                    tooltip += f" | Best with: {', '.join(rec_filters)}"
                
                if st.button(label, key=f"preset_{i}", use_container_width=True, help=tooltip):
                    # Context-aware: inject all active entity filters into the question
                    final_question = question
                    filter_instructions = []
                    if active_filter and len(active_filter) <= 20:
                        filter_instructions.append(
                            f"Focus ONLY on these {len(active_filter)} pipeline(s): {', '.join(active_filter)}.")
                    if active_df_filter and len(active_df_filter) <= 20:
                        filter_instructions.append(
                            f"Focus ONLY on these {len(active_df_filter)} dataflow(s): {', '.join(active_df_filter)}.")
                    if active_trig_filter and len(active_trig_filter) <= 20:
                        filter_instructions.append(
                            f"Focus ONLY on these {len(active_trig_filter)} trigger(s): {', '.join(active_trig_filter)}.")
                    if filter_instructions:
                        final_question += "\n\n⚠️ IMPORTANT: " + " ".join(filter_instructions) + " Do not include data from other entities."
                    st.session_state.pending_question = final_question
                    st.rerun()
                
                # Show filter recommendation as small caption
                if rec_filters:
                    active_match = []
                    if "🔍 Pipeline" in rec_filters and active_filter:
                        active_match.append("✅ Pipeline")
                    elif "🔍 Pipeline" in rec_filters:
                        active_match.append("🔍 Pipeline")
                    if "🔄 DataFlow" in rec_filters and active_df_filter:
                        active_match.append("✅ DataFlow")
                    elif "🔄 DataFlow" in rec_filters:
                        active_match.append("🔄 DataFlow")
                    if "🗓️ Trigger" in rec_filters and active_trig_filter:
                        active_match.append("✅ Trigger")
                    elif "🗓️ Trigger" in rec_filters:
                        active_match.append("🗓️ Trigger")
                    st.caption(f"Best with: {' '.join(active_match)}")
        
        # Filter Usage Guide
        st.markdown("---")
        st.markdown("**📖 Which filter for which question?**")
        guide_data = []
        for label, _ in PRESET_QUESTIONS:
            g = PRESET_FILTER_GUIDE.get(label, {})
            filters_str = ", ".join(g.get("filters", [])) or "None needed"
            guide_data.append(f"| {label} | {filters_str} | {g.get('sheets', 'N/A')} |")
        st.markdown(
            "| Preset | Recommended Filters | Sheets Cross-Referenced |\n"
            "|---|---|---|\n" + "\n".join(guide_data)
        )

    # ── Custom Question Editor with AI Refiner ──
    with st.expander("✏️ **Custom Question Editor** — write, refine & save your own prompts", expanded=False):
        st.caption("Write a rough idea → click **✨ Refine with AI** → get an expert-level prompt automatically!")
        
        # Preset copy buttons
        st.markdown("**📋 Load a preset to edit:**")
        preset_cols = st.columns(4)
        for pi, (plabel, pquestion) in enumerate(PRESET_QUESTIONS[:8]):
            with preset_cols[pi % 4]:
                if st.button(plabel.split(" ", 1)[0], key=f"copy_preset_{pi}", use_container_width=True, help=f"Load '{plabel}' into editor"):
                    st.session_state.custom_question_text = pquestion
                    st.rerun()
        
        st.markdown("---")
        
        custom_text = st.text_area(
            "Your prompt (rough or polished):",
            value=st.session_state.get("custom_question_text", ""),
            height=200,
            key="custom_question_area",
            placeholder="Write your idea here — it can be rough!\n\nExample: 'show me all pipelines that load data from snowflake to azure sql'\n\nThen click ✨ Refine with AI to convert it into an expert-level prompt.",
        )
        st.session_state.custom_question_text = custom_text
        
        # Action buttons row
        btn_col1, btn_col2, btn_col3, btn_col4 = st.columns([2, 2, 1, 1])
        with btn_col1:
            if st.button("🚀 **Send**", key="send_custom", use_container_width=True, type="primary", disabled=not custom_text.strip()):
                final_q = custom_text.strip()
                filter_instructions = []
                if active_filter and len(active_filter) <= 20:
                    filter_instructions.append(
                        f"Focus ONLY on these {len(active_filter)} pipeline(s): {', '.join(active_filter)}.")
                if active_df_filter and len(active_df_filter) <= 20:
                    filter_instructions.append(
                        f"Focus ONLY on these {len(active_df_filter)} dataflow(s): {', '.join(active_df_filter)}.")
                if active_trig_filter and len(active_trig_filter) <= 20:
                    filter_instructions.append(
                        f"Focus ONLY on these {len(active_trig_filter)} trigger(s): {', '.join(active_trig_filter)}.")
                if filter_instructions:
                    final_q += "\n\n⚠️ IMPORTANT: " + " ".join(filter_instructions) + " Do not include data from other entities."
                st.session_state.pending_question = final_q
                st.rerun()
        with btn_col2:
            refine_disabled = not custom_text.strip() or len(custom_text.strip()) < 5
            if st.button("✨ **Refine with AI**", key="refine_custom", use_container_width=True, disabled=refine_disabled):
                with st.spinner("🧠 AI is refining your prompt..."):
                    # Use a lightweight Gemini call to refine the prompt
                    refine_meta_prompt = f"""You are an elite Prompt Engineer specializing in Azure Data Factory (ADF) analysis.

Your task is to take a user's rough query and convert it into a highly professional, expert-level prompt that will be sent to a data analysis AI. The AI will be reading Excel reports of the ADF factory.

User's rough query:
---
{custom_text.strip()}
---

Context available to the analysis AI:
- Available data sheets: {', '.join(ctx_builder.sheets.keys())}
- Key columns in 'Activities': Pipeline, Activity, ActivityType, SourceTable, SinkTable, SourceLinkedService, SinkLinkedService, DataFlow, ValuesInfo, ExecutionStage
- Key columns in 'Datasets': Dataset, LinkedService, Type
- Key columns in 'DataFlows': DataFlow, SourceTables, SinkTables

### Required Prompt Structure:
The refined prompt you generate MUST follow this structure:
1. **Role Definition:** Start with "You are an expert ADF Analyst."
2. **Data Sources:** Explicitly declare which exact sheets and columns the AI must read to answer the query (use exact names from the list above).
3. **Execution Logic (Step-by-Step):** If the query requires joining data (e.g., finding a LinkedService for a Dataset), write explicit, numbered step-by-step instructions on how the AI must join the sheets.
4. **Output Format:** Mandate a strict Markdown table output. Define the exact columns the table should have. Add an example row if helpful.
5. **Anti-Hallucination:** Add a strict instruction: "Only use data explicitly present in the provided sheets. Do not guess or infer."

Write beautifully formatted instructions.
Return ONLY the final refined prompt text. Do NOT wrap it in markdown code blocks (```). Do NOT include any preamble like 'Here is your prompt:'. Just output the raw prompt text."""

                    client = GeminiClient(key_mgr)
                    refined = client.call_api(
                        refine_meta_prompt,
                        model=st.session_state.get("ai_model", DEFAULT_MODEL),
                    )
                    
                    if refined and not refined.startswith("❌"):
                        st.session_state.custom_question_text = refined
                        st.success("✅ Prompt refined! Review it above, edit if needed, then click 🚀 Send.")
                        st.rerun()
                    else:
                        st.error(f"Refinement failed: {refined}")
        with btn_col3:
            # Save current prompt
            if st.button("💾 Save", key="save_custom", use_container_width=True, disabled=not custom_text.strip()):
                saved = st.session_state.get("ai_saved_prompts", [])
                label = custom_text.strip()[:60] + ("..." if len(custom_text.strip()) > 60 else "")
                saved.append({"label": label, "text": custom_text.strip()})
                st.session_state.ai_saved_prompts = saved[-10:]  # Keep last 10
                st.success(f"💾 Saved! ({len(st.session_state.ai_saved_prompts)} prompts)")
        with btn_col4:
            if st.button("🗑️ Clear", key="clear_custom", use_container_width=True):
                st.session_state.custom_question_text = ""
                st.rerun()
        
        # Show saved prompts if any
        saved_prompts = st.session_state.get("ai_saved_prompts", [])
        if saved_prompts:
            st.markdown("---")
            st.markdown(f"**💾 Saved Prompts** ({len(saved_prompts)}):")
            for si, sp in enumerate(saved_prompts):
                scol1, scol2, scol3 = st.columns([5, 1, 1])
                with scol1:
                    st.caption(f"📝 {sp['label']}")
                with scol2:
                    if st.button("Load", key=f"load_saved_{si}", use_container_width=True):
                        st.session_state.custom_question_text = sp["text"]
                        st.rerun()
                with scol3:
                    if st.button("🗑️", key=f"del_saved_{si}", use_container_width=True):
                        st.session_state.ai_saved_prompts.pop(si)
                        st.rerun()

    # ── Model Selection Guide ──
    with st.expander("🧠 **Model Guide: Which one strictly to use?**", expanded=False):
        st.markdown("""
        To avoid **429 Exceeded Quota** or **Context Truncation** errors, strictly follow this guide:

        | 🎯 Your Goal | 🏆 Recommended Model | ⏱️ Why? |
        |---|---|---|
        | **Table Lineage across ALL Pipelines** | **Gemini 2.5 Flash** | **Required for Free Users!** The 1-Million TPM free limit can ingest the massive 170k+ token `Activities` sheet without failing. |
        | **General Counts / Factory Overview** | **Gemini 2.0 Flash** | Fastest response. Stable and perfect for summarizing `Statistics` and `PipelineAnalysis`. |
        | **Column Lineage / Complex Debugging** | **Gemini 2.5 Pro** | Highest reasoning capability for tracing intricate data flows. |
        | **Quick Q&A on Errors / Triggers** | **Gemini 1.5 Pro** | Balances great reasoning with large context. |

        🛑 **CRITICAL WARNING FOR FREE TIER USERS:** 
        If you use **Gemini Pro** for massive Table Lineage queries, Google will strictly block it with HTTP 429 because the Free Tier for 'Pro' is capped at **32,000 Tokens/Min**. 
        You **MUST** use a **Flash** model (1,000,000 Tokens/Min limit) for these queries unless you have a paid API key!
        """)

    # ── Chat Control Bar ──
    tokens_used = st.session_state.get("ai_total_tokens_used", 0)
    msg_count = len(st.session_state.ai_chat_history)
    if msg_count > 0:
        ctrl_col1, ctrl_col2 = st.columns([4, 1])
        with ctrl_col1:
            st.markdown(f"""
            <div class="chat-control-bar">
                <span><span class="status-pulse"></span> {msg_count} messages • ~{tokens_used:,} tokens used</span>
                <span class="chat-id-badge">{st.session_state.ai_chat_id[-8:]}</span>
            </div>
            """, unsafe_allow_html=True)
        with ctrl_col2:
            if st.button("➕ New Chat", key="main_new_chat", use_container_width=True, type="primary"):
                st.session_state.ai_chat_id = f"chat_{int(time.time())}"
                st.session_state.ai_chat_history = []
                st.session_state.ai_api_history = []
                st.session_state.ai_total_tokens_used = 0
                st.rerun()

    # ── Chat History Display ──
    if msg_count == 0:
        # Beautiful empty state
        st.markdown("""
        <div class="empty-state">
            <div class="empty-icon">🤖</div>
            <h3>Ready to Analyze</h3>
            <p>Click a Quick Question above or type your own question below to start analyzing your Azure Data Factory.</p>
        </div>
        """, unsafe_allow_html=True)
    else:
        for msg in st.session_state.ai_chat_history:
            icon = "🧑‍💻" if msg["role"] == "user" else "🤖"
            with st.chat_message(msg["role"], avatar=icon):
                st.markdown(msg["content"])

    # ── Chat Input & Processing ──
    user_input = st.chat_input(
        "Ask about your ADF analysis... (e.g., 'Show all orphaned pipelines')",
        key="ai_chat_input",
    )

    # Handle both manual input and preset questions
    active_question = user_input or st.session_state.pop("pending_question", None)

    if active_question:
        # 1. Render user message immediately
        with st.chat_message("user", avatar="🧑‍💻"):
            st.markdown(active_question)
            
        # 2. Process with visible feedback
        with st.chat_message("assistant", avatar="🤖"):
            with st.status("🧠 Analyzing your ADF Factory...", expanded=True) as status:
                status.write("📂 Building context from Excel sheets...")
                model = st.session_state.get("ai_model", DEFAULT_MODEL)
                pipeline_filter = st.session_state.get("ai_pipeline_filter", []) or None
                df_filter = st.session_state.get("ai_dataflow_filter", []) or None
                trig_filter = st.session_state.get("ai_trigger_filter", []) or None
                data_context, est_tokens, warnings = ctx_builder.get_context_for_question(
                    active_question, model,
                    pipeline_filter=pipeline_filter,
                    dataflow_filter=df_filter,
                    trigger_filter=trig_filter,
                )
                
                active_filters_msg = []
                if pipeline_filter:
                    active_filters_msg.append(f"{len(pipeline_filter)} pipeline(s)")
                if df_filter:
                    active_filters_msg.append(f"{len(df_filter)} dataflow(s)")
                if trig_filter:
                    active_filters_msg.append(f"{len(trig_filter)} trigger(s)")
                if active_filters_msg:
                    status.write(f"🎯 Filter active: {', '.join(active_filters_msg)} selected")
                
                for w in warnings:
                    st.warning(w)
                
                # Pre-flight check: warn if Pro model + large context on free tier
                model_cfg = AVAILABLE_MODELS.get(model, AVAILABLE_MODELS[DEFAULT_MODEL])
                is_pro = "pro" in model.lower()
                if is_pro and est_tokens > 30000:
                    st.warning(
                        f"⚠️ **Free-Tier TPM Warning:** Your query is ~{est_tokens:,} tokens. "
                        f"Gemini Pro's free tier is capped at 32,000 TPM. "
                        f"Consider switching to **Gemini 2.5 Flash** (1M TPM limit) for reliability."
                    )
                
                status.write(f"📡 Sending request (~{est_tokens:,} tokens) to {model_cfg.display_name}...")
                response = _process_question_v2(active_question, ctx_builder, data_context, est_tokens)
                
                # ── Auto-fallback: if Pro model hit 429, retry with Flash ──
                if response and "API Quota Exceeded (429" in response and "pro" in model.lower():
                    fallback_model = "gemini-2.5-flash"
                    fallback_cfg = AVAILABLE_MODELS[fallback_model]
                    status.write(
                        f"⚡ Pro model hit 32K TPM limit. Auto-switching to "
                        f"{fallback_cfg.icon} {fallback_cfg.display_name} (1M TPM)..."
                    )
                    # Temporarily switch model for this request
                    old_model = st.session_state.ai_model
                    st.session_state.ai_model = fallback_model
                    response = _process_question_v2(active_question, ctx_builder, data_context, est_tokens)
                    st.session_state.ai_model = old_model  # Restore user's choice
                    
                    if response and not response.startswith("❌"):
                        response = (
                            f"> ⚡ **Auto-Fallback:** Pro model exceeded free tier (32K TPM). "
                            f"This response was generated by **{fallback_cfg.display_name}** instead.\n\n"
                            + response
                        )
                
                status.update(label="✅ Analysis complete!", state="complete")
            
            # 3. Show final response
            st.markdown(response)
        
        # Persist and rerun to keep history in sync
        st.session_state.ai_chat_history.append({"role": "user", "content": active_question})
        st.session_state.ai_chat_history.append({"role": "assistant", "content": response})
        
        # Save to disk
        ChatPersistenceManager().save_chat(
            st.session_state.ai_chat_id,
            st.session_state.ai_chat_history,
            st.session_state.ai_api_history,
            st.session_state.ai_total_tokens_used,
            st.session_state.ai_model
        )
        
        st.rerun()


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
    max_history = 4  # Tighter cap to prevent context overflow on follow-ups
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



