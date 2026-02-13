"""
Azure Data Factory Analyzer Dashboard
Enterprise Analytics & Visualization Platform

Features:
• Advanced Network Visualizations (2D & 3D)
• 20+ Interactive Charts & Analytics
• Modern Material Design UI
• Smart Search & Filtering
• Real-time Analytics & AI-Powered Insights
• Impact Analysis & Dependency Mapping
• Responsive Design & Multiple Export Formats

Enterprise ADF Analytics Team
Version 10.1
"""

import streamlit as st
import pandas as pd
import os
import sys
import subprocess
import json
import importlib
import copy
import datetime
import io
from pathlib import Path
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
try:
    import networkx as nx
    HAS_NETWORKX = True
except Exception:
    # networkx is optional for some visualizations; allow dashboard to import
    nx = None
    HAS_NETWORKX = False
from datetime import datetime, timedelta
import re
from collections import defaultdict, Counter
from typing import Dict, List, Any, Tuple, Optional, Set
import warnings
import io
import traceback
import hashlib

# AI Chat Integration
try:
    from ai_excel_chat import render_ai_chat_tab, render_ai_sidebar, initialize_ai_session_state
    HAS_AI_CHAT = True
except ImportError:
    HAS_AI_CHAT = False

# Simple caching decorators for expensive operations
@st.cache_data(show_spinner=False)
def cached_read_excel(_excel_file: pd.ExcelFile, excel_key: str) -> Dict[str, pd.DataFrame]:
    """Read all sheets from an ExcelFile into a dict (cached).

    Notes
    -----
    The first argument is prefixed with an underscore so Streamlit won't try to hash
    the non-hashable ExcelFile object. Instead, we supply a separate 'excel_key'
    (typically a content hash or path+mtime signature) to uniquely identify the file.
    """
    data: Dict[str, pd.DataFrame] = {}
    for sheet_name in _excel_file.sheet_names:
        try:
            df = pd.read_excel(_excel_file, sheet_name=sheet_name)
            data[sheet_name] = df
        except Exception:
            # Skip problematic sheets; non-fatal
            continue
    return data

@st.cache_data(show_spinner=False)
def cached_merge_and_normalize(data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Apply split sheet merging and normalization (cached)."""
    try:
        _merge_split_sheets_inplace(data)
        _normalize_sheet_map_inplace(data)
    except Exception:
        pass
    return data

@st.cache_resource(show_spinner=False)
def cached_build_graph(pipeline_df: pd.DataFrame, trigger_df: pd.DataFrame,
                       pipeline_pipeline_df: pd.DataFrame, pipeline_dataflow_df: pd.DataFrame,
                       lineage_df: pd.DataFrame) -> Tuple[Any, Dict[str, Any]]:
    """Build dependency graph as a cached resource.

    Returns
    -------
    (graph, metrics) tuple
    """
    try:
        G = nx.DiGraph()

        # Pipelines
        if not pipeline_df.empty:
            for _, row in pipeline_df.iterrows():
                pipeline_name = (
                    row.get("Pipeline") or row.get("pipeline") or row.get("PipelineName") or ""
                )
                if not pipeline_name:
                    continue
                has_trigger = False
                has_dataflow = False
                is_orphaned = False
                impact = row.get("Impact", row.get("ImpactLevel", "LOW"))
                if "UpstreamTriggerCount" in row:
                    has_trigger = int(row.get("UpstreamTriggerCount", 0)) > 0
                elif "UpstreamTriggers" in row:
                    has_trigger = bool(row.get("UpstreamTriggers", ""))
                elif "Has_Trigger" in row:
                    has_trigger = row.get("Has_Trigger") in ["Yes", True, 1]
                if "DataFlowCount" in row:
                    has_dataflow = int(row.get("DataFlowCount", 0)) > 0
                elif "UsedDataFlows" in row:
                    has_dataflow = bool(row.get("UsedDataFlows", ""))
                elif "Has_DataFlow" in row:
                    has_dataflow = row.get("Has_DataFlow") in ["Yes", True, 1]
                if "IsOrphaned" in row:
                    is_orphaned = row.get("IsOrphaned") in ["Yes", True, 1]
                elif "Is_Orphaned" in row:
                    is_orphaned = row.get("Is_Orphaned") in ["Yes", True, 1]
                G.add_node(
                    pipeline_name,
                    type="pipeline",
                    has_trigger=has_trigger,
                    has_dataflow=has_dataflow,
                    is_orphaned=is_orphaned,
                    impact=str(impact),
                )

        # Trigger edges
        if not trigger_df.empty:
            for _, row in trigger_df.iterrows():
                trigger = row.get("Trigger") or row.get("trigger") or ""
                pipeline = row.get("Pipeline") or row.get("pipeline") or ""
                if trigger and pipeline:
                    if not G.has_node(trigger):
                        G.add_node(trigger, type="trigger")
                    G.add_edge(trigger, pipeline, relation="triggers", weight=3)

        # Pipeline->Pipeline
        if not pipeline_pipeline_df.empty:
            for _, row in pipeline_pipeline_df.iterrows():
                fp = row.get("from_pipeline") or row.get("FromPipeline") or ""
                tp = row.get("to_pipeline") or row.get("ToPipeline") or ""
                if fp and tp:
                    G.add_edge(fp, tp, relation="executes", weight=2)

        # Pipeline->DataFlow
        if not pipeline_dataflow_df.empty:
            for _, row in pipeline_dataflow_df.iterrows():
                pl = row.get("pipeline") or row.get("Pipeline") or ""
                df = row.get("dataflow") or row.get("DataFlow") or ""
                if pl and df:
                    if not G.has_node(df):
                        G.add_node(df, type="dataflow")
                    G.add_edge(pl, df, relation="uses_dataflow", weight=1)

        # Lineage dataset nodes
        if not lineage_df.empty:
            for _, row in lineage_df.iterrows():
                source = row.get("Source", "")
                sink = row.get("Sink", "")
                if source and not G.has_node(source):
                    G.add_node(source, type="dataset")
                if sink and not G.has_node(sink):
                    G.add_node(sink, type="dataset")
                if source and sink:
                    G.add_edge(source, sink, relation="data_flow", weight=1)

        metrics = {
            "nodes": G.number_of_nodes(),
            "edges": G.number_of_edges(),
            "density": nx.density(G) if G.number_of_nodes() > 0 else 0,
            "is_directed": G.is_directed(),
        }
        return G, metrics
    except Exception:
        G = nx.DiGraph()
        metrics = {"nodes": 0, "edges": 0, "density": 0, "is_directed": True}
        return G, metrics

# Suppress warnings
warnings.filterwarnings("ignore")

# Check optional dependencies
try:
    import openpyxl

    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False

# PAGE CONFIGURATION
# Configuration

st.set_page_config(
    page_title="ADF Analyzer Dashboard",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "Get Help": None,
        "Report a bug": None,
        "About": """
        # ADF Analyzer v10.1

        **Enterprise Azure Data Factory Analysis Dashboard**

        Features:
        - Network Visualizations (2D & 3D)
        - Impact Analysis
        - Orphaned Resource Detection
        - Data Lineage Tracking
        - Interactive Charts
        - Smart Filtering
        """,
    },
)

# Styling

def load_custom_css(theme: str | None = None):
    """Load premium glassmorphism CSS with advanced effects and dynamic theme support.

    Parameters
    ----------
    theme : str | None
        'dark' or 'light'. If None, reads from st.session_state['theme'] (defaults to 'dark').
    """
    if theme is None:
        theme = st.session_state.get("theme", "dark")

    # Define root CSS variables per theme (keep rest of stylesheet shared)
    if theme == "light":
        root_vars = """
        :root {
            --bg-1: #f5f7fb;
            --bg-2: #e9edf5;
            --surface: rgba(255,255,255,0.85);
            --tile-bg: rgba(255,255,255,0.92);
            --tile-border-opacity: 0.25;
            --glass: rgba(255,255,255,0.65);
            --accent: #4f46e5;
            --muted: #475569;
            --text-color: #0f172a;
            --heading-color: #0f172a;
            --contrast-text: #0f172a; /* readable on light gradients */
            --sidebar-bg: linear-gradient(180deg, rgba(99,102,241,0.08) 0%, rgba(124,58,237,0.08) 100%);
            --tile-min: 260px;
            --tile-height: 160px;
            --tile-radius: 24px;
            --tile-padding: 1.05rem;
            --tile-accent-start: #6366f1;
            --tile-accent-end: #7c3aed;
            --header-bg: linear-gradient(135deg, #eef2ff 0%, #f8fafc 100%);
            --header-text: #0f172a;
            --title-stroke: 0px transparent;
            /* Chart colors for light theme */
            --chart-text-color: #0f172a;
            --chart-grid-color: rgba(100, 116, 139, 0.35);
            --chart-axis-color: rgba(71, 85, 105, 0.5);
        }

        body, .streamlit-container, .stApp {
            background: linear-gradient(160deg, var(--bg-1) 0%, var(--bg-2) 100%) fixed !important;
            color: var(--text-color) !important;
        }

        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, rgba(99,102,241,0.08) 0%, rgba(124,58,237,0.08) 100%);
        }
        """
    else:  # dark
        root_vars = """
        :root {
            --bg-1: #0b1120;
            --bg-2: #111827;
            --surface: rgba(255,255,255,0.05);
            /* Increased for better visibility */
            --tile-bg: rgba(255,255,255,0.10);
            --tile-border-opacity: 0.15;
            --glass: rgba(255,255,255,0.07);
            --accent: #818cf8;
            --muted: #94a3b8;
            /* Brightened for readability */
            --text-color: #f1f5f9;
            --heading-color: #f8fafc;
            --contrast-text: #ffffff; /* readable on dark gradients */
            --sidebar-bg: linear-gradient(180deg, rgba(102, 126, 234, 0.95) 0%, rgba(118, 75, 162, 0.95) 100%);
            --tile-min: 260px;
            --tile-height: 160px;
            --tile-radius: 26px;
            --tile-padding: 1.15rem;
            --tile-accent-start: #6366f1;
            --tile-accent-end: #7c3aed;
            --header-bg: linear-gradient(135deg, rgba(51,65,85,0.85) 0%, rgba(30,41,59,0.85) 100%);
            --header-text: #f8fafc;
            --title-stroke: 0px transparent;
            /* Chart colors for dark theme */
            --chart-text-color: #f1f5f9;
            --chart-grid-color: rgba(100, 116, 139, 0.5);
            --chart-axis-color: rgba(148, 163, 184, 0.6);
        }

        body, .streamlit-container, .stApp {
            background: linear-gradient(160deg, var(--bg-1) 0%, var(--bg-2) 100%) fixed !important;
            color: var(--text-color) !important;
        }
        """

    css = "<style>\n" + root_vars + """

        /* ═══════════════════════════════════════════════════════════════ */
        /* GLOBAL STYLES & FONTS */
        /* ═══════════════════════════════════════════════════════════════ */
        
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=Poppins:wght@400;500;600;700&display=swap');
        
        /* Theme variables are defined later (dark :root) to avoid duplication.
           Removed an earlier light theme block so the dashboard uses a single
           consistent dark theme. */

        * {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            -webkit-font-smoothing: antialiased;
            -moz-osx-font-smoothing: grayscale;
        }
        
        /* Page background is defined below in the dark theme :root section. */
        
        .main {
            padding: 1rem 2rem;
        }
        
        /* ═══════════════════════════════════════════════════════════════ */
        /* GLASSMORPHISM EFFECTS */
        /* ═══════════════════════════════════════════════════════════════ */
        
        .glass-card {
            background: rgba(255, 255, 255, 0.85);
            backdrop-filter: blur(20px) saturate(180%);
            -webkit-backdrop-filter: blur(20px) saturate(180%);
            border-radius: 20px;
            border: 1px solid rgba(255, 255, 255, 0.3);
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
            transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
        }
        
        .glass-card:hover {
            transform: translateY(-4px);
            box-shadow: 0 12px 48px rgba(0, 0, 0, 0.15);
            border-color: rgba(255, 255, 255, 0.5);
        }
        
        .glass-card-dark {
            background: var(--tile-bg);
            backdrop-filter: blur(18px) saturate(160%);
            -webkit-backdrop-filter: blur(18px) saturate(160%);
            border-radius: var(--tile-radius);
            border: 1px solid rgba(255,255,255,var(--tile-border-opacity));
            box-shadow: 0 8px 28px rgba(0,0,0,0.35);
            color: var(--text-color);
        }
        
        /* ═══════════════════════════════════════════════════════════════ */
        /* PREMIUM HEADER */
        /* ═══════════════════════════════════════════════════════════════ */
        
        .premium-header {
            /* theme-aware header */
            background: var(--header-bg);
            backdrop-filter: blur(8px);
            -webkit-backdrop-filter: blur(8px);
            color: var(--header-text);
            padding: 2.25rem 1.5rem;
            border-radius: 18px;
            margin-bottom: 1.5rem;
            box-shadow: 0 18px 48px rgba(20, 30, 60, 0.55);
            border: 1px solid rgba(255, 255, 255, 0.08);
            position: relative;
            overflow: hidden;
            animation: fadeInDown 0.5s ease-out;
        }
        
        .premium-header::before {
            content: '';
            position: absolute;
            top: -50%;
            left: -50%;
            width: 200%;
            height: 200%;
            /* subtle shimmer with much lower opacity so it doesn't wash text */
            background: linear-gradient(45deg, transparent, rgba(255, 255, 255, 0.06), transparent);
            animation: shimmer 4s infinite;
            pointer-events: none;
            mix-blend-mode: overlay;
        }
        
        @keyframes shimmer {
            0% { transform: translateX(-100%) translateY(-100%) rotate(45deg); }
            100% { transform: translateX(100%) translateY(100%) rotate(45deg); }
        }
        
        .premium-header h1 {
            margin: 0;
            font-size: 2.9em;
            font-weight: 800;
            color: var(--header-text);
            -webkit-text-stroke: var(--title-stroke);
            text-shadow: 0 2px 8px rgba(0, 0, 0, 0.55);
            letter-spacing: -0.6px;
            position: relative;
            z-index: 2;
        }

        /* Global heading defaults: solid, theme-aware and fully opaque */
        .stApp h1, .stApp h2, .stApp h3, .stApp h4 {
            color: var(--heading-color);
            opacity: 1;
            text-shadow: none;
        }
        /* Neutralize overly saturated gradient text in sidebar headings */
        .sidebar-header h2 {
            background: none !important;
            -webkit-text-fill-color: var(--heading-color) !important;
            color: var(--heading-color) !important;
        }
        .sidebar-section h3 { color: var(--heading-color) !important; }
        .metric-label, .tile-label { color: var(--heading-color); }
    .app-heading { color: var(--heading-color) !important; font-weight:600; letter-spacing:0.5px; }
        
        .premium-header p {
            margin: 12px 0 0 0;
            font-size: 1.05em;
            opacity: 0.95;
            font-weight: 500;
            color: rgba(230, 238, 248, 0.95);
            position: relative;
            z-index: 2;
        }
        
        .version-badge {
            display: inline-block;
            background: rgba(255, 255, 255, 0.2);
            padding: 6px 16px;
            border-radius: 20px;
            font-size: 0.85em;
            font-weight: 600;
            border: 1px solid rgba(255, 255, 255, 0.3);
            margin-top: 10px;
            backdrop-filter: blur(10px);
        }
        
        /* ═══════════════════════════════════════════════════════════════ */
        /* PREMIUM METRIC CARDS */
        /* ═══════════════════════════════════════════════════════════════ */
        
        .metric-card-premium {
            /* Align metric cards with tile sizing for consistent KPI look */
            background: var(--tile-bg);
            color: var(--text-color);
            backdrop-filter: blur(18px);
            padding: var(--tile-padding);
            border-radius: var(--tile-radius);
            text-align: center;
            box-shadow: 0 12px 36px rgba(2,6,23,0.6);
            border: 1px solid rgba(255, 255, 255, var(--tile-border-opacity));
            transition: transform 420ms cubic-bezier(.2,.9,.3,1), box-shadow 420ms;
            margin-bottom: 1rem;
            position: relative;
            overflow: hidden;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            min-height: var(--tile-height);
            width: 100%;
        }
        
        .metric-card-premium::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 4px;
            background: var(--primary-gradient);
            transform: scaleX(0);
            transform-origin: left;
            transition: transform 0.4s ease;
        }
        
        .metric-card-premium:hover {
            transform: translateY(-8px) scale(1.02);
            box-shadow: 0 16px 48px rgba(0, 0, 0, 0.15);
            border-color: rgba(255, 255, 255, 0.5);
        }
        
        .metric-card-premium:hover::before {
            transform: scaleX(1);
        }
        
        .metric-icon {
            font-size: 2.2em;
            margin-bottom: 8px;
            filter: drop-shadow(0 6px 18px rgba(0,0,0,0.6));
            animation: iconFloat 6s ease-in-out infinite;
            z-index: 2;
        }
        
        @keyframes float {
            0%, 100% { transform: translateY(0px); }
            50% { transform: translateY(-10px); }
        }
        
        .metric-value {
            font-size: 2.6em;
            font-weight: 800;
            margin: 6px 0 4px 0;
            color: var(--heading-color);
            letter-spacing: -0.6px;
        }

        /* Legacy metric-card used in several places; ensure it matches premium sizing */
        .metric-card {
            background: var(--tile-bg);
            color: var(--text-color);
            backdrop-filter: blur(18px);
            padding: var(--tile-padding);
            border-radius: var(--tile-radius);
            text-align: center;
            box-shadow: 0 12px 36px rgba(2,6,23,0.6);
            border: 1px solid rgba(255, 255, 255, var(--tile-border-opacity));
            transition: transform 420ms cubic-bezier(.2,.9,.3,1), box-shadow 420ms;
            margin-bottom: 1rem;
            position: relative;
            overflow: hidden;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            min-height: var(--tile-height);
            width: 100%;
            color: inherit;
        }

        .metric-card .metric-icon {
            font-size: 2.2em;
            margin-bottom: 8px;
            filter: drop-shadow(0 6px 18px rgba(0,0,0,0.6));
        }

        .metric-card .metric-value {
            font-size: 2.6em;
            font-weight: 800;
            margin: 6px 0 4px 0;
            color: var(--heading-color);
        }

        /* TEXT CLAMPING FOR KPI TILE LABELS & SUBTITLES */
        .metric-label, .tile-label {
            font-weight: 700;
            font-size: 0.95rem;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            max-width: 100%;
            display: block;
        }

        .metric-subtitle, .tile-subtitle {
            display: -webkit-box;
            -webkit-box-orient: vertical;
            -webkit-line-clamp: 2; /* show up to 2 lines */
            overflow: hidden;
            text-overflow: ellipsis;
            line-height: 1.15em;
            max-height: calc(1.15em * 2);
            opacity: 0.9;
            font-size: 0.85rem;
            margin-top: 0.35rem;
        }
        
        .metric-label {
            font-size: 0.9em;
            opacity: 0.8;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 1.2px;
            color: var(--heading-color);
        }
        
        .metric-delta {
            margin-top: 8px;
            font-size: 0.9em;
            font-weight: 600;
        }
        
        .metric-delta.positive {
            color: #10b981;
        }
        
        .metric-delta.negative {
            color: #ef4444;
        }
        
        /* ═══════════════════════════════════════════════════════════════ */
        /* ULTRA-PROFESSIONAL THEME & TILE SIZING (Clean, modern, cool) */
        /* ═══════════════════════════════════════════════════════════════ */

        /* Page background handled in theme root vars above */

        .tiles-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(var(--tile-min), 1fr));
            gap: 1.5rem;
            margin: 2rem 0;
            align-items: stretch; /* ensure equal heights */
        }

        @keyframes gridSlideIn {
            from { opacity: 0; transform: translateY(24px); }
            to { opacity: 1; transform: translateY(0); }
        }

        .tile-card {
            /* premium rounded card style (non-rectangular look) */
            background: var(--tile-bg);
            color: var(--text-color);
            backdrop-filter: blur(20px);
            padding: 1.6rem;
            border-radius: var(--tile-radius);
            text-align: center;
            box-shadow: 0 12px 36px rgba(2,6,23,0.6);
            border: 1px solid rgba(255, 255, 255, var(--tile-border-opacity));
            transition: transform 420ms cubic-bezier(.2,.9,.3,1), box-shadow 420ms;
            margin-bottom: 1rem;
            position: relative;
            overflow: visible;
            min-height: var(--tile-height);
            display: flex;
            flex-direction: column;
            justify-content: center;
            cursor: pointer;
        }
        
        .tile-card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 4px;
            background: var(--primary-gradient);
            transform: scaleX(0);
            transform-origin: left;
            transition: transform 0.4s ease;
        }

        /* Rounded / non-rectangular alternatives */
        .tile-pill {
            /* keep pill visual but ensure consistent sizing with other tiles */
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 0.75rem;
            padding: var(--tile-padding);
            border-radius: calc(var(--tile-radius) * 0.9);
            background: rgba(255,255,255,0.06);
            border: 1px solid rgba(255,255,255,0.06);
            box-shadow: 0 8px 20px rgba(0,0,0,0.28);
            color: var(--contrast-text);
            min-height: var(--tile-height);
            width: 100%;
            flex-direction: column;
            text-align: center;
        }

        .tile-angled {
            /* decorative angled variant but matching sizing */
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            padding: var(--tile-padding);
            border-radius: var(--tile-radius);
            background: var(--tile-bg);
            border: 1px solid rgba(255,255,255,var(--tile-border-opacity));
            color: var(--text-color);
            min-height: var(--tile-height);
            position: relative;
            overflow: visible;
        }

        .tile-angled::after {
            content: '';
            position: absolute;
            right: -40px;
            top: -40px;
            width: 160px;
            height: 160px;
            background: linear-gradient(135deg, rgba(255,255,255,0.06), rgba(255,255,255,0));
            transform: rotate(25deg);
            opacity: 0.35;
            pointer-events: none;
            mix-blend-mode: overlay;
        }
        
        @keyframes gradientShift {
            0%, 100% { background-position: 0% 50%; }
            50% { background-position: 100% 50%; }
        }
        
        .tile-card::after {
            content: '';
            position: absolute;
            top: -50%;
            left: -50%;
            width: 200%;
            height: 200%;
            background: radial-gradient(circle, 
                rgba(102, 126, 234, 0.1) 0%,
                rgba(118, 75, 162, 0.08) 30%,
                transparent 70%);
            opacity: 0;
            transition: opacity 0.5s ease;
            animation: backgroundPulse 4s ease-in-out infinite;
        }
        
        @keyframes backgroundPulse {
            0%, 100% { transform: scale(1); opacity: 0; }
            50% { transform: scale(1.1); opacity: 1; }
        }
        
        .tile-card:hover {
            transform: translateY(-12px) scale(1.03);
            box-shadow: 
                0 25px 70px rgba(0, 0, 0, 0.25),
                inset 0 1px 0 rgba(255, 255, 255, 0.3),
                0 0 0 1px rgba(255, 255, 255, 0.1);
            border-color: rgba(255, 255, 255, 0.2);
            backdrop-filter: blur(30px) saturate(220%);
        }
        
        .tile-card:hover::before {
            transform: scaleX(1);
        }
        
        .tile-card:hover::after {
            opacity: 1;
        }
        
        .tile-icon {
            font-size: 2.4rem;
            margin-bottom: 0.6rem;
            filter: drop-shadow(0 6px 18px rgba(0,0,0,0.6));
            animation: iconFloat 6s ease-in-out infinite;
            position: relative;
            z-index: 2;
            /* color it using per-tile accent for semantic consistency */
            color: var(--tile-accent-start);
        }

        @keyframes iconFloat {
            0%, 100% { transform: translateY(0px); }
            50% { transform: translateY(-6px); }
        }

        .tile-value {
            font-size: 2.2rem;
            font-weight: 800;
            margin: 0.4rem 0;
            color: var(--contrast-text);
            letter-spacing: -0.6px;
            position: relative;
            z-index: 2;
            text-shadow: 0 2px 6px rgba(0,0,0,0.25);
        }

        
        .tile-label {
            font-size: 0.9em;
            opacity: 0.85;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 1px;
            color: var(--heading-color);
            margin-top: 0.5rem;
            position: relative;
            z-index: 2;
        }
        
        .tile-subtitle {
            font-size: 0.75em;
            opacity: 0.7;
            margin-top: 0.35rem;
            color: var(--muted);
            position: relative;
            z-index: 2;
        }
        
        /* ═══════════════════════════════════════════════════════════════ */
        /* ULTRA-MODERN TILE VARIANTS WITH NEON EFFECTS */
        /* ═══════════════════════════════════════════════════════════════ */
        
        .tile-purple {
            --tile-accent-start: #667eea;
            --tile-accent-end: #764ba2;
            background: linear-gradient(135deg, rgba(102,126,234,0.06), rgba(118,75,162,0.04));
            border: 1px solid rgba(102,126,234,0.12);
            box-shadow: 0 8px 28px rgba(102,126,234,0.06), inset 0 1px 0 rgba(255,255,255,0.02);
        }

        .tile-purple::before { }

        .tile-purple:hover {
            box-shadow: 0 22px 60px rgba(102,126,234,0.12), inset 0 1px 0 rgba(255,255,255,0.04);
        }
        
        .tile-pink {
            --tile-accent-start: #f093fb;
            --tile-accent-end: #f5576c;
            background: linear-gradient(135deg, rgba(240,147,251,0.06), rgba(245,87,108,0.04));
            border: 1px solid rgba(240,147,251,0.10);
            box-shadow: 0 8px 28px rgba(240,147,251,0.05), inset 0 1px 0 rgba(255,255,255,0.02);
        }

        .tile-pink::before { }

        .tile-pink:hover {
            box-shadow: 0 22px 60px rgba(240,147,251,0.10), inset 0 1px 0 rgba(255,255,255,0.04);
        }
        
        .tile-blue {
            --tile-accent-start: #4facfe;
            --tile-accent-end: #00f2fe;
            background: linear-gradient(135deg, rgba(79,172,254,0.06), rgba(0,242,254,0.04));
            border: 1px solid rgba(79,172,254,0.10);
            box-shadow: 0 8px 28px rgba(79,172,254,0.05), inset 0 1px 0 rgba(255,255,255,0.02);
        }

        .tile-blue::before { }

        .tile-blue:hover {
            box-shadow: 0 22px 60px rgba(79,172,254,0.10), inset 0 1px 0 rgba(255,255,255,0.04);
        }
        
        .tile-green {
            --tile-accent-start: #43e97b;
            --tile-accent-end: #38f9d7;
            background: linear-gradient(135deg, rgba(67,233,123,0.06), rgba(56,249,215,0.04));
            border: 1px solid rgba(67,233,123,0.10);
            box-shadow: 0 8px 28px rgba(67,233,123,0.05), inset 0 1px 0 rgba(255,255,255,0.02);
        }

        .tile-green::before { }

        .tile-green:hover {
            box-shadow: 0 22px 60px rgba(67,233,123,0.10), inset 0 1px 0 rgba(255,255,255,0.04);
        }
        
        .tile-orange {
            --tile-accent-start: #fa709a;
            --tile-accent-end: #fee140;
            background: linear-gradient(135deg, rgba(250,112,154,0.06), rgba(254,225,64,0.04));
            border: 1px solid rgba(250,112,154,0.10);
            box-shadow: 0 8px 28px rgba(250,112,154,0.05), inset 0 1px 0 rgba(255,255,255,0.02);
        }

        .tile-orange::before { }

        .tile-orange:hover {
            box-shadow: 0 22px 60px rgba(250,112,154,0.10), inset 0 1px 0 rgba(255,255,255,0.04);
        }
        
        .tile-teal {
            --tile-accent-start: #30cfd0;
            --tile-accent-end: #330867;
            background: linear-gradient(135deg, rgba(48,207,208,0.06), rgba(51,8,103,0.03));
            border: 1px solid rgba(48,207,208,0.10);
            box-shadow: 0 8px 28px rgba(48,207,208,0.05), inset 0 1px 0 rgba(255,255,255,0.02);
        }

        .tile-teal::before { }

        .tile-teal:hover {
            box-shadow: 0 22px 60px rgba(48,207,208,0.10), inset 0 1px 0 rgba(255,255,255,0.04);
        }
        
        .tile-fire {
            --tile-accent-start: #f5576c;
            --tile-accent-end: #fecb1b;
            background: linear-gradient(135deg, rgba(245,87,108,0.06), rgba(254,204,27,0.04));
            border: 1px solid rgba(245,87,108,0.10);
            box-shadow: 0 8px 28px rgba(245,87,108,0.05), inset 0 1px 0 rgba(255,255,255,0.02);
        }

        .tile-fire::before { }

        .tile-fire:hover { box-shadow: 0 22px 60px rgba(245,87,108,0.10), inset 0 1px 0 rgba(255,255,255,0.04); }

        .tile-success {
            --tile-accent-start: #22c55e;
            --tile-accent-end: #15803d;
            background: linear-gradient(135deg, rgba(34,197,94,0.06), rgba(21,128,61,0.04));
            border: 1px solid rgba(34,197,94,0.10);
            box-shadow: 0 8px 28px rgba(34,197,94,0.05), inset 0 1px 0 rgba(255,255,255,0.02);
        }

        .tile-success::before { }

        .tile-success:hover { box-shadow: 0 22px 60px rgba(34,197,94,0.10), inset 0 1px 0 rgba(255,255,255,0.04); }

        .tile-warning {
            --tile-accent-start: #fbbf24;
            --tile-accent-end: #f59e0b;
            background: linear-gradient(135deg, rgba(251,191,36,0.06), rgba(245,158,11,0.04));
            border: 1px solid rgba(251,191,36,0.10);
            box-shadow: 0 8px 28px rgba(251,191,36,0.05), inset 0 1px 0 rgba(255,255,255,0.02);
        }

        .tile-warning::before { }

        .tile-warning:hover { box-shadow: 0 22px 60px rgba(251,191,36,0.10), inset 0 1px 0 rgba(255,255,255,0.04); }

        .tile-danger {
            --tile-accent-start: #ef4444;
            --tile-accent-end: #dc2626;
            background: linear-gradient(135deg, rgba(239,68,68,0.06), rgba(220,38,38,0.04));
            border: 1px solid rgba(239,68,68,0.10);
            box-shadow: 0 8px 28px rgba(239,68,68,0.05), inset 0 1px 0 rgba(255,255,255,0.02);
        }

        .tile-danger::before { }

        .tile-danger:hover { box-shadow: 0 22px 60px rgba(239,68,68,0.10), inset 0 1px 0 rgba(255,255,255,0.04); }
        
        /* ═══════════════════════════════════════════════════════════════ */
        /* ADVANCED MICRO-INTERACTIONS */
        /* ═══════════════════════════════════════════════════════════════ */
        
        .tile-card:active {
            transform: translateY(-8px) scale(0.98);
            transition: transform 0.1s ease-out;
        }
        
        .tile-card .tile-icon:hover {
            animation: iconBounce 0.6s ease-in-out;
            transform: scale(1.1);
        }
        
        @keyframes iconBounce {
            0%, 100% { transform: scale(1.1); }
            50% { transform: scale(1.2) rotate(5deg); }
        }
        
        .tile-card .tile-value:hover {
            animation: valueZoom 0.4s ease-in-out;
        }
        
        @keyframes valueZoom {
            0%, 100% { transform: scale(1); }
            50% { transform: scale(1.05); }
        }
        
        /* ═══════════════════════════════════════════════════════════════ */
        /* ADVANCED ANIMATION CLASSES */
        /* ═══════════════════════════════════════════════════════════════ */
        
        .tile-animated {
            animation: slideInUp 0.6s ease-out backwards;
        }
        
        @keyframes slideInUp {
            from {
                opacity: 0;
                transform: translateY(30px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }
        
        .tile-glow:hover {
            filter: brightness(1.1) saturate(1.2);
            transition: filter 0.3s ease;
        }
        
        /* ═══════════════════════════════════════════════════════════════ */
        /* MODERN RESPONSIVE DESIGN */
        /* ═══════════════════════════════════════════════════════════════ */
        
        @media (max-width: 768px) {
            .tiles-grid {
                grid-template-columns: 1fr;
                gap: 1.5rem;
            }
            
            .tile-card {
                padding: 2rem;
                min-height: 180px;
            }
            
            .tile-icon {
                font-size: 3em;
            }
            
            .tile-value {
                font-size: 2.5em;
            }
        }
        
        @media (max-width: 480px) {
            .tile-card {
                padding: 1.5rem;
                min-height: 160px;
                border-radius: 20px;
            }
            
            .tile-icon {
                font-size: 2.5em;
                margin-bottom: 1rem;
            }
            
            .tile-value {
                font-size: 2.2em;
                margin: 0.6rem 0;
            }
            
            .tile-label {
                font-size: 0.9em;
                letter-spacing: 1.2px;
            }
        }

        
        /* ═══════════════════════════════════════════════════════════════ */
        /* RICH GRADIENT VARIANTS (deep vibrant — matching app.py style) */
        /* White text on dark rich gradients — works in both dark & light themes */
        /* ═══════════════════════════════════════════════════════════════ */

        [class*="gradient-"] {
            border-radius: 16px;
            padding: 1.2rem;
            color: white !important;
            transition: all 0.3s ease;
        }

        .gradient-purple {
            background: linear-gradient(135deg, #6B5BFF, #a78bfa) !important;
            box-shadow: 0 6px 18px rgba(107, 91, 255, 0.25);
        }

        .gradient-pink {
            background: linear-gradient(135deg, #ec4899, #f472b6) !important;
            box-shadow: 0 6px 18px rgba(236, 72, 153, 0.25);
        }

        .gradient-blue {
            background: linear-gradient(135deg, #3b82f6, #60a5fa) !important;
            box-shadow: 0 6px 18px rgba(59, 130, 246, 0.25);
        }

        .gradient-green {
            background: linear-gradient(135deg, #10b981, #34d399) !important;
            box-shadow: 0 6px 18px rgba(16, 185, 129, 0.20);
        }

        .gradient-orange {
            background: linear-gradient(135deg, #f59e0b, #fbbf24) !important;
            color: #1e293b !important;
            box-shadow: 0 6px 18px rgba(245, 158, 11, 0.20);
        }

        .gradient-teal {
            background: linear-gradient(135deg, #14b8a6, #2dd4bf) !important;
            box-shadow: 0 6px 18px rgba(20, 184, 166, 0.20);
        }

        .gradient-fire {
            background: linear-gradient(135deg, #ef4444, #f97316) !important;
            box-shadow: 0 6px 18px rgba(239, 68, 68, 0.25);
        }

        .gradient-ocean {
            background: linear-gradient(135deg, #0ea5e9, #06b6d4) !important;
            box-shadow: 0 6px 18px rgba(14, 165, 233, 0.20);
        }

        /* Force white text for ALL child elements inside gradient cards */
        [class*="gradient-"] .metric-value,
        [class*="gradient-"] .metric-label,
        [class*="gradient-"] .metric-icon,
        [class*="gradient-"] .metric-delta,
        [class*="gradient-"] .tile-value,
        [class*="gradient-"] .tile-label,
        [class*="gradient-"] .tile-icon {
            color: white !important;
            -webkit-text-fill-color: white !important;
            text-shadow: 0 1px 3px rgba(0,0,0,0.2) !important;
        }

        /* Exception: orange gradient needs dark text */
        .gradient-orange .metric-value,
        .gradient-orange .metric-label,
        .gradient-orange .metric-icon,
        .gradient-orange .metric-delta,
        .gradient-orange .tile-value,
        .gradient-orange .tile-label,
        .gradient-orange .tile-icon {
            color: #1e293b !important;
            -webkit-text-fill-color: #1e293b !important;
            text-shadow: none !important;
        }

        /* ═══════════════════════════════════════════════════════════════ */
        /* GRADIENT HOVER EFFECTS (gentle lift + soft glow) */
        /* ═══════════════════════════════════════════════════════════════ */

        .gradient-purple:hover,
        .gradient-pink:hover,
        .gradient-blue:hover,
        .gradient-green:hover,
        .gradient-orange:hover,
        .gradient-teal:hover,
        .gradient-fire:hover,
        .gradient-ocean:hover {
            transform: translateY(-4px) scale(1.015);
            box-shadow: 0 30px 60px rgba(0,0,0,0.18);
            filter: brightness(1.08) saturate(1.1);
        }
        
        /* ═══════════════════════════════════════════════════════════════ */
        /* PREMIUM BADGES */
        /* ═══════════════════════════════════════════════════════════════ */
        
        .badge-premium {
            display: inline-block;
            padding: 8px 18px;
            margin: 4px;
            border-radius: 24px;
            font-size: 0.85em;
            font-weight: 600;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
            border: 1px solid rgba(255, 255, 255, 0.3);
            backdrop-filter: blur(10px);
            transition: all 0.3s ease;
        }
        
        .badge-premium:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 16px rgba(0, 0, 0, 0.2);
        }
        
        .badge-critical {
            background: linear-gradient(135deg, #fee2e2, #fecaca);
            color: #7f1d1d;
        }
        
        .badge-high {
            background: linear-gradient(135deg, #ffedd5, #fed7aa);
            color: #7c2d12;
        }
        
        .badge-medium {
            background: linear-gradient(135deg, #fef3c7, #fde68a);
            color: #78350f;
        }
        
        .badge-low {
            background: linear-gradient(135deg, #d1fae5, #a7f3d0);
            color: #065f46;
        }
        
        /* ═══════════════════════════════════════════════════════════════ */
        /* PREMIUM INFO CARDS */
        /* ═══════════════════════════════════════════════════════════════ */
        
        .info-card-premium {
            background: rgba(255, 255, 255, 0.9);
            backdrop-filter: blur(20px);
            padding: 2rem;
            border-radius: 20px;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
            margin-bottom: 1.5rem;
            border: 1px solid rgba(255, 255, 255, 0.3);
            border-left: 4px solid #667eea;
            transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
            position: relative;
            overflow: hidden;
        }
        
        .info-card-premium::after {
            content: '';
            position: absolute;
            top: -50%;
            right: -50%;
            width: 200%;
            height: 200%;
            background: radial-gradient(circle, rgba(102, 126, 234, 0.1) 0%, transparent 70%);
            pointer-events: none;
        }
        
        .info-card-premium:hover {
            transform: translateY(-4px);
            box-shadow: 0 12px 48px rgba(0, 0, 0, 0.15);
            border-left-width: 6px;
        }
        
        .info-card-premium h3,
        .info-card-premium h4 {
            margin: 0 0 12px 0;
            color: #667eea;
            font-weight: 700;
            position: relative;
            z-index: 1;
        }
        
        .info-card-premium p {
            margin: 8px 0;
            color: #475569;
            line-height: 1.6;
            position: relative;
            z-index: 1;
        }
        
        /* ═══════════════════════════════════════════════════════════════ */
        /* PREMIUM TABS (theme-aware like app.py) */
        /* ═══════════════════════════════════════════════════════════════ */
        
        .stTabs [data-baseweb="tab-list"] {
            gap: 6px;
            background: var(--surface);
            backdrop-filter: blur(20px);
            padding: 6px;
            border-radius: 14px;
            box-shadow: 0 4px 16px rgba(0, 0, 0, 0.15);
            border: 1px solid rgba(255, 255, 255, var(--tile-border-opacity));
            overflow-x: auto;
        }
        
        .stTabs [data-baseweb="tab"] {
            padding: 10px 18px;
            background: transparent;
            border-radius: 10px;
            font-weight: 600;
            font-size: 0.88rem;
            letter-spacing: 0.02em;
            transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
            border: 1px solid transparent;
            color: var(--muted);
            white-space: nowrap;
        }
        
        .stTabs [data-baseweb="tab"]:hover {
            background: rgba(99, 102, 241, 0.08);
            transform: translateY(-1px);
            color: var(--text-color);
            border-color: rgba(255, 255, 255, var(--tile-border-opacity));
        }
        
        .stTabs [data-baseweb="tab"][aria-selected="true"] {
            background: linear-gradient(135deg, rgba(99,102,241,0.95) 0%, rgba(124,58,237,0.95) 100%) !important;
            color: #ffffff !important;
            box-shadow: 0 8px 20px rgba(102, 126, 234, 0.28);
            border-color: rgba(255, 255, 255, 0.22);
        }
        
        /* ═══════════════════════════════════════════════════════════════ */
        /* PREMIUM BUTTONS */
        /* ═══════════════════════════════════════════════════════════════ */
        
        .stButton > button {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: var(--contrast-text);
            border: none;
            border-radius: 12px;
            padding: 0.65rem 1.8rem;
            font-weight: 600;
            font-size: 0.95em;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            box-shadow: 0 4px 12px rgba(102, 126, 234, 0.3);
            border: 1px solid rgba(255, 255, 255, 0.2);
        }
        
        .stButton > button:hover {
            transform: translateY(-3px);
            box-shadow: 0 8px 24px rgba(102, 126, 234, 0.5);
            background: linear-gradient(135deg, #764ba2 0%, #667eea 100%);
            color: var(--contrast-text);
        }
        
        .stButton > button:active {
            transform: translateY(-1px);
            box-shadow: 0 4px 12px rgba(102, 126, 234, 0.3);
        }
        
        /* ═══════════════════════════════════════════════════════════════ */
        /* PREMIUM DATAFRAME */
        /* ═══════════════════════════════════════════════════════════════ */
        
        .dataframe-container {
            background: rgba(255, 255, 255, 0.9);
            backdrop-filter: blur(20px);
            border-radius: 16px;
            padding: 1rem;
            box-shadow: 0 4px 16px rgba(0, 0, 0, 0.1);
            border: 1px solid rgba(255, 255, 255, 0.3);
        }
        
        .dataframe {
            border-radius: 12px !important;
            overflow: hidden;
            border: none !important;
        }
        
        /* ═══════════════════════════════════════════════════════════════ */
        /* PREMIUM SIDEBAR */
        /* ═══════════════════════════════════════════════════════════════ */
        
        [data-testid="stSidebar"] {
            background: var(--sidebar-bg);
            backdrop-filter: blur(20px);
        }
        
        [data-testid="stSidebar"] > div:first-child {
            background: transparent;
        }
        
        /* Sidebar text should adapt to theme; avoid hardcoded white on light backgrounds */
        [data-testid="stSidebar"] .stMarkdown {
            color: var(--contrast-text);
        }
        
        [data-testid="stSidebar"] .stSelectbox label,
        [data-testid="stSidebar"] .stMultiSelect label,
        [data-testid="stSidebar"] .stTextInput label {
            color: var(--contrast-text) !important;
            font-weight: 600;
        }
        
        /* ═══════════════════════════════════════════════════════════════ */
        /* ANIMATIONS */
        /* ═══════════════════════════════════════════════════════════════ */
        
        @keyframes fadeIn {
            from {
                opacity: 0;
                transform: translateY(20px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }
        
        @keyframes fadeInDown {
            from {
                opacity: 0;
                transform: translateY(-30px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }
        
        @keyframes fadeInUp {
            from {
                opacity: 0;
                transform: translateY(30px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }
        
        @keyframes slideInLeft {
            from {
                opacity: 0;
                transform: translateX(-30px);
            }
            to {
                opacity: 1;
                transform: translateX(0);
            }
        }
        
        @keyframes slideInRight {
            from {
                opacity: 0;
                transform: translateX(30px);
            }
            to {
                opacity: 1;
                transform: translateX(0);
            }
        }
        
        @keyframes pulse {
            0%, 100% {
                opacity: 1;
            }
            50% {
                opacity: 0.8;
            }
        }
        
        @keyframes glow {
            0%, 100% {
                box-shadow: 0 0 20px rgba(102, 126, 234, 0.5);
            }
            50% {
                box-shadow: 0 0 40px rgba(102, 126, 234, 0.8);
            }
        }
        
        .fade-in {
            animation: fadeIn 0.6s ease-out;
        }
        
        .fade-in-up {
            animation: fadeInUp 0.6s ease-out;
        }
        
        .slide-in-left {
            animation: slideInLeft 0.6s ease-out;
        }
        
        .slide-in-right {
            animation: slideInRight 0.6s ease-out;
        }
        
        /* ═══════════════════════════════════════════════════════════════ */
        /* LOADING SPINNER */
        /* ═══════════════════════════════════════════════════════════════ */
        
        .stSpinner > div {
            border-top-color: #667eea !important;
            animation: glow 2s ease-in-out infinite;
        }
        
        /* ═══════════════════════════════════════════════════════════════ */
        /* SCROLLBAR */
        /* ═══════════════════════════════════════════════════════════════ */
        
        ::-webkit-scrollbar {
            width: 10px;
            height: 10px;
        }
        
        ::-webkit-scrollbar-track {
            background: rgba(255, 255, 255, 0.1);
            border-radius: 10px;
        }
        
        ::-webkit-scrollbar-thumb {
            background: linear-gradient(135deg, #667eea, #764ba2);
            border-radius: 10px;
            border: 2px solid rgba(255, 255, 255, 0.1);
        }
        
        ::-webkit-scrollbar-thumb:hover {
            background: linear-gradient(135deg, #764ba2, #667eea);
        }
        
        /* ═══════════════════════════════════════════════════════════════ */
        /* RESPONSIVE DESIGN */
        /* ═══════════════════════════════════════════════════════════════ */
        
        @media (max-width: 768px) {
            .premium-header h1 {
                font-size: 2em;
            }
            
            .metric-value {
                font-size: 2em;
            }
            
            .metric-icon {
                font-size: 2em;
            }
            
            .main {
                padding: 0.5rem 1rem;
            }
        }
        
        /* ═══════════════════════════════════════════════════════════════ */
        /* HIDE STREAMLIT BRANDING */
        /* ═══════════════════════════════════════════════════════════════ */
        
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)

def _ensure_css_loaded():
    """Ensure the custom CSS is injected and re-injected when theme changes."""
    current_theme = st.session_state.get('theme', 'light')
    last_theme = st.session_state.get('_custom_css_loaded_theme')
    if (not st.session_state.get("_custom_css_loaded", False)) or (last_theme != current_theme):
        load_custom_css(current_theme)
        st.session_state["_custom_css_loaded"] = True
        st.session_state['_custom_css_loaded_theme'] = current_theme


# -----------------------------
# Excel Enhancement UI helpers
# -----------------------------
def _get_enhancement_module():
    """Import and return the enhancement module (core/adf_analyzer_v10_excel_enhancements)."""
    try:
        return importlib.import_module("core.adf_analyzer_v10_excel_enhancements")
    except Exception:
        # fallback: try relative import
        try:
            import core.adf_analyzer_v10_excel_enhancements as enh
            return enh
        except Exception:
            return None


def load_enhancement_config_for_ui():
    """Load enhancement config from module or default location for the dashboard UI."""
    enh = _get_enhancement_module()
    if enh is None:
        return {}

    # prefer module-level ENHANCEMENT_CONFIG if present
    cfg = getattr(enh, 'ENHANCEMENT_CONFIG', None)
    if isinstance(cfg, dict) and cfg:
        return copy.deepcopy(cfg)

    # try file next to module
    try:
        module_path = Path(enh.__file__).parent
        file_path = module_path / 'enhancement_config.json'
        if file_path.exists():
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception:
        pass

    # fallback to class default
    try:
        return copy.deepcopy(enh.EnhancementConfig.DEFAULT_CONFIG)
    except Exception:
        return {}


def save_enhancement_config_from_ui(new_config: Dict) -> Tuple[bool, str]:
    """Save enhancement config next to the enhancements module and update module variable.

    Returns (success, message)
    """
    enh = _get_enhancement_module()
    if enh is None:
        return False, "Enhancement module not found"

    try:
        module_path = Path(enh.__file__).parent
        file_path = module_path / 'enhancement_config.json'
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(new_config, f, indent=2)

        # update running module config so export code picks it up immediately
        try:
            setattr(enh, 'ENHANCEMENT_CONFIG', new_config)
        except Exception:
            pass

        return True, f"Saved config to {file_path}"
    except Exception as e:
        return False, f"Failed to save config: {e}"


def render_excel_enhancements_settings():
    """Render nested toggles for Excel enhancements in the sidebar and allow saving.

    This function writes to core/enhancement_config.json and updates the module ENHANCEMENT_CONFIG
    so export logic uses the toggles immediately.
    """
    cfg = load_enhancement_config_for_ui()
    if not cfg:
        st.sidebar.error("Could not load enhancement config")
        return

    enh = cfg.get('excel_enhancements', {})

    st.sidebar.markdown("---")
    exp = st.sidebar.expander("⚙️ Excel Enhancements settings", expanded=False)

    with exp:
        # master enable
        master = st.checkbox("Enable Excel Enhancements", value=enh.get('enabled', True), key='ui_excel_enh_enabled')

        new_enh = copy.deepcopy(enh)
        new_enh['enabled'] = master

        # Helper to render nested groups (defined inside expander scope)
        def _render_group(prefix: str, group: Dict):
            if not isinstance(group, dict):
                return {}
            result = {}
            subexp = st.expander(prefix, expanded=False)
            with subexp:
                for k, v in group.items():
                    if isinstance(v, bool):
                        cb = st.checkbox(k.replace('_',' ').title(), value=v, key=f"ui_excel_{prefix}_{k}")
                        result[k] = cb
                    elif isinstance(v, dict):
                        result[k] = _render_group(f"{prefix}.{k}", v)
                    else:
                        # keep non-bool values as-is (e.g., passwords, orientation)
                        if k.lower() == 'password':
                            current = v if isinstance(v, str) else ""
                            txt = st.text_input(k.replace('_',' ').title(), value=current, key=f"ui_excel_{prefix}_{k}")
                            result[k] = txt if txt else None
                        elif isinstance(v, str):
                            txt = st.text_input(k.replace('_',' ').title(), value=v, key=f"ui_excel_{prefix}_{k}")
                            result[k] = txt
                        else:
                            result[k] = v
            return result

        # Iterate top-level groups except enabled
        for group_name, group_value in enh.items():
            if group_name == 'enabled':
                continue
            new_enh[group_name] = _render_group(group_name, group_value)

        # Save / Reset buttons
        col1, col2 = st.columns([1,1])
        with col1:
            if st.button("Save Excel Settings", key='ui_excel_save'):
                full_config = {'excel_enhancements': new_enh}
                ok, msg = save_enhancement_config_from_ui(full_config)
                if ok:
                    st.success(msg)
                    # keep in session state
                    st.session_state['enhancement_config'] = full_config
                else:
                    st.error(msg)

        with col2:
            if st.button("Reset to defaults", key='ui_excel_reset'):
                # overwrite with defaults
                default_cfg = _get_enhancement_module().EnhancementConfig.DEFAULT_CONFIG
                ok, msg = save_enhancement_config_from_ui(default_cfg)
                if ok:
                    st.success("Reset to defaults and saved")
                    st.session_state['enhancement_config'] = default_cfg
                else:
                    st.error(msg)

# ═══════════════════════════════════════════════════════════════════════════
# PREMIUM UI COMPONENTS
# ═══════════════════════════════════════════════════════════════════════════

# Premium Color Configuration
class PremiumColors:
    """Advanced color configuration for charts and UI components"""
    
    # Chart palettes with dark shades
    GRADIENTS = {
    # Gradients defined via base brand colors only; keep single source of truth
    'gradient_1': ['#667eea', '#764ba2'],
    'gradient_2': ['#f093fb', '#f5576c'],
    'gradient_3': ['#4facfe', '#00f2fe'],
    'gradient_4': ['#43e97b', '#38f9d7'],
    'gradient_5': ['#fa709a', '#fee140'],
    'gradient_6': ['#30cfd0', '#330867'],
    'gradient_7': ['#a8edea', '#fed6e3'],
    'gradient_8': ['#ff9a56', '#ff6a88'],
    }
    
    # Primary colors
    # Core brand palette
    PRIMARY = '#667eea'
    SECONDARY = '#764ba2'
    ACCENT = '#f093fb'
    
    # Status colors with dark variants
    SUCCESS = '#10b981'
    SUCCESS_DARK = '#059669'
    WARNING = '#f59e0b'
    WARNING_DARK = '#d97706'
    DANGER = '#ef4444'
    DANGER_DARK = '#dc2626'
    INFO = '#3b82f6'
    INFO_DARK = '#2563eb'
    
    # Resource type colors
    PIPELINE = '#60a5fa'
    DATAFLOW = '#a78bfa'
    DATASET = '#34d399'
    TRIGGER = '#fbbf24'
    LINKEDSERVICE = '#f472b6'
    ORPHANED = '#fb923c'
    
    # Dark theme colors
    DARK_BG = 'rgba(15, 23, 42, 0.95)'
    DARK_SURFACE = 'rgba(30, 41, 59, 0.9)'
    DARK_TEXT = '#e2e8f0'
    DARK_MUTED = '#64748b'
    # Pastel ranges for gauge bands (low/medium/high/excellent) unified
    GAUGE_LOW = '#ffebee'
    GAUGE_MEDIUM = '#fff9c4'
    GAUGE_HIGH = '#e1f5fe'
    GAUGE_EXCELLENT = '#e8f5e9'

    @classmethod
    def get_status_color(cls, level: str) -> str:
        """Return semantic color by status/impact level."""
        if not level:
            return cls.INFO
        lv = str(level).upper()
        if lv in ('CRITICAL', 'HIGH'):
            return cls.DANGER if lv == 'CRITICAL' else cls.WARNING
        if lv == 'MEDIUM':
            return cls.INFO
        if lv == 'LOW':
            return cls.SUCCESS
        return cls.INFO


# Advanced Chart Template
PREMIUM_CHART_TEMPLATE = {
    # Keep the shared template intentionally minimal to avoid keyword
    # collisions when individual charts call update_layout(..., title=...,
    # legend=..., xaxis=..., yaxis=..., margin=..., etc.). Charts should
    # supply their own title/legend/xaxis/yaxis/margin settings when needed.
    'layout': {
        'hoverlabel': {
            'bgcolor': 'rgba(255, 255, 255, 0.95)',
            'font': {'size': 13, 'family': 'Inter', 'color': '#1e293b'},
            'bordercolor': 'rgba(102, 126, 234, 0.3)'
        }
    }
}

def render_premium_tile(
    icon: str,
    value: Any,
    label: str,
    variant: str = "purple",
    subtitle: Optional[str] = None,
    tooltip: Optional[str] = None,
    animated: bool = True,
    glow_effect: bool = True,
    style: Optional[str] = None,
):
    """
    Render ultra-modern premium tile with advanced glassmorphism design
    
    Args:
        icon: Emoji icon
        value: Main value to display
        label: Tile label
        variant: Color variant (purple, pink, blue, green, orange, teal, fire, success, warning, danger)
        subtitle: Optional subtitle text
        tooltip: Optional tooltip text
        animated: Enable advanced animations
        glow_effect: Enable neon glow effects on hover
    """
    # Format value if it's a number
    if isinstance(value, int):
        formatted_value = f"{value:,}"
    elif isinstance(value, float):
        formatted_value = f"{value:,.2f}"
    else:
        formatted_value = str(value)
    
    # Safely build subtitle and tooltip attributes (escape double quotes)
    safe_label = str(label).replace('"', '&quot;') if label is not None else ""
    safe_subtitle = str(subtitle).replace('"', '&quot;') if subtitle else None
    safe_tooltip = str(tooltip).replace('"', '&quot;') if tooltip else None

    # Build subtitle HTML with enhanced styling and title for full text
    subtitle_html = f'<div class="tile-subtitle" title="{safe_subtitle}">{subtitle}</div>' if subtitle else ""

    # Build tooltip attribute with enhanced information (if provided)
    tooltip_attr = f'title="{safe_tooltip}"' if safe_tooltip else ""
    
    # Add animation classes
    animation_class = "tile-animated" if animated else ""
    glow_class = "tile-glow" if glow_effect else ""
    # Decide tile shape/style automatically when not specified explicitly.
    # Rules (inferred):
    # - If caller provides 'style' or 'metric_type' in a tile dict, use it (pill/card/angled).
    # - If tile is compact or small (numeric value < 1000) -> pill.
    # - If tile is a typical metric (1000 <= value < 100000) -> rounded card.
    # - If tile is very large or should be highlighted (value >= 100000) -> angled (decorative).
    # These are reasonable heuristics and can be overridden by passing style via tiles_data dict.

    # Note: render_premium_tiles_grid will pass tile dict entries; here we check for
    # an optional 'style' key on the calling context by inspecting the outer frame
    # (best-effort). However most callers will pass style via tiles_data in the grid.

    # Default shape
    shape_class = "tile-card"

    # If the caller included a special flag in the global scope (rare), respect it.
    # Primary usage: tiles_data entries passed to render_premium_tiles_grid should include
    # 'style': 'pill'|'card'|'angled' or 'compact': True
    try:
        # try to get the last tiles_data item (if present in caller scope) - best-effort only
        caller_locals = None
    except Exception:
        caller_locals = None

    # Heuristic based on the numeric value
    try:
        numeric_value = float(value) if isinstance(value, (int, float, str)) and str(value).replace(',','').replace('%','').strip() != '' else None
    except Exception:
        numeric_value = None

    # If caller provided an explicit style via the tooltip_attr (not typical), skip.
    # Prefer explicit tile-level controls when using render_premium_tiles_grid.
    # Heuristic decisions:
    # Allow explicit override from caller
    if style and style in ("pill", "card", "angled"):
        shape_class = f"tile-{style}" if not style.startswith('tile-') else style
    elif isinstance(value, (int, float)):
        if abs(value) < 1000:
            shape_class = "tile-pill"
        elif abs(value) >= 100000:
            shape_class = "tile-angled"
        else:
            shape_class = "tile-card"
    else:
        # Non-numeric values: use compact pill when short label/text, otherwise card
        if isinstance(label, str) and len(label) <= 10:
            shape_class = "tile-pill"
        else:
            shape_class = "tile-card"

    st.markdown(f"""
    <div class="{shape_class} tile-{variant} {animation_class} {glow_class}" {tooltip_attr}>
        <div class="tile-icon">{icon}</div>
        <div class="tile-value">{formatted_value}</div>
        <div class="tile-label" title="{safe_label}">{label}</div>
        {subtitle_html}
    </div>
    """, unsafe_allow_html=True)


def render_premium_tiles_grid(
    tiles_data: List[Dict],
    columns: int = None,
    animated: bool = True,
    default_style: Optional[str] = None,
    force_uniform: bool = False,
):
    """
    Render ultra-modern grid of premium tiles with advanced features
    
    Args:
        tiles_data: List of tile dictionaries with keys: icon, value, label, variant, subtitle, tooltip
        columns: Number of columns (auto-fit if None)
        animated: Enable staggered animations
    """
    # Create responsive grid with modern styling
    grid_style = ""
    if columns:
        grid_style = f"grid-template-columns: repeat({columns}, 1fr);"
    
    st.markdown(f'''
    <div class="tiles-grid" style="{grid_style}">
    ''', unsafe_allow_html=True)
    
    for i, tile in enumerate(tiles_data):
        # Add staggered animation delay (kept for potential inline style usage)
        delay_style = f"animation-delay: {i * 0.1}s;" if animated else ""

        # Determine style to pass to renderer
        if force_uniform:
            # Force the same style for all tiles. If default_style is None, fallback to 'card'.
            style_to_use = default_style or 'card'
        else:
            # Use tile-specified style first, then default_style, else let renderer heuristics run
            style_to_use = tile.get('style') if tile.get('style') is not None else default_style

        render_premium_tile(
            icon=tile.get('icon', '📊'),
            value=tile.get('value', 0),
            label=tile.get('label', 'Metric'),
            variant=tile.get('variant', 'purple'),
            style=style_to_use,
            subtitle=tile.get('subtitle'),
            tooltip=tile.get('tooltip'),
            animated=animated,
            glow_effect=tile.get('glow_effect', True)
        )
    
    st.markdown('</div>', unsafe_allow_html=True)


def create_premium_chart(chart_type: str = "bar", **kwargs):
    """
    Create premium styled chart with glassmorphism design
    
    Args:
        chart_type: Type of chart (bar, line, pie, scatter, etc.)
        **kwargs: Additional chart parameters
    
    Returns:
        Plotly figure with premium styling
    """
    if chart_type == "bar":
        fig = go.Figure(go.Bar(**kwargs))
    elif chart_type == "line":
        fig = go.Figure(go.Scatter(mode='lines+markers', **kwargs))
    elif chart_type == "pie":
        fig = go.Figure(go.Pie(**kwargs))
    elif chart_type == "scatter":
        fig = go.Figure(go.Scatter(mode='markers', **kwargs))
    else:
        fig = go.Figure()
    
    # Apply premium template
    fig.update_layout(**PREMIUM_CHART_TEMPLATE['layout'])
    
    return fig


def apply_dark_theme_to_chart(fig):
    """Apply dark theme styling to chart"""
    fig.update_layout(
        paper_bgcolor=PremiumColors.DARK_BG,
        plot_bgcolor=PremiumColors.DARK_SURFACE,
        font_color=PremiumColors.DARK_TEXT,
        title_font_color=PremiumColors.DARK_TEXT,
        xaxis=dict(
            gridcolor='rgba(71, 85, 105, 0.3)',
            linecolor='rgba(71, 85, 105, 0.5)',
            tickcolor='rgba(71, 85, 105, 0.5)',
            title_font_color=PremiumColors.DARK_MUTED,
            tickfont_color=PremiumColors.DARK_MUTED
        ),
        yaxis=dict(
            gridcolor='rgba(71, 85, 105, 0.3)',
            linecolor='rgba(71, 85, 105, 0.5)',
            tickcolor='rgba(71, 85, 105, 0.5)',
            title_font_color=PremiumColors.DARK_MUTED,
            tickfont_color=PremiumColors.DARK_MUTED
        ),
        legend=dict(
            bgcolor='rgba(30, 41, 59, 0.9)',
            bordercolor='rgba(71, 85, 105, 0.5)',
            font_color=PremiumColors.DARK_TEXT
        ),
        hoverlabel=dict(
            bgcolor='rgba(30, 41, 59, 0.95)',
            font_color=PremiumColors.DARK_TEXT,
            bordercolor='rgba(102, 126, 234, 0.5)'
        )
    )
    return fig

def apply_theme_to_chart(fig):
    """Apply light or dark theme styling dynamically based on session state.

    Replaces apply_dark_theme_to_chart with a unified approach using theme vars.
    """
    import streamlit as st
    theme = st.session_state.get('theme', 'light')
    if theme == 'dark':
        return apply_dark_theme_to_chart(fig)

    # Light theme settings
    fig.update_layout(
        paper_bgcolor='#f5f7fb',
    plot_bgcolor='#ffffff',
        font_color='#1e293b',
        title_font_color='#0f172a',
        xaxis=dict(
            gridcolor='rgba(148, 163, 184, 0.25)',
            linecolor='rgba(100, 116, 139, 0.35)',
            tickcolor='rgba(100, 116, 139, 0.35)',
            title_font_color='#64748b',
            tickfont_color='#64748b'
        ),
        yaxis=dict(
            gridcolor='rgba(148, 163, 184, 0.25)',
            linecolor='rgba(100, 116, 139, 0.35)',
            tickcolor='rgba(100, 116, 139, 0.35)',
            title_font_color='#64748b',
            tickfont_color='#64748b'
        ),
        legend=dict(
            bgcolor='rgba(255,255,255,0.85)',
            bordercolor='rgba(148,163,184,0.35)',
            font_color='#1e293b'
        ),
        hoverlabel=dict(
            bgcolor='rgba(255,255,255,0.95)',
            font_color='#1e293b',
            bordercolor='rgba(102,126,234,0.3)'
        )
    )
    return fig

# Theme-aware helpers for text and line colors inside Plotly traces
def theme_is_dark() -> bool:
    try:
        return st.session_state.get('theme', 'light') == 'dark'
    except Exception:
        return False

def theme_text_color() -> str:
    return '#f1f5f9' if theme_is_dark() else '#1e293b'

def theme_line_color(alpha: float = 0.8) -> str:
    if theme_is_dark():
        return f'rgba(255, 255, 255, {alpha})'
    # deep slate for visibility on light backgrounds
    return f'rgba(17, 24, 39, {alpha})'

def theme_overlay_bg(opacity: float = 0.95) -> str:
    return f'rgba(30, 41, 59, {opacity})' if theme_is_dark() else f'rgba(255, 255, 255, {opacity})'

def render_premium_header():
    """Render premium glassmorphism header"""
    st.markdown(f"""
    <div class="premium-header">
        <h1>🏭 Azure Data Factory Analyzer</h1>
        <p>Enterprise Analysis Dashboard - Premium Edition</p>
        <span class="version-badge">v10.1 • Production Ready</span>
    </div>
    """, unsafe_allow_html=True)


def render_metric_card_premium(
    icon: str,
    label: str,
    value: Any,
    gradient: str = "gradient-purple",
    delta: Optional[str] = None,
    delta_positive: bool = True
):
    """
    Render premium metric card
    
    Args:
        icon: Emoji icon
        label: Metric label
        value: Metric value
        gradient: CSS gradient class
        delta: Optional delta value
        delta_positive: Whether delta is positive (green) or negative (red)
    """
    delta_html = ""
    if delta:
        delta_class = "positive" if delta_positive else "negative"
        delta_html = f'<div class="metric-delta {delta_class}">{delta}</div>'
    
    # Format number if it's an integer
    formatted_value = f"{int(value):,}" if isinstance(value, int) else str(value)
    
    # Safe title attributes (escape double quotes)
    safe_label = str(label).replace('"', '&quot;') if label is not None else ""
    card_title = f"{safe_label} - {formatted_value}"

    st.markdown(f"""
    <div class="metric-card-premium {gradient}" title="{card_title}">
        <div class="metric-icon">{icon}</div>
        <div class="metric-label" title="{safe_label}">{label}</div>
        <div class="metric-value">{formatted_value}</div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)


def render_info_card_premium(title: str, content: str, border_color: str = PremiumColors.PRIMARY):
    """Render premium info card"""
    st.markdown(f"""
    <div class="info-card-premium" style="border-left-color: {border_color};">
        <h4>{title}</h4>
        <p>{content}</p>
    </div>
    """, unsafe_allow_html=True)


def render_premium_footer():
    """Render premium footer"""
    st.markdown("""
    <div style="
        text-align: center;
        padding: 2rem 1rem;
        margin-top: 3rem;
        background: rgba(255, 255, 255, 0.8);
        backdrop-filter: blur(20px);
        border-radius: 16px;
        border: 1px solid rgba(255, 255, 255, 0.3);
    ">
        <p style="margin: 0; opacity: 0.7; font-size: 0.9em;">
            Made with ❤️ by Enterprise ADF Team
        </p>
        <p style="margin: 5px 0 0 0; opacity: 0.6; font-size: 0.85em;">
            © 2024 ADF Analyzer v10.1 Production Edition
        </p>
    </div>
    """, unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════
# LEGACY UI COMPONENTS (Backward Compatibility)
# ═══════════════════════════════════════════════════════════════════════════

def render_info_card(title: str, body: str, color: str = None, small: bool = False):
    """Render a consistent info-card using the app CSS.

    Args:
        title: Heading text (can include emoji)
        body: HTML or plain text for card body (can include small tags)
        color: Optional hex color string to set the left border and title color
        small: If True use a smaller font for body
    """
    _ensure_css_loaded()
    border_style = f"border-left: 4px solid {color};" if color else ""
    title_color = f"color: {color};" if color else ""
    small_class = "font-size:0.95em;" if small else ""

    html = f"""
<div class="info-card" style="{border_style}">
    <h4 style="{title_color}">{title}</h4>
    <div style="{small_class}">{body}</div>
</div>
"""
    st.markdown(html, unsafe_allow_html=True)

def render_feature_card(title: str, bullets: List[str], hint: str = None):
    """Render a visually prominent gradient feature card (matches the sample look).

    Uses a safe subset of CSS (gradient background, rounded corners) that Streamlit
    supports via inline styles.
    """
    _ensure_css_loaded()
    bullets_html = "".join([f"<p>• {b}</p>" for b in bullets])
    hint_html = f"<p style='color:#999; margin-top:12px;'>{hint}</p>" if hint else ""

    html = f"""
<div style="background: linear-gradient(135deg, #667eea15 0%, #764ba215 100%); padding: 20px; border-radius: 12px; margin: 12px 0;">
    <h3 class="app-heading" style="margin-bottom:12px;">{title}</h3>
    <div style="text-align: left; display: inline-block; max-width: 720px;">
        {bullets_html}
    </div>
    {hint_html}
</div>
"""
    st.markdown(html, unsafe_allow_html=True)

def prepare_pie_data(df: pd.DataFrame, label_col: str, value_col: str, top_n: Optional[int] = None):
    """Helper to prepare pie chart labels and values safely.

    - Coerces value_col to numeric
    - Groups by label_col and sums values
    - Sorts descending and optionally takes top_n
    - Drops zero-value entries
    Returns (labels, values) as lists.
    """
    if df is None or df.empty:
        return [], []

    if label_col not in df.columns or value_col not in df.columns:
        return [], []

    tmp = df[[label_col, value_col]].copy()
    tmp[value_col] = pd.to_numeric(tmp[value_col], errors="coerce").fillna(0)
    grouped = tmp.groupby(label_col, as_index=False)[value_col].sum()
    grouped = grouped[grouped[value_col] > 0].sort_values(value_col, ascending=False)

    if top_n:
        grouped = grouped.head(top_n)

    labels = grouped[label_col].astype(str).tolist()
    values = grouped[value_col].astype(int).tolist()
    return labels, values

def to_csv_bytes(df: pd.DataFrame) -> bytes:
    """Return CSV bytes with UTF-8 BOM so Excel opens it correctly."""
    try:
        csv_str = df.to_csv(index=False, encoding="utf-8-sig")
        return csv_str.encode("utf-8-sig")
    except Exception:
        # Fallback without BOM
        return df.to_csv(index=False).encode("utf-8")

def to_json_bytes(obj: Any) -> bytes:
    """Return JSON bytes (utf-8)."""
    return json.dumps(obj, indent=2, default=str).encode("utf-8")

def to_excel_bytes(dfs: Dict[str, pd.DataFrame]) -> bytes:
    """Write a dict of DataFrames to an in-memory Excel workbook and return bytes.

    dfs: mapping of sheet_name -> DataFrame
    """
    buffer = io.BytesIO()
    try:
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            for sheet_name, df in dfs.items():
                try:
                    df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
                except Exception:
                    # If df is not a DataFrame, skip
                    continue
        return buffer.getvalue()
    except Exception:
        return b""

# SESSION STATE INITIALIZATION

def initialize_session_state():
    """Initialize all session state variables with defaults"""

    # Data state
    defaults = {
        "data_loaded": False,
        "excel_data": {},
        "dependency_graph": None,
        "analysis_metadata": {},
            "show_debug_panel": False,
        # UI state
        "selected_theme": "dark",
        "filter_options": ["All"],
        "search_query": "",
        "selected_pipeline": None,
        # Cache
        "cached_graphs": {},
        "cached_metrics": {},
        # File upload tracking
        "uploaded_file_name": None,
        "last_load_time": None,
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

# UTILITY FUNCTIONS

def safe_get_dataframe(sheet_name: str, *alternative_names: str) -> pd.DataFrame:
    """
    Safely get DataFrame from excel_data with fallback names

    Args:
        sheet_name: Primary sheet name to look for
        *alternative_names: Alternative sheet names to try

    Returns:
        DataFrame if found, empty DataFrame otherwise
    """
    # Try primary name (exact)
    excel_data = st.session_state.excel_data or {}

    if sheet_name in excel_data:
        df = excel_data[sheet_name]
        if isinstance(df, pd.DataFrame):
            return df

    # Try alternatives (exact)
    for alt_name in alternative_names:
        if alt_name in excel_data:
            df = excel_data[alt_name]
            if isinstance(df, pd.DataFrame):
                return df

    # Fallback: try normalized matching (ignore case, underscores, spaces)
    def _normalize(key: str) -> str:
        return re.sub(r"[_\s]+", "", str(key)).lower()

    target_norm = _normalize(sheet_name)

    # Check exact keys normalized
    for key, df in excel_data.items():
        try:
            if _normalize(key) == target_norm and isinstance(df, pd.DataFrame):
                return df
        except Exception:
            continue

    # Try normalized alternatives
    for alt_name in alternative_names:
        alt_norm = _normalize(alt_name)
        for key, df in excel_data.items():
            try:
                if _normalize(key) == alt_norm and isinstance(df, pd.DataFrame):
                    return df
            except Exception:
                continue

    # Special-case fallbacks / synthesized sheets
    # 1) GlobalParameterUsage: if not present but GlobalParameters exists, synthesize a usage table
    try:
        if target_norm in ("globalparameterusage", "globalparameter_usage"):
            # find GlobalParameters sheet (normalized)
            for key, df in excel_data.items():
                if _normalize(key) == "globalparameters" and isinstance(df, pd.DataFrame):
                    gp = df
                    # Synthesize usage counts = 0 (best-effort) so charts render
                    synth_rows = []
                    # try to find a column that looks like name
                    name_col = None
                    for c in gp.columns:
                        if "name" in c.lower() or "parameter" in c.lower():
                            name_col = c
                            break
                    if name_col is None and len(gp.columns) > 0:
                        name_col = gp.columns[0]

                    for _, r in gp.iterrows():
                        pname = r.get(name_col, "") if name_col else ""
                        synth_rows.append({
                            "ParameterName": pname,
                            "TotalUsages": 0,
                            "UniqueResources": 0,
                            "UsageByType": "",
                            "SampleUsages": "",
                        })

                    return pd.DataFrame(synth_rows)
    except Exception:
        pass

    # 2) FactoryInfo: synthesize basic factory info from Summary or uploaded file name
    try:
        if target_norm in ("factoryinfo", "factory_info"):
            # try to build from Summary sheet metrics if available
            summary_df = excel_data.get("Summary") or excel_data.get("summary")
            factory_name = None
            location = "Unknown"
            identity = "Unknown"
            public_network = "Unknown"
            encryption = "Unknown"

            if isinstance(summary_df, pd.DataFrame) and not summary_df.empty:
                try:
                    metrics = dict(summary_df.set_index("Metric")["Value"])
                    factory_name = metrics.get("FactoryName") or metrics.get("Factory Name")
                    location = metrics.get("Location", location)
                    identity = metrics.get("IdentityType", identity)
                    public_network = metrics.get("PublicNetworkAccess", public_network)
                    encryption = metrics.get("EncryptionEnabled", encryption)
                except Exception:
                    pass

            # Fallback to uploaded file name if still unknown
            if not factory_name:
                factory_name = st.session_state.get("uploaded_file_name") or "UnknownFactory"

            return pd.DataFrame([
                {
                    "FactoryName": factory_name,
                    "Location": location,
                    "IdentityType": identity,
                    "PublicNetworkAccess": public_network,
                    "EncryptionEnabled": encryption,
                }
            ])
    except Exception:
        pass

    # 3) DataDictionary: synthesize a light-weight data dictionary by inspecting available sheets
    try:
        if target_norm in ("datadictionary", "data_dictionary"):
            rows = []
            for sname, df in excel_data.items():
                if not isinstance(df, pd.DataFrame):
                    continue
                for col in df.columns:
                    try:
                        dtype = str(df[col].dtype)
                    except Exception:
                        dtype = "object"
                    example = ""
                    try:
                        sample = df[col].dropna()
                        if not sample.empty:
                            example = str(sample.iloc[0])
                    except Exception:
                        example = ""

                    rows.append(
                        {
                            "Sheet": sname,
                            "Column": col,
                            "Description": "",
                            "DataType": dtype,
                            "Example": example,
                        }
                    )

            return pd.DataFrame(rows)
    except Exception:
        pass

    # 4) Credentials: synthesize an empty credentials table if missing
    try:
        if target_norm in ("credentials", "credential", "credentialinfo"):
            return pd.DataFrame(
                columns=["LinkedService", "CredentialType", "SecretName", "Notes"]
            )
    except Exception:
        pass

    # 5) Managed Private Endpoints / Managed VNets: provide empty placeholders
    try:
        if target_norm in ("managedprivateendpoints", "managed_private_endpoints"):
            return pd.DataFrame(columns=["Name", "ResourceId", "LinkedService", "State"])
    except Exception:
        pass

    try:
        if target_norm in ("managedvnets", "managed_vnets", "managedvnet"):
            return pd.DataFrame(columns=["Name", "ResourceId", "Type", "Notes"])
    except Exception:
        pass

    # 6) Errors: empty errors table
    try:
        if target_norm in ("errors", "errorlog"):
            return pd.DataFrame(columns=["ErrorType", "Message", "Object"])
    except Exception:
        pass

    # 7) CircularDependencies: empty placeholder
    try:
        if target_norm in ("circulardependencies", "circular_dependencies"):
            return pd.DataFrame(columns=["Pipeline", "CyclePath"])
    except Exception:
        pass
    except Exception:
        pass

    # Not found
    return pd.DataFrame()

def get_summary_metric(metric_name: str, default: Any = 0) -> Any:
    """
    Get metric from Summary sheet

    Args:
        metric_name: Name of the metric
        default: Default value if not found

    Returns:
        Metric value or default
    """
    summary = safe_get_dataframe("Summary")

    if summary.empty or "Metric" not in summary.columns:
        return default

    try:
        raw = summary.set_index("Metric")["Value"].to_dict()
        # Coerce numeric-looking values to numbers so callers (counts/metrics)
        # can rely on numeric types even when Summary sheet stores strings.
        metrics = {}
        for k, v in raw.items():
            try:
                # Preserve NaN/null
                if pd.isna(v):
                    metrics[k] = v
                    continue

                # Strings like '1,234' or '1234' -> numbers
                if isinstance(v, str):
                    s = v.strip().replace(",", "")
                    # Percentage values like '90%'
                    if s.endswith("%"):
                        try:
                            metrics[k] = float(s.rstrip("%")) / 100.0
                            continue
                        except Exception:
                            pass

                num = pd.to_numeric(v, errors="coerce")
                if not pd.isna(num):
                    # Convert integer-valued floats to ints for cleaner display
                    if float(num).is_integer():
                        metrics[k] = int(num)
                    else:
                        metrics[k] = float(num)
                else:
                    metrics[k] = v
            except Exception:
                metrics[k] = v

        return metrics.get(metric_name, default)
    except Exception:
        return default

def get_count_with_fallback(metric_name: str, fallback_sheets: List[str]) -> int:
    """
    Retrieve a numeric count from the Summary sheet, coercing strings to numbers,
    and fallback to counting rows in one of the provided sheets when the metric
    is missing or zero.

    Args:
        metric_name: Metric name in Summary sheet (e.g., 'Pipelines')
        fallback_sheets: List of possible sheet names to check for row counts

    Returns:
        int count (0 if nothing found)
    """
    val = get_summary_metric(metric_name, 0)

    try:
        if isinstance(val, (int, float)) and not (isinstance(val, bool)):
            if int(val) > 0:
                return int(val)
        # If val is a numeric-looking string, get_summary_metric already coerces it.
    except Exception:
        pass

    # Fallback: inspect sheets for counts
    for s in fallback_sheets:
        df = safe_get_dataframe(s)
        if isinstance(df, pd.DataFrame) and not df.empty:
            return len(df)

    # Another fallback: if dependency graph exists and metric_name mentions 'Dependencies'
    if "Dependency" in metric_name or "Dependencies" in metric_name:
        g = st.session_state.get("dependency_graph")
        if g is not None:
            try:
                return g.number_of_edges()
            except Exception:
                pass

    return 0

def format_number(num: int) -> str:
    """Format number with thousand separators"""
    try:
        return f"{int(num):,}"
    except:
        return str(num)

def sum_numeric_columns_by_keywords(df: pd.DataFrame, keywords: List[str]) -> int:
    """Sum numeric-looking columns whose names contain any of the provided keywords.

    This is a robust helper for when sheet column names vary (e.g. "Sources",
    "SourceCount", "NumSources", "Source(s)"). Case-insensitive.
    """
    if df is None or df.empty:
        return 0

    total = 0
    for col in df.columns:
        try:
            name = str(col).lower()
            if any(k.lower() in name for k in keywords):
                # coerce column to numeric then sum
                series = pd.to_numeric(df[col], errors="coerce").fillna(0)
                total += int(series.sum())
        except Exception:
            continue
    return int(total)

def truncate_text(text: str, max_length: int = 50) -> str:
    """Truncate text with ellipsis"""
    text = str(text)
    if len(text) <= max_length:
        return text
    return text[: max_length - 3] + "..."

def _merge_split_sheets_inplace(excel_dict: Dict[str, pd.DataFrame]) -> None:
    """Detect sheets split with suffix _P1/_P2/... and merge them into a single sheet.

    This mutates the supplied dict and creates a merged DataFrame under the base
    name if that base name does not already exist. The analyzer uses the pattern
    <SheetName>_P1, <SheetName>_P2 for auto-split exports.
    """
    groups = {}
    for name in list(excel_dict.keys()):
        m = re.match(r"^(.+)_P(\d+)$", name, re.IGNORECASE)
        if m:
            base = m.group(1)
            idx = int(m.group(2))
            groups.setdefault(base, []).append((idx, name))

    for base, parts in groups.items():
        parts.sort()
        frames = []
        for _, part_name in parts:
            df = excel_dict.get(part_name)
            if isinstance(df, pd.DataFrame):
                frames.append(df)

        if frames:
            try:
                merged = pd.concat(frames, ignore_index=True)
                # Only add merged if base not already present (to avoid overwriting)
                if base not in excel_dict:
                    excel_dict[base] = merged
                else:
                    # provide a merged alias if base exists
                    excel_dict[f"{base}_MERGED"] = merged
            except Exception:
                # If concat fails, skip merging but preserve original parts
                continue

def _normalize_sheet_map_inplace(excel_dict: Dict[str, pd.DataFrame]) -> None:
    """Create convenient aliases in the excel_data map for common variants.

    This does not duplicate dataframes unnecessarily; it only adds new keys
    that reference the same DataFrame objects for tolerant lookups.
    """
    def norm(k: str) -> str:
        return re.sub(r"[_\s]+", "", str(k)).lower()

    # Build a mapping of normalized -> existing key (prefer exact matches)
    norm_map: Dict[str, str] = {}
    for key in list(excel_dict.keys()):
        try:
            n = norm(key)
            if n not in norm_map:
                norm_map[n] = key
        except Exception:
            continue

    # Add aliases for some commonly referenced names (safety net)
    aliases = [
        ("linkedserviceusage", ["LinkedServiceUsage", "LinkedService_Usage", "linkedservice_usage"]),
        ("integrationruntimeusage", ["IntegrationRuntimeUsage", "IntegrationRuntime_Usage", "integrationruntime_usage"]),
        ("globalparameterusage", ["GlobalParameterUsage", "Global_Parameter_Usage", "globalparameter_usage"]),
        ("datasetusage", ["DatasetUsage", "Dataset_Usage", "dataset_usage"]),
    ]

    for canonical, variants in aliases:
        # if canonical already resolves, skip
        if canonical in norm_map:
            continue
        for v in variants:
            if v in excel_dict:
                norm_map[canonical] = v
                break

    # Inject alias keys that point to existing DataFrames
    for nkey, existing in norm_map.items():
        if nkey in excel_dict:
            continue
        df = excel_dict.get(existing)
        if isinstance(df, pd.DataFrame):
            excel_dict[nkey] = df

def safe_plotly(
    fig: Optional[go.Figure],
    df: Optional[pd.DataFrame] = None,
    required_columns: Optional[List[str]] = None,
    info_message: Optional[str] = None,
    use_container_width: bool = True,
):
    """
    Safely render a plotly figure in Streamlit.

    - If `df` is provided, ensure it's a DataFrame and (optionally) contains the
      `required_columns`. If checks fail, show a friendly message instead of
      attempting to render the chart.
    - This allows centralizing chart guards so individual renderers remain concise.
    """
    try:
        # If no figure was provided, show a friendly message instead of raising
        if fig is None:
            st.info(info_message or " No chart available to render")
            return

        if df is not None:
            if not isinstance(df, pd.DataFrame) or df.empty:
                st.info(info_message or " No data available for this chart")
                return

            if required_columns:
                missing = [c for c in required_columns if c not in df.columns]
                if missing:
                    st.info(
                        info_message
                        or f" Chart data missing required columns: {', '.join(missing)}"
                    )
                    return

        # If checks passed (or none required), theme the figure and render
        try:
            fig = apply_theme_to_chart(fig)
        except Exception:
            pass
        st.plotly_chart(fig, use_container_width=use_container_width)
    except Exception as e:
        st.error(f" Could not render chart: {e}")
        return

# MAIN APPLICATION CLASS

class ADF_Dashboard:
    """Enterprise ADF Analysis Dashboard"""

    # Color schemes
    COLORS = {
        "primary": PremiumColors.PRIMARY,
        "secondary": PremiumColors.SECONDARY,
        "success": PremiumColors.SUCCESS,
        "danger": PremiumColors.DANGER,
        "warning": PremiumColors.WARNING,
        "info": PremiumColors.INFO,
        "trigger": PremiumColors.TRIGGER,
        "dataflow": PremiumColors.DATAFLOW,
        "pipeline": PremiumColors.PIPELINE,
        "dataset": PremiumColors.DATASET,
        "orphaned": PremiumColors.ORPHANED,
    }

    def __init__(self):
        """Initialize dashboard"""
        initialize_session_state()
        # Initialize theme in session_state if not present
        if 'theme' not in st.session_state:
            st.session_state['theme'] = 'dark'
        # Load CSS with current theme
        load_custom_css(st.session_state['theme'])

    def run(self):
        """Main entry point"""

        # Render header
        self.render_header()

        # Render sidebar
        with st.sidebar:
            self.render_sidebar()

        # Main content - Check if launcher should be shown
        if not st.session_state.get("app_mode_selected", False):
            self.render_launcher()
        else:
            self.render_main_content_with_tabs()

    def render_header(self):
        """Render premium glassmorphism header"""
        render_premium_header()

    def render_sidebar(self):
        """Render sidebar controls"""

        # Enhanced Branding with Liquid Theme
        st.markdown(
            f"""
        <div class="sidebar-header fade-in">
            <h2 style="margin:0; color: var(--heading-color); font-weight:800; font-size:1.55em; letter-spacing:0.5px;">🎛️ Control Center</h2>
            <p style="margin:8px 0 0 0; opacity:0.75; font-size:0.9em; font-weight:500; color: var(--muted);">
                ✨ Enterprise Analytics Suite
            </p>
            <div style="margin-top:12px; padding:6px 14px; background: var(--tile-bg); border:1px solid rgba(0,0,0,0.06); border-radius:16px; font-size:0.75em; font-weight:600; color: var(--heading-color);">
                🚀 Production Ready v10.1
            </div>
        </div>
            """,
            unsafe_allow_html=True,
        )

        # THEME TOGGLE CONTROLS
        with st.container():
            col_theme_1, col_theme_2 = st.columns(2)
            with col_theme_1:
                if st.button("🌙 Dark", key="set_theme_dark", use_container_width=True,
                             help="Switch to dark theme for reduced glare"):
                    st.session_state['theme'] = 'dark'
                    load_custom_css(st.session_state['theme'])
                    st.rerun()
            with col_theme_2:
                if st.button("☀ Light", key="set_theme_light", use_container_width=True,
                             help="Switch to light theme for maximum contrast"):
                    st.session_state['theme'] = 'light'
                    load_custom_css(st.session_state['theme'])
                    st.rerun()

        st.markdown("---")

        # Excel Enhancements settings (renders the sidebar UI controls)
        try:
            render_excel_enhancements_settings()
        except Exception:
            # non-fatal: continue if UI helper isn't available
            pass

        st.markdown("---")

        # AI Chat sidebar configuration
        if HAS_AI_CHAT:
            try:
                initialize_ai_session_state()
                render_ai_sidebar()
                st.markdown("---")
            except Exception:
                pass

        # FILE UPLOAD SECTION with Enhanced Styling
        mode = st.session_state.get('app_mode', 'generate')

        if mode != 'analyze':
            st.markdown(
                """
            <div class="sidebar-section slide-in">
                <h3 class="app-heading" style="margin:0 0 15px 0; font-size:1.05em;">
                    📁 Data Input
                </h3>
            </div>
            """,
                unsafe_allow_html=True,
            )
            
            uploaded_file = st.file_uploader(
                "Upload Analysis Excel",
                type=["xlsx", "xls"],
                help="📊 Upload your ADF analysis Excel file",
                label_visibility="collapsed"
            )

            col1, col2 = st.columns(2)
            with col1:
                if uploaded_file:
                    if st.button("📤 Load", type="primary", use_container_width=True):
                        self.load_excel_file(uploaded_file)
                        
            with col2:
                if st.button("🎮 Sample", use_container_width=True, help="Load demo data"):
                    self.load_sample_data()
        else:
            st.markdown(
                """
            <div class="sidebar-section slide-in">
                <h3 class="app-heading" style="margin:0 0 10px 0; font-size:1.05em;">
                    📁 Upload & Analyze Mode
                </h3>
                <p style="text-align: center; padding: 15px; 
                          background: rgba(79, 172, 254, 0.1); 
                          border-radius: 12px; margin: 0; font-weight: 500;">
                    📊 Use the main area to upload your Excel file
                </p>
            </div>
            """,
                unsafe_allow_html=True,
            )

        # NAVIGATION
        st.markdown("---")
        st.markdown(
            """
        <div class="sidebar-section">
            <h3 class="app-heading" style="margin:0 0 15px 0; font-size:1.05em;">
                🧭 Navigation
            </h3>
        </div>
        """,
            unsafe_allow_html=True,
        )
        
        if st.button("🏠 Back to Launcher", key="sidebar_back_launcher", use_container_width=True):
            for k in ['app_mode', 'app_mode_selected']:
                if k in st.session_state:
                    del st.session_state[k]
            st.rerun()

        # STATUS SECTION with Enhanced Design
        st.markdown("---")
        st.markdown(
            """
        <div class="sidebar-section">
            <h3 class="app-heading" style="margin:0 0 15px 0; font-size:1.05em;">
                📊 Status Dashboard
            </h3>
        </div>
        """,
            unsafe_allow_html=True,
        )

        if st.session_state.data_loaded:
            # Enhanced Status Display
            st.markdown(
                """
            <div style="background: rgba(67, 233, 123, 0.1); padding: 15px; 
                        border-radius: 12px; margin-bottom: 15px; border: 1px solid rgba(67, 233, 123, 0.3);">
                <div style="display: flex; align-items: center; margin-bottom: 10px;">
                    <span style="font-size: 1.2em; margin-right: 8px;">✅</span>
                    <strong style="color: #43e97b;">Data Loaded Successfully</strong>
                </div>
                <div style="font-size: 0.9em; opacity: 0.8;">
                    ⏰ <strong>{}</strong>
                </div>
            </div>
            """.format(
                st.session_state.last_load_time.strftime('%H:%M:%S') 
                if st.session_state.last_load_time else 'Just now'
            ),
                unsafe_allow_html=True,
            )
            
            # Quick Stats with Liquid Theme
            self.render_sidebar_stats()
            
            # Advanced Filters
            st.markdown("---")
            self.render_sidebar_filters()
        else:
            st.markdown(
                """
            <div style="background: rgba(255, 154, 86, 0.1); padding: 15px; 
                        border-radius: 12px; margin-bottom: 15px; border: 1px solid rgba(255, 154, 86, 0.3);">
                <div style="display: flex; align-items: center; margin-bottom: 8px;">
                    <span style="font-size: 1.2em; margin-right: 8px;">⏳</span>
                    <strong style="color: #ff9a56;">Waiting for Data</strong>
                </div>
                <div style="font-size: 0.9em; opacity: 0.8;">
                    Upload an Excel file or load sample data to begin analysis
                </div>
            </div>
            """,
                unsafe_allow_html=True,
            )

        # ENHANCED DOCUMENTATION ACCESS
        st.markdown("---")
        st.markdown(
            """
        <div class="sidebar-section">
            <h3 class="app-heading" style="margin:0 0 15px 0; font-size:1.05em;">
                📚 Documentation Hub
            </h3>
        </div>
        """,
            unsafe_allow_html=True,
        )

        # Documentation viewer with fixed paths
        doc_option = st.selectbox(
            "View Documentation",
            ["Select document...", "📋 Tile Reference (TILES.md)", "🧠 Logic Documentation (LOGIC.md)"],
            key="doc_viewer"
        )

        if doc_option == "📋 Tile Reference (TILES.md)":
            with st.expander("📋 View TILES.md", expanded=False):
                try:
                    # Fixed path to docs folder
                    tiles_path = Path(__file__).parent / "docs" / "TILES.md"
                    if tiles_path.exists():
                        with open(tiles_path, 'r', encoding='utf-8') as f:
                            tiles_content = f.read()
                        st.markdown(tiles_content)
                    else:
                        st.warning("📋 TILES.md not found in docs/ directory")
                        st.info("💡 Make sure TILES.md exists in the docs/ folder")
                except Exception as e:
                    st.error(f"❌ Error loading TILES.md: {e}")

        elif doc_option == "🧠 Logic Documentation (LOGIC.md)":
            with st.expander("🧠 View LOGIC.md", expanded=False):
                try:
                    # Fixed path to docs folder
                    logic_path = Path(__file__).parent / "docs" / "LOGIC.md"
                    if logic_path.exists():
                        with open(logic_path, 'r', encoding='utf-8') as f:
                            logic_content = f.read()
                        st.markdown(logic_content)
                    else:
                        st.warning("🧠 LOGIC.md not found in docs/ directory")
                        st.info("💡 Make sure LOGIC.md exists in the docs/ folder")
                except Exception as e:
                    st.error(f"❌ Error loading LOGIC.md: {e}")

        # ENHANCED FOOTER
        st.markdown("---")
        
        # Developer debug toggle with enhanced styling
        st.markdown(
            """
        <div class="sidebar-section">
            <h3 class="app-heading" style="margin:0 0 10px 0; font-size:1em;">
                🔧 Developer Tools
            </h3>
        </div>
        """,
            unsafe_allow_html=True,
        )
        st.checkbox("🐛 Show debug panel", value=st.session_state.get("show_debug_panel", False), key="show_debug_panel")

        st.markdown("---")
        st.markdown(
            """
        <div style="text-align: center; opacity: 0.7; font-size: 0.8em; 
                    background: rgba(255, 255, 255, 0.05); backdrop-filter: blur(10px);
                    border-radius: 12px; padding: 15px; margin-top: 20px;">
            <div style="margin-bottom: 8px;">
                <span style="font-size: 1.2em;">💖</span>
            </div>
            <p style="margin: 0; font-weight: 500;">Made with passion by ADF Analytics Team</p>
            <p style="margin: 5px 0 0 0; font-size: 0.75em;">
                🚀 Enterprise Edition v10.1 | 
                <span style="color: #43e97b;">✨ Production Ready</span>
            </p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    def render_sidebar_stats(self):
        """Render quick stats in sidebar with premium metric cards"""

        st.markdown(
            """
        <div class="sidebar-section">
            <h3 class="app-heading" style="margin:0 0 15px 0; font-size:1.05em;">
                ⚡ Quick Metrics
            </h3>
        </div>
        """,
            unsafe_allow_html=True,
        )
        
        # Use robust fallbacks in case Summary is missing or contains strings
        pipelines = get_count_with_fallback(
            "Pipelines", ["ImpactAnalysis", "PipelineAnalysis", "Pipeline_Analysis", "Pipelines"]
        )
        dataflows = get_count_with_fallback(
            "DataFlows", ["DataFlows", "DataFlowLineage", "DataFlow_Summary"]
        )
        orphaned = get_count_with_fallback(
            "Orphaned Pipelines", ["OrphanedPipelines", "Orphaned_Pipelines"]
        )

        # Collapse duplicate KPIs: these are already shown in show_load_summary.
        # Keep only the orphaned status metric here to add value without repeating counts.
        orphan_gradient = "gradient-fire" if orphaned > 0 else "gradient-green"
        orphan_icon = "⚠️" if orphaned > 0 else "✅"
        orphan_text = "ORPHANED" if orphaned > 0 else "ALL CONNECTED"

        render_metric_card_premium(
            icon=orphan_icon,
            label=orphan_text,
            value=orphaned,
            gradient=orphan_gradient
        )

    def render_sidebar_filters(self):
        """Render filter controls with enhanced liquid styling"""

        st.markdown(
            """
        <div class="sidebar-section">
            <h3 class="app-heading" style="margin:0 0 15px 0; font-size:1.05em;">
                🔍 Smart Filters
            </h3>
        </div>
        """,
            unsafe_allow_html=True,
        )

        # Impact filter (if available)
        impact_df = safe_get_dataframe("ImpactAnalysis", "Pipeline_Analysis")

        if not impact_df.empty and "Impact" in impact_df.columns:
            st.markdown(
                """
            <div style="margin-bottom: 10px;">
                <label style="font-size: 0.9em; font-weight: 600; color: #667eea;">
                    🎯 Impact Level
                </label>
            </div>
            """,
                unsafe_allow_html=True,
            )
            
            impact_filter = st.multiselect(
                "Impact Level",
                ["CRITICAL", "HIGH", "MEDIUM", "LOW"],
                default=["CRITICAL", "HIGH"],
                key="impact_filter",
                label_visibility="collapsed"
            )

        # Search with enhanced styling
        st.markdown(
            """
        <div style="margin: 15px 0 10px 0;">
            <label style="font-size: 0.9em; font-weight: 600; color: #667eea;">
                🔎 Search Resources
            </label>
        </div>
        """,
            unsafe_allow_html=True,
        )
        
        search = st.text_input(
            "Search Resources", 
            placeholder="🔍 Search pipelines, datasets, activities...", 
            label_visibility="collapsed",
            key="sidebar_search"
        )
        st.session_state.search_query = search

        # Advanced filters section
        with st.expander("🎛️ Advanced Filters", expanded=False):
            st.markdown("**📊 Resource Types**")
            
            col1, col2 = st.columns(2)
            with col1:
                st.checkbox("📦 Pipelines", value=True, key="filter_pipelines")
                st.checkbox("🌊 DataFlows", value=True, key="filter_dataflows")
                
            with col2:
                st.checkbox("📋 Datasets", value=True, key="filter_datasets")
                st.checkbox("⏰ Triggers", value=True, key="filter_triggers")
            
            st.markdown("---")
            st.markdown("**🔗 Connection Status**")
            
            col1, col2 = st.columns(2)
            with col1:
                st.checkbox("✅ Connected", value=True, key="filter_connected")
            with col2:
                st.checkbox("⚠️ Orphaned", value=True, key="filter_orphaned")

    # DATA LOADING & PROCESSING

    def load_excel_file(self, file_or_path):
        """
        Load and process Excel file from uploaded file object or file path

        FIXED:
        - Move summary outside sidebar
        - Proper error handling
        - Progress tracking
        - Support both file objects and file paths
        """

        try:
            with st.spinner("🔄 Loading analysis file..."):

                # Progress tracking
                progress_bar = st.progress(0)
                status_text = st.empty()

                # Step 1: Read Excel file (cached)
                status_text.text("📖 Reading Excel file...")
                progress_bar.progress(10)

                # Handle both file objects and file paths -> read bytes and build ExcelFile from bytes
                if isinstance(file_or_path, str):
                    file_path = Path(file_or_path)
                    if not file_path.exists():
                        st.error(f"File not found: {file_path}")
                        return
                    file_bytes = file_path.read_bytes()
                    file_name = file_path.name
                else:
                    # Uploaded file-like object
                    file_name = getattr(file_or_path, 'name', 'uploaded.xlsx')
                    try:
                        # Prefer memoryview when available (Streamlit UploadedFile)
                        file_bytes = file_or_path.getbuffer().tobytes()  # type: ignore[attr-defined]
                    except Exception:
                        # Fallback: read() and rewind if possible
                        try:
                            file_bytes = file_or_path.read()
                            if hasattr(file_or_path, 'seek'):
                                file_or_path.seek(0)
                        except Exception:
                            st.error("Could not read uploaded file bytes.")
                            return

                # Build a stable cache key from content bytes (md5 + size)
                md5 = hashlib.md5(file_bytes).hexdigest()
                excel_key = f"bytes:{md5}:{len(file_bytes)}"

                # Create ExcelFile from an in-memory buffer
                excel_file = pd.ExcelFile(io.BytesIO(file_bytes))

                # Load all sheets using cache (keyed by excel_key)
                data = cached_read_excel(excel_file, excel_key)

                status_text.text(f" Found {len(data.keys())} sheets...")
                progress_bar.progress(20)

                # Step 2: Post-process (cached) to merge split sheets and normalize names
                status_text.text("💾 Normalizing sheets...")
                progress_bar.progress(70)
                data = cached_merge_and_normalize(data)

                st.session_state.excel_data = data
                st.session_state.uploaded_file_name = file_name

                # Step 3: Extract metadata
                status_text.text(" Extracting metadata...")
                progress_bar.progress(80)

                self.extract_metadata()

                # Step 4: Build dependency graph (cached resource)
                status_text.text("🕸 Building dependency graph...")
                progress_bar.progress(90)

                pipeline_df = safe_get_dataframe(
                    "ImpactAnalysis", "PipelineAnalysis", "Pipeline_Analysis", "Pipelines"
                )
                trigger_df = safe_get_dataframe("TriggerDetails", "Trigger_Pipeline", "Triggers")
                pipeline_pipeline_df = safe_get_dataframe("Pipeline_Pipeline", "PipelinePipeline")
                pipeline_dataflow_df = safe_get_dataframe("Pipeline_DataFlow", "PipelineDataFlow")
                lineage_df = safe_get_dataframe("DataLineage", "Data_Lineage")

                G, metrics = cached_build_graph(
                    pipeline_df, trigger_df, pipeline_pipeline_df, pipeline_dataflow_df, lineage_df
                )
                st.session_state.dependency_graph = G
                st.session_state.graph_metrics = metrics

                # Step 5: Complete
                status_text.text(" Loading complete!")
                progress_bar.progress(100)

                st.session_state.data_loaded = True
                st.session_state.last_load_time = datetime.now()

                # Clear progress indicators
                import time

                time.sleep(0.5)
                progress_bar.empty()
                status_text.empty()
                st.success(f" Successfully loaded: {file_name}")
                st.session_state.show_load_summary = True

        except Exception as e:
            st.error(f" Error loading file: {str(e)}")

            # Show detailed error
            with st.expander(" Error Details"):
                st.code(traceback.format_exc())

    def extract_metadata(self):
        """
        Extract and store metadata from loaded data

        FIXED:
        - Safe dictionary access
        - Type validation
        - Default values
        """

        metadata = {
            "loaded_at": datetime.now(),
            "sheets": list(st.session_state.excel_data.keys()),
            "sheet_counts": {},
            "file_name": st.session_state.uploaded_file_name or "Unknown",
        }

        # Count records in each sheet
        for sheet_name, df in st.session_state.excel_data.items():
            if isinstance(df, pd.DataFrame):
                metadata["sheet_counts"][sheet_name] = len(df)

        # Extract summary information
        summary = safe_get_dataframe("Summary")
        if (
            not summary.empty
            and "Metric" in summary.columns
            and "Value" in summary.columns
        ):
            try:
                metadata["summary"] = summary.set_index("Metric")["Value"].to_dict()
            except:
                metadata["summary"] = {}
        else:
            metadata["summary"] = {}

        st.session_state.analysis_metadata = metadata

    def show_load_summary(self):
        """Show summary after successful load"""

        metadata = st.session_state.analysis_metadata

        st.markdown("### 📊 Load Summary")

        # Replace basic st.metric tiles with premium metric-card layout for uniform KPI styling
        sheets_loaded = len(metadata.get("sheets", []))
        total_records = sum(metadata.get("sheet_counts", {}).values())
        pipelines = get_count_with_fallback(
            "Pipelines", ["ImpactAnalysis", "PipelineAnalysis", "Pipeline_Analysis", "Pipelines"]
        )
        dataflows = get_count_with_fallback(
            "DataFlows", ["DataFlows", "DataFlowLineage", "DataFlow_Summary"]
        )

        # Display in two rows: first row 3 cards, second row 1 wide card (or adaptively 2/2)
        row1_col1, row1_col2, row1_col3 = st.columns(3)
        with row1_col1:
            render_metric_card_premium(
                icon="📄", label="SHEETS", value=sheets_loaded, gradient="gradient-purple"
            )
        with row1_col2:
            render_metric_card_premium(
                icon="📑", label="RECORDS", value=format_number(total_records), gradient="gradient-blue"
            )
        with row1_col3:
            render_metric_card_premium(
                icon="📦", label="PIPELINES", value=format_number(pipelines), gradient="gradient-green"
            )

        row2_col1, row2_col2, row2_col3 = st.columns([1,1,1])
        with row2_col1:
            render_metric_card_premium(
                icon="🌊", label="DATAFLOWS", value=format_number(dataflows), gradient="gradient-pink"
            )
        # Leave remaining columns open for future KPIs (orphaned, triggers) to keep layout flexible

    def build_dependency_graph(self):
        """
        Build NetworkX dependency graph from loaded data

        FIXED:
        - Compatible with v9.1 analyzer output
        - Proper sheet name matching
        - Error handling
        - Node attribute validation
        """

        try:
            G = nx.DiGraph()

            # Add Pipeline Nodes

            # Try multiple sheet names (v9.1 uses ImpactAnalysis)
            pipeline_df = safe_get_dataframe(
                "ImpactAnalysis", "PipelineAnalysis", "Pipeline_Analysis", "Pipelines"
            )

            if not pipeline_df.empty:
                for _, row in pipeline_df.iterrows():
                    # Extract pipeline name (try multiple column names)
                    pipeline_name = (
                        row.get("Pipeline")
                        or row.get("pipeline")
                        or row.get("PipelineName")
                        or ""
                    )

                    if not pipeline_name:
                        continue

                    # Extract attributes with safe defaults
                    has_trigger = False
                    has_dataflow = False
                    is_orphaned = False
                    impact = "LOW"

                    # Check for triggers (multiple column formats)
                    if "UpstreamTriggerCount" in row:
                        has_trigger = int(row.get("UpstreamTriggerCount", 0)) > 0
                    elif "UpstreamTriggers" in row:
                        has_trigger = bool(row.get("UpstreamTriggers", ""))
                    elif "Has_Trigger" in row:
                        has_trigger = row.get("Has_Trigger") in ["Yes", True, 1]

                    # Check for dataflows
                    if "DataFlowCount" in row:
                        has_dataflow = int(row.get("DataFlowCount", 0)) > 0
                    elif "UsedDataFlows" in row:
                        has_dataflow = bool(row.get("UsedDataFlows", ""))
                    elif "Has_DataFlow" in row:
                        has_dataflow = row.get("Has_DataFlow") in ["Yes", True, 1]

                    # Check orphaned status
                    if "IsOrphaned" in row:
                        is_orphaned = row.get("IsOrphaned") in ["Yes", True, 1]
                    elif "Is_Orphaned" in row:
                        is_orphaned = row.get("Is_Orphaned") in ["Yes", True, 1]

                    # Get impact level
                    impact = row.get("Impact", row.get("ImpactLevel", "LOW"))

                    # Add node with attributes
                    G.add_node(
                        pipeline_name,
                        type="pipeline",
                        has_trigger=has_trigger,
                        has_dataflow=has_dataflow,
                        is_orphaned=is_orphaned,
                        impact=str(impact),
                    )

            # Add Trigger → Pipeline Edges

            trigger_df = safe_get_dataframe(
                "TriggerDetails", "Trigger_Pipeline", "Triggers"
            )

            if not trigger_df.empty:
                for _, row in trigger_df.iterrows():
                    trigger = row.get("Trigger") or row.get("trigger") or ""
                    pipeline = row.get("Pipeline") or row.get("pipeline") or ""

                    if trigger and pipeline:
                        # Add trigger node if not exists
                        if not G.has_node(trigger):
                            G.add_node(trigger, type="trigger")

                        # Add edge
                        G.add_edge(trigger, pipeline, relation="triggers", weight=3)

            # Add Pipeline → Pipeline Edges

            pipeline_pipeline_df = safe_get_dataframe(
                "Pipeline_Pipeline", "PipelinePipeline"
            )

            if not pipeline_pipeline_df.empty:
                for _, row in pipeline_pipeline_df.iterrows():
                    from_pipeline = (
                        row.get("from_pipeline") or row.get("FromPipeline") or ""
                    )
                    to_pipeline = row.get("to_pipeline") or row.get("ToPipeline") or ""

                    if from_pipeline and to_pipeline:
                        G.add_edge(
                            from_pipeline, to_pipeline, relation="executes", weight=2
                        )

            # Add Pipeline → DataFlow Edges

            pipeline_dataflow_df = safe_get_dataframe(
                "Pipeline_DataFlow", "PipelineDataFlow"
            )

            if not pipeline_dataflow_df.empty:
                for _, row in pipeline_dataflow_df.iterrows():
                    pipeline = row.get("pipeline") or row.get("Pipeline") or ""
                    dataflow = row.get("dataflow") or row.get("DataFlow") or ""

                    if pipeline and dataflow:
                        # Add dataflow node if not exists
                        if not G.has_node(dataflow):
                            G.add_node(dataflow, type="dataflow")

                        # Add edge
                        G.add_edge(
                            pipeline, dataflow, relation="uses_dataflow", weight=1
                        )

            # Add Dataset Nodes from DataLineage

            lineage_df = safe_get_dataframe("DataLineage", "Data_Lineage")

            if not lineage_df.empty:
                for _, row in lineage_df.iterrows():
                    source = row.get("Source", "")
                    sink = row.get("Sink", "")

                    if source and not G.has_node(source):
                        G.add_node(source, type="dataset")

                    if sink and not G.has_node(sink):
                        G.add_node(sink, type="dataset")

                    if source and sink:
                        G.add_edge(source, sink, relation="data_flow", weight=1)

            # Store graph
            st.session_state.dependency_graph = G

            # Calculate metrics
            st.session_state.graph_metrics = {
                "nodes": G.number_of_nodes(),
                "edges": G.number_of_edges(),
                "density": nx.density(G) if G.number_of_nodes() > 0 else 0,
                "is_directed": G.is_directed(),
            }

        except Exception as e:
            st.error(f" Error building dependency graph: {e}")
            # Create empty graph as fallback
            st.session_state.dependency_graph = nx.DiGraph()
            st.session_state.graph_metrics = {
                "nodes": 0,
                "edges": 0,
                "density": 0,
                "is_directed": True,
            }

    def load_sample_data(self):
        """
        Load comprehensive sample data for demonstration

        FIXED:
        - Compatible with v9.1 analyzer output format
        - Realistic data structure
        - All required sheets
        """

        with st.spinner("🎮 Loading sample data..."):

            # Create realistic sample data matching v9.1 output
            sample_data = {
                "Summary": pd.DataFrame(
                    [
                        {
                            "Metric": "Analysis Date",
                            "Value": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        },
                        {"Metric": "Source File", "Value": "sample_factory.json"},
                        {
                            "Metric": "Analyzer Version",
                            "Value": "9.1 - Fixed & Enhanced",
                        },
                        {"Metric": "", "Value": ""},
                        {"Metric": "=== RESOURCES ===", "Value": ""},
                        {"Metric": "Pipelines", "Value": 25},
                        {"Metric": "DataFlows", "Value": 12},
                        {"Metric": "Datasets", "Value": 45},
                        {"Metric": "LinkedServices", "Value": 18},
                        {"Metric": "Triggers", "Value": 15},
                        {"Metric": "Integration Runtimes", "Value": 5},
                        {"Metric": "", "Value": ""},
                        {"Metric": "=== DEPENDENCIES ===", "Value": ""},
                        {"Metric": "Total Dependencies", "Value": 127},
                        {"Metric": "Trigger → Pipeline", "Value": 35},
                        {"Metric": "Pipeline → DataFlow", "Value": 28},
                        {"Metric": "Pipeline → Pipeline", "Value": 18},
                        {"Metric": "", "Value": ""},
                        {"Metric": "=== ORPHANED RESOURCES ===", "Value": ""},
                        {"Metric": "Orphaned Pipelines", "Value": 3},
                        {"Metric": "Orphaned Datasets", "Value": 5},
                        {"Metric": "Orphaned LinkedServices", "Value": 2},
                        {"Metric": "", "Value": ""},
                        {"Metric": "=== QUALITY ===", "Value": ""},
                        {"Metric": "Parse Errors", "Value": 0},
                    ]
                ),
                "ImpactAnalysis": pd.DataFrame(
                    [
                        {
                            "Pipeline": "PL_MainDataIngestion",
                            "Impact": "CRITICAL",
                            "BlastRadius": 15,
                            "DirectUpstreamTriggers": "TR_Hourly, TR_Daily",
                            "DirectUpstreamTriggerCount": 2,
                            "DirectUpstreamPipelines": "",
                            "DirectUpstreamPipelineCount": 0,
                            "DirectDownstreamPipelines": "PL_Transform, PL_Validate",
                            "DirectDownstreamPipelineCount": 2,
                            "UsedDataFlows": "DF_CleanData",
                            "DataFlowCount": 1,
                            "UsedDatasets": "DS_RawData, DS_StagingData",
                            "DatasetCount": 2,
                            "IsOrphaned": "No",
                        },
                        {
                            "Pipeline": "PL_DataTransformation",
                            "Impact": "HIGH",
                            "BlastRadius": 12,
                            "DirectUpstreamTriggers": "TR_Hourly",
                            "DirectUpstreamTriggerCount": 1,
                            "DirectUpstreamPipelines": "PL_MainDataIngestion",
                            "DirectUpstreamPipelineCount": 1,
                            "DirectDownstreamPipelines": "PL_DataQuality",
                            "DirectDownstreamPipelineCount": 1,
                            "UsedDataFlows": "DF_Transform, DF_Aggregate",
                            "DataFlowCount": 2,
                            "UsedDatasets": "DS_StagingData, DS_ProcessedData",
                            "DatasetCount": 2,
                            "IsOrphaned": "No",
                        },
                        {
                            "Pipeline": "PL_DataQuality",
                            "Impact": "MEDIUM",
                            "BlastRadius": 8,
                            "DirectUpstreamTriggers": "",
                            "DirectUpstreamTriggerCount": 0,
                            "DirectUpstreamPipelines": "PL_DataTransformation",
                            "DirectUpstreamPipelineCount": 1,
                            "DirectDownstreamPipelines": "",
                            "DirectDownstreamPipelineCount": 0,
                            "UsedDataFlows": "DF_Validate",
                            "DataFlowCount": 1,
                            "UsedDatasets": "DS_ProcessedData, DS_QualityReports",
                            "DatasetCount": 2,
                            "IsOrphaned": "No",
                        },
                        {
                            "Pipeline": "PL_OrphanedPipeline",
                            "Impact": "LOW",
                            "BlastRadius": 0,
                            "DirectUpstreamTriggers": "",
                            "DirectUpstreamTriggerCount": 0,
                            "DirectUpstreamPipelines": "",
                            "DirectUpstreamPipelineCount": 0,
                            "DirectDownstreamPipelines": "",
                            "DirectDownstreamPipelineCount": 0,
                            "UsedDataFlows": "",
                            "DataFlowCount": 0,
                            "UsedDatasets": "",
                            "DatasetCount": 0,
                            "IsOrphaned": "Yes",
                        },
                        {
                            "Pipeline": "PL_CustomerAnalytics",
                            "Impact": "HIGH",
                            "BlastRadius": 10,
                            "DirectUpstreamTriggers": "TR_Daily",
                            "DirectUpstreamTriggerCount": 1,
                            "DirectUpstreamPipelines": "",
                            "DirectUpstreamPipelineCount": 0,
                            "DirectDownstreamPipelines": "PL_CustomerReports",
                            "DirectDownstreamPipelineCount": 1,
                            "UsedDataFlows": "DF_CustomerMetrics",
                            "DataFlowCount": 1,
                            "UsedDatasets": "DS_CustomerData, DS_Analytics",
                            "DatasetCount": 2,
                            "IsOrphaned": "No",
                        },
                    ]
                ),
                "TriggerDetails": pd.DataFrame(
                    [
                        {
                            "Trigger": "TR_Hourly",
                            "Pipeline": "PL_MainDataIngestion",
                            "TriggerType": "ScheduleTrigger",
                            "Schedule": "Every 1 hour",
                            "State": "Started",
                        },
                        {
                            "Trigger": "TR_Hourly",
                            "Pipeline": "PL_DataTransformation",
                            "TriggerType": "ScheduleTrigger",
                            "Schedule": "Every 1 hour",
                            "State": "Started",
                        },
                        {
                            "Trigger": "TR_Daily",
                            "Pipeline": "PL_MainDataIngestion",
                            "TriggerType": "ScheduleTrigger",
                            "Schedule": "Daily at 00:00",
                            "State": "Started",
                        },
                        {
                            "Trigger": "TR_Daily",
                            "Pipeline": "PL_CustomerAnalytics",
                            "TriggerType": "ScheduleTrigger",
                            "Schedule": "Daily at 00:00",
                            "State": "Started",
                        },
                        {
                            "Trigger": "TR_Weekly",
                            "Pipeline": "PL_WeeklyReport",
                            "TriggerType": "ScheduleTrigger",
                            "Schedule": "Weekly on Monday",
                            "State": "Started",
                        },
                    ]
                ),
                "Pipeline_DataFlow": pd.DataFrame(
                    [
                        {
                            "pipeline": "PL_MainDataIngestion",
                            "dataflow": "DF_CleanData",
                            "activity": "ExecuteDF_Clean",
                        },
                        {
                            "pipeline": "PL_DataTransformation",
                            "dataflow": "DF_Transform",
                            "activity": "ExecuteDF_Transform",
                        },
                        {
                            "pipeline": "PL_DataTransformation",
                            "dataflow": "DF_Aggregate",
                            "activity": "ExecuteDF_Aggregate",
                        },
                        {
                            "pipeline": "PL_DataQuality",
                            "dataflow": "DF_Validate",
                            "activity": "ExecuteDF_Validate",
                        },
                        {
                            "pipeline": "PL_CustomerAnalytics",
                            "dataflow": "DF_CustomerMetrics",
                            "activity": "ExecuteDF_Metrics",
                        },
                    ]
                ),
                "Pipeline_Pipeline": pd.DataFrame(
                    [
                        {
                            "from_pipeline": "PL_MainDataIngestion",
                            "to_pipeline": "PL_DataTransformation",
                            "activity": "ExecutePL_Transform",
                        },
                        {
                            "from_pipeline": "PL_DataTransformation",
                            "to_pipeline": "PL_DataQuality",
                            "activity": "ExecutePL_Quality",
                        },
                        {
                            "from_pipeline": "PL_CustomerAnalytics",
                            "to_pipeline": "PL_CustomerReports",
                            "activity": "ExecutePL_Reports",
                        },
                    ]
                ),
                "ActivityCount": pd.DataFrame(
                    [
                        {"ActivityType": "Copy", "Count": 45, "Percentage": "35.7%"},
                        {
                            "ActivityType": "ExecuteDataFlow",
                            "Count": 28,
                            "Percentage": "22.2%",
                        },
                        {"ActivityType": "Lookup", "Count": 18, "Percentage": "14.3%"},
                        {
                            "ActivityType": "SetVariable",
                            "Count": 15,
                            "Percentage": "11.9%",
                        },
                        {
                            "ActivityType": "ExecutePipeline",
                            "Count": 10,
                            "Percentage": "7.9%",
                        },
                        {
                            "ActivityType": "SqlServerStoredProcedure",
                            "Count": 6,
                            "Percentage": "4.8%",
                        },
                        {
                            "ActivityType": "GetMetadata",
                            "Count": 4,
                            "Percentage": "3.2%",
                        },
                        {
                            "ActivityType": "=== TOTAL ===",
                            "Count": 126,
                            "Percentage": "100.0%",
                        },
                    ]
                ),
                "OrphanedPipelines": pd.DataFrame(
                    [
                        {
                            "Pipeline": "PL_OrphanedPipeline",
                            "Reason": "Not referenced by any trigger or ExecutePipeline activity",
                            "Type": "Orphaned",
                            "Recommendation": "Review for deletion",
                        },
                        {
                            "Pipeline": "PL_LegacyPipeline",
                            "Reason": "Not referenced by any trigger or ExecutePipeline activity",
                            "Type": "Orphaned",
                            "Recommendation": "Consider removing",
                        },
                        {
                            "Pipeline": "PL_TestPipeline",
                            "Reason": "Not referenced by any trigger or ExecutePipeline activity",
                            "Type": "Orphaned",
                            "Recommendation": "Archive or delete",
                        },
                    ]
                ),
                "OrphanedDatasets": pd.DataFrame(
                    [
                        {
                            "Dataset": "DS_UnusedData",
                            "Reason": "Not used by any pipeline or dataflow",
                            "Type": "Orphaned",
                            "Recommendation": "Consider removing",
                        },
                        {
                            "Dataset": "DS_LegacyData",
                            "Reason": "Not used by any pipeline or dataflow",
                            "Type": "Orphaned",
                            "Recommendation": "Archive or delete",
                        },
                    ]
                ),
                "DataLineage": pd.DataFrame(
                    [
                        {
                            "Pipeline": "PL_MainDataIngestion",
                            "Activity": "CopyRawData",
                            "Type": "Copy",
                            "Source": "DS_RawData",
                            "SourceTable": "raw.data",
                            "Sink": "DS_StagingData",
                            "SinkTable": "staging.data",
                            "Transformation": "SqlSource→AzureSqlSink",
                        },
                        {
                            "Pipeline": "PL_DataTransformation",
                            "Activity": "ExecuteDF_Transform",
                            "Type": "DataFlow",
                            "Source": "DS_StagingData",
                            "SourceTable": "staging.data",
                            "Sink": "DS_ProcessedData",
                            "SinkTable": "processed.data",
                            "Transformation": "DataFlow: DF_Transform (Select, DerivedColumn, Aggregate)",
                        },
                    ]
                ),
            }

            # Store sample data
            st.session_state.excel_data = sample_data
            st.session_state.uploaded_file_name = "sample_data.xlsx"
            st.session_state.data_loaded = True
            st.session_state.last_load_time = datetime.now()

            # Extract metadata
            self.extract_metadata()

            # Build graph
            self.build_dependency_graph()

            st.success(" Sample data loaded successfully!")
            st.balloons()

            # Show summary
            self.show_load_summary()

    # LAUNCHER SCREEN

    def render_launcher(self):
        """Render the initial launcher screen"""

        st.markdown("## 🚀 Welcome to ADF Analyzer v10.1")
        st.markdown("Choose how you want to start your analysis:")

        # Create two prominent option buttons
        col1, col2 = st.columns(2, gap="large")

        with col1:
            st.markdown("""
            <div style="background: linear-gradient(135deg, #667eea15 0%, #764ba215 100%); padding: 30px; border-radius: 15px; text-align: center; margin: 20px 0;">
                <h3 class="app-heading" style="margin-bottom:15px;">🔧 Generate Excel</h3>
                <p style="margin-bottom: 20px;">Run the ADF analyzer with patches to create a fresh Excel workbook from your ADF template JSON.</p>
                <p style="font-size: 0.9em; color: #666;">• Applies 20+ patches for new activity types<br/>• Creates professional Excel reports<br/>• Includes health score dashboard</p>
            </div>
            """, unsafe_allow_html=True)

            if st.button("🔧 Generate Excel", type="primary", use_container_width=True):
                st.session_state['app_mode'] = 'generate'
                st.session_state['app_mode_selected'] = True
                st.rerun()

        with col2:
            st.markdown("""
            <div style="background: linear-gradient(135deg, #43e97b15 0%, #38f9d715 100%); padding: 30px; border-radius: 15px; text-align: center; margin: 20px 0;">
                <h3 class="app-heading" style="margin-bottom:15px;"> Upload & Analyze</h3>
                <p style="margin-bottom: 20px;">Upload an existing ADF analysis Excel file to view interactive dashboards and insights.</p>
                <p style="font-size: 0.9em; color: #666;">• Interactive network visualizations<br/>• Impact analysis and insights<br/>• Data lineage tracking</p>
            </div>
            """, unsafe_allow_html=True)

            if st.button(" Upload & Analyze", type="secondary", use_container_width=True):
                st.session_state['app_mode'] = 'analyze'
                st.session_state['app_mode_selected'] = True
                st.rerun()

        # Add helpful information
        st.markdown("---")
        st.markdown("""
        ###  Quick Start Guide

        **For Generate Excel:**
        - Have your ADF template JSON file ready
        - Click "Generate Excel" and follow the configuration steps
        - The generated workbook will automatically load into the dashboard

        **For Upload & Analyze:**
        - Have an existing `adf_analysis_latest.xlsx` file from ADF Analyzer v9.1+
        - Click "Upload & Analyze" and use the sidebar to upload your file
        - Explore interactive dashboards and insights
        """)

    # WELCOME SCREEN

    def render_main_content_with_tabs(self):
        """Render main content with both Generate and Upload options as top-level tabs"""

        # Show back to launcher button
        if st.button("◀ Back to Launcher", key="main_back_launcher"):
            for k in ['app_mode', 'app_mode_selected']:
                if k in st.session_state:
                    del st.session_state[k]
            st.rerun()

        st.markdown("---")

        # Check which mode was selected and show appropriate default tab
        mode = st.session_state.get('app_mode', 'generate')

        # Always show tabs in the same order but highlight the selected mode
        main_tabs = st.tabs(["⚙ Generate Excel", " Upload & Analyze", "🤖 AI Chat", "📚 Documentation"])

        # Show info about which mode was selected
        if mode == 'analyze':
            st.info("You selected: Upload & Analyze mode. Click the  Upload & Analyze tab above.")
        elif mode == 'generate':
            st.info("You selected: Generate Excel mode. Click the ⚙ Generate Excel tab above.")
        elif mode == 'docs':
            st.info("You selected: Documentation mode. Click the 📚 Documentation tab above.")

        # TAB 1: GENERATE EXCEL (Patch Runner)

        with main_tabs[0]:
            st.header("🔧 Generate Excel using Patch Runner")
            st.markdown("Run the ADF analyzer with patches to generate a fresh Excel workbook from ADF template JSON.")

            self.render_generate_excel_tab()

        # TAB 2: UPLOAD & ANALYZE EXCEL

        with main_tabs[1]:
            if st.session_state.data_loaded:
                # Show full dashboard if data is loaded
                st.header(" Dashboard Analysis")

                # Show enhanced metrics first
                self.render_enhanced_metrics()
                st.markdown("---")

                # Then show the dashboard tabs
                self.render_dashboard_tabs()
            else:
                # Show upload interface directly in the tab
                st.header(" Upload Excel for Analysis")
                st.markdown("Upload an existing ADF analysis Excel file to view interactive dashboard.")

                # Direct upload in the tab (not sidebar)
                uploaded_file = st.file_uploader(
                    "Choose Excel File",
                    type=["xlsx", "xls"],
                    help="Upload adf_analysis_latest.xlsx from ADF Analyzer v9.1+",
                    key="main_upload"
                )

                col1, col2, col3 = st.columns([1, 1, 1])

                with col1:
                    if uploaded_file:
                        if st.button(" Load Excel", type="primary", use_container_width=True):
                            self.load_excel_file(uploaded_file)
                            st.rerun()  # Refresh to show dashboard

                with col2:
                    if st.button(" Load Sample Data", use_container_width=True):
                        self.load_sample_data()
                        st.rerun()  # Refresh to show dashboard

                with col3:
                    st.markdown("")  # Empty column for spacing

                # Show helpful information
                st.markdown("---")
                st.markdown("""
                ###  Upload Tips

                **Supported Files:**
                - `adf_analysis_latest.xlsx` from ADF Analyzer v9.1+
                - Excel files with standard analysis sheets

                **What happens after upload:**
                - Enhanced metrics tiles will appear at the top
                - Interactive dashboard tabs will become available
                - You can explore network graphs, impact analysis, and more

                **Sample Data:**
                - Use "Load Sample Data" to try the dashboard with demo data
                - Perfect for testing and learning the interface
                """)

        # TAB 3: AI CHAT

        with main_tabs[2]:
            if HAS_AI_CHAT:
                try:
                    excel_data = st.session_state.get('excel_data', {})
                    render_ai_chat_tab(excel_data=excel_data)
                except Exception as e:
                    st.error(f"Error rendering AI Chat: {e}")
                    with st.expander("Debug Info"):
                        st.code(traceback.format_exc())
            else:
                st.warning("🤖 AI Chat module not available. Ensure `ai_excel_chat.py` is in the project directory.")

        # TAB 4: DOCUMENTATION

        with main_tabs[3]:
            self.render_comprehensive_documentation()

    def render_generate_excel_tab(self):
        """Render the Generate Excel functionality"""

        # Add explanatory section about what the patched runner does
        st.markdown("""
        ### 🔧 What does the Patched Runner do?

        The **ADF Analyzer Patched Runner** is a complete workflow that:

        1. **📦 Applies Functional Patches** - Adds support for 20+ new activity types, dataset types, and trigger types
        2. **✨ Applies Excel Enhancements** - Creates beautiful, professional Excel reports with charts and dashboards
        3. ** Runs Core Analysis** - Performs comprehensive ADF template analysis
        4. ** Generates Output** - Creates `adf_analysis_latest.xlsx` with 30+ analysis sheets

        **What you get:**
        -  Professional Excel report with health score dashboard
        -  Network visualization data and dependency analysis
        -  Orphaned resource detection and impact analysis
        -  Activity distribution charts and performance insights
        -  Data lineage tracking and transformation analysis
        """)

        st.markdown("---")

        base_dir = Path(__file__).parent
        output_dir = base_dir / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        # JSON INPUT FILE SELECTION

        st.subheader("📁 Input ADF Template")

        # Option 1: Upload JSON file
        uploaded_json = st.file_uploader(
            "Upload ADF Template JSON",
            type=["json"],
            help="Upload your Azure Data Factory ARM template JSON file"
        )

        # Option 2: Select from existing files
        json_files = list(base_dir.parent.glob("*.json")) + list(base_dir.glob("*.json"))
        json_file_names = ["(Select existing file)"] + [f.name for f in json_files]

        selected_json = st.selectbox(
            "Or select existing JSON file:",
            options=json_file_names,
            help="Choose from JSON files in the project directory"
        )

        # ENHANCEMENT CONFIGURATION

        self.render_enhancement_config()

        # SCRIPT SELECTION WITH DETAILED INFORMATION

        st.subheader("⚙ Script Selection & Configuration")

        # Runner selection with detailed information
        runners = [p.name for p in base_dir.glob('adf_*.py') if p.is_file()]

        # Create detailed information about each script
        script_info = {
            'adf_analyzer_v10_patched_runner.py': {
                'name': '🚀 Patched Runner (RECOMMENDED)',
                'description': 'Complete workflow: Applies patches → Excel enhancements → Runs analysis',
                'includes': 'All patches + Ultimate Excel formatting + Full analysis',
                'files_needed': 'adf_analyzer_v10_patch.py, adf_analyzer_v10_excel_enhancements.py, adf_analyzer_v10_complete.py',
                'best_for': 'Production use - Complete automated workflow'
            },
            'adf_analyzer_v10_complete.py': {
                'name': ' Core Analyzer Only',
                'description': 'Core analysis engine without patches or enhancements',
                'includes': 'Basic analysis + Standard Excel export',
                'files_needed': 'None (standalone)',
                'best_for': 'Basic analysis without enhancements'
            },
            'adf_analyzer_v10_patch.py': {
                'name': '🔧 Patch Module Only',
                'description': 'Patch application module (not a standalone runner)',
                'includes': 'Activity/Dataset/Trigger patches',
                'files_needed': 'adf_analyzer_v10_complete.py',
                'best_for': 'Manual patch application (advanced users)'
            },
            'adf_runner_wrapper.py': {
                'name': '🛡 Safe Wrapper',
                'description': 'Unicode-safe wrapper for running other scripts',
                'includes': 'Encoding fixes + Auto-detection of best runner',
                'files_needed': 'Any of the above runner scripts',
                'best_for': 'When having Unicode/encoding issues'
            }
        }

        # Display script options with information
        st.markdown("**Select Analysis Script:**")

        script_options = ['(auto - use best available)'] + sorted(runners)
        selected_script = st.selectbox(
            'Choose script:',
            options=script_options,
            key='gen_sel_runner'
        )

        # Show information about selected script
        if selected_script != '(auto - use best available)':
            script_name = selected_script
            if script_name in script_info:
                info = script_info[script_name]

                with st.expander(f"ℹ About {info['name']}", expanded=True):
                    st.markdown(f"**Description:** {info['description']}")
                    st.markdown(f"**Includes:** {info['includes']}")
                    st.markdown(f"**Required Files:** {info['files_needed']}")
                    st.markdown(f"**Best For:** {info['best_for']}")
        else:
            st.info("🤖 **Auto Mode:** Will automatically select the best available script (patched runner preferred)")

        # QUICK EXECUTION OPTIONS

        st.subheader("⚡ Quick Options")
        st.info(" **Enhancement details configured above** - These are quick execution toggles")

        col1, col2, col3 = st.columns(3)

        with col1:
            enable_patches = st.checkbox(
                '🔧 Apply Patches',
                value=True,
                key='gen_patches',
                help="Apply functional patches (see configuration above for details)"
            )

        with col2:
            enable_excel = st.checkbox(
                '✨ Apply Enhancements',
                value=True,
                key='gen_excel',
                help="Apply Excel enhancements (see configuration above for details)"
            )

        with col3:
            enable_discovery = st.checkbox(
                ' Discovery Mode',
                value=True,
                key='gen_discovery',
                help="Enhanced parsing and discovery mode"
            )

        # Output configuration
        st.subheader("📤 Output Configuration")
        col1, col2 = st.columns(2)

        with col1:
            output_filename = st.text_input(
                'Output Excel filename:',
                value='adf_analysis_latest.xlsx',
                key='gen_output_filename'
            )

        with col2:
            load_after = st.checkbox(
                '🔄 Auto-load into dashboard after generation',
                value=True,
                key='gen_load_after'
            )

        # EXECUTION SUMMARY & CONFIRMATION

        st.subheader("🚀 Execute Generator")

        # Show what will be executed
        with st.expander(" Execution Summary", expanded=True):
            st.markdown("**What will happen when you click 'Run':**")

            if selected_script == '(auto - use best available)':
                st.markdown("1. 🤖 **Auto-select best script** (patched runner preferred)")
            else:
                st.markdown(f"1.  **Run script:** `{selected_script}`")

            if 'enable_patches' in locals() and enable_patches:
                st.markdown("2. 🔧 **Apply functional patches** (new activity/dataset/trigger types)")

            if 'enable_excel' in locals() and enable_excel:
                st.markdown("3. ✨ **Apply Excel enhancements** (beautiful formatting + dashboards)")

            st.markdown("4.  **Run core analysis** on your ADF template")
            st.markdown(f"5.  **Generate Excel:** `{output_filename}`")

            if load_after:
                st.markdown("6. 🔄 **Auto-load into dashboard** for immediate viewing")

        # Safety confirmation
        confirm_run = st.checkbox(
            ' I understand this will execute Python code and generate an Excel file',
            value=False,
            key='gen_confirm'
        )

        col1, col2 = st.columns([1, 2])

        with col1:
            if st.button('▶ Run Patch Runner', type="primary", use_container_width=True):

                # Validate inputs
                if not confirm_run:
                    st.error(' Please confirm execution to proceed.')
                    return

                # Determine input file
                input_file = None
                if uploaded_json is not None:
                    # Save uploaded file temporarily
                    temp_json = base_dir / f"temp_{uploaded_json.name}"
                    temp_json.write_bytes(uploaded_json.read())
                    input_file = str(temp_json)
                    st.success(f"📄 Using uploaded file: {uploaded_json.name}")

                elif selected_json != "(Select existing file)":
                    # Use selected file
                    for f in json_files:
                        if f.name == selected_json:
                            input_file = str(f)
                            st.success(f"📄 Using selected file: {selected_json}")
                            break

                if not input_file:
                    st.error(' Please upload a JSON file or select an existing one.')
                    return

                # Determine runner
                runner_name = None
                if selected_script == '(auto - use best available)':
                    # Prioritize safe wrapper first, then patched runner
                    preferred_runners = [
                        'adf_runner_wrapper.py',
                        'adf_analyzer_v10_patched_runner.py',
                        'adf_analyzer_v10_patch.py',
                        'adf_analyzer_v10_complete.py'
                    ]
                    for cand in preferred_runners:
                        if cand in runners:
                            runner_name = cand
                            break
                    if runner_name is None and runners:
                        runner_name = sorted(runners)[0]
                else:
                    runner_name = selected_script

                if not runner_name:
                    st.error(' No runner script found in armv10/.')
                    return

                runner_path = base_dir / runner_name
                if not runner_path.exists():
                    st.error(f' Runner not found: {runner_path}')
                    return

                st.info(f" Selected runner: {runner_name}")

                # Execute the runner
                self.execute_patch_runner(
                    runner_path=runner_path,
                    input_file=input_file,
                    output_filename=output_filename,
                    output_dir=output_dir,
                    enable_patches=enable_patches,
                    enable_excel=enable_excel,
                    enable_discovery=enable_discovery,
                    load_after=load_after
                )

    def execute_patch_runner(self, runner_path, input_file, output_filename, output_dir,
                           enable_patches, enable_excel, enable_discovery, load_after):
        """Execute the patch runner with configuration"""

        try:
            # Prepare temp config
            cfg = {
                'functional_patches': bool(enable_patches),
                'excel_enhancements': {'enabled': bool(enable_excel)},
                'discovery_mode': bool(enable_discovery),
                'excel': {'output_filename': output_filename, 'output_dir': str(output_dir)}
            }

            tmpcfg = runner_path.parent / 'adf_runner_temp_config.json'
            tmpcfg.write_text(json.dumps(cfg, indent=2), encoding='utf-8')
            st.success(f' Created temp config: {tmpcfg.name}')

            # Show execution progress
            with st.status("🔄 Running patch runner...", expanded=True) as status:
                st.write("🏃‍♂ Starting generator process...")

                # Setup environment with proper Unicode handling
                env = os.environ.copy()
                env['ADF_ANALYZER_CONFIG_JSON'] = str(tmpcfg)
                env['ADF_OUTPUT_FILENAME'] = output_filename
                env['ADF_OUTPUT_DIR'] = str(output_dir)
                # Force UTF-8 encoding for Python subprocess
                env['PYTHONIOENCODING'] = 'utf-8'
                env['PYTHONLEGACYWINDOWSFSENCODING'] = '1'

                # Execute process with virtual environment Python
                # Use the current Python executable instead of a hardcoded path for portability
                venv_python = sys.executable
                cmd = [venv_python, str(runner_path), input_file]
                st.write(f"💻 Command: {' '.join(cmd)}")

                # Create output area for real-time logs
                output_placeholder = st.empty()

                try:
                    process = subprocess.Popen(
                        cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        env=env,
                        text=True,
                        encoding='utf-8',
                        errors='replace',  # Replace invalid characters instead of failing
                        cwd=runner_path.parent
                    )

                    output_lines = []
                    while True:
                        line = process.stdout.readline()
                        if not line and process.poll() is not None:
                            break
                        if line:
                            # Clean the line to remove any problematic characters
                            clean_line = line.encode('utf-8', errors='replace').decode('utf-8')
                            output_lines.append(clean_line)
                            # Show last 20 lines to avoid overwhelming the UI
                            recent_lines = output_lines[-20:]
                            output_placeholder.text_area(
                                "Generator Output:",
                                value=''.join(recent_lines),
                                height=200
                            )

                    ret_code = process.poll()

                except Exception as proc_error:
                    st.error(f" Failed to start process: {proc_error}")
                    return

                if ret_code == 0:
                    status.update(label=" Generator completed successfully!", state="complete")
                    st.success('🎉 Excel generation completed successfully!')
                else:
                    status.update(label=f" Generator failed (exit code: {ret_code})", state="error")
                    st.error(f' Generator failed with exit code: {ret_code}')
                    return

            # Handle post-generation actions
            produced_file = output_dir / output_filename

            if produced_file.exists():
                st.success(f'📁 Generated file: {produced_file}')

                # Download button
                try:
                    file_data = produced_file.read_bytes()
                    st.download_button(
                        label=' Download Generated Excel',
                        data=file_data,
                        file_name=output_filename,
                        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                        type="primary"
                    )
                except Exception as e:
                    st.warning(f'Could not create download button: {e}')

                # Auto-load if requested
                if load_after:
                    try:
                        st.info('🔄 Auto-loading generated Excel into dashboard...')
                        self.load_excel_file(str(produced_file))
                        st.success(' Generated Excel loaded into dashboard!')
                        st.info('👉 Switch to "Upload & Analyze" tab to view the dashboard.')
                    except Exception as e:
                        st.error(f' Failed to auto-load: {e}')
            else:
                st.error(f' Expected output file not found: {produced_file}')

        except Exception as e:
            st.error(f' Execution failed: {e}')
            with st.expander(" Error Details"):
                st.code(traceback.format_exc())

    def render_enhancement_config(self):
        """Render enhancement configuration options"""

        st.subheader("🎨 Enhancement Configuration")

        # Load current enhancement config
        try:
            import json
            config_path = Path(__file__).parent / "enhancement_config.json"
            if config_path.exists():
                with open(config_path, 'r') as f:
                    config = json.load(f)
            else:
                # Default config if file doesn't exist
                config = {
                    "excel_enhancements": {
                        "enabled": True,
                        "core_formatting": {"enabled": True},
                        "conditional_formatting": {"enabled": True},
                        "hyperlinks": {"enabled": True},
                        "enhanced_summary": {"enabled": True},
                        "advanced_dashboard": {"enabled": True}
                    }
                }
        except Exception as e:
            st.warning(f"Could not load enhancement config: {e}")
            return

        excel_config = config.get("excel_enhancements", {})

        st.markdown("**Configure Excel enhancement features:**")

        # Main toggle
        enable_enhancements = st.checkbox(
            "✨ Enable Excel Enhancements",
            value=excel_config.get("enabled", True),
            key="enhancement_main_toggle",
            help="Master switch for all Excel enhancements"
        )

        if enable_enhancements:
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("** Core Features**")

                core_formatting = st.checkbox(
                    "🎨 Core Formatting",
                    value=excel_config.get("core_formatting", {}).get("enabled", True),
                    key="enhancement_core_formatting",
                    help="Column sizing, number formatting, borders, headers"
                )

                conditional_formatting = st.checkbox(
                    "🌈 Conditional Formatting",
                    value=excel_config.get("conditional_formatting", {}).get("enabled", True),
                    key="enhancement_conditional_formatting",
                    help="Data bars, color scales, icon sets, status highlighting"
                )

                hyperlinks = st.checkbox(
                    "🔗 Hyperlinks",
                    value=excel_config.get("hyperlinks", {}).get("enabled", True),
                    key="enhancement_hyperlinks",
                    help="Navigation links between sheets and auto-convert references"
                )

            with col2:
                st.markdown("**🚀 Advanced Features**")

                enhanced_summary = st.checkbox(
                    " Enhanced Summary",
                    value=excel_config.get("enhanced_summary", {}).get("enabled", True),
                    key="enhancement_enhanced_summary",
                    help="Project banner, executive summary, critical alerts, metrics dashboard"
                )

                advanced_dashboard = st.checkbox(
                    " Advanced Dashboard",
                    value=excel_config.get("advanced_dashboard", {}).get("enabled", True),
                    key="enhancement_advanced_dashboard",
                    help="Health score, complexity heat map, performance insights, top pipelines"
                )

                # Advanced dashboard sub-options
                if advanced_dashboard:
                    with st.expander("🔧 Advanced Dashboard Options"):
                        adv_config = excel_config.get("advanced_dashboard", {})

                        col3, col4 = st.columns(2)
                        with col3:
                            health_score = st.checkbox(
                                "🏥 Health Score",
                                value=adv_config.get("health_score", True),
                                key="enhancement_health_score"
                            )

                            complexity_heat_map = st.checkbox(
                                "🔥 Complexity Heat Map",
                                value=adv_config.get("complexity_heat_map", True),
                                key="enhancement_complexity_heat_map"
                            )

                            performance_insights = st.checkbox(
                                "⚡ Performance Insights",
                                value=adv_config.get("performance_insights", True),
                                key="enhancement_performance_insights"
                            )

                        with col4:
                            top_pipelines = st.checkbox(
                                " Top Pipelines",
                                value=adv_config.get("top_pipelines", True),
                                key="enhancement_top_pipelines"
                            )

                            security_checklist = st.checkbox(
                                "🔒 Security Checklist",
                                value=adv_config.get("security_checklist", True),
                                key="enhancement_security_checklist"
                            )

                            cost_analysis = st.checkbox(
                                "💰 Cost Analysis",
                                value=adv_config.get("cost_analysis", False),
                                key="enhancement_cost_analysis"
                            )

            # Save configuration button
            col1, col2, col3 = st.columns([1, 1, 1])
            with col2:
                if st.button("💾 Save Enhancement Config", type="primary", use_container_width=True):
                    try:
                        # Update config with user selections
                        new_config = {
                            "excel_enhancements": {
                                "enabled": enable_enhancements,
                                "core_formatting": {
                                    "enabled": core_formatting
                                },
                                "conditional_formatting": {
                                    "enabled": conditional_formatting
                                },
                                "hyperlinks": {
                                    "enabled": hyperlinks
                                },
                                "enhanced_summary": {
                                    "enabled": enhanced_summary
                                },
                                "advanced_dashboard": {
                                    "enabled": advanced_dashboard,
                                    "health_score": st.session_state.get("enhancement_health_score", True),
                                    "complexity_heat_map": st.session_state.get("enhancement_complexity_heat_map", True),
                                    "performance_insights": st.session_state.get("enhancement_performance_insights", True),
                                    "top_pipelines": st.session_state.get("enhancement_top_pipelines", True),
                                    "security_checklist": st.session_state.get("enhancement_security_checklist", True),
                                    "cost_analysis": st.session_state.get("enhancement_cost_analysis", False)
                                }
                            }
                        }

                        # Save to file
                        with open(config_path, 'w') as f:
                            json.dump(new_config, f, indent=2)

                        st.success(" Enhancement configuration saved!")

                    except Exception as e:
                        st.error(f" Failed to save config: {e}")

        st.markdown("---")

    def render_upload_interface(self):
        """Render the upload interface for existing Excel files"""

        # Welcome message
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.markdown(
                """
                <div class="info-card fade-in" style="text-align: center; padding: 2rem;">
                    <div style="font-size: 3em; margin-bottom: 12px;"></div>
                    <h3 class="app-heading" style="margin-bottom:6px;">Upload Existing Analysis</h3>
                    <p style="margin: 0; opacity: 0.8;">Upload your ADF Analysis Excel file to unlock powerful insights</p>
                </div>
                """,
                unsafe_allow_html=True,
            )

        st.markdown("<br>", unsafe_allow_html=True)

        # File upload section
        uploaded_file = st.file_uploader(
            "Choose Excel File",
            type=["xlsx", "xls"],
            help="Upload adf_analysis_latest.xlsx or similar analysis file",
        )

        col1, col2, col3 = st.columns([1, 1, 1])

        with col1:
            if uploaded_file and st.button(" Load Analysis", type="primary", use_container_width=True):
                self.load_excel_file(uploaded_file)

        with col2:
            if st.button(" Load Sample Data", use_container_width=True):
                self.load_sample_data()

        with col3:
            # Check for existing output files
            output_dir = Path(__file__).parent / "output"
            if output_dir.exists():
                excel_files = list(output_dir.glob("*.xlsx"))
                if excel_files:
                    latest_file = max(excel_files, key=lambda f: f.stat().st_mtime)
                    if st.button(f"📂 Load Latest\n({latest_file.name})", use_container_width=True):
                        try:
                            self.load_excel_file(str(latest_file))
                        except Exception as e:
                            st.error(f"Failed to load: {e}")

        # Show key features
        self.render_feature_highlights()

    def render_feature_highlights(self):
        """Render feature highlights"""

        st.markdown("### ✨ Key Features")

        render_feature_card(
            "🌐 Network Visualizations",
            [
                "Interactive 2D & 3D dependency graphs",
                "See how your pipelines, datasets, and triggers connect",
                "Identify bottlenecks and critical paths"
            ]
        )

        render_feature_card(
            " Advanced Charts",
            [
                "50+ chart types for comprehensive analysis",
                "Impact analysis - Understand change impact before making it",
                "Activity distribution and usage metrics"
            ]
        )

        render_feature_card(
            " Orphan Detection",
            [
                "Find unused resources automatically",
                "Identify broken references and missing dependencies",
                "Clean up recommendations"
            ]
        )

        render_feature_card(
            " Smart Reports",
            [
                "Interactive charts and detailed analytics",
                "Export - Download filtered data and reports",
                "Statistics - Activity distribution and usage metrics"
            ]
        )

    def render_dashboard_tabs(self):
        """Render the main dashboard tabs when data is loaded"""

        # Dashboard navigation tabs (exact copy from original)
        # Build tab list — AI Chat only if module is available
        tab_labels = [
            "🏠 Overview",
            "🌐 Network Graph",
            " Impact Analysis",
            " Orphaned Resources",
            " Statistics",
            "🌊 DataFlow Analysis",
            "🔗 Data Lineage",
            " Data Explorer",
            " Export",
        ]

        tabs = st.tabs(tab_labels)

        with tabs[0]:
            try:
                self.render_overview_tab()
            except Exception as e:
                st.error(f"Error rendering Overview: {e}")
                with st.expander("Debug Info"):
                    st.code(traceback.format_exc())

        with tabs[1]:
            try:
                self.render_network_tab()
            except Exception as e:
                st.error(f"Error rendering Network: {e}")
                with st.expander("Debug Info"):
                    st.code(traceback.format_exc())

        with tabs[2]:
            try:
                self.render_impact_analysis_tab()
            except Exception as e:
                st.error(f"Error rendering Impact Analysis: {e}")
                with st.expander("Debug Info"):
                    st.code(traceback.format_exc())

        with tabs[3]:
            try:
                self.render_orphaned_resources_tab()
            except Exception as e:
                st.error(f"Error rendering Orphaned Resources: {e}")
                with st.expander("Debug Info"):
                    st.code(traceback.format_exc())

        with tabs[4]:
            try:
                self.render_statistics_tab()
            except Exception as e:
                st.error(f"Error rendering Statistics: {e}")
                with st.expander("Debug Info"):
                    st.code(traceback.format_exc())

        with tabs[5]:
            try:
                self.render_dataflow_tab()
            except Exception as e:
                st.error(f"Error rendering DataFlow Analysis: {e}")
                with st.expander("Debug Info"):
                    st.code(traceback.format_exc())

        with tabs[6]:
            try:
                self.render_lineage_tab()
            except Exception as e:
                st.error(f"Error rendering Data Lineage: {e}")
                with st.expander("Debug Info"):
                    st.code(traceback.format_exc())

        with tabs[7]:
            try:
                self.render_explorer_tab()
            except Exception as e:
                st.error(f"Error rendering Data Explorer: {e}")
                with st.expander("Debug Info"):
                    st.code(traceback.format_exc())

        with tabs[8]:
            try:
                self.render_export_tab()
            except Exception as e:
                st.error(f"Error rendering Export: {e}")
                with st.expander("Debug Info"):
                    st.code(traceback.format_exc())

    def render_welcome_screen(self):
        """Render welcome screen with feature highlights"""

        # Hero section
        col1, col2, col3 = st.columns([1, 2, 1])

        with col2:
            # Hero (concise): emoji, title, subtitle
            st.markdown(
                """
            <div class="info-card fade-in" style="text-align: center; padding: 2rem; margin-top: 1rem;">
                <div style="font-size: 4em; margin-bottom: 12px;">🏭</div>
                <h2 class="app-heading" style="margin-bottom:6px;">Welcome to ADF Analyzer v10.1!</h2>
                <p style="font-size: 1.05em; color: #666; margin-bottom: 12px;">
                    Upload your ADF Analysis Excel file to unlock powerful insights
                </p>
            </div>
            """,
                unsafe_allow_html=True,
            )

            # Feature card (uses safe helper)
            bullets = [
                "🌐 Network Visualizations - Interactive 2D & 3D dependency graphs",
                " Advanced Charts - 15+ chart types for comprehensive analysis",
                " Impact Analysis - Understand change impact before making it",
                " Orphan Detection - Find unused resources automatically",
                " Smart Search - Quickly find any resource",
                " Statistics - Activity distribution and usage metrics",
                " Export - Download filtered data and reports",
            ]

            render_feature_card("✨ Key Features", bullets, hint="👈 Use the sidebar to upload your analysis file or load sample data")

        # Feature cards
        st.markdown("<br>", unsafe_allow_html=True)

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.markdown(
                """
            <div class="info-card fade-in" style="text-align: center;">
                <div style="font-size: 3em; color: #667eea;">🌐</div>
                <h4 class="app-heading">Network Graphs</h4>
                <p style="font-size: 0.9em; color: #666;">
                    Visualize dependencies in interactive 2D & 3D graphs
                </p>
            </div>
            """,
                unsafe_allow_html=True,
            )

        with col2:
            st.markdown(
                """
            <div class="info-card fade-in" style="text-align: center;">
                <div style="font-size: 3em; color: #f5576c;"></div>
                <h4 class="app-heading">Impact Analysis</h4>
                <p style="font-size: 0.9em; color: #666;">
                    See what breaks when you make changes
                </p>
            </div>
            """,
                unsafe_allow_html=True,
            )

        with col3:
            st.markdown(
                """
            <div class="info-card fade-in" style="text-align: center;">
                <div style="font-size: 3em; color: #43e97b;"></div>
                <h4 class="app-heading">Orphan Detection</h4>
                <p style="font-size: 0.9em; color: #666;">
                    Identify unused resources for cleanup
                </p>
            </div>
            """,
                unsafe_allow_html=True,
            )

        with col4:
            st.markdown(
                """
            <div class="info-card fade-in" style="text-align: center;">
                <div style="font-size: 3em; color: #4facfe;"></div>
                <h4 class="app-heading">Smart Reports</h4>
                <p style="font-size: 0.9em; color: #666;">
                    Interactive charts and detailed analytics
                </p>
            </div>
            """,
                unsafe_allow_html=True,
            )

        # Quick start guide
        st.markdown("<br>", unsafe_allow_html=True)

        with st.expander("📚 Quick Start Guide"):
            st.markdown(
                """
            ### Getting Started

            1. **Run the Analyzer**
               ```bash
               python adf_analyzer_v9_1_fixed.py your_template.json
               ```

            2. **Upload the Excel Output**
               - Click "Upload Analysis Excel" in the sidebar
               - Select `adf_analysis_latest.xlsx`
               - Click " Load"

            3. **Explore the Dashboard**
               - Navigate tabs for different views
               - Use filters to focus on specific resources
               - Click items for detailed information

            4. **Export Results**
               - Use the Export tab to download filtered data
               - Generate custom reports

            ### Or Try Sample Data

            Click the " Sample" button in the sidebar to load demo data and explore features.
            """
            )

        # Try sample button
        st.markdown("<br>", unsafe_allow_html=True)
        col1, col2, col3 = st.columns([1, 1, 1])
        with col2:
            if st.button(
                "🎮 Try Sample Data", type="primary", use_container_width=True
            ):
                self.load_sample_data()

    # MAIN DASHBOARD RENDERING

    def render_main_dashboard(self):
        """
        Render main dashboard with all tabs

        FIXED:
        - Proper tab structure
        - Error handling for each tab
        - Consistent layout
        """
        if st.session_state.get("show_load_summary", False):
            self.show_load_summary()
            st.session_state.show_load_summary = False
            st.markdown("---")

        # Launcher: let user choose to run the analyzer (create workbook) or
        # upload an existing workbook to analyze. This provides two "apps" in
        # one experience and avoids forcing users to hunt for the runner.
        if not st.session_state.get("app_mode_selected", False):
            st.markdown("## 🚀 Start – create or analyze an ADF workbook")
            st.markdown("Choose whether to run the analyzer/patcher (creates the workbook) or upload an existing workbook to analyze.")
            c1, c2 = st.columns(2)
            with c1:
                if st.button("🔧 Run analyzer / Create workbook", use_container_width=True):
                    st.session_state['app_mode'] = 'run'
                    st.session_state['app_mode_selected'] = True
                    st.experimental_rerun()
            with c2:
                if st.button(" Upload / Analyze existing workbook", use_container_width=True):
                    st.session_state['app_mode'] = 'analyze'
                    st.session_state['app_mode_selected'] = True
                    st.experimental_rerun()

            # Provide a small hint to the sidebar upload area
            st.info("Tip: If you choose 'Upload / Analyze', use the 'Data Input' controls in the left sidebar to upload your Excel workbook, then press 'Load'.")
            return

        # Enhanced metrics row
        self.render_enhanced_metrics()

        st.markdown("<br>", unsafe_allow_html=True)

        # Top-level tabs: Generate Excel vs View Dashboard
        top_tabs = st.tabs(["⚙ Generate Excel", " View Dashboard"])

        # ---- Generate Excel tab ----
        with top_tabs[0]:
            st.header("Generate Excel workbook")
            st.markdown("Use this tab to run the analyzer/patcher generator and produce a fresh workbook, then automatically load it into the dashboard.")

            base_dir = Path(__file__).parent
            output_dir = base_dir / "output"
            output_dir.mkdir(parents=True, exist_ok=True)

            # Runner selection (prefer patched runner if present)
            runners = [p.name for p in base_dir.glob('adf_*.py') if p.is_file()]
            pref = None
            for choice in ['adf_analyzer_v10_patched_runner.py', 'adf_analyzer_v10_patched_runner.py', 'adf_analyzer_v10_patched_runner.py']:
                if choice in runners:
                    pref = choice
                    break
            sel = st.selectbox('Select generator/runner script', options=['(auto)'] + sorted(runners), index=0, key='gen_sel_runner')

            # Simple patch toggles (map to temp config keys)
            st.markdown('**Generator options**')
            aff_analyser = st.checkbox('Enable AFF analyser', value=True, key='gen_aff')
            adf_patch = st.checkbox('Enable ADF Patch', value=True, key='gen_adf_patch')
            excel_enh = st.checkbox('Enable ADF Excel Enhancements', value=True, key='gen_excel_enh')

            output_filename = st.text_input('Output Excel filename', value=st.session_state.get('adf_output_filename', 'adf_analysis_latest.xlsx'), key='gen_output_filename')
            load_after = st.checkbox('Load produced workbook into dashboard after generation', value=True, key='gen_load_after')

            confirm_run = st.checkbox('I understand this will execute a local Python script', value=False, key='gen_confirm')

            run_col, log_col = st.columns([1, 2])
            with run_col:
                if st.button('▶ Run Generator', key='gen_run'):
                    if not confirm_run:
                        st.warning('Please confirm execution to proceed.')
                    else:
                        runner_name = None
                        if sel == '(auto)':
                            # pick common names in order
                            for cand in ['adf_analyzer_v10_patched_runner.py', 'adf_analyzer_v10_patched_runner.py', 'adf_analyzer_v10_complete.py', 'adf_analyzer_v10_patched_runner.py']:
                                if cand in runners:
                                    runner_name = cand
                                    break
                            if runner_name is None and runners:
                                runner_name = sorted(runners)[0]
                        else:
                            runner_name = sel

                        if not runner_name:
                            st.error('No runner script found in armv10/.')
                        else:
                            runner_path = base_dir / runner_name
                            if not runner_path.exists():
                                st.error(f'Runner not found: {runner_path}')
                            else:
                                # prepare temp config
                                cfg = {
                                    'aff_analyser': bool(aff_analyser),
                                    'adf_patch': bool(adf_patch),
                                    'excel_enhancements': {'enabled': bool(excel_enh)},
                                    'excel': {'output_filename': output_filename, 'output_dir': str(output_dir)}
                                }
                                tmpcfg = base_dir / 'adf_runner_temp_config.json'
                                try:
                                    tmpcfg.write_text(json.dumps(cfg, indent=2), encoding='utf-8')
                                    st.success(f'Wrote temp config to {tmpcfg}')
                                except Exception as e:
                                    st.error(f'Could not write temp config: {e}')

                                # run the generator and stream output
                                out_area = st.empty()
                                status = out_area.text_area('Generator output', value='', height=300)
                                try:
                                    env = os.environ.copy()
                                    env['ADF_ANALYZER_CONFIG_JSON'] = str(tmpcfg)
                                    env['ADF_OUTPUT_FILENAME'] = output_filename
                                    env['ADF_OUTPUT_DIR'] = str(output_dir)

                                    proc = subprocess.Popen([sys.executable, str(runner_path)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env, text=True)
                                    out_lines = []
                                    while True:
                                        line = proc.stdout.readline()
                                        if not line and proc.poll() is not None:
                                            break
                                        if line:
                                            out_lines.append(line)
                                            out_area.text_area('Generator output', value=''.join(out_lines), height=400)

                                    ret = proc.poll()
                                    if ret == 0:
                                        st.success('Generator finished successfully')
                                    else:
                                        st.error(f'Generator exited with code {ret}')

                                    # If requested, load the produced workbook
                                    produced = output_dir / output_filename
                                    if load_after and produced.exists():
                                        try:
                                            # call existing loader which accepts a file path
                                            self.load_excel_file(str(produced))
                                            st.success(f'Loaded produced workbook: {produced}')
                                        except Exception as e:
                                            st.error(f'Failed to load produced workbook: {e}')

                                    # Provide download button if file exists
                                    if produced.exists():
                                        try:
                                            data = produced.read_bytes()
                                            st.download_button('Download generated Excel', data=data, file_name=produced.name, mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                                        except Exception as e:
                                            st.warning(f'Could not create download button: {e}')

                                except Exception as e:
                                    st.error(f'Failed to run generator: {e}')

        # ---- View Dashboard tab: existing dashboard tabs ----
        with top_tabs[1]:
            # Main navigation tabs
            tabs = st.tabs(
                [
                    "🏠 Overview",
                    "🌐 Network Graph",
                    " Impact Analysis",
                    " Orphaned Resources",
                    " Statistics",
                    "🌊 DataFlow Analysis",
                    " Data Lineage",
                    " Data Explorer",
                    " Export",
                ]
            )

            with tabs[0]:
                try:
                    self.render_overview_tab()
                except Exception as e:
                    st.error(f"Error rendering Overview: {e}")
                    with st.expander("Debug Info"):
                        st.code(traceback.format_exc())

            with tabs[1]:
                try:
                    self.render_network_tab()
                except Exception as e:
                    st.error(f"Error rendering Network: {e}")
                    with st.expander("Debug Info"):
                        st.code(traceback.format_exc())

            with tabs[2]:
                try:
                    self.render_impact_analysis_tab()
                except Exception as e:
                    st.error(f"Error rendering Impact Analysis: {e}")
                    with st.expander("Debug Info"):
                        st.code(traceback.format_exc())

            with tabs[3]:
                try:
                    self.render_orphaned_resources_tab()
                except Exception as e:
                    st.error(f"Error rendering Orphaned Resources: {e}")
                    with st.expander("Debug Info"):
                        st.code(traceback.format_exc())

            with tabs[4]:
                try:
                    self.render_statistics_tab()
                except Exception as e:
                    st.error(f"Error rendering Statistics: {e}")
                    with st.expander("Debug Info"):
                        st.code(traceback.format_exc())

            with tabs[5]:
                try:
                    self.render_dataflow_tab()
                except Exception as e:
                    st.error(f"Error rendering DataFlow Analysis: {e}")
                    with st.expander("Debug Info"):
                        st.code(traceback.format_exc())

            with tabs[6]:
                try:
                    self.render_lineage_tab()
                except Exception as e:
                    st.error(f"Error rendering Data Lineage: {e}")
                    with st.expander("Debug Info"):
                        st.code(traceback.format_exc())

        with tabs[7]:
            try:
                self.render_explorer_tab()
            except Exception as e:
                st.error(f"Error rendering Explorer: {e}")
                with st.expander("Debug Info"):
                    st.code(traceback.format_exc())

        with tabs[8]:
            try:
                self.render_export_tab()
            except Exception as e:
                st.error(f"Error rendering Export: {e}")
                with st.expander("Debug Info"):
                    st.code(traceback.format_exc())

    def render_enhanced_metrics(self):
        """
        Render enhanced metrics row

        FIXED:
        - Safe metric extraction
        - Default values
        - Proper formatting
        - Added comprehensive error handling
        - Added debug information
        """

        st.markdown("###  Factory Metrics Dashboard")

        # Show a small status indicator
        if 'excel_data' in st.session_state and st.session_state.excel_data:
            total_sheets = len(st.session_state.excel_data)
            st.success(f" Successfully loaded {total_sheets} data sheets")
        else:
            st.warning(" No data loaded - tiles will show zero values")

        # Get metrics with safe defaults (use fallbacks when Summary is missing)
        try:
            pipelines = get_count_with_fallback(
                "Pipelines", ["ImpactAnalysis", "PipelineAnalysis", "Pipeline_Analysis", "Pipelines"]
            )
        except Exception as e:
            st.warning(f"Error calculating pipelines count: {e}")
            pipelines = 0

        try:
            dataflows = get_count_with_fallback(
                "DataFlows", ["DataFlows", "DataFlowLineage", "DataFlow_Summary"]
            )
        except Exception as e:
            st.warning(f"Error calculating dataflows count: {e}")
            dataflows = 0

        try:
            datasets = get_count_with_fallback("Datasets", ["Datasets"])
        except Exception as e:
            st.warning(f"Error calculating datasets count: {e}")
            datasets = 0
        # Triggers: prefer canonical `Triggers` sheet (one row per trigger). If absent,
        # dedupe unique names from `TriggerDetails` (which may contain multiple
        # rows per trigger) — this mirrors `scripts/validate_tiles.py`.
        triggers = 0
        try:
            tr_df = safe_get_dataframe("Triggers")
            if not tr_df.empty:
                triggers = len(tr_df)
            else:
                td = safe_get_dataframe("TriggerDetails")
                if not td.empty:
                    # try to find a trigger name column
                    cand = None
                    for c in td.columns:
                        if 'trigger' in str(c).lower() and 'name' in str(c).lower():
                            cand = c
                            break
                    if cand is not None:
                        triggers = int(td[cand].dropna().astype(str).str.strip().nunique())
                    else:
                        triggers = len(td)
        except Exception as e:
            st.warning(f"Error calculating triggers count: {e}")
            # fallback: previous behavior
            try:
                triggers = get_count_with_fallback("Triggers", ["TriggerDetails", "Triggers"])
            except Exception:
                triggers = 0
        try:
            dependencies = get_count_with_fallback(
                "Total Dependencies", ["ActivityExecutionOrder", "DataLineage", "Pipeline_Pipeline", "Pipeline_DataFlow"]
            )
        except Exception as e:
            st.warning(f"Error calculating dependencies count: {e}")
            dependencies = 0

        try:
            orphaned = get_count_with_fallback("Orphaned Pipelines", ["OrphanedPipelines", "Orphaned_Pipelines"])
        except Exception as e:
            st.warning(f"Error calculating orphaned count: {e}")
            orphaned = 0

        # Calculate health score (use same formula as health gauge)
        try:
            if pipelines > 0:
                health_score = max(0, min(100, int((1 - orphaned / pipelines) * 100)))
            else:
                health_score = 100
        except (ZeroDivisionError, TypeError, ValueError):
            health_score = 100

        # Calculate source/target metrics from DataLineage
        total_source_datasets = 0
        total_target_datasets = 0
        total_source_static = 0
        total_target_static = 0
        total_source_dynamic = 0
        total_target_dynamic = 0

        try:
            lineage_df = safe_get_dataframe("DataLineage", "Data_Lineage")

            if not lineage_df.empty:
                # Count unique source/sink datasets
                if "Source" in lineage_df.columns:
                    total_source_datasets = lineage_df["Source"].dropna().nunique()

                if "Sink" in lineage_df.columns:
                    total_target_datasets = lineage_df["Sink"].dropna().nunique()

                # Analyze SourceTable/SinkTable for static vs dynamic using
                # case-insensitive regex to detect parameterization patterns.
                param_pattern = re.compile(r"@dataset|@\{|pipeline\(|activity\(", re.IGNORECASE)
                if "SourceTable" in lineage_df.columns:
                    source_tables = lineage_df["SourceTable"].dropna()
                    for tbl in source_tables:
                        tbl_str = str(tbl)
                        # Check if parameterized/dynamic (case-insensitive)
                        if param_pattern.search(tbl_str):
                            total_source_dynamic += 1
                        else:
                            total_source_static += 1

                if "SinkTable" in lineage_df.columns:
                    sink_tables = lineage_df["SinkTable"].dropna()
                    for tbl in sink_tables:
                        tbl_str = str(tbl)
                        # Check if parameterized/dynamic (case-insensitive)
                        if param_pattern.search(tbl_str):
                            total_target_dynamic += 1
                        else:
                            total_target_static += 1
        except Exception as e:
            st.warning(f"Error calculating lineage metrics: {e}")
            lineage_df = pd.DataFrame()  # Ensure we have an empty dataframe

        # Compute totals for files/tables by aggregating DataLineage and DataFlowLineage
        try:
            dflow_lineage = safe_get_dataframe("DataFlowLineage", "DataFlow_Lineage")

            def _aggregate_unique_local(dfs, candidates):
                vals = set()
                try:
                    for df in dfs:
                        if df is None or getattr(df, "empty", True):
                            continue
                        for c in candidates:
                            if c in df.columns:
                                svals = df[c].dropna().astype(str).str.strip()
                                vals.update([v for v in svals if v != ""])
                                break
                except Exception:
                    pass
                return int(len(vals))

            src_file_cols = ["SourceFile", "Source_File", "SourceFilename", "SourceName", "Source"]
            tgt_file_cols = ["TargetFile", "Target_File", "TargetFilename", "SinkName", "Sink"]
            src_table_cols = ["SourceTable", "Source_Table"]
            tgt_table_cols = ["SinkTable", "Sink_Table"]

            total_source_files = _aggregate_unique_local([lineage_df, dflow_lineage], src_file_cols)
            total_target_files = _aggregate_unique_local([lineage_df, dflow_lineage], tgt_file_cols)
            total_source_tables = _aggregate_unique_local([lineage_df, dflow_lineage], src_table_cols)
            total_target_tables = _aggregate_unique_local([lineage_df, dflow_lineage], tgt_table_cols)
        except Exception as e:
            st.warning(f"Error calculating file/table aggregations: {e}")
            total_source_files = 0
            total_target_files = 0
            total_source_tables = 0
            total_target_tables = 0

        # Primary KPI cards (7-up layout) including Dependencies, Health, Orphaned
        tile_descriptions = {
            "Pipelines": "Total pipelines detected in the ARM template / analyzer.",
            "DataFlows": "Count of Data Flow resources.",
            "Datasets": "Distinct dataset definitions discovered.",
            "Triggers": "Active triggers defined in the factory.",
            "Dependencies": "Total dependency edges inferred (execution & references).",
            "Health": "Health score = (1 - orphaned/pipelines) * 100 (bounded 0-100).",
            "Orphaned": "Pipelines not referenced by any trigger/pipeline.",
        }
        cols = st.columns(7)
        orphan_gradient = "gradient-fire" if orphaned > 0 else "gradient-green"
        orphan_icon = "⚠️" if orphaned > 0 else "✅"
        kpis = [
            (cols[0], "Pipelines", pipelines, "gradient-purple", "📦"),
            (cols[1], "DataFlows", dataflows, "gradient-pink", "🌊"),
            (cols[2], "Datasets", datasets, "gradient-blue", "📁"),
            (cols[3], "Triggers", triggers, "gradient-green", "⏰"),
            (cols[4], "Dependencies", dependencies, "gradient-purple", "🕸"),
            (cols[5], "Health", f"{health_score}%", "gradient-blue", "💚"),
            (cols[6], "Orphaned", orphaned, orphan_gradient, orphan_icon),
        ]
        last_verifier = st.session_state.get("last_verifier_report")
        for col, label, value, gradient, icon in kpis:
            desc = tile_descriptions.get(label, "")
            verifier_key = label if label != "Orphaned" else "OrphanedPipelines"
            with col:
                badge_html = ""
                if last_verifier and verifier_key in last_verifier:
                    vv = last_verifier[verifier_key].get("value")
                    try:
                        dv = int(value) if not isinstance(value, str) and value is not None else value
                    except Exception:
                        dv = value
                    badge_html = '<div style="font-size:0.9em;color:green"></div>' if vv == dv else f'<div style="font-size:0.8em;color:{PremiumColors.DANGER_DARK}"> {vv}</div>'
                st.markdown(
                    f"""
                    <div class="metric-card {gradient}" title="{desc}">
                        <div style="font-size: 1.6em;">{icon}</div>
                        <div class="metric-label">{label}</div>
                        <div class="metric-value">{value if isinstance(value, str) else format_number(value)}</div>
                        <div style="margin-top:6px;">{badge_html}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    # Debug information section (collapsible)
        with st.expander("🐛 Debug Info - Data Sources & Sheets", expanded=False):
            st.markdown("**Data sheets loaded in session:**")
            if 'excel_data' in st.session_state and st.session_state.excel_data:
                available_sheets = list(st.session_state.excel_data.keys())
                for sheet in sorted(available_sheets):
                    df = st.session_state.excel_data[sheet]
                    row_count = len(df) if not df.empty else 0
                    st.markdown(f"- **{sheet}**: {row_count} rows")
            else:
                st.warning("No Excel data loaded in session. Upload an Excel file first.")

            st.markdown("**Computed metrics:**")
            st.markdown(f"- Pipelines: {pipelines}")
            st.markdown(f"- DataFlows: {dataflows}")
            st.markdown(f"- Datasets: {datasets}")
            st.markdown(f"- Triggers: {triggers}")
            st.markdown(f"- Dependencies: {dependencies}")
            st.markdown(f"- Orphaned: {orphaned}")
            st.markdown(f"- Health Score: {health_score}%")

    # Expander with lineage details and the Verify action
        with st.expander("🔎 Lineage & Details (expand for dataset/tables breakdown and Verify)", expanded=False):
            # Add an explicit Verify button so users can re-run the in-app verifier
            verify_col1, verify_col2 = st.columns([1, 3])
            with verify_col1:
                if st.button(" Verify tiles"):
                    # Build a verifier snapshot (mirrors dashboard heuristics)
                    try:
                        vr = {}
                        vr["Pipelines"] = {"value": pipelines, "source": "Computed"}
                        vr["DataFlows"] = {"value": dataflows, "source": "Computed"}
                        vr["Datasets"] = {"value": datasets, "source": "Computed"}
                        vr["Triggers"] = {"value": triggers, "source": "Computed"}
                        vr["Dependencies"] = {"value": dependencies, "source": "Computed"}
                        vr["OrphanedPipelines"] = {"value": orphaned, "source": "Computed"}
                        vr["Health"] = {"value": health_score, "source": "Computed"}
                        vr["Total Source Files"] = {"value": total_source_files, "source": "Aggregated(DataLineage,DataFlowLineage)"}
                        vr["Total Target Files"] = {"value": total_target_files, "source": "Aggregated(DataLineage,DataFlowLineage)"}
                        vr["Total Source Tables"] = {"value": total_source_tables, "source": "Aggregated(DataLineage,DataFlowLineage)"}
                        vr["Total Target Tables"] = {"value": total_target_tables, "source": "Aggregated(DataLineage,DataFlowLineage)"}
                        # store in session and persist
                        st.session_state["last_verifier_report"] = vr
                        try:
                            ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                            fname = (Path(__file__).parent / "output" / f"verify_{ts}.json")
                            fname.write_text(json.dumps(vr, indent=2), encoding='utf-8')
                            st.session_state["last_verifier_persisted"] = str(fname)
                        except Exception:
                            pass
                        st.success("Verification snapshot saved to session")
                    except Exception as e:
                        st.error(f"Verification failed: {e}")

            with verify_col2:
                st.markdown("_Press **Verify tiles** to recompute and persist a verification snapshot._")
            # Top-line lineage tiles (compact 4-up)
            row1c = st.columns(4)
            tiles_row1 = [
                ("Source Datasets", total_source_datasets, "gradient-blue", "📁"),
                ("Target Datasets", total_target_datasets, "gradient-green", ""),
                ("Total Source Files", total_source_files, "gradient-blue", "📁"),
                ("Total Target Files", total_target_files, "gradient-green", "📁"),
            ]

            for col, (label, value, gradient, icon) in zip(row1c, tiles_row1):
                desc = tile_descriptions.get(label, "")
                col.markdown(
                    f"""
                    <div class="metric-card {gradient}" title="{desc}">
                        <div class="metric-icon">{icon}</div>
                        <div class="metric-label">{label}</div>
                        <div class="metric-value">{format_number(value)}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            # Row 2: tables and static/dynamic breakdown (4-up)
            row2c = st.columns(4)
            tiles_row2 = [
                ("Static Sources", total_source_static, "gradient-purple", "🧱"),
                ("Static Targets", total_target_static, "gradient-pink", "🧱"),
                ("Total Source Tables", total_source_tables, "gradient-purple", "📚"),
                ("Total Target Tables", total_target_tables, "gradient-pink", "📚"),
            ]
            for col, (label, value, gradient, icon) in zip(row2c, tiles_row2):
                desc = tile_descriptions.get(label, "")
                col.markdown(
                    f"""
                    <div class="metric-card {gradient}" title="{desc}">
                        <div class="metric-icon">{icon}</div>
                        <div class="metric-label">{label}</div>
                        <div class="metric-value">{format_number(value)}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            # Build top-N source/target counts for charts
            def _value_counts_concat(df_list, colname):
                s = []
                try:
                    for d in df_list:
                        if d is None or getattr(d, "empty", True):
                            continue
                        if colname in d.columns:
                            s.extend(d[colname].dropna().astype(str).str.strip().tolist())
                except Exception:
                    pass
                return pd.Series(s).value_counts() if s else pd.Series(dtype=object)

            try:
                src_counts = _value_counts_concat([lineage_df, dflow_lineage], "Source")
                tgt_counts = _value_counts_concat([lineage_df, dflow_lineage], "Sink")
            except Exception as e:
                st.warning(f"Error building chart data: {e}")
                src_counts = pd.Series(dtype=object)
                tgt_counts = pd.Series(dtype=object)

            # Layout: left column charts, right column business logic diagram (Sankey)
            c1, c2 = st.columns([2, 3])

            # Left: top sources and targets bar charts
            with c1:
                st.markdown("#### Top Sources")
                try:
                    if not src_counts.empty:
                        top_src = src_counts.head(10)
                        # Create premium horizontal bar chart for sources
                        fig_src = create_premium_chart()
                        fig_src.add_trace(go.Bar(
                            x=top_src.values.tolist(), 
                            y=top_src.index.tolist(), 
                            orientation='h',
                            marker=dict(
                                color=PremiumColors.GRADIENTS['gradient_1'][0],
                                line=dict(color='rgba(255, 255, 255, 0.8)', width=1.5)
                            ),
                            hovertemplate="<b>%{y}</b><br>" +
                                          "Count: <b>%{x:,}</b>" +
                                          "<extra></extra>"
                        ))
                        fig_src.update_layout(
                            height=350, 
                            title=dict(
                                text="📊 Top Source Datasets",
                                font=dict(size=18, color=PremiumColors.PRIMARY, family='Inter')
                            ),
                            xaxis_title="Count",
                            yaxis_title="Source Datasets"
                        )
                        st.plotly_chart(fig_src, use_container_width=True)
                    else:
                        st.info("No source dataset counts available")
                except Exception as e:
                    st.error(f"Error rendering source chart: {e}")

                st.markdown("#### Top Targets")
                try:
                    if not tgt_counts.empty:
                        top_tgt = tgt_counts.head(10)
                        # Create premium horizontal bar chart for targets
                        fig_tgt = create_premium_chart()
                        fig_tgt.add_trace(go.Bar(
                            x=top_tgt.values.tolist(), 
                            y=top_tgt.index.tolist(), 
                            orientation='h',
                            marker=dict(
                                color=PremiumColors.GRADIENTS['gradient_2'][0],
                                line=dict(color='rgba(255, 255, 255, 0.8)', width=1.5)
                            ),
                            hovertemplate="<b>%{y}</b><br>" +
                                          "Count: <b>%{x:,}</b>" +
                                          "<extra></extra>"
                        ))
                        fig_tgt.update_layout(
                            height=350, 
                            title=dict(
                                text="📈 Top Target Datasets",
                                font=dict(size=18, color=PremiumColors.PRIMARY, family='Inter')
                            ),
                            xaxis_title="Count",
                            yaxis_title="Target Datasets"
                        )
                        st.plotly_chart(fig_tgt, use_container_width=True)
                    else:
                        st.info("No target dataset counts available")
                except Exception as e:
                    st.error(f"Error rendering target chart: {e}")

            # Right: Business logic Sankey (source -> target) using top N nodes
            with c2:
                st.markdown("#### Business logic diagram (Sankey) — top flows")
                # Build link counts
                def _build_sankey_df(df_list, src_col="Source", tgt_col="Sink"):
                    rows = []
                    for d in df_list:
                        if d is None or getattr(d, "empty", True):
                            continue
                        if src_col in d.columns and tgt_col in d.columns:
                            tmp = d[[src_col, tgt_col]].dropna()
                            rows.extend(list(tmp.itertuples(index=False, name=None)))
                    if not rows:
                        return pd.DataFrame(columns=[src_col, tgt_col])
                    return pd.DataFrame(rows, columns=[src_col, tgt_col])

                sankey_df = _build_sankey_df([lineage_df, dflow_lineage])
                if not sankey_df.empty:
                    link_counts = sankey_df.groupby(["Source", "Sink"]).size().reset_index(name="count").sort_values("count", ascending=False)
                    # limit nodes to top 12 combined
                    src_top = link_counts.groupby("Source")["count"].sum().nlargest(10).index.tolist()
                    tgt_top = link_counts.groupby("Sink")["count"].sum().nlargest(10).index.tolist()
                    nodes = list(dict.fromkeys(src_top + tgt_top))
                    if not nodes:
                        st.info("Not enough data for Sankey")
                    else:
                        node_idx = {n: i for i, n in enumerate(nodes)}
                        filtered = link_counts[link_counts["Source"].isin(nodes) & link_counts["Sink"].isin(nodes)].head(50)
                        source_idx = [node_idx[s] for s in filtered["Source"]]
                        target_idx = [node_idx[t] for t in filtered["Sink"]]
                        values = filtered["count"].tolist()
                        sankey_fig = go.Figure(data=[go.Sankey(
                            node=dict(label=nodes, pad=15, thickness=15, color=px.colors.qualitative.Dark24[:len(nodes)]),
                            link=dict(source=source_idx, target=target_idx, value=values)
                        )])
                        sankey_fig.update_layout(height=650, margin=dict(l=10, r=10, t=20, b=20))
                        st.plotly_chart(sankey_fig, use_container_width=True)
                else:
                    st.info("No lineage rows available to build business logic diagram")

            # Show compact verifier results snapshot if present
            vr = st.session_state.get("last_verifier_report")
            if vr:
                st.markdown("---")
                st.markdown("### Verification snapshot")
                rows = []
                for k in ["Pipelines", "DataFlows", "Datasets", "Triggers", "Dependencies", "OrphanedPipelines", "Health", "Total Source Files", "Total Target Files", "Total Source Tables", "Total Target Tables"]:
                    v = vr.get(k, {}).get("value")
                    src = vr.get(k, {}).get("source", "-")
                    rows.append(f"- **{k}**: {format_number(v) if isinstance(v, int) else v}  — source: `{src}`")
                st.markdown("\n".join(rows))

    # OVERVIEW TAB

    def render_overview_tab(self):
        """
        Render overview dashboard

        FIXED:
        - Safe data access
        - Fallback for missing data
        - Proper chart rendering
        """

        st.markdown("### 🏠 Factory Overview Dashboard")

        # Row 1: Pipeline distribution and health
        col1, col2 = st.columns([2, 1])

        with col1:
            self.render_pipeline_distribution_chart()

        with col2:
            self.render_health_gauge()

        st.markdown("---")

        # Row 2: Activity breakdown and resource summary
        col1, col2 = st.columns(2)

        with col1:
            self.render_activity_distribution()

        with col2:
            self.render_resource_summary()

        st.markdown("---")

        # Row 3: Analysis info
        self.render_analysis_info()

    def render_pipeline_distribution_chart(self):
        """Render pipeline category distribution"""

        impact_df = safe_get_dataframe(
            "ImpactAnalysis", "PipelineAnalysis", "Pipeline_Analysis"
        )

        if impact_df.empty:
            st.info(" No pipeline data available")
            return

        # Calculate categories safely
        categories = {}

        # With Triggers
        if "DirectUpstreamTriggerCount" in impact_df.columns:
            categories["With Triggers"] = (
                impact_df["DirectUpstreamTriggerCount"].fillna(0).astype(int) > 0
            ).sum()
        elif "UpstreamTriggerCount" in impact_df.columns:
            categories["With Triggers"] = (
                impact_df["UpstreamTriggerCount"].fillna(0).astype(int) > 0
            ).sum()
        else:
            categories["With Triggers"] = 0

        # With DataFlows
        if "DataFlowCount" in impact_df.columns:
            categories["With DataFlows"] = (
                impact_df["DataFlowCount"].fillna(0).astype(int) > 0
            ).sum()
        else:
            categories["With DataFlows"] = 0

        # Calling Pipelines
        if "DirectDownstreamPipelineCount" in impact_df.columns:
            categories["Calling Pipelines"] = (
                impact_df["DirectDownstreamPipelineCount"].fillna(0).astype(int) > 0
            ).sum()
        elif "DownstreamPipelineCount" in impact_df.columns:
            categories["Calling Pipelines"] = (
                impact_df["DownstreamPipelineCount"].fillna(0).astype(int) > 0
            ).sum()
        else:
            categories["Calling Pipelines"] = 0

        # Orphaned
        if "IsOrphaned" in impact_df.columns:
            categories["Orphaned"] = (impact_df["IsOrphaned"] == "Yes").sum()
        else:
            categories["Orphaned"] = 0

        # Create premium horizontal bar chart
        fig = create_premium_chart()

        # Use premium gradient colors
        premium_colors = [
            PremiumColors.GRADIENTS['gradient_1'][0],
            PremiumColors.GRADIENTS['gradient_2'][0], 
            PremiumColors.GRADIENTS['gradient_3'][0],
            PremiumColors.GRADIENTS['gradient_4'][0]
        ]

        fig.add_trace(
            go.Bar(
                y=list(categories.keys()),
                x=list(categories.values()),
                orientation="h",
                marker=dict(
                    color=premium_colors, 
                    line=dict(color=theme_line_color(0.8), width=2)
                ),
                text=list(categories.values()),
                textposition="auto",
                textfont=dict(size=14, color=theme_text_color(), family='Inter'),
                hovertemplate="<b>%{y}</b><br>" +
                              "Count: <b>%{x:,}</b>" +
                              "<extra></extra>",
            )
        )

        fig.update_layout(
            title={
                "text": "📊 Pipeline Categories",
                "font": {"size": 22, "color": PremiumColors.PRIMARY, "family": "Inter"},
                "x": 0.5,
                "xanchor": "center"
            },
            xaxis_title="Count",
            yaxis_title="Categories",
            height=400,
            showlegend=False,
        )

        safe_plotly(fig, df=impact_df, required_columns=["Impact"], info_message=" No pipeline impact data to display")

    def render_health_gauge(self):
        """Render factory health score gauge"""

        pipelines = get_count_with_fallback(
            "Pipelines", ["ImpactAnalysis", "PipelineAnalysis", "Pipeline_Analysis", "Pipelines"]
        )
        orphaned = get_count_with_fallback(
            "Orphaned Pipelines", ["OrphanedPipelines", "Orphaned_Pipelines"]
        )

        # Calculate health score (0-100)
        if pipelines > 0:
            health_score = int((1 - orphaned / pipelines) * 100)
        else:
            health_score = 100

        # Determine status
        if health_score >= 90:
            color = PremiumColors.SUCCESS
            status = "Excellent"
            icon = ""
        elif health_score >= 75:
            color = PremiumColors.INFO
            status = "Good"
            icon = "🔵"
        elif health_score >= 60:
            color = PremiumColors.WARNING
            status = "Fair"
            icon = ""
        else:
            color = PremiumColors.DANGER
            status = "Needs Attention"
            icon = ""

        # Create gauge
        fig = go.Figure(
            go.Indicator(
                mode="gauge+number+delta",
                value=health_score,
                domain={"x": [0, 1], "y": [0, 1]},
                title={"text": f"{icon} Health Score", "font": {"size": 18}},
                delta={"reference": 80, "increasing": {"color": PremiumColors.SUCCESS}},
                gauge={
                    "axis": {"range": [None, 100], "tickwidth": 1},
                    "bar": {"color": color},
                    "bgcolor": theme_overlay_bg(0.95),
                    "borderwidth": 2,
                    "bordercolor": "rgba(100,116,139,0.4)",
                    "steps": [
                        {"range": [0, 60], "color": PremiumColors.GAUGE_LOW},
                        {"range": [60, 75], "color": PremiumColors.GAUGE_MEDIUM},
                        {"range": [75, 90], "color": PremiumColors.GAUGE_HIGH},
                        {"range": [90, 100], "color": PremiumColors.GAUGE_EXCELLENT},
                    ],
                    "threshold": {
                        "line": {"color": "red", "width": 4},
                        "thickness": 0.75,
                        "value": 90,
                    },
                },
            )
        )

        fig.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=60, b=20),
            paper_bgcolor="rgba(0,0,0,0)",
        )

        safe_plotly(fig)

    def render_activity_distribution(self):
        """Render activity type distribution pie chart"""

        activity_df = safe_get_dataframe("ActivityCount")

        if activity_df.empty:
            st.info(" No activity data available")
            return

        # Filter out total rows and coerce Count to numeric
        if "ActivityType" in activity_df.columns:
            activity_df = activity_df[~activity_df["ActivityType"].astype(str).str.contains("TOTAL", na=False)]
        if "Count" in activity_df.columns:
            activity_df["Count"] = pd.to_numeric(activity_df["Count"], errors="coerce").fillna(0).astype(int)

        # Aggregate by ActivityType to ensure pie percentages are accurate
        if "ActivityType" in activity_df.columns and "Count" in activity_df.columns:
            grouped = (
                activity_df.groupby("ActivityType", as_index=False)["Count"].sum().sort_values("Count", ascending=False)
            )
        else:
            grouped = pd.DataFrame(columns=["ActivityType", "Count"])

        # Take top 10 activity types
        grouped = grouped.head(10)

        if grouped.empty:
            st.info(" No activity data to display")
            return

        # Compute totals and percentages explicitly to avoid any Plotly rounding surprises
        total = int(grouped["Count"].sum())
        if total == 0:
            st.info(" Activity counts sum to zero, nothing to display")
            return

        # Prepare labels, values and percent customdata
        labels = grouped["ActivityType"].astype(str).tolist()
        values = grouped["Count"].astype(int).tolist()
        percents = [v / total for v in values]

        # Create premium pie chart with glassmorphism styling
        fig = go.Figure(
            data=[
                go.Pie(
                    labels=labels,
                    values=values,
                    customdata=percents,
                    hole=0.4,
                    marker=dict(
                        colors=[
                            PremiumColors.GRADIENTS['gradient_1'][0],
                            PremiumColors.GRADIENTS['gradient_2'][0], 
                            PremiumColors.GRADIENTS['gradient_3'][0],
                            PremiumColors.GRADIENTS['gradient_4'][0],
                            PremiumColors.GRADIENTS['gradient_5'][0],
                            PremiumColors.GRADIENTS['gradient_6'][0],
                            PremiumColors.GRADIENTS['gradient_7'][0],
                            PremiumColors.GRADIENTS['gradient_8'][0],
                            PremiumColors.WARNING,
                            PremiumColors.INFO
                        ],
                        line=dict(color='rgba(255, 255, 255, 0.8)', width=2)
                    ),
                    textinfo="none",
                    texttemplate="%{label}<br>%{customdata:.1%} (%{value})",
                    insidetextorientation="radial",
                    textfont=dict(size=11, color='white', family='Inter'),
                    hovertemplate="<b>%{label}</b><br>" +
                                  "Count: <b>%{value:,}</b><br>" +
                                  "Percentage: <b>%{customdata:.1%}</b>" +
                                  "<extra></extra>",
                )
            ]
        )

        # Apply premium chart template and enhanced styling
        fig.update_layout(
            **PREMIUM_CHART_TEMPLATE['layout'],
            title={
                "text": "⚡ Activity Distribution",
                "font": {"size": 22, "color": PremiumColors.PRIMARY, "family": "Inter"},
                "x": 0.5,
                "xanchor": "center"
            },
            height=450,
            showlegend=True,
            legend=dict(
                orientation="v", 
                yanchor="middle", 
                y=0.5, 
                xanchor="left", 
                x=1.05,
                bgcolor='rgba(255, 255, 255, 0.9)',
                bordercolor='rgba(102, 126, 234, 0.3)',
                borderwidth=1,
                font=dict(size=11, family='Inter')
            ),
            annotations=[
                dict(
                    text=f"Total Activities<br><b>{total:,}</b>",
                    x=0.5, y=0.5,
                    font=dict(size=16, color=PremiumColors.PRIMARY, family='Inter'),
                    showarrow=False
                )
            ]
        )

        safe_plotly(fig, df=activity_df, required_columns=["ActivityType", "Count"], info_message=" No activity data to display")

    def render_resource_summary(self):
        """Render resource summary treemap"""

        # Get resource counts
        resources = []
        counts = []

        # Build resource type counts robustly by inspecting loaded sheets first
        def sheet_count(*names):
            """Return number of rows from the first matching sheet name."""
            for n in names:
                df = safe_get_dataframe(n)
                if not df.empty:
                    return len(df)
            return 0

        resource_types = [
            ("Pipelines", sheet_count("PipelineAnalysis", "Pipelines")),
            ("DataFlows", sheet_count("DataFlows", "DataFlowLineage", "DataFlow_Summary")),
            ("Datasets", sheet_count("Datasets")),
            ("LinkedServices", sheet_count("LinkedServices")),
            ("Triggers", sheet_count("Triggers", "TriggerDetails")),
            ("Integration Runtimes", sheet_count("IntegrationRuntimes", "Integration_Runtime")),

            # New resource types introduced in analyzer v10.x
            ("Credentials", sheet_count("Credentials", "credentials")),
            ("Managed VNets", sheet_count("ManagedVNets", "ManagedVnets", "managed_vnets")),
            ("Managed Private Endpoints", sheet_count("ManagedPrivateEndpoints", "managed_private_endpoints")),
            ("Global Parameters", sheet_count("GlobalParameterUsage", "GlobalParameters", "global_parameters")),
        ]

        for label, count in resource_types:
            if count > 0:
                resources.append(label)
                counts.append(count)

        if not resources:
            st.info(" No resource data available")
            return

        # Create treemap
        fig = go.Figure(
            go.Treemap(
                labels=resources,
                parents=[""] * len(resources),
                values=counts,
                textinfo="label+value+percent root",
                marker=dict(colorscale="Viridis", line=dict(width=2, color="white")),
                hovertemplate="<b>%{label}</b><br>Count: %{value}<br>%{percentRoot}<extra></extra>",
            )
        )

        fig.update_layout(
            title={
                "text": "📦 Resources Overview",
                "font": {"size": 20, "color": "#667eea"},
            },
            height=400,
            margin=dict(l=20, r=20, t=60, b=20),
            paper_bgcolor="rgba(0,0,0,0)",
        )

        safe_plotly(fig)

    def render_analysis_info(self):
        """Render analysis information cards"""

        st.markdown("### 📅 Analysis Information")

        col1, col2, col3, col4 = st.columns(4)

        analysis_date = get_summary_metric("Analysis Date", "N/A")
        source_file = get_summary_metric("Source File", "N/A")
        version = get_summary_metric("Analyzer Version", "N/A")
        errors = get_summary_metric("Parse Errors", 0)

        with col1:
            render_info_card("📅 Analysis Date", f"<p>{analysis_date}</p>")

        with col2:
            filename = Path(str(source_file)).name if source_file != "N/A" else "N/A"
            render_info_card("📁 Source File", f"<p title='{source_file}'>{truncate_text(filename, 30)}</p>")

        with col3:
            render_info_card("🔧 Version", f"<p>{truncate_text(str(version), 30)}</p>")

        with col4:
            color = PremiumColors.SUCCESS if errors == 0 else PremiumColors.DANGER
            status = "No Errors" if errors == 0 else f"{errors} Errors"
            render_info_card(" Status", f"<p style='color: {color}; font-weight: 600;'>{status}</p>", color=color)

        # Additional lineage/file/table totals (cross-verify)
        lineage_df = safe_get_dataframe("DataLineage", "Data_Lineage")
        df_lineage = safe_get_dataframe("DataFlowLineage", "DataFlow_Lineage")

        # Helper to count unique non-null values across several possible column names
        def _unique_count(df: pd.DataFrame, candidates: list) -> int:
            if df is None or df.empty:
                return 0
            for c in candidates:
                if c in df.columns:
                    return int(df[c].dropna().nunique())
            return 0

        # For files, check several possible column names across both lineage tables
        src_file_cols = ["SourceFile", "Source_File", "SourceFilename", "SourceName", "Source"]
        tgt_file_cols = ["TargetFile", "Target_File", "TargetFilename", "SinkName", "Sink"]

        src_table_cols = ["SourceTable", "Source_Table"]
        tgt_table_cols = ["SinkTable", "Sink_Table"]

        # Aggregate unique values across DataLineage and DataFlowLineage when possible
        def _aggregate_unique(dfs: list, candidates: list) -> int:
            values = set()
            for df in dfs:
                if df is None or df.empty:
                    continue
                for c in candidates:
                    if c in df.columns:
                        vals = df[c].dropna().astype(str).str.strip()
                        values.update(vals[vals != ""].unique().tolist())
                        break
            return int(len(values))

        total_source_files = _aggregate_unique([lineage_df, df_lineage], src_file_cols)
        total_target_files = _aggregate_unique([lineage_df, df_lineage], tgt_file_cols)
        total_source_tables = _aggregate_unique([lineage_df, df_lineage], src_table_cols)
        total_target_tables = _aggregate_unique([lineage_df, df_lineage], tgt_table_cols)

        # Display as a compact metric row
        mcol1, mcol2, mcol3, mcol4 = st.columns(4)
        with mcol1:
            render_metric_card_premium(icon="📦", label="SRC FILES", value=format_number(total_source_files), gradient="gradient-purple")
        with mcol2:
            render_metric_card_premium(icon="📦", label="TGT FILES", value=format_number(total_target_files), gradient="gradient-blue")
        with mcol3:
            render_metric_card_premium(icon="🗄", label="SRC TABLES", value=format_number(total_source_tables), gradient="gradient-green")
        with mcol4:
            render_metric_card_premium(icon="🗄", label="TGT TABLES", value=format_number(total_target_tables), gradient="gradient-pink")

    def render_debug_panel(self):
        """Developer debug panel to inspect loaded sheets and key DataFrames"""
        if not st.session_state.get("show_debug_panel", False):
            return

        st.markdown("---")
        st.markdown("### 🐞 Debug Panel (Developer)")

        # Show loaded sheet keys
        excel_data = st.session_state.get("excel_data", {}) or {}
        st.write("Loaded sheet keys:", list(excel_data.keys()))

        # Show sheet_map and hyperlink_map if available
        if "sheet_map" in st.session_state:
            st.write("sheet_map:", st.session_state.get("sheet_map"))
        if "hyperlink_map" in st.session_state:
            st.write("hyperlink_map (sample 20):", dict(list(st.session_state.get("hyperlink_map", {}).items())[:20]))

        # Show small preview of Summary and ActivityCount
        try:
            summary_df = safe_get_dataframe("Summary")
            st.write("Summary (top 20):")
            if summary_df is None or summary_df.empty:
                st.write("(empty)")
            else:
                st.dataframe(summary_df.head(20))
        except Exception as e:
            st.write("Could not preview Summary:", e)

        try:
            act = safe_get_dataframe("ActivityCount")
            st.write("ActivityCount (top 20):")
            if act is None or act.empty:
                st.write("(empty)")
            else:
                # coerce Count if present
                if "Count" in act.columns:
                    act["Count"] = pd.to_numeric(act["Count"], errors="coerce").fillna(0).astype(int)
                st.dataframe(act.head(20))
        except Exception as e:
            st.write("Could not preview ActivityCount:", e)

    # NETWORK VISUALIZATION TAB

    def render_network_tab(self):
        """
        Render network visualization

        FIXED:
        - Safe graph access
        - Proper node filtering
        - Layout options
        - Error handling
        """

        st.markdown("### 🌐 Dependency Network Visualization")
        st.markdown("*Interactive visualization of your data factory dependencies*")

        if st.session_state.dependency_graph is None:
            st.warning(" No dependency graph available. Please load data first.")
            return

        G = st.session_state.dependency_graph

        if G.number_of_nodes() == 0:
            st.warning(" Dependency graph is empty. No relationships found.")
            return

        # Controls
        col1, col2, col3 = st.columns(3)

        with col1:
            show_node_types = st.multiselect(
                "🎨 Show Node Types",
                ["Triggers", "Pipelines", "DataFlows", "Datasets"],
                default=["Triggers", "Pipelines", "DataFlows"],
                key="net_node_types",
            )

        with col2:
            layout_type = st.selectbox(
                "📐 Layout Algorithm",
                ["Spring (Force)", "Circular", "Hierarchical", "Shell"],
                index=0,
                key="net_layout",
            )

        with col3:
            show_labels = st.checkbox("Show Labels", value=True, key="net_labels")

        # Filter graph by node types
        filtered_nodes = []
        for node, data in G.nodes(data=True):
            node_type = data.get("type", "unknown")

            if (
                (node_type == "trigger" and "Triggers" in show_node_types)
                or (node_type == "pipeline" and "Pipelines" in show_node_types)
                or (node_type == "dataflow" and "DataFlows" in show_node_types)
                or (node_type == "dataset" and "Datasets" in show_node_types)
            ):
                filtered_nodes.append(node)

        if not filtered_nodes:
            st.warning(" No nodes match the selected filters")
            return

        # --- Node selection options (allow user to focus the graph) ---
        with st.expander("🔎 Node Selection / Focus (optional)", expanded=False):
            sel_col1, sel_col2 = st.columns([2, 1])

            with sel_col1:
                node_mode = st.radio(
                    "Select nodes by:",
                    ["All (filtered types)", "Select nodes", "Top N by degree", "Search (substring/regex)"],
                    index=0,
                    key="net_node_mode",
                )

            with sel_col2:
                include_neighbors = st.checkbox("Include neighbors (1-hop)", value=False, key="net_include_neighbors")

            selected_nodes = set(filtered_nodes)

            if node_mode == "Select nodes":
                # Show a searchable multiselect of filtered nodes (limit to 1000)
                pick = st.multiselect(
                    "Choose nodes to display",
                    sorted(filtered_nodes),
                    default=[],
                    key="net_node_multiselect",
                )
                if pick:
                    selected_nodes = set(pick)
                else:
                    # If user didn't pick any, keep empty so we can warn later
                    selected_nodes = set()

            elif node_mode == "Top N by degree":
                max_n = min(50, max(5, int(len(filtered_nodes) / 5)))
                n = st.slider("Top N nodes by degree", min_value=3, max_value=max_n, value=min(15, max_n), key="net_topn")
                # compute degrees on filtered nodes
                degs = [(n_, G.degree(n_)) for n_ in filtered_nodes]
                degs_sorted = sorted(degs, key=lambda x: x[1], reverse=True)[:n]
                selected_nodes = set([d[0] for d in degs_sorted])

            elif node_mode == "Search (substring/regex)":
                q = st.text_input("Search nodes (substring or regex)", value="", key="net_search")
                try:
                    if q.strip():
                        pattern = re.compile(q, re.IGNORECASE)
                        matched = [n for n in filtered_nodes if pattern.search(n)]
                    else:
                        matched = []
                except re.error:
                    # treat as simple substring
                    matched = [n for n in filtered_nodes if q.lower() in n.lower()]

                selected_nodes = set(matched)

            # If include_neighbors, expand selection to 1-hop neighbors
            if include_neighbors and selected_nodes:
                neighbors = set()
                for n in list(selected_nodes):
                    try:
                        neighbors.update(set(G.predecessors(n)))
                        neighbors.update(set(G.successors(n)))
                    except Exception:
                        # Graph may not be directed or methods unavailable — try neighbors()
                        try:
                            neighbors.update(set(G.neighbors(n)))
                        except Exception:
                            pass
                selected_nodes.update(neighbors)

            # If user chose All (filtered types), keep selected_nodes as filtered_nodes
            if node_mode == "All (filtered types)":
                final_nodes = set(filtered_nodes)
            else:
                final_nodes = selected_nodes

            if not final_nodes:
                st.warning(" No nodes selected — adjust selection or switch to 'All (filtered types)'.")
                return

        # Create subgraph from final node set
        H = G.subgraph(sorted(final_nodes))

        if H.number_of_nodes() == 0:
            st.warning(" Filtered graph is empty")
            return

        # Calculate layout
        try:
            if layout_type.startswith("Spring"):
                pos = nx.spring_layout(
                    H, k=1 / np.sqrt(H.number_of_nodes()), iterations=50, seed=42
                )
            elif layout_type.startswith("Circular"):
                pos = nx.circular_layout(H)
            elif layout_type.startswith("Hierarchical"):
                # Try hierarchical, fallback to spring
                try:
                    pos = nx.kamada_kawai_layout(H)
                except:
                    pos = nx.spring_layout(H, seed=42)
            else:  # Shell
                pos = nx.shell_layout(H)
        except Exception as e:
            st.error(f"Layout calculation error: {e}")
            pos = nx.spring_layout(H, seed=42)

        # Render 2D network
        self.render_2d_network(H, pos, show_labels)

        # Network statistics
        st.markdown("---")
        st.markdown("###  Network Statistics")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            render_metric_card_premium(icon="🧩", label="NODES", value=H.number_of_nodes(), gradient="gradient-purple")

        with col2:
            render_metric_card_premium(icon="🕸", label="EDGES", value=H.number_of_edges(), gradient="gradient-blue")

        with col3:
            density = nx.density(H) if H.number_of_nodes() > 0 else 0
            render_metric_card_premium(icon="⚖", label="DENSITY", value=f"{density:.3f}", gradient="gradient-green")

        with col4:
            # Count node types
            node_types = Counter(
                data.get("type", "unknown") for _, data in H.nodes(data=True)
            )
            st.metric("Node Types", len(node_types))

    def render_2d_network(self, G, pos: dict, show_labels: bool):
        """
        Render 2D network using Plotly

        Args:
            G: NetworkX graph
            pos: Node positions
            show_labels: Whether to show node labels
        """

        # Extract edge coordinates
        edge_x = []
        edge_y = []

        for edge in G.edges():
            x0, y0 = pos[edge[0]]
            x1, y1 = pos[edge[1]]
            edge_x.extend([x0, x1, None])
            edge_y.extend([y0, y1, None])

        # Create premium edge trace with glassmorphism styling
        edge_trace = go.Scatter(
            x=edge_x,
            y=edge_y,
            mode="lines",
            line=dict(width=2, color="rgba(102, 126, 234, 0.4)"),
            hoverinfo="none",
            showlegend=False,
        )

        # Extract node coordinates and attributes
        node_x = []
        node_y = []
        node_colors = []
        node_text = []
        node_sizes = []

        for node in G.nodes():
            x, y = pos[node]
            node_x.append(x)
            node_y.append(y)

            # Get node data
            node_data = G.nodes[node]
            node_type = node_data.get("type", "unknown")

            # Determine premium color and size using gradients
            if node_type == "trigger":
                color = PremiumColors.GRADIENTS['gradient_1'][0]
                icon = "🔔"
                size = 30
            elif node_type == "pipeline":
                if node_data.get("is_orphaned"):
                    color = PremiumColors.WARNING
                    icon = "⚠️"
                elif node_data.get("has_trigger"):
                    color = PremiumColors.GRADIENTS['gradient_5'][0]
                    icon = "🚀"
                else:
                    color = PremiumColors.GRADIENTS['gradient_2'][0]
                    icon = "📦"
                size = 25
            elif node_type == "dataflow":
                color = PremiumColors.GRADIENTS['gradient_3'][0]
                icon = "🌊"
                size = 25
            elif node_type == "dataset":
                color = PremiumColors.GRADIENTS['gradient_4'][0]
                icon = "📊"
                size = 20
            else:
                color = PremiumColors.GRADIENTS['gradient_8'][0]
                icon = "❓"
                size = 20

            node_colors.append(color)
            node_text.append(f"{icon} {node}")

            # Size based on connections
            degree = G.degree(node)
            node_sizes.append(size + degree * 2)

        # Create premium node trace with enhanced styling
        node_trace = go.Scatter(
            x=node_x,
            y=node_y,
            mode="markers+text" if show_labels else "markers",
            marker=dict(
                size=node_sizes, 
                color=node_colors, 
                line=dict(color='rgba(255, 255, 255, 0.8)', width=2),
                opacity=0.9
            ),
            text=node_text if show_labels else None,
            textposition="top center",
            textfont=dict(size=11, color=PremiumColors.PRIMARY, family='Inter'),
            hovertext=node_text,
            hoverinfo="text",
            showlegend=False,
        )

        # Create premium figure with glassmorphism layout
        fig = go.Figure(data=[edge_trace, node_trace])

        fig.update_layout(
            **PREMIUM_CHART_TEMPLATE['layout'],
            title={
                "text": f"🌐 Dependency Network ({G.number_of_nodes()} nodes, {G.number_of_edges()} edges)",
                "font": {"size": 22, "color": PremiumColors.PRIMARY, "family": "Inter"},
                "x": 0.5,
                "xanchor": "center"
            },
            showlegend=False,
            hovermode="closest",
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            height=650,
        )

        st.plotly_chart(fig, use_container_width=True)

        # Legend
        st.markdown("---")
        st.markdown("### 📖 Legend")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.markdown(
                '<span class="badge" style="background: #FFD700; color: black;">🔔 Triggers</span>',
                unsafe_allow_html=True,
            )
        with col2:
            st.markdown(
                '<span class="badge" style="background: #90EE90; color: black;"> Pipelines (Triggered)</span>',
                unsafe_allow_html=True,
            )
        with col3:
            st.markdown(
                '<span class="badge" style="background: #DDA0DD; color: #111827;">🌊 DataFlows</span>' if not theme_is_dark() else '<span class="badge" style="background: #DDA0DD; color: white;">🌊 DataFlows</span>',
                unsafe_allow_html=True,
            )
        with col4:
            st.markdown(
                '<span class="badge" style="background: #FFA07A; color: #111827;"> Orphaned</span>' if not theme_is_dark() else '<span class="badge" style="background: #FFA07A; color: white;"> Orphaned</span>',
                unsafe_allow_html=True,
            )

    # IMPACT ANALYSIS TAB

    def render_impact_analysis_tab(self):
        """
        Render impact analysis with visual hierarchy

        FIXED:
        - All HTML properly rendered
        - Pie chart fixed
        - Sankey diagram working
        """

        st.markdown("###  Impact Analysis Dashboard")
        st.markdown("*Understand the blast radius of changes before making them*")

        impact_df = safe_get_dataframe('ImpactAnalysis', 'PipelineAnalysis', 'Pipeline_Analysis')

        if impact_df.empty:
            st.warning(" No impact analysis data available")
            return

        # Ensure required columns exist
        if 'Pipeline' not in impact_df.columns:
            st.error(" Missing 'Pipeline' column in impact data")
            return

        # Add Impact column if missing (default to LOW)
        if 'Impact' not in impact_df.columns:
            impact_df['Impact'] = 'LOW'

        # Impact Distribution Overview

        col1, col2 = st.columns([1, 2])

        with col1:
            # Impact level counts
            impact_counts = impact_df['Impact'].value_counts()
            labels = []
            values = []
            colors_list = []

            for impact_level in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
                count = impact_counts.get(impact_level, 0)
                if count > 0:  # Only add non-zero counts
                    labels.append(impact_level)
                    values.append(count)

                    # Assign colors
                    if impact_level == 'CRITICAL':
                        colors_list.append('#FF4444')
                    elif impact_level == 'HIGH':
                        colors_list.append('#FF8800')
                    elif impact_level == 'MEDIUM':
                        colors_list.append('#FFBB33')
                    else:  # LOW
                        colors_list.append('#00C851')
            if labels and values:
                fig = go.Figure(data=[go.Pie(
                    labels=labels,
                    values=values,
                    hole=0.5,
                    marker=dict(colors=colors_list),
                    textinfo='none',
                    texttemplate='<b>%{label}</b><br>%{value}<br>(%{percent:.1%})',
                    hovertemplate='<b>%{label}</b><br>Count: %{value}<br>%{percent:.1%}<extra></extra>'
                )])

                fig.update_layout(
                    title="Impact Distribution",
                    height=300,
                    margin=dict(l=20, r=20, t=40, b=20),
                    showlegend=True,
                    legend=dict(
                        orientation="v",
                        yanchor="middle",
                        y=0.5,
                        xanchor="left",
                        x=1.05
                    )
                )

                safe_plotly(fig)
            else:
                st.info(" No impact data to visualize")

        with col2:
            # Impact level metrics
            st.markdown("####  Impact Summary")

            metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)

            critical_count = impact_counts.get('CRITICAL', 0)
            high_count = impact_counts.get('HIGH', 0)
            medium_count = impact_counts.get('MEDIUM', 0)
            low_count = impact_counts.get('LOW', 0)

            with metric_col1:
                st.markdown(f"""
                <div class="metric-card badge-critical" style="padding: 1rem;">
                    <div class="metric-label">CRITICAL</div>
                    <div class="metric-value" style="font-size: 2em;">{critical_count}</div>
                </div>
                """, unsafe_allow_html=True)

            with metric_col2:
                st.markdown(f"""
                <div class="metric-card badge-high" style="padding: 1rem;">
                    <div class="metric-label">HIGH</div>
                    <div class="metric-value" style="font-size: 2em;">{high_count}</div>
                </div>
                """, unsafe_allow_html=True)

            with metric_col3:
                st.markdown(f"""
                <div class="metric-card badge-medium" style="padding: 1rem;">
                    <div class="metric-label">MEDIUM</div>
                    <div class="metric-value" style="font-size: 2em;">{medium_count}</div>
                </div>
                """, unsafe_allow_html=True)

            with metric_col4:
                st.markdown(f"""
                <div class="metric-card badge-low" style="padding: 1rem;">
                    <div class="metric-label">LOW</div>
                    <div class="metric-value" style="font-size: 2em;">{low_count}</div>
                </div>
                """, unsafe_allow_html=True)

        st.markdown("---")

        # Filter Controls

        col1, col2, col3 = st.columns(3)

        with col1:
            impact_filter = st.multiselect(
                " Filter by Impact",
                ["CRITICAL", "HIGH", "MEDIUM", "LOW"],
                default=["CRITICAL", "HIGH"],
                key="impact_filter_main"
            )

        with col2:
            orphan_filter = st.selectbox(
                " Show Orphaned",
                ["All", "Only Orphaned", "Exclude Orphaned"],
                index=0,
                key="impact_orphan_filter"
            )

        with col3:
            sort_by = st.selectbox(
                " Sort By",
                ["Impact (Critical First)", "Blast Radius (High to Low)", "Name (A-Z)"],
                index=0,
                key="impact_sort"
            )

        # Apply filters
        filtered_df = impact_df.copy()

        if impact_filter:
            filtered_df = filtered_df[filtered_df['Impact'].isin(impact_filter)]

        if 'IsOrphaned' in filtered_df.columns:
            if orphan_filter == "Only Orphaned":
                filtered_df = filtered_df[filtered_df['IsOrphaned'] == 'Yes']
            elif orphan_filter == "Exclude Orphaned":
                filtered_df = filtered_df[filtered_df['IsOrphaned'] != 'Yes']

        # Sort
        if sort_by == "Impact (Critical First)":
            impact_order = {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2, 'LOW': 3}
            filtered_df['_sort'] = filtered_df['Impact'].map(impact_order).fillna(999)
            filtered_df = filtered_df.sort_values('_sort').drop('_sort', axis=1)
        elif sort_by == "Blast Radius (High to Low)":
            if 'BlastRadius' in filtered_df.columns:
                filtered_df = filtered_df.sort_values('BlastRadius', ascending=False)
        else:  # Name A-Z
            filtered_df = filtered_df.sort_values('Pipeline')

        if filtered_df.empty:
            st.info("📭 No pipelines match the selected filters")
            return

        st.markdown(f"###  Pipeline Impact Details ({len(filtered_df)} pipelines)")

        # Pipeline Selection for Detailed View

        selected_pipeline = st.selectbox(
            " Select pipeline for detailed analysis",
            filtered_df['Pipeline'].tolist(),
            key="impact_selected_pipeline"
        )

        if selected_pipeline:
            pipeline_data = filtered_df[filtered_df['Pipeline'] == selected_pipeline].iloc[0]

            # Detailed view
            col1, col2 = st.columns([1, 2])

            with col1:
                # Pipeline info card
                impact = pipeline_data.get('Impact', 'LOW')
                blast_radius = pipeline_data.get('BlastRadius', 0)
                is_orphaned = pipeline_data.get('IsOrphaned', 'No')

                impact_color = {
                    'CRITICAL': '#FF4444',
                    'HIGH': '#FF8800',
                    'MEDIUM': '#FFBB33',
                    'LOW': '#00C851'
                }.get(impact, '#999999')
                status_html = (
                    "<span style='color: #FF4444;'> Orphaned</span>"
                    if is_orphaned == 'Yes'
                    else "<span style='color: #00C851;'> Active</span>"
                )

                # Theme-aware badge text: ensure contrast for light theme on mid-tone backgrounds
                if theme_is_dark():
                    badge_text = "#ffffff"
                else:
                    badge_text = "#111827" if impact in ("MEDIUM", "LOW", "LOW") else "#ffffff"
                body = (
                    f"<h3 class='app-heading' style='margin-bottom:8px;'>{selected_pipeline}</h3>"
                    f"<div style='margin: 8px 0;'><strong>Impact Level:</strong><br>"
                    f"<span class='badge' style='background: {impact_color}; color: {badge_text}; font-size: 1.05em;'>{impact}</span></div>"
                    f"<div style='margin: 8px 0;'><strong>Blast Radius:</strong> {blast_radius} resources</div>"
                    f"<div style='margin: 8px 0;'><strong>Status:</strong> {status_html}</div>"
                )

                render_info_card(selected_pipeline, body, color=impact_color)

                # Metrics
                st.markdown("####  Dependency Counts")

                trigger_count = pipeline_data.get('DirectUpstreamTriggerCount', 0)
                upstream_count = pipeline_data.get('DirectUpstreamPipelineCount', 0)
                downstream_count = pipeline_data.get('DirectDownstreamPipelineCount', 0)
                dataflow_count = pipeline_data.get('DataFlowCount', 0)

                dep_col1, dep_col2 = st.columns(2)
                with dep_col1:
                    render_metric_card_premium(icon="⏰", label="TRIGGERS", value=int(trigger_count) if pd.notna(trigger_count) else 0, gradient="gradient-purple")
                with dep_col2:
                    render_metric_card_premium(icon="⬆", label="UPSTREAM", value=int(upstream_count) if pd.notna(upstream_count) else 0, gradient="gradient-blue")

                dep_col3, dep_col4 = st.columns(2)
                with dep_col3:
                    render_metric_card_premium(icon="⬇", label="DOWNSTREAM", value=int(downstream_count) if pd.notna(downstream_count) else 0, gradient="gradient-green")
                with dep_col4:
                    render_metric_card_premium(icon="🌊", label="DATAFLOWS", value=int(dataflow_count) if pd.notna(dataflow_count) else 0, gradient="gradient-pink")

            with col2:
                # Dependency visualization
                st.markdown("#### 🌐 Dependency Map")

                self.render_pipeline_dependency_sankey(pipeline_data)

        st.markdown("---")

        # Full Table View

        with st.expander(" View All Pipeline Details"):
            # Select columns to display
            display_columns = ['Pipeline', 'Impact', 'BlastRadius']

            optional_columns = [
                'DirectUpstreamTriggerCount',
                'DirectUpstreamPipelineCount',
                'DirectDownstreamPipelineCount',
                'DataFlowCount',
                'IsOrphaned'
            ]

            for col in optional_columns:
                if col in filtered_df.columns:
                    display_columns.append(col)

            display_df = filtered_df[display_columns].copy()
            def style_impact_row(row):
                """Style entire row based on impact"""
                impact = row['Impact']

                if impact == 'CRITICAL':
                    return ['background-color: #ffebee'] * len(row)
                elif impact == 'HIGH':
                    return ['background-color: #fff3e0'] * len(row)
                elif impact == 'MEDIUM':
                    return ['background-color: #fffde7'] * len(row)
                elif impact == 'LOW':
                    return ['background-color: #e8f5e9'] * len(row)
                return [''] * len(row)

            # Apply styling
            styled_df = display_df.style.apply(style_impact_row, axis=1)

            st.dataframe(styled_df, use_container_width=True, height=400)

            # Export button
            csv_bytes = to_csv_bytes(display_df)
            st.download_button(
                label=" Download Impact Analysis CSV",
                data=csv_bytes,
                file_name="impact_analysis.csv",
                mime="text/csv",
                key="download_impact_csv",
            )

    def render_pipeline_dependency_sankey(self, pipeline_data):
        """
        Render Sankey diagram for pipeline dependencies

        FIXED:
        - Proper None/empty handling
        - Better visualization
        - Fallback messages
        """

        # Extract dependencies
        pipeline_name = pipeline_data.get('Pipeline', 'Unknown')
        def safe_split(value):
            """Split string safely, return empty list if None/empty"""
            if pd.isna(value):
                return []

            value_str = str(value).strip()

            if not value_str or value_str in ['', 'None', 'nan', 'NaN']:
                return []

            return [x.strip() for x in value_str.split(',') if x.strip() and x.strip() not in ['None', 'nan', 'NaN', '']]

        # Extract all dependency types
        triggers = safe_split(pipeline_data.get('DirectUpstreamTriggers', ''))
        upstream = safe_split(pipeline_data.get('DirectUpstreamPipelines', ''))
        downstream = safe_split(pipeline_data.get('DirectDownstreamPipelines', ''))
        dataflows = safe_split(pipeline_data.get('UsedDataFlows', ''))
        total_deps = len(triggers) + len(upstream) + len(downstream) + len(dataflows)

        if total_deps == 0:
            st.info("📭 No dependencies to visualize for this pipeline")

            # Show details
            with st.expander("ℹ Why is this empty?"):
                st.markdown(f"""
                **Pipeline:** `{pipeline_name}`

                **Dependency Counts:**
                - ⏰ Upstream Triggers: {len(triggers)}
                - ⬆ Upstream Pipelines: {len(upstream)}
                - ⬇ Downstream Pipelines: {len(downstream)}
                - 🌊 DataFlows Used: {len(dataflows)}

                **Possible reasons:**
                - Pipeline is orphaned (no trigger)
                - Pipeline is a leaf node (no downstream)
                - Pipeline doesn't use DataFlows

                **Check Impact Analysis tab for full details.**
                """)
            return

        # Build Sankey data
        labels = []
        sources = []
        targets = []
        values = []
        colors = []

        # Node index mapping
        node_index = {}
        current_idx = 0

        # Add pipeline as central node
        labels.append(pipeline_name)
        node_index[pipeline_name] = current_idx
        current_idx += 1

        # Add triggers → pipeline
        for trigger in triggers[:5]:  # Limit to 5 for clarity
            if trigger not in node_index:
                labels.append(trigger)
                node_index[trigger] = current_idx
                current_idx += 1

            sources.append(node_index[trigger])
            targets.append(node_index[pipeline_name])
            values.append(3)
            colors.append('rgba(255, 215, 0, 0.5)')  # Gold

        # Add upstream pipelines → pipeline
        for pipe in upstream[:5]:
            if pipe not in node_index:
                labels.append(pipe)
                node_index[pipe] = current_idx
                current_idx += 1

            sources.append(node_index[pipe])
            targets.append(node_index[pipeline_name])
            values.append(2)
            colors.append('rgba(135, 206, 235, 0.5)')  # Sky blue

        # Add pipeline → downstream pipelines
        for pipe in downstream[:5]:
            if pipe not in node_index:
                labels.append(pipe)
                node_index[pipe] = current_idx
                current_idx += 1

            sources.append(node_index[pipeline_name])
            targets.append(node_index[pipe])
            values.append(2)
            colors.append('rgba(144, 238, 144, 0.5)')  # Light green

        # Add pipeline → dataflows
        for df in dataflows[:5]:
            if df not in node_index:
                labels.append(df)
                node_index[df] = current_idx
                current_idx += 1

            sources.append(node_index[pipeline_name])
            targets.append(node_index[df])
            values.append(1)
            colors.append('rgba(221, 160, 221, 0.5)')  # Plum
        if not sources or not targets:
            st.warning(" Could not build dependency graph - no valid links found")
            return

        # Create Sankey diagram
        try:
            fig = go.Figure(data=[go.Sankey(
                node=dict(
                    pad=15,
                    thickness=20,
                    line=dict(color="white", width=2),
                    label=labels,
                    color=[
                        '#90EE90' if l == pipeline_name else  # Light green for main pipeline
                        '#FFD700' if l in triggers else       # Gold for triggers
                        '#DDA0DD' if l in dataflows else      # Plum for dataflows
                        '#87CEEB'                             # Sky blue for pipelines
                        for l in labels
                    ],
                    hovertemplate='<b>%{label}</b><extra></extra>'
                ),
                link=dict(
                    source=sources,
                    target=targets,
                    value=values,
                    color=colors,
                    hovertemplate='%{source.label} → %{target.label}<extra></extra>'
                )
            )])

            fig.update_layout(
                title={
                    'text': f"Dependencies: {pipeline_name}",
                    'font': {'size': 16}
                },
                height=400,
                margin=dict(l=20, r=20, t=50, b=20),
                font=dict(size=10)
            )

            safe_plotly(fig)

            # Legend
            st.markdown("""
            **Legend:**
             Triggers · 🔵 Upstream Pipelines ·  Downstream Pipelines · 🟣 DataFlows
            """)

        except Exception as e:
            st.error(f" Could not render Sankey diagram: {e}")

            with st.expander(" Debug Info"):
                st.write("Labels:", labels)
                st.write("Sources:", sources)
                st.write("Targets:", targets)
                st.write("Values:", values)

    # ORPHANED RESOURCES TAB

    def render_orphaned_resources_tab(self):
        """
        Render orphaned resources analysis

        FIXED:
        - Multiple orphaned resource types
        - Recommendations
        - Cleanup suggestions
        """

        st.markdown("###  Orphaned Resources Analysis")
        st.markdown("*Identify unused resources that can be cleaned up*")

        # Summary Cards

        orphaned_pipelines = safe_get_dataframe(
            "OrphanedPipelines", "Orphaned_Pipelines"
        )
        orphaned_datasets = safe_get_dataframe("OrphanedDatasets", "Orphaned_Datasets")
        orphaned_linkedservices = safe_get_dataframe(
            "OrphanedLinkedServices", "Orphaned_LinkedServices"
        )
        orphaned_triggers = safe_get_dataframe("OrphanedTriggers", "Orphaned_Triggers")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            count = len(orphaned_pipelines)
            st.markdown(
                f"""
            <div class="metric-card gradient-fire">
                <div style="font-size: 2em;">📦</div>
                <div class="metric-label">Orphaned Pipelines</div>
                <div class="metric-value">{count}</div>
            </div>
            """,
                unsafe_allow_html=True,
            )

        with col2:
            count = len(orphaned_datasets)
            st.markdown(
                f"""
            <div class="metric-card gradient-orange">
                <div style="font-size: 2em;"></div>
                <div class="metric-label">Orphaned Datasets</div>
                <div class="metric-value">{count}</div>
            </div>
            """,
                unsafe_allow_html=True,
            )

        with col3:
            count = len(orphaned_linkedservices)
            st.markdown(
                f"""
            <div class="metric-card gradient-pink">
                <div style="font-size: 2em;">🔗</div>
                <div class="metric-label">Orphaned Services</div>
                <div class="metric-value">{count}</div>
            </div>
            """,
                unsafe_allow_html=True,
            )

        with col4:
            count = len(orphaned_triggers)
            st.markdown(
                f"""
            <div class="metric-card gradient-teal">
                <div style="font-size: 2em;">⏰</div>
                <div class="metric-label">Broken/Inactive Triggers</div>
                <div class="metric-value">{count}</div>
            </div>
            """,
                unsafe_allow_html=True,
            )

        st.markdown("---")

        # Orphaned Resources Breakdown

        tabs = st.tabs(
            ["📦 Pipelines", " Datasets", "🔗 Linked Services", "⏰ Triggers"]
        )

        # Tab 1: Orphaned Pipelines
        with tabs[0]:
            if orphaned_pipelines.empty:
                st.success(" No orphaned pipelines found!")
            else:
                st.markdown(f"#### 📦 Orphaned Pipelines ({len(orphaned_pipelines)})")
                st.markdown("*Pipelines with no triggers or callers*")

                # Display table
                if "Pipeline" in orphaned_pipelines.columns:
                    display_cols = ["Pipeline"]
                    if "Reason" in orphaned_pipelines.columns:
                        display_cols.append("Reason")
                    if "Recommendation" in orphaned_pipelines.columns:
                        display_cols.append("Recommendation")

                    st.dataframe(
                        orphaned_pipelines[display_cols],
                        use_container_width=True,
                        height=400,
                    )

                    # Export button
                    csv_bytes = to_csv_bytes(orphaned_pipelines)
                    st.download_button(
                        label=" Download Orphaned Pipelines CSV",
                        data=csv_bytes,
                        file_name="orphaned_pipelines.csv",
                        mime="text/csv",
                        key="download_orphaned_pipelines",
                    )
                else:
                    st.dataframe(orphaned_pipelines, use_container_width=True)

        # Tab 2: Orphaned Datasets
        with tabs[1]:
            if orphaned_datasets.empty:
                st.success(" No orphaned datasets found!")
            else:
                st.markdown(f"####  Orphaned Datasets ({len(orphaned_datasets)})")
                st.markdown("*Datasets not used by any pipeline or dataflow*")

                if "Dataset" in orphaned_datasets.columns:
                    display_cols = ["Dataset"]
                    if "Reason" in orphaned_datasets.columns:
                        display_cols.append("Reason")
                    if "Recommendation" in orphaned_datasets.columns:
                        display_cols.append("Recommendation")

                    st.dataframe(
                        orphaned_datasets[display_cols],
                        use_container_width=True,
                        height=400,
                    )

                    csv_bytes = to_csv_bytes(orphaned_datasets)
                    st.download_button(
                        label=" Download Orphaned Datasets CSV",
                        data=csv_bytes,
                        file_name="orphaned_datasets.csv",
                        mime="text/csv",
                        key="download_orphaned_datasets",
                    )
                else:
                    st.dataframe(orphaned_datasets, use_container_width=True)

        # Tab 3: Orphaned Linked Services
        with tabs[2]:
            if orphaned_linkedservices.empty:
                st.success(" No orphaned linked services found!")
            else:
                st.markdown(
                    f"#### 🔗 Orphaned Linked Services ({len(orphaned_linkedservices)})"
                )
                st.markdown("*Linked services not used by any dataset or dataflow*")

                if "LinkedService" in orphaned_linkedservices.columns:
                    display_cols = ["LinkedService"]
                    if "Reason" in orphaned_linkedservices.columns:
                        display_cols.append("Reason")
                    if "Recommendation" in orphaned_linkedservices.columns:
                        display_cols.append("Recommendation")

                    st.dataframe(
                        orphaned_linkedservices[display_cols],
                        use_container_width=True,
                        height=400,
                    )

                    csv_bytes = to_csv_bytes(orphaned_linkedservices)
                    st.download_button(
                        label=" Download Orphaned Services CSV",
                        data=csv_bytes,
                        file_name="orphaned_linkedservices.csv",
                        mime="text/csv",
                        key="download_orphaned_services",
                    )
                else:
                    st.dataframe(orphaned_linkedservices, use_container_width=True)

        # Tab 4: Orphaned/Broken Triggers
        with tabs[3]:
            if orphaned_triggers.empty:
                st.success(" No broken or inactive triggers found!")
            else:
                st.markdown(
                    f"#### ⏰ Broken/Inactive Triggers ({len(orphaned_triggers)})"
                )
                st.markdown("*Triggers that are stopped or misconfigured*")

                # Group by type if available
                if "Type" in orphaned_triggers.columns:
                    type_counts = orphaned_triggers["Type"].value_counts()
                    total_count = int(len(orphaned_triggers))

                    col1, col2, col3 = st.columns(3)

                    with col1:
                        render_metric_card_premium(
                            icon="🛑", label="INACTIVE", value=int(type_counts.get("Inactive", 0)), gradient="gradient-purple"
                        )
                    with col2:
                        render_metric_card_premium(
                            icon="🔗", label="BROKEN REF", value=int(type_counts.get("BrokenReference", 0)), gradient="gradient-blue"
                        )
                    with col3:
                        render_metric_card_premium(
                            icon="⚠", label="MISCONFIG", value=int(type_counts.get("Misconfigured", 0)), gradient="gradient-pink"
                        )

                    st.markdown("---")

                display_cols = []
                for col in [
                    "Trigger",
                    "Pipeline",
                    "State",
                    "Reason",
                    "Type",
                    "Recommendation",
                ]:
                    if col in orphaned_triggers.columns:
                        display_cols.append(col)

                if display_cols:
                    st.dataframe(
                        orphaned_triggers[display_cols],
                        use_container_width=True,
                        height=400,
                    )
                else:
                    st.dataframe(orphaned_triggers, use_container_width=True)

                csv_bytes = to_csv_bytes(orphaned_triggers)
                st.download_button(
                    label=" Download Trigger Issues CSV",
                    data=csv_bytes,
                    file_name="orphaned_triggers.csv",
                    mime="text/csv",
                    key="download_orphaned_triggers",
                )

        st.markdown("---")

        # Cleanup Recommendations

        st.markdown("###  Cleanup Recommendations")

        total_orphaned = (
            len(orphaned_pipelines)
            + len(orphaned_datasets)
            + len(orphaned_linkedservices)
            + len(orphaned_triggers)
        )

        if total_orphaned == 0:
            st.success(
                "🎉 Excellent! No orphaned resources found. Your factory is well-maintained!"
            )
        else:
            body = (
                f"<p>Found <strong>{total_orphaned}</strong> orphaned or broken resources.</p>"
                "<h4 style='margin-top:12px;'>Recommended Steps:</h4>"
                "<ol>"
                "<li><strong>Review orphaned pipelines</strong> - Verify they're truly unused before deletion</li>"
                "<li><strong>Check broken trigger references</strong> - Fix or remove broken triggers</li>"
                "<li><strong>Clean up datasets</strong> - Remove datasets not used by any pipeline</li>"
                "<li><strong>Archive linked services</strong> - Keep for future use or remove if obsolete</li>"
                "<li><strong>Document before deletion</strong> - Export the lists above for records</li>"
                "</ol>"
                "<p style='margin-top: 12px; padding: 10px; background: #fff3cd; border-radius: 5px;'>"
                " <strong>Tip:</strong> Use the download buttons above to export lists before cleanup."
                " Start with pipelines that have LOW impact first."
                "</p>"
            )
            render_info_card(" Action Required", body, color="#FF8800")

    # STATISTICS TAB

    def render_statistics_tab(self):
        """
        Render statistics dashboard

        FIXED:
        - Multiple chart types
        - Activity distribution
        - Resource usage
        - Trend analysis
        """

        st.markdown("###  Statistics & Analytics Dashboard")

        # Activity statistics
        activity_df = safe_get_dataframe("ActivityCount")

        if not activity_df.empty:
            st.markdown("#### ⚡ Activity Type Distribution")

            # Remove total row
            activity_df = activity_df[
                ~activity_df["ActivityType"].str.contains("TOTAL", na=False)
            ]

            # Ensure Count column is numeric for charts
            if "Count" in activity_df.columns:
                activity_df["Count"] = pd.to_numeric(
                    activity_df["Count"], errors="coerce"
                ).fillna(0).astype(int)

            col1, col2 = st.columns(2)

            with col1:
                # Premium horizontal bar chart for activity types
                fig = create_premium_chart()
                fig.add_trace(
                    go.Bar(
                        y=activity_df["ActivityType"].head(10),
                        x=activity_df["Count"].head(10),
                        orientation="h",
                        marker=dict(
                            color=[PremiumColors.GRADIENTS[f'gradient_{i%8+1}'][0] for i in range(10)],
                            line=dict(color=theme_line_color(0.8), width=1.5)
                        ),
                        text=activity_df["Count"].head(10),
                        textposition="auto",
                        textfont=dict(size=12, color=theme_text_color(), family='Inter'),
                        hovertemplate="<b>%{y}</b><br>" +
                                      "Count: <b>%{x:,}</b>" +
                                      "<extra></extra>",
                    )
                )

                fig.update_layout(
                    title={
                        "text": "🚀 Top 10 Activity Types",
                        "font": {"size": 20, "color": PremiumColors.PRIMARY, "family": "Inter"},
                        "x": 0.5,
                        "xanchor": "center"
                    },
                    xaxis_title="Count",
                    yaxis_title="Activity Type",
                    height=450,
                )

                safe_plotly(fig, df=activity_df, required_columns=["ActivityType", "Count"], info_message=" No activity data to display")

            with col2:
                # Pie chart (ensure percentages display correctly)
                # Prepare pie data (group, coerce numeric, drop zeros)
                labels_slice, values_slice = prepare_pie_data(activity_df, "ActivityType", "Count", top_n=8)

                if not labels_slice:
                    st.info(" No activity breakdown to display")
                else:
                    # Premium pie chart for activity breakdown
                    fig = go.Figure(
                        data=[
                            go.Pie(
                                labels=labels_slice,
                                values=values_slice,
                                hole=0.4,
                                marker=dict(
                                    colors=[PremiumColors.GRADIENTS[f'gradient_{i%8+1}'][0] for i in range(len(labels_slice))],
                                    line=dict(color=theme_line_color(0.8), width=2)
                                ),
                                textinfo="none",
                                texttemplate="%{label}<br>%{percent:.1%} (%{value})",
                                insidetextorientation="radial",
                                textfont=dict(size=11, color=theme_text_color(), family='Inter'),
                                hovertemplate="<b>%{label}</b><br>" +
                                              "Count: <b>%{value:,}</b><br>" +
                                              "Percentage: <b>%{percent:.1%}</b>" +
                                              "<extra></extra>",
                            )
                        ]
                    )

                fig.update_layout(
                    **PREMIUM_CHART_TEMPLATE['layout'],
                    title={
                        "text": "📊 Activity Type Breakdown",
                        "font": {"size": 20, "color": PremiumColors.PRIMARY, "family": "Inter"},
                        "x": 0.5,
                        "xanchor": "center"
                    },
                    height=450,
                    showlegend=True,
                    legend=dict(
                        orientation="v", 
                        yanchor="middle", 
                        y=0.5, 
                        xanchor="left", 
                        x=1.05,
                        bgcolor=theme_overlay_bg(0.9),
                        bordercolor='rgba(102, 126, 234, 0.3)',
                        borderwidth=1,
                        font=dict(size=11, family='Inter', color=theme_text_color())
                    ),
                )

                safe_plotly(fig, df=activity_df, required_columns=["ActivityType", "Count"], info_message=" No activity data to display")

        st.markdown("---")

        # Resource Usage Statistics

        dataset_usage = safe_get_dataframe(
            "DatasetUsage", "Dataset_Usage", "Datasetusage", "datasetusage"
        )

        if not dataset_usage.empty:
            st.markdown("####  Dataset Usage Statistics")

            # Top used datasets
            if "UsageCount" in dataset_usage.columns:
                top_datasets = dataset_usage.nlargest(10, "UsageCount")

                # Premium vertical bar chart for dataset usage
                fig = create_premium_chart()
                fig.add_trace(
                    go.Bar(
                        x=top_datasets["Dataset"],
                        y=top_datasets["UsageCount"],
                        marker=dict(
                            color=[PremiumColors.GRADIENTS[f'gradient_{i%8+1}'][0] for i in range(len(top_datasets))],
                            line=dict(color=theme_line_color(0.8), width=1.5)
                        ),
                        text=top_datasets["UsageCount"],
                        textposition="auto",
                        textfont=dict(size=12, color=theme_text_color(), family='Inter'),
                        hovertemplate="<b>%{x}</b><br>" +
                                      "Usage Count: <b>%{y:,}</b>" +
                                      "<extra></extra>",
                    )
                )

                fig.update_layout(
                    title={
                        "text": "📈 Top 10 Most Used Datasets",
                        "font": {"size": 20, "color": PremiumColors.PRIMARY, "family": "Inter"},
                        "x": 0.5,
                        "xanchor": "center"
                    },
                    xaxis_title="Dataset",
                    yaxis_title="Usage Count",
                    height=450,
                    xaxis={"tickangle": -45},
                )

                st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # Transformation Usage

        trans_usage = safe_get_dataframe(
            "TransformationUsage",
            "Transformation_Usage",
            "Transformationusage",
            "transformationusage",
        )

        if not trans_usage.empty:
            st.markdown("#### 🔄 DataFlow Transformation Usage")

            col1, col2 = st.columns(2)

            with col1:
                # Premium bar chart for transformations
                fig = create_premium_chart()
                fig.add_trace(
                    go.Bar(
                        x=trans_usage["TransformationType"],
                        y=trans_usage["UsageCount"],
                        marker=dict(
                            color=[PremiumColors.GRADIENTS[f'gradient_{i%8+1}'][0] for i in range(len(trans_usage))],
                            line=dict(color=theme_line_color(0.8), width=1.5)
                        ),
                        text=trans_usage["UsageCount"],
                        textposition="auto",
                        textfont=dict(size=12, color=theme_text_color(), family='Inter'),
                        hovertemplate="<b>%{x}</b><br>" +
                                      "Usage Count: <b>%{y:,}</b>" +
                                      "<extra></extra>",
                    )
                )

                fig.update_layout(
                    title={
                        "text": "🔄 Transformation Types",
                        "font": {"size": 20, "color": PremiumColors.PRIMARY, "family": "Inter"},
                        "x": 0.5,
                        "xanchor": "center"
                    },
                    xaxis_title="Type",
                    yaxis_title="Count",
                    height=400,
                    xaxis={"tickangle": -45},
                )

                st.plotly_chart(fig, use_container_width=True)

            with col2:
                # Table view
                st.dataframe(
                    trans_usage[["TransformationType", "UsageCount", "Percentage"]],
                    use_container_width=True,
                    height=350,
                )

    # DATAFLOW ANALYSIS TAB

    def render_dataflow_tab(self):
        """
        Render DataFlow analysis

        FIXED:
        - DataFlow lineage visualization
        - Transformation analysis
        - Source/Sink tracking
        """

        st.markdown("### 🌊 DataFlow Analysis Dashboard")

        dataflow_df = safe_get_dataframe("DataFlows", "DataFlow_Summary")
        lineage_df = safe_get_dataframe("DataFlowLineage", "DataFlow_Lineage")
        trans_df = safe_get_dataframe(
            "DataFlowTransformations", "DataFlow_Transformations"
        )

        if dataflow_df.empty:
            st.info(" No DataFlow data available")
            return

        # DataFlow Overview

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total DataFlows", len(dataflow_df))

        with col2:
            # Robustly sum any column that represents sources (e.g. Sources, SourceCount, NumSources)
            total_sources = sum_numeric_columns_by_keywords(dataflow_df, ["source", "sources"]) or 0
            st.metric("Total Sources", int(total_sources))

        with col3:
            total_sinks = sum_numeric_columns_by_keywords(dataflow_df, ["sink", "sinks", "target"]) or 0
            st.metric("Total Sinks", int(total_sinks))

        with col4:
            total_trans = sum_numeric_columns_by_keywords(dataflow_df, ["transform", "transformation", "transformations"]) or 0
            st.metric("Total Transformations", int(total_trans))

        st.markdown("---")

        # DataFlow List

        if "DataFlow" in dataflow_df.columns:
            selected_dataflow = st.selectbox(
                " Select DataFlow for details",
                dataflow_df["DataFlow"].tolist(),
                key="dataflow_selector",
            )

            if selected_dataflow:
                df_data = dataflow_df[
                    dataflow_df["DataFlow"] == selected_dataflow
                ].iloc[0]

                # DataFlow details
                col1, col2 = st.columns([1, 2])

                with col1:
                    body = (
                        f"<h3 style='color: #667eea; margin-bottom:8px;'>{selected_dataflow}</h3>"
                        f"<div style='margin: 8px 0;'><strong>Type:</strong> {df_data.get('Type', 'MappingDataFlow')}</div>"
                        f"<div style='margin: 8px 0;'><strong>Sources:</strong> {df_data.get('Sources', 0)}</div>"
                        f"<div style='margin: 8px 0;'><strong>Sinks:</strong> {df_data.get('Sinks', 0)}</div>"
                        f"<div style='margin: 8px 0;'><strong>Transformations:</strong> {df_data.get('Transformations', 0)}</div>"
                    )
                    render_info_card(selected_dataflow, body)

                with col2:
                    # Show lineage for this dataflow
                    df_lineage = (
                        lineage_df[lineage_df["DataFlow"] == selected_dataflow]
                        if not lineage_df.empty and "DataFlow" in lineage_df.columns
                        else pd.DataFrame()
                    )

                    if not df_lineage.empty:
                        st.markdown("#### 🔄 Data Lineage")

                        # Display as table
                        display_cols = []
                        for col in [
                            "SourceName",
                            "SourceTable",
                            "SinkName",
                            "SinkTable",
                            "TransformationTypes",
                        ]:
                            if col in df_lineage.columns:
                                display_cols.append(col)

                        if display_cols:
                            st.dataframe(
                                df_lineage[display_cols], use_container_width=True
                            )
                    else:
                        st.info("No lineage data available for this DataFlow")

    # DATA LINEAGE TAB

    def render_lineage_tab(self):
        """
        Render data lineage visualization

        FIXED:
        - Source to Sink flow visualization
        - Interactive Sankey diagram
        - Filterable lineage table
        """

        st.markdown("###  Data Lineage Analysis")
        st.markdown("*Track data flow from source to sink across your factory*")

        lineage_df = safe_get_dataframe("DataLineage", "Data_Lineage")

        if lineage_df.empty:
            st.info(" No data lineage information available")
            return

        # Lineage Overview

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Lineage Records", len(lineage_df))

        with col2:
            unique_sources = (
                lineage_df["Source"].nunique() if "Source" in lineage_df.columns else 0
            )
            st.metric("Unique Sources", unique_sources)

        with col3:
            unique_sinks = (
                lineage_df["Sink"].nunique() if "Sink" in lineage_df.columns else 0
            )
            st.metric("Unique Sinks", unique_sinks)

        with col4:
            copy_count = (
                len(lineage_df[lineage_df["Type"] == "Copy"])
                if "Type" in lineage_df.columns
                else 0
            )
            st.metric("Copy Activities", copy_count)

        st.markdown("---")

        # Filters

        col1, col2, col3 = st.columns(3)

        with col1:
            if "Pipeline" in lineage_df.columns:
                pipelines = ["All"] + sorted(lineage_df["Pipeline"].unique().tolist())
                pipeline_filter = st.selectbox(
                    " Filter by Pipeline", pipelines, key="lineage_pipeline_filter"
                )

        with col2:
            if "Type" in lineage_df.columns:
                types = ["All"] + sorted(lineage_df["Type"].unique().tolist())
                type_filter = st.selectbox(
                    " Filter by Type", types, key="lineage_type_filter"
                )

        with col3:
            search_term = st.text_input(
                " Search Source/Sink", "", key="lineage_search"
            )

        # Apply filters
        filtered_df = lineage_df.copy()

        if pipeline_filter != "All" and "Pipeline" in filtered_df.columns:
            filtered_df = filtered_df[filtered_df["Pipeline"] == pipeline_filter]

        if type_filter != "All" and "Type" in filtered_df.columns:
            filtered_df = filtered_df[filtered_df["Type"] == type_filter]

        if search_term:
            if "Source" in filtered_df.columns and "Sink" in filtered_df.columns:
                mask = filtered_df["Source"].str.contains(
                    search_term, case=False, na=False
                ) | filtered_df["Sink"].str.contains(search_term, case=False, na=False)
                filtered_df = filtered_df[mask]

        if filtered_df.empty:
            st.info("📭 No lineage records match the selected filters")
            return

        st.markdown(f"###  Lineage Flow ({len(filtered_df)} records)")

        # Sankey Diagram

        if len(filtered_df) > 0 and len(filtered_df) <= 100:  # Limit for performance
            st.markdown("#### 🌊 Data Flow Visualization")

            self.render_lineage_sankey(filtered_df)
        elif len(filtered_df) > 100:
            st.warning(
                f" Too many records ({len(filtered_df)}) for visualization. Showing table only. Apply filters to reduce dataset."
            )

        st.markdown("---")

        # Lineage Table

        st.markdown("####  Detailed Lineage Table")

        # Select columns to display
        display_cols = []
        for col in [
            "Pipeline",
            "Activity",
            "Type",
            "Source",
            "SourceTable",
            "Sink",
            "SinkTable",
            "Transformation",
        ]:
            if col in filtered_df.columns:
                display_cols.append(col)

        if display_cols:
            st.dataframe(
                filtered_df[display_cols], use_container_width=True, height=400
            )
        else:
            st.dataframe(filtered_df, use_container_width=True, height=400)

        # Export button
        csv_bytes = to_csv_bytes(filtered_df)
        st.download_button(
            label=" Download Lineage Data (CSV)",
            data=csv_bytes,
            file_name="data_lineage.csv",
            mime="text/csv",
            key="download_lineage",
        )

    def render_lineage_sankey(self, lineage_df: pd.DataFrame):
        """
        Render Sankey diagram for data lineage

        Args:
            lineage_df: Filtered lineage DataFrame
        """

        # Build Sankey data
        labels = []
        sources = []
        targets = []
        values = []
        colors = []

        node_index = {}
        current_idx = 0

        # Process each lineage record (limit to 50 for performance)
        for _, row in lineage_df.head(50).iterrows():
            source = row.get("Source", "")
            sink = row.get("Sink", "")

            if not source or not sink:
                continue

            # Add source node
            if source not in node_index:
                labels.append(source)
                node_index[source] = current_idx
                current_idx += 1

            # Add sink node
            if sink not in node_index:
                labels.append(sink)
                node_index[sink] = current_idx
                current_idx += 1

            # Add link
            sources.append(node_index[source])
            targets.append(node_index[sink])
            values.append(1)

            # Color by type
            flow_type = row.get("Type", "Unknown")
            if flow_type == "Copy":
                colors.append("rgba(102, 126, 234, 0.4)")
            elif flow_type == "DataFlow":
                colors.append("rgba(221, 160, 221, 0.4)")
            else:
                colors.append("rgba(135, 206, 235, 0.4)")

        if not sources:
            st.info("📭 No data to visualize")
            return

        # Create Sankey diagram
        fig = go.Figure(
            data=[
                go.Sankey(
                    node=dict(
                        pad=15,
                        thickness=20,
                        line=dict(color="white", width=2),
                        label=labels,
                        color=[
                            "#4facfe" if i % 2 == 0 else "#f093fb"
                            for i in range(len(labels))
                        ],
                    ),
                    link=dict(
                        source=sources, target=targets, value=values, color=colors
                    ),
                )
            ]
        )

        fig.update_layout(
            title="Data Flow: Source → Sink",
            height=500,
            margin=dict(l=20, r=20, t=40, b=20),
            font=dict(size=10),
        )

        st.plotly_chart(fig, use_container_width=True)

    # DATA EXPLORER TAB

    def render_explorer_tab(self):
        """
        Render data explorer for raw data browsing

        FIXED:
        - All sheets accessible
        - Search and filter
        - Export functionality
        """

        st.markdown("###  Data Explorer")
        st.markdown("*Browse and export raw analysis data*")

        if not st.session_state.excel_data:
            st.warning(" No data loaded")
            return

        # Sheet Selection

        sheet_names = list(st.session_state.excel_data.keys())

        if not sheet_names:
            st.warning(" No sheets available")
            return

        # Group sheets by category
        core_sheets = [
            s
            for s in sheet_names
            if any(
                x in s
                for x in [
                    "Pipeline",
                    "Activity",
                    "DataFlow",
                    "Dataset",
                    "Trigger",
                    "LinkedService",
                ]
            )
        ]
        analysis_sheets = [
            s
            for s in sheet_names
            if any(x in s for x in ["Impact", "Lineage", "Orphaned", "Usage"])
        ]
        other_sheets = [
            s for s in sheet_names if s not in core_sheets and s not in analysis_sheets
        ]

        col1, col2 = st.columns([1, 3])

        with col1:
            st.markdown("#### 📚 Sheet Categories")

            category = st.radio(
                "Select Category",
                ["Core Resources", "Analysis", "Other", "All Sheets"],
                key="explorer_category",
            )

            if category == "Core Resources":
                available_sheets = core_sheets
            elif category == "Analysis":
                available_sheets = analysis_sheets
            elif category == "Other":
                available_sheets = other_sheets
            else:
                available_sheets = sheet_names

            if not available_sheets:
                st.info("No sheets in this category")
                return

            selected_sheet = st.selectbox(
                "Select Sheet", available_sheets, key="explorer_sheet"
            )

        with col2:
            if selected_sheet:
                df = st.session_state.excel_data.get(selected_sheet)

                if df is None or not isinstance(df, pd.DataFrame):
                    st.warning(f" Sheet '{selected_sheet}' is not a valid DataFrame")
                    return

                st.markdown(f"####  {selected_sheet}")

                # Sheet info
                info_col1, info_col2, info_col3 = st.columns(3)

                with info_col1:
                    st.metric("Rows", len(df))

                with info_col2:
                    st.metric("Columns", len(df.columns))

                with info_col3:
                    memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
                    st.metric("Memory", f"{memory_mb:.2f} MB")

                st.markdown("---")

                # Search and Filter

                with st.expander(" Search & Filter Options"):
                    search_col, filter_col = st.columns(2)

                    with search_col:
                        search_term = st.text_input(
                            " Search all columns",
                            "",
                            key=f"explorer_search_{selected_sheet}",
                        )

                    with filter_col:
                        if not df.empty:
                            filter_column = st.selectbox(
                                "Filter by Column",
                                ["None"] + df.columns.tolist(),
                                key=f"explorer_filter_col_{selected_sheet}",
                            )

                            if filter_column != "None":
                                unique_values = df[filter_column].unique()
                                if len(unique_values) <= 50:
                                    filter_value = st.multiselect(
                                        f"Select {filter_column}",
                                        unique_values,
                                        key=f"explorer_filter_val_{selected_sheet}",
                                    )
                                else:
                                    st.info(
                                        f"Too many unique values ({len(unique_values)}) for filter"
                                    )
                                    filter_value = None
                            else:
                                filter_value = None
                        else:
                            filter_column = "None"
                            filter_value = None

                # Apply filters
                display_df = df.copy()

                if search_term:
                    # Search across all string columns
                    mask = False
                    for col in display_df.select_dtypes(include=["object"]).columns:
                        mask |= (
                            display_df[col]
                            .astype(str)
                            .str.contains(search_term, case=False, na=False)
                        )
                    display_df = display_df[mask]

                if filter_column != "None" and filter_value:
                    display_df = display_df[
                        display_df[filter_column].isin(filter_value)
                    ]

                # Display data
                st.markdown(f"**Showing {len(display_df)} of {len(df)} rows**")

                # Pagination for large datasets
                rows_per_page = 100
                total_pages = (len(display_df) - 1) // rows_per_page + 1

                if total_pages > 1:
                    page = st.slider(
                        "Page", 1, total_pages, 1, key=f"explorer_page_{selected_sheet}"
                    )
                    start_idx = (page - 1) * rows_per_page
                    end_idx = min(start_idx + rows_per_page, len(display_df))
                    page_df = display_df.iloc[start_idx:end_idx]
                else:
                    page_df = display_df

                st.dataframe(page_df, use_container_width=True, height=500)

                # Export Options

                st.markdown("---")
                st.markdown("####  Export Options")

                col1, col2, col3 = st.columns(3)

                with col1:
                    # CSV Export
                    csv_bytes = to_csv_bytes(display_df)
                    st.download_button(
                        label="📄 Download as CSV",
                        data=csv_bytes,
                        file_name=f"{selected_sheet}.csv",
                        mime="text/csv",
                        key=f"download_csv_{selected_sheet}",
                    )

                with col2:
                    # JSON Export
                    json_bytes = to_json_bytes(display_df.to_dict(orient="records"))
                    st.download_button(
                        label=" Download as JSON",
                        data=json_bytes,
                        file_name=f"{selected_sheet}.json",
                        mime="application/json",
                        key=f"download_json_{selected_sheet}",
                    )

                with col3:
                    # Excel Export (single sheet)
                    if HAS_OPENPYXL:
                        buffer = io.BytesIO()
                        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                            display_df.to_excel(
                                writer, sheet_name=selected_sheet[:31], index=False
                            )

                        st.download_button(
                            label=" Download as Excel",
                            data=buffer.getvalue(),
                            file_name=f"{selected_sheet}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"download_excel_{selected_sheet}",
                        )
                    else:
                        st.info("Install openpyxl for Excel export")

                # Column statistics
                with st.expander(" Column Statistics"):
                    if not display_df.empty:
                        stats_df = display_df.describe(include="all").transpose()
                        st.dataframe(stats_df, use_container_width=True)
                    else:
                        st.info("No data to analyze")

    # EXPORT TAB

    def render_export_tab(self):
        """
        Render export options for bulk data download

        FIXED:
        - Multiple format support
        - Custom sheet selection
        - Batch export
        """

        st.markdown("###  Export Dashboard")
        st.markdown("*Download analysis data in multiple formats*")

        if not st.session_state.excel_data:
            st.warning(" No data loaded")
            return

        # Export Configuration

        st.markdown("####  Select Data to Export")

        sheet_names = list(st.session_state.excel_data.keys())

        # Preset selections
        col1, col2 = st.columns(2)

        with col1:
            if st.button(" Select All Sheets", use_container_width=True):
                st.session_state.export_selected_sheets = sheet_names

        with col2:
            if st.button(" Clear Selection", use_container_width=True):
                st.session_state.export_selected_sheets = []

        # Sheet selection
        if "export_selected_sheets" not in st.session_state:
            st.session_state.export_selected_sheets = sheet_names[
                :5
            ]  # Default to first 5

        selected_sheets = st.multiselect(
            "Select Sheets to Export",
            sheet_names,
            default=st.session_state.export_selected_sheets,
            key="export_sheets_multiselect",
        )

        st.session_state.export_selected_sheets = selected_sheets

        if not selected_sheets:
            st.info("👆 Select at least one sheet to export")
            return

        st.markdown(f"**Selected: {len(selected_sheets)} sheets**")

        st.markdown("---")

        # Export Format Selection

        st.markdown("####  Export Format")

        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown("##### 📄 CSV (Zip)")
            st.markdown("*One CSV file per sheet*")

            if st.button(
                " Download CSV Bundle", type="primary", use_container_width=True
            ):
                self.export_as_csv_zip(selected_sheets)

        with col2:
            st.markdown("#####  Excel Workbook")
            st.markdown("*All sheets in one file*")

            if HAS_OPENPYXL:
                if st.button(
                    " Download Excel File", type="primary", use_container_width=True
                ):
                    self.export_as_excel(selected_sheets)
            else:
                st.info("Install openpyxl for Excel export")

        with col3:
            st.markdown("#####  JSON")
            st.markdown("*Structured JSON format*")

            if st.button(
                " Download JSON Bundle", type="primary", use_container_width=True
            ):
                self.export_as_json(selected_sheets)

        st.markdown("---")

        # Quick Reports

        st.markdown("####  Quick Reports")

        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown("#####  Impact Report")
            st.markdown("*CRITICAL & HIGH impact pipelines*")

            if st.button(" Download Impact Report", use_container_width=True):
                self.export_impact_report()

        with col2:
            st.markdown("#####  Cleanup Report")
            st.markdown("*All orphaned resources*")

            if st.button(" Download Cleanup Report", use_container_width=True):
                self.export_cleanup_report()

        with col3:
            st.markdown("#####  Summary Report")
            st.markdown("*Executive summary*")

            if st.button(" Download Summary Report", use_container_width=True):
                self.export_summary_report()

    def export_as_csv_zip(self, sheet_names: List[str]):
        """Export selected sheets as CSV files in a zip archive"""
        import zipfile

        try:
            buffer = io.BytesIO()

            with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
                for sheet_name in sheet_names:
                    df = st.session_state.excel_data.get(sheet_name)

                    if df is not None and isinstance(df, pd.DataFrame):
                        csv_bytes = to_csv_bytes(df)
                        zip_file.writestr(f"{sheet_name}.csv", csv_bytes)

            st.download_button(
                label=" Click to Download ZIP",
                data=buffer.getvalue(),
                file_name="adf_analysis_export.zip",
                mime="application/zip",
                key="download_csv_zip",
            )

            st.success(f" Created ZIP with {len(sheet_names)} CSV files")

        except Exception as e:
            st.error(f" Export failed: {e}")

    def export_as_excel(self, sheet_names: List[str]):
        """Export selected sheets as Excel workbook"""
        try:
            buffer = io.BytesIO()

            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                for sheet_name in sheet_names:
                    df = st.session_state.excel_data.get(sheet_name)

                    if df is not None and isinstance(df, pd.DataFrame):
                        # Truncate sheet name to 31 chars (Excel limit)
                        safe_name = sheet_name[:31]
                        df.to_excel(writer, sheet_name=safe_name, index=False)

            st.download_button(
                label=" Click to Download Excel",
                data=buffer.getvalue(),
                file_name="adf_analysis_export.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_excel_workbook",
            )

            st.success(f" Created Excel workbook with {len(sheet_names)} sheets")

        except Exception as e:
            st.error(f" Export failed: {e}")

    def export_as_json(self, sheet_names: List[str]):
        """Export selected sheets as JSON"""
        try:
            export_data = {}

            for sheet_name in sheet_names:
                df = st.session_state.excel_data.get(sheet_name)

                if df is not None and isinstance(df, pd.DataFrame):
                    export_data[sheet_name] = df.to_dict(orient="records")

            json_bytes = to_json_bytes(export_data)

            st.download_button(
                label=" Click to Download JSON",
                data=json_bytes,
                file_name="adf_analysis_export.json",
                mime="application/json",
                key="download_json_bundle",
            )

            st.success(f" Created JSON with {len(sheet_names)} sheets")

        except Exception as e:
            st.error(f" Export failed: {e}")

    def export_impact_report(self):
        """Export focused impact report"""
        try:
            impact_df = safe_get_dataframe("ImpactAnalysis", "PipelineAnalysis")

            if impact_df.empty:
                st.warning(" No impact data available")
                return

            # Filter CRITICAL and HIGH only
            if "Impact" in impact_df.columns:
                critical_high = impact_df[
                    impact_df["Impact"].isin(["CRITICAL", "HIGH"])
                ]
            else:
                critical_high = impact_df

            csv_bytes = to_csv_bytes(critical_high)

            st.download_button(
                label=" Click to Download Impact Report",
                data=csv_bytes,
                file_name="impact_report_critical_high.csv",
                mime="text/csv",
                key="download_impact_report",
            )

            st.success(f" Created impact report with {len(critical_high)} pipelines")

        except Exception as e:
            st.error(f" Export failed: {e}")

    def export_cleanup_report(self):
        """Export orphaned resources report"""
        try:
            # Combine all orphaned resources
            orphaned_data = {}

            for sheet_name in [
                "OrphanedPipelines",
                "OrphanedDatasets",
                "OrphanedLinkedServices",
                "OrphanedTriggers",
            ]:
                df = safe_get_dataframe(sheet_name)
                if not df.empty:
                    orphaned_data[sheet_name] = df

            if not orphaned_data:
                st.warning(" No orphaned resources found")
                return

            # Export as Excel with multiple sheets
            buffer = io.BytesIO()

            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                for sheet_name, df in orphaned_data.items():
                    df.to_excel(writer, sheet_name=sheet_name[:31], index=False)

            st.download_button(
                label=" Click to Download Cleanup Report",
                data=buffer.getvalue(),
                file_name="cleanup_report_orphaned_resources.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_cleanup_report",
            )

            total_orphaned = sum(len(df) for df in orphaned_data.values())
            st.success(
                f" Created cleanup report with {total_orphaned} orphaned resources"
            )

        except Exception as e:
            st.error(f" Export failed: {e}")

    def export_summary_report(self):
        """Export executive summary report"""
        try:
            summary_df = safe_get_dataframe("Summary")

            if summary_df.empty:
                st.warning(" No summary data available")
                return

            csv_bytes = to_csv_bytes(summary_df)

            st.download_button(
                label=" Click to Download Summary Report",
                data=csv_bytes,
                file_name="executive_summary.csv",
                mime="text/csv",
                key="download_summary_report",
            )

            st.success(" Created executive summary report")

        except Exception as e:
            st.error(f" Export failed: {e}")

    def render_comprehensive_documentation(self):
        """Render comprehensive documentation with all guides and references"""

        st.header("📚 Complete Documentation Suite")
        st.markdown("Access all documentation, guides, and technical references in one place.")

        # Documentation navigation - Complete documentation suite
        doc_tabs = st.tabs([
            " Dashboard Tiles",
            "🧠 Technical Logic",
            "🐍 Python Files",
            "📖 Complete Guide",
            "⚙ Configuration"
        ])

        # DASHBOARD TILES REFERENCE

        with doc_tabs[0]:
            st.subheader(" Dashboard Tiles Reference")
            st.markdown("Complete reference for every metric tile shown in the dashboard.")

            try:
                tiles_path = Path(__file__).parent / "docs" / "TILES.md"
                if tiles_path.exists():
                    with open(tiles_path, 'r', encoding='utf-8') as f:
                        tiles_content = f.read()
                    st.markdown(tiles_content)
                else:
                    st.warning("TILES.md not found in current directory")

                    # Fallback: Show basic tile information
                    st.markdown("""
                    ###  Top-Row Metric Tiles

                    1. **Pipelines** - Total number of pipeline resources
                    2. **DataFlows** - Number of DataFlow resources
                    3. **Datasets** - Number of dataset resources
                    4. **Triggers** - Number of trigger configurations
                    5. **Dependencies** - Total dependency relationships
                    6. **Health** - Factory health score (0-100)
                    7. **Orphaned** - Unused/orphaned resources

                    ###  Secondary Metrics

                    - **Source/Target Datasets** - Lineage endpoint counts
                    - **Static vs Dynamic** - Parameterization analysis
                    - **Impact Levels** - CRITICAL/HIGH/MEDIUM/LOW distributions

                    ### 🔗 Data Sources

                    - **Primary:** Summary sheet metrics
                    - **Fallback:** Individual sheet row counts
                    - **Lineage:** DataLineage sheet analysis
                    """)

            except Exception as e:
                st.error(f"Error loading TILES.md: {e}")

        # TECHNICAL LOGIC REFERENCE

        with doc_tabs[1]:
            st.subheader("🧠 Technical Logic & Algorithms")
            st.markdown("Detailed technical reference for scoring algorithms and detection logic.")

            try:
                logic_path = Path(__file__).parent / "docs" / "LOGIC.md"
                if logic_path.exists():
                    with open(logic_path, 'r', encoding='utf-8') as f:
                        logic_content = f.read()
                    st.markdown(logic_content)
                else:
                    st.warning("LOGIC.md not found in current directory")

                    # Fallback: Show basic algorithm information
                    st.markdown("""
                    ### 🏥 Health Score Algorithm

                    ```python
                    # Health Score Formula
                    if pipelines > 0:
                        health_score = int((1 - orphaned / pipelines) * 100)
                    else:
                        health_score = 100
                    ```

                    **Status Thresholds:**
                    - **90-100:** Excellent ()
                    - **75-89:** Good (🔵)
                    - **60-74:** Fair ()
                    - **<60:** Needs Attention ()

                    ###  Quality Score (Excel Reports)

                    Starting from 100, deductions applied for:
                    1. **Circular Dependencies:** -10 points per cycle (max -30)
                    2. **Orphaned Resources:** Based on percentage (max -20)
                    3. **Broken Triggers:** -5 points per broken trigger (max -15)

                    ### 🔄 Circular Dependency Detection

                    - **Algorithm:** DFS traversal with back-edge detection
                    - **Deduplication:** Canonical cycle representation
                    - **Severity:** Marked as CRITICAL (production blocker)
                    """)

            except Exception as e:
                st.error(f"Error loading LOGIC.md: {e}")

        # PYTHON FILES REFERENCE

        with doc_tabs[2]:
            st.subheader("🐍 Python Files Overview")
            st.markdown("Complete reference for all Python files and their purposes.")

            try:
                python_files_path = Path(__file__).parent / "PYTHON_FILES_REFERENCE.md"
                if python_files_path.exists():
                    with open(python_files_path, 'r', encoding='utf-8') as f:
                        python_content = f.read()
                    st.markdown(python_content)
                else:
                    st.warning("PYTHON_FILES_REFERENCE.md not found")

                    # Fallback: Show basic file structure
                    st.markdown("""
                    ### 🚀 Core Analysis Engine

                    - **`adf_analyzer_v10_complete.py`** - Main analysis engine
                    - **`adf_runner_wrapper.py`** - Production wrapper (recommended)
                    - **`adf_analyzer_v10_patched_runner.py`** - Enhanced orchestrator

                    ### 🎨 Enhancement Layer

                    - **`adf_analyzer_v10_excel_enhancements.py`** - Excel beautification
                    - **`adf_analyzer_v10_patch.py`** - Functional patches

                    ###  Dashboard & UI

                    - **`adf_dashboard.py`** - Main Streamlit dashboard
                    - **`streamlit_app/`** - Application structure

                    ### 🔧 Utilities & Scripts

                    - **`scripts/setup_environment.py`** - Environment setup
                    - **`scripts/run_analysis.py`** - Direct execution
                    - **`scripts/verify_installation.py`** - System validation

                    ##

                    - **`test_metrics.py`** - Comprehensive testing
                    - **`verify_real_world.py`** - Production testing
                    """)

            except Exception as e:
                st.error(f"Error loading Python files reference: {e}")

        # COMPLETE PROJECT GUIDE

        with doc_tabs[3]:
            st.subheader("📖 Complete Project Guide")
            st.markdown("Comprehensive project documentation and user guide.")

            try:
                readme_path = Path(__file__).parent / "README_v10_UPDATED.md"
                if readme_path.exists():
                    with open(readme_path, 'r', encoding='utf-8') as f:
                        readme_content = f.read()
                    st.markdown(readme_content)
                else:
                    # Try alternative README names
                    alt_paths = [
                        Path(__file__).parent / "README_v10.md",
                        Path(__file__).parent / "README.md"
                    ]

                    content_loaded = False
                    for alt_path in alt_paths:
                        if alt_path.exists():
                            with open(alt_path, 'r', encoding='utf-8') as f:
                                readme_content = f.read()
                            st.markdown(readme_content)
                            content_loaded = True
                            break

                    if not content_loaded:
                        st.warning("README files not found")

                        # Fallback: Show basic project information
                        st.markdown("""
                        # 🚀 ADF Analyzer v10.1 - Ultimate Interactive Edition

                        ##  Overview

                        Production-ready, enterprise-grade toolkit for Azure Data Factory ARM template analysis with interactive dashboard and comprehensive Excel reporting.

                        ## ⚡ Quick Start

                        ```bash
                        # Quick analysis (recommended)
                        python adf_runner_wrapper.py your_template.json

                        # Dashboard mode
                        streamlit run adf_dashboard.py
                        ```

                        ##  Key Features

                        - **Comprehensive Analysis** - ARM template parsing, activity detection
                        - **Impact Analysis** - Health scoring, orphaned detection, circular dependencies
                        - **Enhanced Reporting** - Professional Excel with charts and dashboards
                        - **Interactive Dashboard** - Real-time analytics and visualizations

                        ##  Dashboard Features

                        - **Dual-Mode Operation** - Generate Excel + Upload & Analyze
                        - **Enhancement Configuration** - User-friendly feature toggles
                        - **Interactive Analytics** - Health gauge, network graphs, metrics
                        """)

            except Exception as e:
                st.error(f"Error loading project guide: {e}")

        # CONFIGURATION GUIDE

        with doc_tabs[4]:
            st.subheader("⚙ Configuration Guide")
            st.markdown("Complete guide to configuration files and settings.")

            # Enhancement Configuration
            st.markdown("###  Enhancement Configuration (`enhancement_config.json`)")

            try:
                config_path = Path(__file__).parent / "enhancement_config.json"
                if config_path.exists():
                    with open(config_path, 'r') as f:
                        config = json.load(f)

                    st.code(json.dumps(config, indent=2), language='json')

                    st.markdown("""
                    **Configuration Options:**

                    - **`core_formatting`** - Basic Excel styling (column sizing, borders, colors)
                    - **`conditional_formatting`** - Data bars, color scales, icon sets
                    - **`hyperlinks`** - Navigation links between sheets
                    - **`enhanced_summary`** - Executive dashboard and project banner
                    - **`advanced_dashboard`** - Health score, complexity heat maps, insights

                    **Advanced Dashboard Sub-Options:**
                    - **`health_score`** - Factory health indicator (0-100)
                    - **`complexity_heat_map`** - Visual complexity analysis
                    - **`performance_insights`** - Bottleneck and optimization recommendations
                    - **`top_pipelines`** - Most important/complex pipelines ranking
                    - **`security_checklist`** - Security assessment and recommendations
                    - **`cost_analysis`** - Resource utilization and cost implications
                    """)
                else:
                    st.warning("enhancement_config.json not found")

            except Exception as e:
                st.error(f"Error loading configuration: {e}")

            # Dashboard Configuration
            st.markdown("### 🎨 Dashboard Configuration (`streamlit_config.json`)")

            try:
                streamlit_config_path = Path(__file__).parent / "streamlit_config.json"
                if streamlit_config_path.exists():
                    with open(streamlit_config_path, 'r') as f:
                        streamlit_config = json.load(f)

                    st.code(json.dumps(streamlit_config, indent=2), language='json')
                else:
                    st.info("streamlit_config.json not found - using default settings")

                    # Show example configuration
                    example_config = {
                        "ui": {
                            "theme": "default",
                            "sidebar_state": "expanded"
                        },
                        "performance": {
                            "cache_enabled": True,
                            "max_file_size": "200MB"
                        },
                        "features": {
                            "network_graphs": True,
                            "advanced_charts": True
                        }
                    }

                    st.code(json.dumps(example_config, indent=2), language='json')

            except Exception as e:
                st.error(f"Error loading dashboard configuration: {e}")

            # Usage Instructions
            st.markdown("""
            ###  How to Configure

            **Via Dashboard UI (Recommended):**
            1. Go to Generate Excel tab
            2. Use the Enhancement Configuration section
            3. Toggle features with checkboxes
            4. Click "Save Enhancement Config"

            **Via File Editing:**
            1. Edit `enhancement_config.json` directly
            2. Ensure valid JSON format
            3. Restart dashboard to apply changes

            **Best Practices:**
            - Start with all enhancements enabled
            - Disable specific features if Excel generation is slow
            - Use cost analysis sparingly (resource intensive)
            - Keep health score enabled for best insights
            """)

# MAIN ENTRY POINT

def main():
    """
    Main application entry point

    FIXED:
    - Proper error handling
    - Session state initialization
    - Clean UI flow
    """

    try:
        # Create and run dashboard
        dashboard = ADF_Dashboard()
        dashboard.run()

    except Exception as e:
        st.error(f" Application Error: {e}")

        with st.expander(" Debug Information"):
            st.code(traceback.format_exc())

        st.markdown("---")
        st.markdown(
            """
        ### 🔧 Troubleshooting

        **Common Issues:**
        1. **File Upload Error** - Ensure Excel file is from ADF Analyzer v9.1
        2. **Missing Sheets** - Check that all required sheets exist in Excel file
        3. **Memory Error** - Try with smaller dataset or close other applications
        4. **Display Issues** - Try refreshing the page (F5)

        **Quick Fixes:**
        - Clear browser cache and refresh
        - Upload file again
        - Try sample data to verify app is working

        **Need Help?**
        - Check that dependencies are installed: `pip install streamlit pandas plotly networkx openpyxl`
        - Ensure Python 3.7+ is being used
        - Verify Excel file is not corrupted
        """
        )

if __name__ == "__main__":
    main()
