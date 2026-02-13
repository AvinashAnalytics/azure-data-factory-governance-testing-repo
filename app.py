"""
Azure Data Factory Analyzer Dashboard - Enhanced v10.1
Enterprise Analytics & Visualization Platform

MODERN FEATURES:
• Advanced Network Visualizations (2D & 3D)
• 20+ Interactive Charts & Analytics
• Material Design UI with Glassmorphism
• Smart Search & Filtering
• Real-time Analytics & AI-Powered Insights
• Impact Analysis & Dependency Mapping
• Responsive Design & Multiple Export Formats
• Performance Optimized with Caching

Enterprise ADF Analytics Team
Version 10.1 - Enhanced Edition
"""

# ═══════════════════════════════════════════════════════════════════════════
# IMPORTS - Organized by Category
# ═══════════════════════════════════════════════════════════════════════════

# Standard Library
import os
import sys
import time
import zipfile
import json
import hashlib
import unicodedata
import re
import traceback
from pathlib import Path
from datetime import datetime, timedelta, date
from typing import Dict, List, Any, Optional, Tuple, Union
from collections import defaultdict, Counter
import warnings

# Third-Party Core
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

# AI Chat Integration
try:
    from ai_excel_chat import render_ai_chat_tab, render_ai_sidebar, initialize_ai_session_state
    HAS_AI_CHAT = True
except ImportError:
    HAS_AI_CHAT = False

# Network Analysis (Optional)
try:
    import networkx as nx
    HAS_NETWORKX = True
except ImportError:
    nx = None
    HAS_NETWORKX = False

# Excel Support (Optional)
try:
    import openpyxl
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False

# Suppress warnings for cleaner output
warnings.filterwarnings("ignore")

# ═══════════════════════════════════════════════════════════════════════════
# PAGE CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="ADF Analyzer Dashboard",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "Get Help": None,
        "Report a bug": None,
        "About": """
        # ADF Analyzer v10.1 - Enhanced Edition

        **Enterprise Azure Data Factory Analysis Dashboard**

        ✨ **New in Enhanced Edition:**
        - Performance optimized with smart caching
        - Modern modular architecture
        - Improved accessibility
        - Mobile-responsive design
        
        📊 **Features:**
        - Network Visualizations (2D & 3D)
        - Impact Analysis & Health Scoring
        - Orphaned Resource Detection
        - Data Lineage Tracking
        - Interactive Charts & Dashboards
        - Smart Filtering & Search
        """,
    },
)

# ═══════════════════════════════════════════════════════════════════════════
# THEME & COLOR SYSTEM
# ═══════════════════════════════════════════════════════════════════════════

class PremiumColors:
    """
    Centralized color system for consistent theming.
    All UI components should use these constants.
    """
    
    # Primary Brand Colors
    PRIMARY = '#667eea'
    SECONDARY = '#764ba2'
    ACCENT = '#f093fb'
    
    # Status Colors
    SUCCESS = '#10b981'
    SUCCESS_DARK = '#059669'
    WARNING = '#f59e0b'
    WARNING_DARK = '#d97706'
    DANGER = '#ef4444'
    DANGER_DARK = '#dc2626'
    INFO = '#3b82f6'
    INFO_DARK = '#2563eb'
    
    # Resource Type Colors
    PIPELINE = '#60a5fa'
    DATAFLOW = '#a78bfa'
    DATASET = '#34d399'
    TRIGGER = '#fbbf24'
    LINKEDSERVICE = '#f472b6'
    ORPHANED = '#fb923c'
    
    # Dark Theme Colors
    DARK_BG = 'rgba(15, 23, 42, 0.95)'
    DARK_SURFACE = 'rgba(30, 41, 59, 0.9)'
    DARK_TEXT = '#e2e8f0'
    DARK_MUTED = '#64748b'
    
    # Gradient Palettes (for charts)
    GRADIENTS = {
        'gradient_1': ['#667eea', '#764ba2'],
        'gradient_2': ['#f093fb', '#f5576c'],
        'gradient_3': ['#4facfe', '#00f2fe'],
        'gradient_4': ['#43e97b', '#38f9d7'],
        'gradient_5': ['#fa709a', '#fee140'],
        'gradient_6': ['#30cfd0', '#330867'],
        'gradient_7': ['#a8edea', '#fed6e3'],
        'gradient_8': ['#ff9a56', '#ff6a88'],
    }
    
    @classmethod
    def get_gradient(cls, name: str, index: int = 0) -> str:
        """Get a color from a gradient palette."""
        gradient = cls.GRADIENTS.get(name, cls.GRADIENTS['gradient_1'])
        return gradient[index % len(gradient)]
    
    @classmethod
    def get_status_color(cls, status: str) -> str:
        """Get color based on status level."""
        status_map = {
            'CRITICAL': cls.DANGER,
            'HIGH': cls.WARNING,
            'MEDIUM': cls.INFO,
            'LOW': cls.SUCCESS,
            'EXCELLENT': cls.SUCCESS,
            'GOOD': cls.INFO,
            'FAIR': cls.WARNING,
            'POOR': cls.DANGER,
        }
        return status_map.get(status.upper(), cls.DARK_MUTED)


# ═══════════════════════════════════════════════════════════════════════════
# MODULAR CSS SYSTEM
# ═══════════════════════════════════════════════════════════════════════════

def load_custom_css(theme: str = "midnight"):
    """
    Load optimized, modular CSS for premium glassmorphism UI.
    Supports 3 theme modes: midnight, obsidian, forest.
    """
    # ── Theme Definitions ──
    themes = {
        "midnight": {
            "bg_1": "#071229", "bg_2": "#0f1724",
            "surface": "rgba(30, 41, 59, 0.7)",
            "primary": "#667eea", "secondary": "#764ba2",
            "primary_rgb": "102, 126, 234", "secondary_rgb": "118, 75, 162",
            "text": "#e6eef8", "text_muted": "rgba(255,255,255,0.6)",
            "glow_1": "rgba(102, 126, 234, 0.12)", "glow_2": "rgba(118, 75, 162, 0.12)",
            "header_from": "rgba(60, 70, 120, 0.9)", "header_to": "rgba(80, 50, 120, 0.9)",
            "card_border": "rgba(255, 255, 255, 0.1)",
            "value_grad_from": "#818cf8", "value_grad_to": "#a78bfa",
        },
        "obsidian": {
            "bg_1": "#0a0a0a", "bg_2": "#141414",
            "surface": "rgba(28, 28, 28, 0.85)",
            "primary": "#a78bfa", "secondary": "#c084fc",
            "primary_rgb": "167, 139, 250", "secondary_rgb": "192, 132, 252",
            "text": "#f0f0f0", "text_muted": "rgba(255,255,255,0.55)",
            "glow_1": "rgba(167, 139, 250, 0.08)", "glow_2": "rgba(192, 132, 252, 0.06)",
            "header_from": "rgba(40, 30, 60, 0.95)", "header_to": "rgba(55, 25, 75, 0.95)",
            "card_border": "rgba(255, 255, 255, 0.08)",
            "value_grad_from": "#c084fc", "value_grad_to": "#e879f9",
        },
        "forest": {
            "bg_1": "#0a1a0f", "bg_2": "#121f17",
            "surface": "rgba(20, 40, 28, 0.75)",
            "primary": "#34d399", "secondary": "#059669",
            "primary_rgb": "52, 211, 153", "secondary_rgb": "5, 150, 105",
            "text": "#e8f5e9", "text_muted": "rgba(255,255,255,0.55)",
            "glow_1": "rgba(52, 211, 153, 0.10)", "glow_2": "rgba(5, 150, 105, 0.08)",
            "header_from": "rgba(15, 60, 40, 0.9)", "header_to": "rgba(10, 45, 50, 0.9)",
            "card_border": "rgba(255, 255, 255, 0.08)",
            "value_grad_from": "#34d399", "value_grad_to": "#6ee7b7",
        },
    }
    t = themes.get(theme, themes["midnight"])

    # 1. Base CSS with theme variables
    st.markdown(f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
        :root {{
            --primary: {t['primary']};
            --secondary: {t['secondary']};
            --primary-rgb: {t['primary_rgb']};
            --secondary-rgb: {t['secondary_rgb']};
            --bg-1: {t['bg_1']};
            --bg-2: {t['bg_2']};
            --surface: {t['surface']};
            --text-primary: {t['text']};
            --text-muted: {t['text_muted']};
            --card-border: {t['card_border']};
            --transition-base: 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        }}
        * {{ font-family: 'Inter', sans-serif; -webkit-font-smoothing: antialiased; }}
        [data-testid="stAppViewContainer"] {{
            background: 
                radial-gradient(circle at 10% 20%, {t['glow_1']} 0%, transparent 40%),
                radial-gradient(circle at 90% 80%, {t['glow_2']} 0%, transparent 40%),
                linear-gradient(160deg, var(--bg-1) 0%, var(--bg-2) 100%) fixed;
        }}
        [data-testid="stSidebar"] {{
            background: linear-gradient(180deg, {t['bg_1']} 0%, {t['bg_2']} 100%) !important;
        }}
    </style>
    """, unsafe_allow_html=True)

    # 2. Metric Cards CSS
    st.markdown(f"""
    <style>
        .metric-card {{
            background: {t['surface']} !important;
            backdrop-filter: blur(16px);
            padding: 1.5rem !important;
            border-radius: 20px !important;
            box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.4);
            border: 1px solid {t['card_border']};
            transition: transform 0.3s ease, box-shadow 0.3s ease;
            display: flex; flex-direction: column; align-items: center; justify-content: center;
            min-height: 160px; position: relative; overflow: hidden;
        }}
        .metric-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.5);
        }}
        .metric-icon {{ font-size: 2.5rem; margin-bottom: 0.5rem; }}
        .metric-value {{
            font-size: 2.4rem; font-weight: 800;
            background: linear-gradient(135deg, {t['value_grad_from']}, {t['value_grad_to']});
            -webkit-background-clip: text; -webkit-text-fill-color: transparent;
        }}
        .metric-label {{
            font-size: 0.85rem; font-weight: 700;
            text-transform: uppercase; color: {t['text_muted']};
            letter-spacing: 0.05em; margin-top: 0.3rem;
        }}

        /* ── Gradient Variants (all 8) ── */
        [class*="gradient-"] {{
            border-radius: 16px; padding: 1.2rem;
            color: white; transition: all 0.3s ease;
        }}
        .gradient-purple  {{ background: linear-gradient(135deg, #6B5BFF, #a78bfa) !important; }}
        .gradient-pink    {{ background: linear-gradient(135deg, #ec4899, #f472b6) !important; }}
        .gradient-blue    {{ background: linear-gradient(135deg, #3b82f6, #60a5fa) !important; }}
        .gradient-green   {{ background: linear-gradient(135deg, #10b981, #34d399) !important; }}
        .gradient-orange  {{ background: linear-gradient(135deg, #f59e0b, #fbbf24) !important; }}
        .gradient-teal    {{ background: linear-gradient(135deg, #14b8a6, #2dd4bf) !important; }}
        .gradient-fire    {{ background: linear-gradient(135deg, #ef4444, #f97316) !important; }}
        .gradient-premium {{ background: linear-gradient(135deg, var(--secondary), var(--primary)) !important; }}
    </style>
    """, unsafe_allow_html=True)

    # 3. Header & UI Components
    st.markdown(f"""
    <style>
        .premium-header {{
            background: linear-gradient(135deg, {t['header_from']} 0%, {t['header_to']} 100%);
            padding: 2.5rem; border-radius: 24px; border: 1px solid {t['card_border']};
        }}
        .premium-header h1 {{ color: {t['text']}; }}
        .premium-header p  {{ color: {t['text_muted']}; }}
        /* ═══ Premium Tab System ═══ */
        .stTabs [data-baseweb="tab-list"] {{
            gap: 6px;
            background: {t['surface']}88;
            border-radius: 14px;
            padding: 6px;
            border: 1px solid {t['card_border']};
            overflow-x: auto;
        }}
        .stTabs [data-baseweb="tab"] {{
            background: transparent;
            border-radius: 10px;
            padding: 10px 18px;
            font-weight: 600;
            font-size: 0.88rem;
            letter-spacing: 0.02em;
            color: {t['text_muted']};
            border: 1px solid transparent;
            transition: all 0.25s cubic-bezier(.4,0,.2,1);
            white-space: nowrap;
        }}
        .stTabs [data-baseweb="tab"]:hover {{
            background: rgba(255,255,255,0.06);
            color: {t['text']};
            border-color: {t['card_border']};
            transform: translateY(-1px);
        }}
        .stTabs [aria-selected="true"] {{
            background: linear-gradient(135deg, {t['primary']}22, {t['secondary']}18) !important;
            color: {t['text']} !important;
            border-color: {t['primary']}66 !important;
            box-shadow: 0 0 12px {t['primary']}33, inset 0 -2px 0 {t['primary']};
        }}
        .stTabs [data-baseweb="tab-border"] {{
            display: none;
        }}
        .stTabs [data-baseweb="tab-panel"] {{
            padding-top: 1.5rem;
        }}

        /* ═══ Animations ═══ */
        @keyframes fadeIn     {{ from {{ opacity: 0; transform: translateY(20px); }}  to {{ opacity: 1; transform: translateY(0); }} }}
        @keyframes fadeInDown {{ from {{ opacity: 0; transform: translateY(-30px); }} to {{ opacity: 1; transform: translateY(0); }} }}
        @keyframes slideInUp  {{ from {{ opacity: 0; transform: translateY(30px); }}  to {{ opacity: 1; transform: translateY(0); }} }}
        .fade-in      {{ animation: fadeIn 0.6s ease-out; }}
        .fade-in-down {{ animation: fadeInDown 0.6s ease-out; }}
        .slide-in-up  {{ animation: slideInUp 0.6s ease-out; }}

        /* ═══ Responsive ═══ */
        @media (max-width: 768px) {{
            .premium-header h1 {{ font-size: 2em; }}
            .metric-value {{ font-size: 2em; }}
            .metric-icon  {{ font-size: 2em; }}
            .main {{ padding: 0.5rem 1rem; }}
        }}
        @media (max-width: 480px) {{
            .metric-card     {{ min-height: 120px; padding: 1rem; }}
            .premium-header  {{ padding: 1.5rem 1rem; }}
        }}

        /* ═══ Utility ═══ */
        .text-center {{ text-align: center; }}
        .text-muted  {{ color: var(--text-muted); }}
        .fw-bold     {{ font-weight: 700; }}
        .mb-1 {{ margin-bottom: 0.5rem; }}
        .mb-2 {{ margin-bottom: 1rem; }}
        .mb-3 {{ margin-bottom: 1.5rem; }}

        /* ═══ Premium st.metric Cards ═══ */
        [data-testid="stMetric"] {{
            background: {t['surface']};
            border: 1px solid {t['card_border']};
            border-radius: 14px;
            padding: 1.2rem 1.4rem;
            position: relative;
            overflow: hidden;
            transition: transform 0.3s ease, box-shadow 0.3s ease, border-color 0.3s ease;
        }}
        [data-testid="stMetric"]::before {{
            content: '';
            position: absolute;
            top: 0; left: 0; right: 0;
            height: 3px;
            background: linear-gradient(90deg, {t['primary']}, {t['secondary']});
            border-radius: 14px 14px 0 0;
        }}
        [data-testid="stMetric"]:hover {{
            transform: translateY(-3px);
            box-shadow: 0 8px 24px rgba(0,0,0,0.3);
            border-color: {t['primary']}44;
        }}
        [data-testid="stMetricLabel"] {{
            color: {t['text_muted']} !important;
            font-size: 0.82rem !important;
            font-weight: 600 !important;
            text-transform: uppercase;
            letter-spacing: 0.06em;
        }}
        [data-testid="stMetricValue"] {{
            background: linear-gradient(135deg, {t['value_grad_from']}, {t['value_grad_to']});
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            font-weight: 800 !important;
            font-size: 2rem !important;
        }}
        [data-testid="stMetricDelta"] {{
            font-size: 0.85rem;
            font-weight: 600;
        }}

        /* ═══ Premium Tables / DataFrames ═══ */
        [data-testid="stDataFrame"] {{
            border: 1px solid {t['card_border']};
            border-radius: 12px;
            overflow: hidden;
        }}
        [data-testid="stDataFrame"] [data-testid="StyledLinkIconContainer"] {{
            display: none;
        }}
        .stDataFrame thead tr th {{
            background: linear-gradient(180deg, {t['primary']}18, {t['primary']}08) !important;
            color: {t['text']} !important;
            font-weight: 700 !important;
            font-size: 0.82rem !important;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            border-bottom: 2px solid {t['primary']}33 !important;
            padding: 0.7rem 1rem !important;
        }}
        .stDataFrame tbody tr {{
            transition: background 0.2s ease;
        }}
        .stDataFrame tbody tr:hover {{
            background: {t['primary']}0A !important;
        }}
        .stDataFrame tbody tr td {{
            border-bottom: 1px solid {t['card_border']} !important;
            font-size: 0.88rem !important;
            color: {t['text']} !important;
            padding: 0.5rem 1rem !important;
        }}
        /* Themed scrollbar for dataframes */
        [data-testid="stDataFrame"] ::-webkit-scrollbar {{
            width: 6px; height: 6px;
        }}
        [data-testid="stDataFrame"] ::-webkit-scrollbar-track {{
            background: transparent;
        }}
        [data-testid="stDataFrame"] ::-webkit-scrollbar-thumb {{
            background: {t['primary']}44;
            border-radius: 4px;
        }}

        /* ═══ Premium Sidebar Sections ═══ */
        [data-testid="stSidebar"] .sidebar-section h3 {{
            background: linear-gradient(135deg, {t['primary']}, {t['secondary']}) !important;
            -webkit-background-clip: text !important;
            -webkit-text-fill-color: transparent !important;
            font-weight: 700;
            font-size: 1.05rem;
        }}
        [data-testid="stSidebar"] hr {{
            border-color: {t['card_border']} !important;
            margin: 1rem 0;
        }}
        [data-testid="stSidebar"] [data-testid="stMetric"] {{
            padding: 0.8rem 1rem;
            border-radius: 10px;
        }}

        /* ═══ Premium Expanders ═══ */
        [data-testid="stExpander"] {{
            border: 1px solid {t['card_border']} !important;
            border-radius: 12px !important;
            overflow: hidden;
        }}
        [data-testid="stExpander"] summary {{
            font-weight: 600;
            color: {t['text']};
        }}
        [data-testid="stExpander"] summary:hover {{
            color: {t['primary']};
        }}

        /* ═══ Premium Inputs ═══ */
        .stSelectbox > div > div,
        .stTextInput > div > div {{
            border-color: {t['card_border']} !important;
            border-radius: 10px !important;
            transition: border-color 0.3s ease, box-shadow 0.3s ease;
        }}
        .stSelectbox > div > div:focus-within,
        .stTextInput > div > div:focus-within {{
            border-color: {t['primary']} !important;
            box-shadow: 0 0 0 2px {t['primary']}22 !important;
        }}

        /* Hide Streamlit branding */
        #MainMenu {{ visibility: hidden; }}
        footer    {{ visibility: hidden; }}
        header    {{ visibility: hidden; }}
    </style>
    """, unsafe_allow_html=True)



# ═══════════════════════════════════════════════════════════════════════════
# CHART CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

# Premium Chart Template (minimal to avoid keyword conflicts)
PREMIUM_CHART_TEMPLATE = {
    'layout': {
        'hoverlabel': {
            'bgcolor': 'rgba(255, 255, 255, 0.95)',
            'font': {'size': 13, 'family': 'Inter', 'color': '#1e293b'},
            'bordercolor': 'rgba(102, 126, 234, 0.3)'
        },
        'font': {'family': 'Inter', 'color': PremiumColors.DARK_TEXT},
        'paper_bgcolor': 'rgba(0,0,0,0)',
        'plot_bgcolor': 'rgba(0,0,0,0)',
    }
}


# ═══════════════════════════════════════════════════════════════════════════
# SESSION STATE INITIALIZATION
# ═══════════════════════════════════════════════════════════════════════════

def initialize_session_state():
    """Initialize all session state variables with defaults."""
    defaults = {
        # Data state
        "data_loaded": False,
        "excel_data": {},
        "dependency_graph": None,
        "analysis_metadata": {},
        "graph_metrics": {},
        
        # UI state
        "selected_theme": "dark",
        "filter_options": ["All"],
        "search_query": "",
        "selected_pipeline": None,
        "show_debug_panel": False,
        
        # Cache flags
        "_custom_css_loaded": False,
        "cached_graphs": {},
        "cached_metrics": {},
        
        # File tracking
        "uploaded_file_name": None,
        "last_load_time": None,
        "show_load_summary": False,
        
        # App mode
        "app_mode": None,
        "app_mode_selected": False,
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


# ═══════════════════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS - Enhanced with Type Hints & Caching
# ═══════════════════════════════════════════════════════════════════════════

# @st.cache_data(ttl=300) <--- REMOVED: Caching mutable global state (st.session_state.excel_data) breaks updates!
def safe_get_dataframe(sheet_name: str, *alternative_names: str, copy: bool = False) -> pd.DataFrame:
    """
    Safely retrieve a dataframe from session state by name.
    Args:
        sheet_name: Primary sheet name to look for.
        alternative_names: Fallback sheet names.
        copy: If True, returns a copy (use when modifying). Defaults to False for performance.
    """
    if "excel_data" not in st.session_state or not st.session_state.excel_data:
        return pd.DataFrame()
    
    data = st.session_state.excel_data
    
    # Try normalization-aware search
    def normalize(s): return str(s).lower().replace(" ", "").replace("_", "").replace("-", "")
    target_norm = normalize(sheet_name)
    alt_norms = [normalize(a) for a in alternative_names]
    
    # 1. Direct match
    if sheet_name in data:
        df = data[sheet_name]
        return df.copy() if copy else df
    
    # 2. Alternative direct matches
    for alt in alternative_names:
        if alt in data:
            df = data[alt]
            return df.copy() if copy else df
            
    # 3. Normalized matches
    for k, df in data.items():
        k_norm = normalize(k)
        if k_norm == target_norm or k_norm in alt_norms:
            return df.copy() if copy else df
            
    return pd.DataFrame()


def get_summary_metric(metric_name: str, default: Any = 0) -> Any:
    """
    Get metric from Summary sheet with type coercion.
    
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
        metrics_dict = summary.set_index("Metric")["Value"].to_dict()
        value = metrics_dict.get(metric_name, default)
        
        # Try to coerce to number if possible
        if isinstance(value, str):
            value_clean = value.strip().replace(",", "")
            try:
                # Try int first
                return int(value_clean)
            except ValueError:
                try:
                    # Try float
                    return float(value_clean)
                except ValueError:
                    # Return as-is if not numeric
                    return value
        
        return value
    except Exception:
        return default


def get_count_with_fallback(metric_name: str, fallback_sheets: List[str]) -> int:
    """
    Retrieve numeric count with fallback to sheet row counts.
    
    Args:
        metric_name: Metric name in Summary sheet
        fallback_sheets: List of sheet names to check for row counts
    
    Returns:
        int count (0 if nothing found)
    """
    val = get_summary_metric(metric_name, 0)
    
    try:
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            if int(val) > 0:
                return int(val)
    except Exception:
        pass
    
    # Fallback: inspect sheets for counts
    for sheet_name in fallback_sheets:
        df = safe_get_dataframe(sheet_name)
        if isinstance(df, pd.DataFrame) and not df.empty:
            return len(df)
    
    return 0


def format_number(num: Union[int, float, str]) -> str:
    """Format number with thousand separators."""
    try:
        return f"{int(num):,}"
    except (ValueError, TypeError):
        return str(num)


def truncate_text(text: str, max_length: int = 50) -> str:
    """Truncate text with ellipsis."""
    text = str(text)
    if len(text) <= max_length:
        return text
    return text[:max_length - 3] + "..."


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    """Return CSV bytes with UTF-8 BOM for Excel compatibility."""
    try:
        csv_str = df.to_csv(index=False, encoding="utf-8-sig")
        return csv_str.encode("utf-8-sig")
    except Exception:
        return df.to_csv(index=False).encode("utf-8")


def to_json_bytes(obj: Any) -> bytes:
    """Return JSON bytes (utf-8)."""
    return json.dumps(obj, indent=2, default=str).encode("utf-8")


def to_excel_bytes(dfs: Dict[str, pd.DataFrame]) -> bytes:
    """
    Write dict of DataFrames to Excel workbook bytes.
    
    Args:
        dfs: Mapping of sheet_name -> DataFrame
    
    Returns:
        Excel file as bytes
    """
    if not HAS_OPENPYXL:
        return b""
    
    buffer = io.BytesIO()
    try:
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            for sheet_name, df in dfs.items():
                try:
                    df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
                except Exception:
                    continue
        return buffer.getvalue()
    except Exception:
        return b""


# ═══════════════════════════════════════════════════════════════════════════
# PREMIUM UI COMPONENTS - Modular & Reusable
# ═══════════════════════════════════════════════════════════════════════════

def render_premium_header():
    """Render premium glassmorphism header."""
    st.markdown("""
    <div class="premium-header fade-in-down">
        <h1>🏭 Azure Data Factory Analyzer</h1>
        <p>Enterprise Analysis Dashboard - Enhanced Edition</p>
        <span class="version-badge">v10.1 • Production Ready • Optimized</span>
    </div>
    """, unsafe_allow_html=True)


def render_premium_tile(
    icon: str,
    value: Union[int, float, str],
    label: str,
    variant: str = "purple",
    subtitle: Optional[str] = None,
    tooltip: Optional[str] = None,
) -> None:
    """
    Render ultra-modern premium tile.
    
    Args:
        icon: Emoji icon
        value: Main value to display
        label: Tile label
        variant: Color variant (purple, pink, blue, green, etc.)
        subtitle: Optional subtitle text
        tooltip: Optional tooltip text
    """
    # Format value
    if isinstance(value, int):
        formatted_value = format_number(value)
    elif isinstance(value, float):
        formatted_value = f"{value:,.2f}"
    else:
        formatted_value = str(value)
    
    # Escape for HTML safety
    safe_label = str(label).replace('"', '&quot;')
    safe_tooltip = str(tooltip).replace('"', '&quot;') if tooltip else safe_label
    
    # Subtitle HTML
    subtitle_html = f'<div class="metric-label mb-1" style="opacity:0.7;">{subtitle}</div>' if subtitle else ""
    
    st.markdown(f"""
    <div class="metric-card gradient-{variant}" title="{safe_tooltip}" role="article" aria-label="{safe_label}">
        <div class="metric-icon">{icon}</div>
        <div class="metric-value">{formatted_value}</div>
        <div class="metric-label">{label}</div>
        {subtitle_html}
    </div>
    """, unsafe_allow_html=True)


def create_premium_chart(chart_type: str = "bar", **kwargs) -> go.Figure:
    """
    Create premium styled chart with glassmorphism design.
    
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


def safe_plotly(
    fig: Optional[go.Figure],
    df: Optional[pd.DataFrame] = None,
    required_columns: Optional[List[str]] = None,
    info_message: Optional[str] = None,
    width: str = 'stretch',
) -> None:
    """
    Safely render a plotly figure with data validation.
    
    Args:
        fig: Plotly figure to render
        df: Optional DataFrame for validation
        required_columns: Optional list of required columns
        info_message: Custom message if rendering fails
        width: Whether to use 'stretch' or 'content' width
    """
    try:
        if fig is None:
            st.info(info_message or "📊 No chart available to render")
            return
        
        if df is not None:
            if not isinstance(df, pd.DataFrame) or df.empty:
                st.info(info_message or "📊 No data available for this chart")
                return
            
            if required_columns:
                missing = [c for c in required_columns if c not in df.columns]
                if missing:
                    st.info(info_message or f"📊 Missing columns: {', '.join(missing)}")
                    return
        
        st.plotly_chart(fig, width='stretch')
    
    except Exception as e:
        st.error(f"❌ Could not render chart: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# END OF PART 1
# ═══════════════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════════════
# MAIN APPLICATION CLASS - Enhanced & Modular
# ═══════════════════════════════════════════════════════════════════════════

class ADF_Dashboard:
    """
    Enterprise ADF Analysis Dashboard.
    Enhanced with modular architecture and performance optimizations.
    """

    def __init__(self):
        """Initialize dashboard with session state."""
        initialize_session_state()
        
        # Load CSS with selected theme
        active_theme = st.session_state.get('selected_theme', 'midnight')
        load_custom_css(theme=active_theme)

    def _safe_dataframe(self, df: pd.DataFrame, height: int = 400):
        """Render dataframe safely to avoid Arrow conversion errors and deprecation warnings."""
        if not isinstance(df, pd.DataFrame) or df.empty:
            st.info("📊 No data available to display")
            return
        
        try:
            # Fix Arrow conversion error: convert object columns with mixed types to strings
            df_display = df.copy()
            for col in df_display.columns:
                if df_display[col].dtype == 'object':
                    # Convert to string and handle nan values
                    df_display[col] = df_display[col].astype(str).replace(['nan', 'NaN', 'None', '<NA>'], '')
            
            # Use width='stretch' instead of width='stretch' (Streamlit 2026+)
            st.dataframe(df_display, width='stretch', height=height)
        except Exception as e:
            # Fallback for extreme cases
            st.dataframe(df.astype(str), width='stretch', height=height)

    def run(self):
        """Main entry point for dashboard."""
        # Render header
        render_premium_header()
        
        # Render sidebar
        with st.sidebar:
            self.render_sidebar()
        
        # Main content - Check if launcher should be shown
        if not st.session_state.get("app_mode_selected", False):
            self.render_launcher()
        else:
            self.render_main_content_with_tabs()

    # ═══════════════════════════════════════════════════════════════════════
    # SIDEBAR - Refactored into Modular Functions
    # ═══════════════════════════════════════════════════════════════════════

    def render_sidebar(self):
        """Main sidebar rendering (orchestrator)."""
        self._render_sidebar_branding()
        st.markdown("---")
        
        self._render_sidebar_file_upload()
        st.markdown("---")
        
        self._render_sidebar_navigation()
        st.markdown("---")
        
        self._render_sidebar_status()
        
        if st.session_state.data_loaded:
            st.markdown("---")
            self._render_sidebar_quick_stats()
            st.markdown("---")
            self._render_sidebar_filters()
        
        # AI Chat Settings
        if HAS_AI_CHAT:
            st.markdown("---")
            render_ai_sidebar()
        
        st.markdown("---")
        self._render_sidebar_theme_selector()
        st.markdown("---")
        self._render_sidebar_documentation()
        st.markdown("---")
        self._render_sidebar_footer()

    def _render_sidebar_branding(self):
        """Render sidebar branding section."""
        st.markdown("""
        <div class="sidebar-section fade-in">
            <h2 style="margin: 0; background: linear-gradient(135deg, var(--primary, #667eea), var(--secondary, #764ba2)); 
                       -webkit-background-clip: text; -webkit-text-fill-color: transparent; 
                       font-weight: 800; font-size: 1.8em;">🎛️ Control Center</h2>
            <p style="margin: 8px 0 0 0; opacity: 0.8; font-size: 0.95em; font-weight: 500;">
                ✨ Enterprise Analytics Suite
            </p>
            <div style="margin-top: 12px; padding: 8px 16px; 
                        background: rgba(var(--primary-rgb, 102, 126, 234), 0.1); 
                        border-radius: 20px; font-size: 0.85em; font-weight: 600;">
                🚀 Enhanced v10.1
            </div>
        </div>
        """, unsafe_allow_html=True)

    def _render_sidebar_file_upload(self):
        """Render file upload section."""
        mode = st.session_state.get('app_mode', 'generate')
        
        if mode != 'analyze':
            st.markdown("""
            <div class="sidebar-section">
                <h3 style="margin: 0 0 15px 0; color: var(--primary, #667eea); font-weight: 700; font-size: 1.1em;">
                    📁 Data Input
                </h3>
            </div>
            """, unsafe_allow_html=True)
            
            uploaded_file = st.file_uploader(
                "Upload Analysis Excel",
                type=["xlsx", "xls"],
                help="📊 Upload your ADF analysis Excel file",
                label_visibility="collapsed"
            )
            
            col1, col2 = st.columns(2)
            
            with col1:
                if uploaded_file:
                    if st.button("📤 Load", type="primary", width='stretch'):
                        self.load_excel_file(uploaded_file)
            
            with col2:
                if st.button("🎮 Sample", width='stretch', help="Load demo data"):
                    self.load_sample_data()
        else:
            st.markdown("""
            <div class="sidebar-section">
                <h3 style="margin: 0 0 10px 0; color: var(--primary, #667eea); font-weight: 700; font-size: 1.1em;">
                    📁 Upload & Analyze Mode
                </h3>
                <p style="text-align: center; padding: 15px; 
                          background: rgba(79, 172, 254, 0.1); 
                          border-radius: 12px; margin: 0; font-weight: 500;">
                    📊 Use the main area to upload your Excel file
                </p>
            </div>
            """, unsafe_allow_html=True)

    def _render_sidebar_navigation(self):
        """Render navigation section."""
        st.markdown("""
        <div class="sidebar-section">
            <h3 style="margin: 0 0 15px 0; color: var(--primary, #667eea); font-weight: 700; font-size: 1.1em;">
                🧭 Navigation
            </h3>
        </div>
        """, unsafe_allow_html=True)
        
        if st.button("🏠 Back to Launcher", key="sidebar_back_launcher", width='stretch'):
            for k in ['app_mode', 'app_mode_selected']:
                if k in st.session_state:
                    del st.session_state[k]
            st.rerun()

    def _render_sidebar_status(self):
        """Render status section."""
        st.markdown("""
        <div class="sidebar-section">
            <h3 style="margin: 0 0 15px 0; color: var(--primary, #667eea); font-weight: 700; font-size: 1.1em;">
                📊 Status Dashboard
            </h3>
        </div>
        """, unsafe_allow_html=True)
        
        if st.session_state.data_loaded:
            load_time = st.session_state.last_load_time
            time_str = load_time.strftime('%H:%M:%S') if load_time else 'Just now'
            
            st.markdown(f"""
            <div style="background: rgba(67, 233, 123, 0.1); padding: 15px; 
                        border-radius: 12px; margin-bottom: 15px; border: 1px solid rgba(67, 233, 123, 0.3);">
                <div style="display: flex; align-items: center; margin-bottom: 10px;">
                    <span style="font-size: 1.2em; margin-right: 8px;">✅</span>
                    <strong style="color: #43e97b;">Data Loaded</strong>
                </div>
                <div style="font-size: 0.9em; opacity: 0.8;">
                    ⏰ <strong>{time_str}</strong>
                </div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div style="background: rgba(255, 154, 86, 0.1); padding: 15px; 
                        border-radius: 12px; margin-bottom: 15px; border: 1px solid rgba(255, 154, 86, 0.3);">
                <div style="display: flex; align-items: center; margin-bottom: 8px;">
                    <span style="font-size: 1.2em; margin-right: 8px;">⏳</span>
                    <strong style="color: #ff9a56;">Waiting for Data</strong>
                </div>
                <div style="font-size: 0.9em; opacity: 0.8;">
                    Upload an Excel file or load sample data
                </div>
            </div>
            """, unsafe_allow_html=True)

    def _render_sidebar_quick_stats(self):
        """Render quick statistics cards."""
        st.markdown("""
        <div class="sidebar-section">
            <h3 style="margin: 0 0 15px 0; color: var(--primary, #667eea); font-weight: 700; font-size: 1.1em;">
                ⚡ Quick Metrics
            </h3>
        </div>
        """, unsafe_allow_html=True)
        
        # Get metrics with fallbacks
        pipelines = get_count_with_fallback("Pipelines", ["ImpactAnalysis", "PipelineAnalysis"])
        dataflows = get_count_with_fallback("DataFlows", ["DataFlows", "DataFlowLineage"])
        orphaned = get_count_with_fallback("Orphaned Pipelines", ["OrphanedPipelines"])
        
        # Display metrics
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown(f"""
            <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                        padding: 1rem; border-radius: 12px; text-align: center; color: white;">
                <div style="font-size: 1.2em;">📦</div>
                <div style="font-size: 1.8em; font-weight: 700;">{pipelines}</div>
                <div style="font-size: 0.85em; opacity: 0.9;">Pipelines</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div style="background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); 
                        padding: 1rem; border-radius: 12px; text-align: center; color: white;">
                <div style="font-size: 1.2em;">🌊</div>
                <div style="font-size: 1.8em; font-weight: 700;">{dataflows}</div>
                <div style="font-size: 0.85em; opacity: 0.9;">DataFlows</div>
            </div>
            """, unsafe_allow_html=True)
        
        # Orphaned metric
        orphan_gradient = "linear-gradient(135deg, #ef4444 0%, #dc2626 100%)" if orphaned > 0 else "linear-gradient(135deg, #10b981 0%, #059669 100%)"
        orphan_icon = "⚠️" if orphaned > 0 else "✅"
        orphan_text = "Orphaned" if orphaned > 0 else "All OK"
        
        st.markdown(f"""
        <div style="background: {orphan_gradient}; 
                    padding: 1rem; border-radius: 12px; text-align: center; color: white; margin-top: 0.5rem;">
            <div style="font-size: 1.2em;">{orphan_icon}</div>
            <div style="font-size: 1.8em; font-weight: 700;">{orphaned}</div>
            <div style="font-size: 0.85em; opacity: 0.9;">{orphan_text}</div>
        </div>
        """, unsafe_allow_html=True)

    def _render_sidebar_filters(self):
        """Render filter controls."""
        st.markdown("""
        <div class="sidebar-section">
            <h3 style="margin: 0 0 15px 0; color: var(--primary, #667eea); font-weight: 700; font-size: 1.1em;">
                🔍 Smart Filters
            </h3>
        </div>
        """, unsafe_allow_html=True)
        
        # Search
        search = st.text_input(
            "Search Resources",
            placeholder="🔍 Search pipelines, datasets...",
            label_visibility="collapsed",
            key="sidebar_search"
        )
        st.session_state.search_query = search
        
        # Advanced filters
        with st.expander("🎛️ Advanced Filters", expanded=False):
            st.markdown("**📊 Resource Types**")
            
            col1, col2 = st.columns(2)
            with col1:
                st.checkbox("📦 Pipelines", value=True, key="filter_pipelines")
                st.checkbox("🌊 DataFlows", value=True, key="filter_dataflows")
            with col2:
                st.checkbox("📋 Datasets", value=True, key="filter_datasets")
                st.checkbox("⏰ Triggers", value=True, key="filter_triggers")

    def _render_sidebar_theme_selector(self):
        """Render theme selector with custom accent & background color pickers."""
        st.markdown("""
        <div class="sidebar-section">
            <h3 style="margin: 0 0 15px 0; color: var(--primary, #667eea); font-weight: 700; font-size: 1.1em;">
                🎨 Theme & Colors
            </h3>
        </div>
        """, unsafe_allow_html=True)

        theme_labels = {
            "midnight": "🌊 Midnight",
            "obsidian": "🌑 Obsidian",
            "forest":   "🌿 Forest",
        }
        # Migrate legacy theme values from previous sessions
        current = st.session_state.get("selected_theme", "midnight")
        if current not in theme_labels:
            current = "midnight"
            st.session_state["selected_theme"] = current

        chosen = st.radio(
            "Select Theme",
            options=list(theme_labels.keys()),
            format_func=lambda k: theme_labels[k],
            index=list(theme_labels.keys()).index(current),
            key="theme_radio_selector",
            label_visibility="collapsed",
            horizontal=True,
        )

        if chosen != current:
            st.session_state["selected_theme"] = chosen
            st.rerun()

        # ── Custom Color Pickers ──
        with st.expander("🎯 Custom Accent Colors", expanded=False):
            st.caption("Override theme colors for text highlights & cards")

            col1, col2 = st.columns(2)
            with col1:
                accent_color = st.color_picker(
                    "✨ Accent Color",
                    value=st.session_state.get("user_accent_color", "#667eea"),
                    key="accent_picker",
                    help="Changes text highlights, card accents, and active tab glow"
                )
            with col2:
                bg_tint = st.color_picker(
                    "🖌️ Background Tint",
                    value=st.session_state.get("user_bg_tint", "#0a0a1a"),
                    key="bg_tint_picker",
                    help="Adds a subtle background color overlay"
                )

            # Store in session state
            st.session_state["user_accent_color"] = accent_color
            st.session_state["user_bg_tint"] = bg_tint

            # Convert hex to RGB for rgba() usage
            def hex_to_rgb(hex_color):
                h = hex_color.lstrip('#')
                return f"{int(h[0:2], 16)}, {int(h[2:4], 16)}, {int(h[4:6], 16)}"

            accent_rgb = hex_to_rgb(accent_color)

            # Inject user overrides as CSS custom properties
            st.markdown(f"""
            <style>
                :root {{
                    --user-accent: {accent_color};
                    --user-accent-rgb: {accent_rgb};
                    --user-bg-tint: {bg_tint};
                }}
                /* Apply accent to metric values */
                [data-testid="stMetricValue"] {{
                    background: linear-gradient(135deg, {accent_color}, {accent_color}cc) !important;
                    -webkit-background-clip: text !important;
                    -webkit-text-fill-color: transparent !important;
                }}
                /* Apply accent to active tab */
                .stTabs [aria-selected="true"] {{
                    border-color: {accent_color}66 !important;
                    box-shadow: 0 0 12px {accent_color}33, inset 0 -2px 0 {accent_color} !important;
                }}
                /* Apply accent stripe on metric cards */
                [data-testid="stMetric"]::before {{
                    background: linear-gradient(90deg, {accent_color}, {accent_color}88) !important;
                }}
            </style>
            """, unsafe_allow_html=True)

            if st.button("🔄 Reset to Theme Defaults", key="reset_accent_colors", use_container_width=True):
                # Reset to theme defaults
                theme_defaults = {
                    "midnight": ("#667eea", "#0a0a1a"),
                    "obsidian": ("#a78bfa", "#0a0a0a"),
                    "forest": ("#34d399", "#0a1a0f"),
                }
                defaults = theme_defaults.get(current, ("#667eea", "#0a0a1a"))
                st.session_state["user_accent_color"] = defaults[0]
                st.session_state["user_bg_tint"] = defaults[1]
                st.rerun()

    def _render_sidebar_documentation(self):
        """Render documentation access section."""
        st.markdown("""
        <div class="sidebar-section">
            <h3 style="margin: 0 0 15px 0; color: var(--primary, #667eea); font-weight: 700; font-size: 1.1em;">
                📚 Quick Docs
            </h3>
        </div>
        """, unsafe_allow_html=True)
        
        doc_option = st.selectbox(
            "View Documentation",
            ["Select document...", "📋 Tile Reference", "🧠 Logic Guide"],
            key="sidebar_doc_viewer"
        )
        
        if doc_option == "📋 Tile Reference":
            with st.expander("📋 Tile Reference", expanded=False):
                st.markdown("""
                **Primary Metrics:**
                - **Pipelines** - Total pipeline count
                - **DataFlows** - DataFlow resources
                - **Datasets** - Dataset resources
                - **Triggers** - Trigger configurations
                - **Health** - Factory health score (0-100)
                - **Orphaned** - Unused resources
                
                **Sources:** Summary sheet or row counts
                """)
        
        elif doc_option == "🧠 Logic Guide":
            with st.expander("🧠 Logic Guide", expanded=False):
                st.markdown("""
                **Health Score Formula:**
                ```python
                health = int((1 - orphaned/pipelines) * 100)
                ```
                
                **Status Levels:**
                - 90-100: Excellent ✨
                - 75-89: Good 🔵
                - 60-74: Fair ⚠️
                - <60: Needs Attention ❌
                """)

    def _render_sidebar_footer(self):
        """Render sidebar footer."""
        st.checkbox("🐛 Debug panel", value=False, key="show_debug_panel")
        
        st.markdown("""
        <div style="text-align: center; opacity: 0.7; font-size: 0.8em; 
                    background: rgba(255, 255, 255, 0.05); backdrop-filter: blur(10px);
                    border-radius: 12px; padding: 15px; margin-top: 20px;">
            <div style="margin-bottom: 8px;">
                <span style="font-size: 1.2em;">💖</span>
            </div>
            <p style="margin: 0; font-weight: 500;">ADF Analytics Team</p>
            <p style="margin: 5px 0 0 0; font-size: 0.75em;">
                🚀 Enhanced Edition v10.1
            </p>
        </div>
        """, unsafe_allow_html=True)

    # ═══════════════════════════════════════════════════════════════════════
    # DATA LOADING - Enhanced with Caching & Progress Tracking
    # ═══════════════════════════════════════════════════════════════════════

    def sanitize_filename(name: str) -> str:
        """Strip path components, normalize, whitelist characters to prevent path traversal."""
        if not name:
            return "uploaded_file.json"
        # Take only the basename
        name = Path(name).name
        # Normalize and remove non-ASCII
        name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode()
        # Whitelist: alphanumeric, dash, underscore, dot
        name = re.sub(r'[^\w\-.]', '_', name)
        # Prevent empty or dot-only names
        if not name or name.strip('.') == '':
            return 'uploaded_file.json'
        return name

    def load_excel_file(self, file_or_path: Union[str, Path, Any]):
        """
        Load and process Excel file with progress tracking.
        
        Args:
            file_or_path: Uploaded file object or file path
        """
        try:
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Step 1: Read Excel file
            status_text.text("📖 Reading Excel file...")
            progress_bar.progress(10)
            
            # Handle both file objects and file paths
            if isinstance(file_or_path, (str, Path)):
                file_path = Path(file_or_path)
                if not file_path.exists():
                    st.error(f"❌ File not found: {file_path}")
                    return
                excel_file = pd.ExcelFile(file_path)
                file_name = file_path.name
            else:
                excel_file = pd.ExcelFile(file_or_path)
                file_name = file_or_path.name
            
            sheet_names = excel_file.sheet_names
            status_text.text(f"📄 Found {len(sheet_names)} sheets...")
            progress_bar.progress(20)
            
            # Step 2: Load all sheets
            data = {}
            total_sheets = len(sheet_names)
            
            for i, sheet_name in enumerate(sheet_names):
                status_text.text(f"📄 Loading: {sheet_name}...")
                progress = 20 + int((i / total_sheets) * 50)
                progress_bar.progress(progress)
                
                try:
                    df = pd.read_excel(excel_file, sheet_name=sheet_name)
                    data[sheet_name] = df
                except Exception as e:
                    st.warning(f"⚠️ Could not load '{sheet_name}': {e}")
                    continue
            
            status_text.text("💾 Processing data...")
            progress_bar.progress(70)
            
            # Post-process: merge split sheets and create aliases
            self._process_loaded_sheets(data)
            
            st.session_state.excel_data = data
            st.session_state.uploaded_file_name = file_name
            
            # Step 3: Extract metadata
            status_text.text("📊 Extracting metadata...")
            progress_bar.progress(80)
            self.extract_metadata()
            
            # Step 4: Build dependency graph
            status_text.text("🕸️ Building dependency graph...")
            progress_bar.progress(90)
            self.build_dependency_graph()
            
            # Complete
            status_text.text("✅ Loading complete!")
            progress_bar.progress(100)
            
            st.session_state.data_loaded = True
            st.session_state.last_load_time = datetime.now()
            
            # ✅ Reset AI context so it rebuilds with new data
            st.session_state.ai_context_builder = None
            st.session_state.ai_context_hash = ""
            
            # Clear progress indicators
            import time
            time.sleep(0.5)
            progress_bar.empty()
            status_text.empty()
            
            st.success(f"✅ Successfully loaded: {file_name}")
            st.session_state.show_load_summary = True
        
        except Exception as e:
            st.error(f"❌ Error loading file: {str(e)}")
            with st.expander("🔍 Error Details"):
                st.code(traceback.format_exc())

    def _process_loaded_sheets(self, data: Dict[str, pd.DataFrame]):
        """
        Post-process loaded sheets: merge splits and create aliases.
        
        Args:
            data: Dictionary of loaded sheets (modified in-place)
        """
        # Merge split sheets (e.g., Sheet_P1, Sheet_P2)
        groups = {}
        for name in list(data.keys()):
            match = re.match(r"^(.+)_P(\d+)$", name, re.IGNORECASE)
            if match:
                base = match.group(1)
                idx = int(match.group(2))
                groups.setdefault(base, []).append((idx, name))
        
        for base, parts in groups.items():
            parts.sort()
            frames = [data[part_name] for _, part_name in parts if isinstance(data.get(part_name), pd.DataFrame)]
            
            if frames and base not in data:
                try:
                    data[base] = pd.concat(frames, ignore_index=True)
                except Exception:
                    pass
        
        # Use canonical casing for aliases to avoid confusion
        aliases = {
            "LinkedServiceUsage": ["linkedserviceusage", "LinkedService_Usage", "LS_Usage", "LinkedServices"],
            "PipelineAnalysis": ["pipelineanalysis", "Pipeline_Analysis", "Pipelines"],
            "ActivityCount": ["activitycount", "Activity_Count", "Activities"],
            "ImpactAnalysis": ["impactanalysis", "Impact_Analysis", "Impact"],
            "DataLineage": ["datalineage", "DataLineage", "Lineage"],
            "TriggerDetails": ["triggerdetails", "Triggers", "Trigger_Details"],
            "OrphanedPipelines": ["orphanedpipelines", "Orphaned_Pipelines", "Orphans"],
        }
        
        # Helper to normalize for lookup
        def normalize(s): return str(s).lower().replace(" ", "").replace("_", "").replace("-", "")
        
        for canonical, variants in aliases.items():
            # If canonical name (properly cased) exists, skip
            if canonical in data:
                continue
                
            # Check if any variant (or normalized version of variant) exists
            for variant in variants:
                if variant in data:
                    data[canonical] = data[variant]
                    break
                
                # Check normalized keys in data
                for existing_key in list(data.keys()):
                    if normalize(existing_key) == normalize(variant):
                        data[canonical] = data[existing_key]
                        break
                if canonical in data:
                    break

    # @st.cache_data(ttl=600)  <-- REMOVED: Caching breaks session state updates!
    def extract_metadata(_self):
        """Extract and cache metadata from loaded data."""
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
        
        # Extract summary info
        summary = safe_get_dataframe("Summary")
        if not summary.empty and "Metric" in summary.columns and "Value" in summary.columns:
            try:
                metadata["summary"] = summary.set_index("Metric")["Value"].to_dict()
            except:
                metadata["summary"] = {}
        else:
            metadata["summary"] = {}
        
        st.session_state.analysis_metadata = metadata
        return metadata

    def build_dependency_graph(self):
        """Build NetworkX dependency graph from loaded data."""
        if not HAS_NETWORKX:
            st.warning("⚠️ NetworkX not installed - graph features disabled")
            st.session_state.dependency_graph = None
            return
        
        try:
            G = nx.DiGraph()
            
            # Add Pipeline Nodes
            pipeline_df = safe_get_dataframe("ImpactAnalysis", "PipelineAnalysis")
            
            if not pipeline_df.empty:
                for _, row in pipeline_df.iterrows():
                    pipeline_name = row.get("Pipeline") or row.get("PipelineName") or ""
                    if not pipeline_name:
                        continue
                    
                    # Extract attributes
                    has_trigger = bool(row.get("DirectUpstreamTriggerCount", 0))
                    has_dataflow = bool(row.get("DataFlowCount", 0))
                    is_orphaned = row.get("IsOrphaned") in ["Yes", True, 1]
                    impact = str(row.get("Impact", "LOW"))
                    
                    G.add_node(
                        pipeline_name,
                        type="pipeline",
                        has_trigger=has_trigger,
                        has_dataflow=has_dataflow,
                        is_orphaned=is_orphaned,
                        impact=impact,
                    )
            
            # Add edges (simplified for performance)
            self._add_graph_edges(G)
            
            # Store graph
            st.session_state.dependency_graph = G
            st.session_state.graph_metrics = {
                "nodes": G.number_of_nodes(),
                "edges": G.number_of_edges(),
                "density": nx.density(G) if G.number_of_nodes() > 0 else 0,
            }
        
        except Exception as e:
            st.error(f"❌ Error building graph: {e}")
            st.session_state.dependency_graph = nx.DiGraph()

    def _add_graph_edges(self, G: nx.DiGraph):
        """Add edges to dependency graph."""
        # Trigger → Pipeline edges
        trigger_df = safe_get_dataframe("TriggerDetails", "Triggers")
        if not trigger_df.empty:
            for _, row in trigger_df.iterrows():
                trigger = row.get("Trigger", "")
                pipeline = row.get("Pipeline", "")
                if trigger and pipeline:
                    if not G.has_node(trigger):
                        G.add_node(trigger, type="trigger")
                    G.add_edge(trigger, pipeline, relation="triggers", weight=3)
        
        # Pipeline → Pipeline edges
        pp_df = safe_get_dataframe("Pipeline_Pipeline", "PipelinePipeline")
        if not pp_df.empty:
            for _, row in pp_df.iterrows():
                from_pl = row.get("from_pipeline", "")
                to_pl = row.get("to_pipeline", "")
                if from_pl and to_pl:
                    G.add_edge(from_pl, to_pl, relation="executes", weight=2)
        
        # Pipeline → DataFlow edges
        pdf_df = safe_get_dataframe("Pipeline_DataFlow", "PipelineDataFlow")
        if not pdf_df.empty:
            for _, row in pdf_df.iterrows():
                pipeline = row.get("pipeline", "")
                dataflow = row.get("dataflow", "")
                if pipeline and dataflow:
                    if not G.has_node(dataflow):
                        G.add_node(dataflow, type="dataflow")
                    G.add_edge(pipeline, dataflow, relation="uses_dataflow", weight=1)

    def load_sample_data(self):
        """Load comprehensive sample data for demonstration."""
        with st.spinner("🎮 Loading sample data..."):
            # Use the same sample data structure from original
            sample_data = self._generate_sample_data()
            
            st.session_state.excel_data = sample_data
            st.session_state.uploaded_file_name = "sample_data.xlsx"
            st.session_state.data_loaded = True
            st.session_state.last_load_time = datetime.now()
            
            self.extract_metadata()
            self.build_dependency_graph()
            
            st.success("✅ Sample data loaded successfully!")
            st.balloons()

    def _generate_sample_data(self) -> Dict[str, pd.DataFrame]:
        """Generate a comprehensive set of sample data covering all dashboard features."""
        return {
            "Summary": pd.DataFrame([
                {"Metric": "Factories", "Value": 1},
                {"Metric": "Pipelines", "Value": 45},
                {"Metric": "Triggers", "Value": 12},
                {"Metric": "DataSets", "Value": 84},
            ]),
            "ImpactAnalysis": pd.DataFrame([
                {
                    "Pipeline": "PL_Core_Sales",
                    "Impact": "CRITICAL",
                    "BlastRadius": 15,
                    "DirectUpstreamTriggerCount": 2,
                    "DirectDownstreamPipelineCount": 2,
                    "DataFlowCount": 1,
                    "IsOrphaned": "No",
                },
                {
                    "Pipeline": "PL_DataTransformation",
                    "Impact": "HIGH",
                    "BlastRadius": 12,
                    "DirectUpstreamTriggerCount": 1,
                    "DirectDownstreamPipelineCount": 1,
                    "DataFlowCount": 2,
                    "IsOrphaned": "No",
                }
            ]),
            "ActivityCount": pd.DataFrame([
                {"Activity": "Copy", "Count": 45},
                {"Activity": "ExecuteDataFlow", "Count": 28},
                {"Activity": "Lookup", "Count": 18},
                {"Activity": "Wait", "Count": 12},
                {"Activity": "Validation", "Count": 8},
            ]),
            "DataFlows": pd.DataFrame([
                {"DataFlow": "DF_Cleanse_Sales", "Sources": 2, "Sinks": 1, "Transformations": 12},
                {"DataFlow": "DF_Aggr_Finance", "Sources": 1, "Sinks": 1, "Transformations": 8},
            ]),
            "DataLineage": pd.DataFrame([
                {"Source": "SQL_Sales", "Sink": "ADLS_Raw", "Pipeline": "PL_Ingest_Sales"},
                {"Source": "CRM_Dynamics", "Sink": "ADLS_Raw", "Pipeline": "PL_Ingest_CRM"},
            ])
        }

    # ═══════════════════════════════════════════════════════════════════════
    # LAUNCHER & MAIN CONTENT RENDERING
    # ═══════════════════════════════════════════════════════════════════════

    def render_launcher(self):
        """Render the initial launcher screen."""
        st.markdown("## 🚀 Welcome to ADF Analyzer v10.1 Enhanced")
        st.markdown("Choose how you want to start your analysis:")
        
        col1, col2 = st.columns(2, gap="large")
        
        with col1:
            st.markdown("""
            <div style="background: linear-gradient(135deg, rgba(var(--primary-rgb, 102, 126, 234), 0.08) 0%, rgba(var(--secondary-rgb, 118, 75, 162), 0.08) 100%); 
                        padding: 30px; border-radius: 15px; text-align: center; margin: 20px 0;">
                <h3 style="color: var(--primary, #667eea); margin-bottom: 15px;">🔧 Generate Excel</h3>
                <p style="margin-bottom: 20px;">Run the ADF analyzer to create a fresh Excel workbook.</p>
                <p style="font-size: 0.9em; color: #666;">
                    • Applies 20+ patches<br/>
                    • Professional Excel reports<br/>
                    • Health score dashboard
                </p>
            </div>
            """, unsafe_allow_html=True)
            
            if st.button("🔧 Generate Excel", type="primary", width='stretch'):
                st.session_state['app_mode'] = 'generate'
                st.session_state['app_mode_selected'] = True
                st.rerun()
        
        with col2:
            st.markdown("""
            <div style="background: linear-gradient(135deg, #43e97b15 0%, #38f9d715 100%); 
                        padding: 30px; border-radius: 15px; text-align: center; margin: 20px 0;">
                <h3 style="color: #43e97b; margin-bottom: 15px;">📊 Upload & Analyze</h3>
                <p style="margin-bottom: 20px;">Upload existing Excel to view interactive dashboards.</p>
                <p style="font-size: 0.9em; color: #666;">
                    • Network visualizations<br/>
                    • Impact analysis<br/>
                    • Data lineage tracking
                </p>
            </div>
            """, unsafe_allow_html=True)
            
            if st.button("📊 Upload & Analyze", type="secondary", width='stretch'):
                st.session_state['app_mode'] = 'analyze'
                st.session_state['app_mode_selected'] = True
                st.rerun()

    def render_main_content_with_tabs(self):
        """Render the main dashboard content organized by tabs."""
        # 🟢 DELETE: Duplicate "Back to Launcher" button removed as per code review
        
        st.markdown("---")
        
        # Mode-based info
        mode = st.session_state.get('app_mode', 'generate')
        if mode == 'analyze':
            st.info("📊 **Upload & Analyze Mode** - Use sidebar or tabs to upload Excel")
        elif mode == 'generate':
            st.info("🔧 **Generate Excel Mode** - Click tab to configure analyzer")
        
        # Main tabs
        tab_labels = ["⚙️ Generate Excel", "📊 Upload & Analyze", "🤖 AI Chat", "📚 Documentation"]
        main_tabs = st.tabs(tab_labels)
        
        with main_tabs[0]:
            self.render_generate_excel_tab()
        
        with main_tabs[1]:
            if st.session_state.data_loaded:
                self.render_enhanced_metrics()
                st.markdown("---")
                self.render_dashboard_tabs()
            else:
                self.render_upload_interface()
        
        with main_tabs[2]:
            if HAS_AI_CHAT:
                excel_data = st.session_state.get('excel_data', {})
                render_ai_chat_tab(excel_data=excel_data)
            else:
                st.warning("⚠️ AI Chat module not available. Please ensure `ai_excel_chat.py` is in the project directory.")
                st.code("pip install requests python-dotenv", language="bash")
        
        with main_tabs[3]:
            self.render_comprehensive_documentation()

    def render_generate_excel_tab(self):
        """Render Generate Excel tab with FIXED Python path."""
        st.header("🔧 Generate Excel Workbook")
        st.markdown("Run the ADF analyzer with patches to generate a fresh workbook.")
        
        base_dir = Path(__file__).parent
        output_dir = base_dir / "output"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # JSON Input
        st.subheader("📁 Input ADF Template")
        
        uploaded_json = st.file_uploader("Upload ADF Template JSON", type=["json"])
        
        json_files = list(base_dir.parent.glob("*.json")) + list(base_dir.glob("*.json"))
        json_file_names = ["(Select existing file)"] + [f.name for f in json_files]
        selected_json = st.selectbox("Or select existing JSON:", json_file_names)
        
        # Script Selection
        st.subheader("⚙️ Script Selection")
        
        runners = [p.name for p in base_dir.glob('adf_*.py') if p.is_file()]
        script_options = ['(auto - use best available)'] + sorted(runners)
        selected_script = st.selectbox('Choose script:', script_options, key='gen_sel_runner')
        
        # Quick Options
        st.subheader("⚡ Quick Options")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            enable_patches = st.checkbox('🔧 Apply Patches', value=True, key='gen_patches')
        with col2:
            enable_excel = st.checkbox('✨ Apply Enhancements', value=True, key='gen_excel')
        with col3:
            enable_discovery = st.checkbox('🔍 Discovery Mode', value=True, key='gen_discovery')
        
        # Output Config
        st.subheader("📤 Output Configuration")
        
        col1, col2 = st.columns(2)
        with col1:
            output_filename = st.text_input('Output filename:', value='adf_analysis_latest.xlsx')
        with col2:
            load_after = st.checkbox('🔄 Auto-load after generation', value=True)
        
        # Execution
        st.subheader("🚀 Execute")
        
        confirm_run = st.checkbox('✅ I understand this will execute Python code', value=False)
        
        if st.button('▶️ Run Generator', type="primary", width='stretch'):
            if not confirm_run:
                st.error('❌ Please confirm execution')
                return
            
            # Determine input file
            input_file = None
            if uploaded_json:
                try:
                    # ✅ Security Fix: Sanitize filename to prevent path traversal
                    safe_name = ADF_Dashboard.sanitize_filename(uploaded_json.name)
                    temp_json = base_dir / f"temp_{safe_name}"
                    
                    # Belt + suspenders: Verify it's still under base_dir
                    if not temp_json.resolve().is_relative_to(base_dir.resolve()):
                        st.error("❌ Link traversal detected. Invalid filename.")
                        return

                    temp_json.write_bytes(uploaded_json.read())
                except Exception as e:
                    st.error(f"❌ Failed to save temporary file: {e}")
                    return
                input_file = str(temp_json)
            elif selected_json != "(Select existing file)":
                for f in json_files:
                    if f.name == selected_json:
                        input_file = str(f)
                        break
            
            if not input_file:
                st.error('❌ Please upload or select a JSON file')
                return
            
            # Determine runner
            runner_name = selected_script if selected_script != '(auto - use best available)' else None
            if not runner_name:
                for cand in ['adf_runner_wrapper.py', 'adf_analyzer_v10_patched_runner.py']:
                    if cand in runners:
                        runner_name = cand
                        break
            
            if not runner_name:
                st.error('❌ No runner script found')
                return
            
            runner_path = base_dir / runner_name
            
            # Execute with FIXED Python path
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
        """
        Execute patch runner with configuration.
        
        ✅ FIXED: Now uses sys.executable instead of hardcoded path
        """
        import subprocess
        
        try:
            # Create temp config
            cfg = {
                'functional_patches': bool(enable_patches),
                'excel_enhancements': {'enabled': bool(enable_excel)},
                'discovery_mode': bool(enable_discovery),
                'excel': {'output_filename': output_filename, 'output_dir': str(output_dir)}
            }
            
            tmpcfg = runner_path.parent / 'adf_runner_temp_config.json'
            tmpcfg.write_text(json.dumps(cfg, indent=2), encoding='utf-8')
            
            with st.status("🔄 Running generator...", expanded=True) as status:
                st.write("🏃‍♂️ Starting process...")
                
                # Setup environment
                env = os.environ.copy()
                env['ADF_ANALYZER_CONFIG_JSON'] = str(tmpcfg)
                env['ADF_OUTPUT_FILENAME'] = output_filename
                env['ADF_OUTPUT_DIR'] = str(output_dir)
                env['PYTHONIOENCODING'] = 'utf-8'
                
                # ✅ FIXED: Use sys.executable instead of hardcoded path
                python_executable = sys.executable
                cmd = [python_executable, str(runner_path), input_file]
                
                st.write(f"💻 Command: {' '.join(cmd)}")
                
                # Execute
                output_placeholder = st.empty()
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    env=env,
                    text=True,
                    encoding='utf-8',
                    errors='replace',
                    cwd=runner_path.parent
                )
                
                output_lines = []
                while True:
                    line = process.stdout.readline()
                    if not line and process.poll() is not None:
                        break
                    if line:
                        output_lines.append(line)
                        recent = output_lines[-20:]
                        output_placeholder.text_area("Output:", value=''.join(recent), height=200)
                
                ret_code = process.poll()
                
                if ret_code == 0:
                    status.update(label="✅ Complete!", state="complete")
                    st.success('🎉 Generation successful!')
                else:
                    status.update(label=f"❌ Failed (code: {ret_code})", state="error")
                    st.error(f'❌ Failed with exit code: {ret_code}')
                    return
            
            # Handle output
            produced_file = output_dir / output_filename
            if produced_file.exists():
                st.success(f'📁 Generated: {produced_file}')
                
                # Download button
                file_data = produced_file.read_bytes()
                st.download_button(
                    label='📥 Download Excel',
                    data=file_data,
                    file_name=output_filename,
                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    type="primary"
                )
                
                # Auto-load
                if load_after:
                    st.info('🔄 Auto-loading...')
                    self.load_excel_file(str(produced_file))
                    # ✅ Reset AI context for fresh data
                    st.session_state.ai_context_builder = None
                    st.session_state.ai_context_hash = ""
                    st.success('✅ Loaded into dashboard! Switch to 🤖 AI Chat tab to query with AI.')
        
        except Exception as e:
            st.error(f'❌ Execution failed: {e}')
            with st.expander("🔍 Details"):
                st.code(traceback.format_exc())


# ═══════════════════════════════════════════════════════════════════════════
# END OF PART 2
# ═══════════════════════════════════════════════════════════════════════════
    # ═══════════════════════════════════════════════════════════════════════
    # ENHANCED METRICS - Refactored into Modular Functions
    # ═══════════════════════════════════════════════════════════════════════

    def render_enhanced_metrics(self):
        """
        Main orchestrator for enhanced metrics display.
        Delegates to specialized functions for better maintainability.
        """
        st.markdown("### 📊 Factory Metrics Dashboard")
        
        # Status indicator
        self._show_data_load_status()
        
        # Primary metrics row
        self._render_primary_metrics()
        
        # Secondary metrics row
        self._render_secondary_metrics()
        
        # Lineage & verification section (collapsible)
        with st.expander("🔎 Lineage & Details", expanded=False):
            self._render_lineage_metrics()
            self._render_verification_panel()

    def _show_data_load_status(self):
        """Display data load status indicator."""
        if st.session_state.get('excel_data'):
            total_sheets = len(st.session_state.excel_data)
            st.success(f"✅ Successfully loaded {total_sheets} data sheets")
        else:
            st.warning("⚠️ No data loaded - metrics will show zero values")

    # Remove @st.cache_data for UI metrics to prevent stale results
    def _get_cached_metrics(self) -> Dict[str, int]:
        """
        Get cached metrics for performance.
        Returns dict of metric_name -> value.
        """
        metrics = {}
        
        # Core metrics
        metrics['pipelines'] = get_count_with_fallback(
            "Pipelines", ["ImpactAnalysis", "PipelineAnalysis"]
        )
        metrics['dataflows'] = get_count_with_fallback(
            "DataFlows", ["DataFlows", "DataFlowLineage"]
        )
        metrics['datasets'] = get_count_with_fallback(
            "Datasets", ["Datasets"]
        )
        
        # Triggers (prefer canonical Triggers sheet)
        triggers = 0
        tr_df = safe_get_dataframe("Triggers")
        if not tr_df.empty:
            triggers = len(tr_df)
        else:
            td = safe_get_dataframe("TriggerDetails")
            if not td.empty and 'Trigger' in td.columns:
                triggers = td['Trigger'].nunique()
        metrics['triggers'] = triggers
        
        metrics['dependencies'] = get_count_with_fallback(
            "Total Dependencies", ["ActivityExecutionOrder", "DataLineage"]
        )
        metrics['orphaned'] = get_count_with_fallback(
            "Orphaned Pipelines", ["OrphanedPipelines"]
        )
        
        # Health score
        if metrics['pipelines'] > 0:
            metrics['health'] = int((1 - metrics['orphaned'] / metrics['pipelines']) * 100)
        else:
            metrics['health'] = 100
        
        return metrics

    def _render_primary_metrics(self):
        """Render primary metrics row (4 tiles)."""
        metrics = self._get_cached_metrics()
        
        col1, col2, col3, col4 = st.columns(4)
        
        tiles = [
            (col1, "📦", metrics['pipelines'], "Pipelines", "purple", "Total pipeline resources"),
            (col2, "🌊", metrics['dataflows'], "DataFlows", "pink", "DataFlow resources"),
            (col3, "📋", metrics['datasets'], "Datasets", "blue", "Dataset resources"),
            (col4, "⏰", metrics['triggers'], "Triggers", "green", "Trigger configurations"),
        ]
        
        for col, icon, value, label, variant, tooltip in tiles:
            with col:
                render_premium_tile(
                    icon=icon,
                    value=value,
                    label=label,
                    variant=variant,
                    tooltip=tooltip
                )

    def _render_secondary_metrics(self):
        """Render secondary metrics row (3 tiles)."""
        metrics = self._get_cached_metrics()
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            render_premium_tile(
                icon="🔗",
                value=metrics['dependencies'],
                label="Dependencies",
                variant="orange",
                tooltip="Total dependency relationships"
            )
        
        with col2:
            render_premium_tile(
                icon="🏥",
                value=f"{metrics['health']}%",
                label="Health Score",
                variant="teal",
                tooltip="Factory health score (0-100)"
            )
        
        with col3:
            variant = "fire" if metrics['orphaned'] > 0 else "green"
            icon = "⚠️" if metrics['orphaned'] > 0 else "✅"
            render_premium_tile(
                icon=icon,
                value=metrics['orphaned'],
                label="Orphaned",
                variant=variant,
                tooltip="Unused/orphaned pipelines"
            )

    def _render_lineage_metrics(self):
        """Render lineage metrics and charts."""
        lineage_df = safe_get_dataframe("DataLineage")
        
        if lineage_df.empty:
            st.info("📊 No lineage data available")
            return
        
        # Calculate lineage metrics
        total_source_datasets = lineage_df['Source'].nunique() if 'Source' in lineage_df.columns else 0
        total_target_datasets = lineage_df['Sink'].nunique() if 'Sink' in lineage_df.columns else 0
        
        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Source Datasets", format_number(total_source_datasets))
        with col2:
            st.metric("Target Datasets", format_number(total_target_datasets))
        with col3:
            st.metric("Total Flows", format_number(len(lineage_df)))
        with col4:
            copy_count = len(lineage_df[lineage_df['Type'] == 'Copy']) if 'Type' in lineage_df.columns else 0
            st.metric("Copy Activities", format_number(copy_count))
        
        # Charts
        st.markdown("---")
        self._render_lineage_charts(lineage_df)

    # Remove @st.cache_data for rendering methods to prevent stale results
    def _render_lineage_charts(self, lineage_df: pd.DataFrame):
        """Render lineage charts (cached for performance)."""
        if lineage_df.empty or 'Source' not in lineage_df.columns:
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 📊 Top Sources")
            top_sources = lineage_df['Source'].value_counts().head(10)
            
            if not top_sources.empty:
                fig = go.Figure(go.Bar(
                    x=top_sources.values,
                    y=top_sources.index,
                    orientation='h',
                    marker=dict(
                        color=PremiumColors.get_gradient('gradient_1', 0),
                        line=dict(color='rgba(255,255,255,0.8)', width=1.5)
                    ),
                    hovertemplate="<b>%{y}</b><br>Count: <b>%{x:,}</b><extra></extra>"
                ))
                
                fig.update_layout(
                    height=350,
                    title=dict(text="Top 10 Sources", font=dict(size=16, color=PremiumColors.PRIMARY)),
                    xaxis_title="Count",
                    yaxis_title="Source",
                    **PREMIUM_CHART_TEMPLATE['layout']
                )
                
                st.plotly_chart(fig, width='stretch')
        
        with col2:
            st.markdown("#### 📈 Top Targets")
            if 'Sink' in lineage_df.columns:
                top_targets = lineage_df['Sink'].value_counts().head(10)
                
                if not top_targets.empty:
                    fig = go.Figure(go.Bar(
                        x=top_targets.values,
                        y=top_targets.index,
                        orientation='h',
                        marker=dict(
                            color=PremiumColors.get_gradient('gradient_2', 0),
                            line=dict(color='rgba(255,255,255,0.8)', width=1.5)
                        ),
                        hovertemplate="<b>%{y}</b><br>Count: <b>%{x:,}</b><extra></extra>"
                    ))
                    
                    fig.update_layout(
                        height=350,
                        title=dict(text="Top 10 Targets", font=dict(size=16, color=PremiumColors.PRIMARY)),
                        xaxis_title="Count",
                        yaxis_title="Target",
                        **PREMIUM_CHART_TEMPLATE['layout']
                    )
                    
                    st.plotly_chart(fig, width='stretch')

    def _render_verification_panel(self):
        """Render verification panel for metrics."""
        st.markdown("---")
        st.markdown("### ✅ Verify Metrics")
        
        col1, col2 = st.columns([1, 3])
        
        with col1:
            if st.button("🔍 Run Verification"):
                metrics = self._get_cached_metrics()
                verification_report = {
                    "Pipelines": {"value": metrics['pipelines'], "source": "Computed"},
                    "DataFlows": {"value": metrics['dataflows'], "source": "Computed"},
                    "Datasets": {"value": metrics['datasets'], "source": "Computed"},
                    "Triggers": {"value": metrics['triggers'], "source": "Computed"},
                    "Dependencies": {"value": metrics['dependencies'], "source": "Computed"},
                    "Orphaned": {"value": metrics['orphaned'], "source": "Computed"},
                    "Health": {"value": metrics['health'], "source": "Computed"},
                }
                
                st.session_state['last_verifier_report'] = verification_report
                
                # Persist to file
                try:
                    output_dir = Path(__file__).parent / "output"
                    output_dir.mkdir(exist_ok=True)
                    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
                    verify_file = output_dir / f"verify_{ts}.json"
                    verify_file.write_text(json.dumps(verification_report, indent=2))
                    st.success(f"✅ Saved to: {verify_file.name}")
                except Exception as e:
                    st.warning(f"Could not save verification: {e}")
        
        with col2:
            st.info("Click to recompute and verify all metric values")
        
        # Show verification results
        if 'last_verifier_report' in st.session_state:
            st.markdown("**Verification Results:**")
            report = st.session_state['last_verifier_report']
            
            for key, data in report.items():
                value = data['value']
                source = data['source']
                st.markdown(f"- **{key}**: {format_number(value) if isinstance(value, int) else value} _(source: {source})_")

    # ═══════════════════════════════════════════════════════════════════════
    # DASHBOARD TABS - Overview, Network, Impact, etc.
    # ═══════════════════════════════════════════════════════════════════════

    def render_dashboard_tabs(self):
        """Render main dashboard tabs."""
        tabs = st.tabs([
            "🏠 Overview",
            "🌐 Network",
            "💥 Impact",
            "🚫 Orphaned",
            "📊 Statistics",
            "⚡ DataFlow",
            "🔄 Lineage",
            "📁 Explorer",
            "📤 Export",
        ])
        
        with tabs[0]:
            self.render_overview_tab()
        
        with tabs[1]:
            self.render_network_tab()
        
        with tabs[2]:
            self.render_impact_analysis_tab()
        
        with tabs[3]:
            self.render_orphaned_resources_tab()
        
        with tabs[4]:
            self.render_statistics_tab()
        
        with tabs[5]:
            self.render_dataflow_tab()
        
        with tabs[6]:
            self.render_lineage_tab()
        
        with tabs[7]:
            self.render_explorer_tab()
        
        with tabs[8]:
            self.render_export_tab()

    # ═══════════════════════════════════════════════════════════════════════
    # OVERVIEW TAB - Enhanced
    # ═══════════════════════════════════════════════════════════════════════

    def render_overview_tab(self):
        """Render overview dashboard with enhanced charts."""
        st.markdown("### 🏠 Factory Overview")
        
        # Row 1: Distribution and Health
        col1, col2 = st.columns([2, 1])
        
        with col1:
            self._render_pipeline_distribution_chart()
        
        with col2:
            self._render_health_gauge()
        
        st.markdown("---")
        
        # Row 2: Activity and Resources
        col1, col2 = st.columns(2)
        
        with col1:
            self._render_activity_distribution()
        
        with col2:
            self._render_resource_summary()

    # Remove @st.cache_data for rendering methods to prevent stale results
    def _render_pipeline_distribution_chart(self):
        """Render pipeline distribution chart (cached)."""
        impact_df = safe_get_dataframe("ImpactAnalysis", "PipelineAnalysis")
        
        if impact_df.empty:
            st.info("📊 No pipeline data available")
            return
        
        # Calculate categories
        categories = {
            "With Triggers": (impact_df.get('DirectUpstreamTriggerCount', pd.Series([0])).fillna(0) > 0).sum(),
            "With DataFlows": (impact_df.get('DataFlowCount', pd.Series([0])).fillna(0) > 0).sum(),
            "Calling Pipelines": (impact_df.get('DirectDownstreamPipelineCount', pd.Series([0])).fillna(0) > 0).sum(),
            "Orphaned": (impact_df.get('IsOrphaned', pd.Series(['No'])) == 'Yes').sum(),
        }
        
        # Create chart
        fig = go.Figure(go.Bar(
            y=list(categories.keys()),
            x=list(categories.values()),
            orientation='h',
            marker=dict(
                color=[
                    PremiumColors.get_gradient('gradient_1', 0),
                    PremiumColors.get_gradient('gradient_2', 0),
                    PremiumColors.get_gradient('gradient_3', 0),
                    PremiumColors.get_gradient('gradient_4', 0),
                ],
                line=dict(color='rgba(255,255,255,0.8)', width=2)
            ),
            text=list(categories.values()),
            textposition='auto',
            textfont=dict(size=14, color='white'),
            hovertemplate="<b>%{y}</b><br>Count: <b>%{x:,}</b><extra></extra>"
        ))
        
        fig.update_layout(
            title=dict(text="📊 Pipeline Categories", font=dict(size=20, color=PremiumColors.PRIMARY)),
            xaxis_title="Count",
            height=400,
            showlegend=False,
            **PREMIUM_CHART_TEMPLATE['layout']
        )
        
        st.plotly_chart(fig, width='stretch')

    def _render_health_gauge(self):
        """Render health score gauge."""
        metrics = self._get_cached_metrics()
        health_score = metrics['health']
        
        # Determine color and status
        if health_score >= 90:
            color = PremiumColors.SUCCESS
            status = "Excellent"
            icon = "✨"
        elif health_score >= 75:
            color = PremiumColors.INFO
            status = "Good"
            icon = "🔵"
        elif health_score >= 60:
            color = PremiumColors.WARNING
            status = "Fair"
            icon = "⚠️"
        else:
            color = PremiumColors.DANGER
            status = "Needs Attention"
            icon = "❌"
        
        # Create gauge
        fig = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=health_score,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': f"{icon} Health Score", 'font': {'size': 18}},
            delta={'reference': 80, 'increasing': {'color': PremiumColors.SUCCESS}},
            gauge={
                'axis': {'range': [None, 100], 'tickwidth': 1},
                'bar': {'color': color},
                'bgcolor': 'white',
                'borderwidth': 2,
                'bordercolor': 'gray',
                'steps': [
                    {'range': [0, 60], 'color': '#ffebee'},
                    {'range': [60, 75], 'color': '#fff9c4'},
                    {'range': [75, 90], 'color': '#e1f5fe'},
                    {'range': [90, 100], 'color': '#e8f5e9'},
                ],
                'threshold': {
                    'line': {'color': 'red', 'width': 4},
                    'thickness': 0.75,
                    'value': 90,
                },
            },
        ))
        
        fig.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=60, b=20),
            **PREMIUM_CHART_TEMPLATE['layout']
        )
        
        st.plotly_chart(fig, width='stretch')

    def _render_activity_distribution(self):
        """Render activity distribution pie chart."""
        activity_df = safe_get_dataframe("ActivityCount")
        
        if activity_df.empty:
            st.info("📊 No activity data")
            return
        
        # Clean data
        activity_df = activity_df[~activity_df['ActivityType'].astype(str).str.contains('TOTAL', na=False)]
        activity_df['Count'] = pd.to_numeric(activity_df['Count'], errors='coerce').fillna(0).astype(int)
        
        top_activities = activity_df.nlargest(10, 'Count')
        
        if top_activities.empty:
            st.info("📊 No activity data to display")
            return
        
        # Create pie chart
        fig = go.Figure(go.Pie(
            labels=top_activities['ActivityType'],
            values=top_activities['Count'],
            hole=0.4,
            marker=dict(
                colors=[PremiumColors.get_gradient(f'gradient_{i%8+1}', 0) for i in range(len(top_activities))],
                line=dict(color='rgba(255,255,255,0.8)', width=2)
            ),
            textinfo='label+percent',
            hovertemplate="<b>%{label}</b><br>Count: <b>%{value:,}</b><br>%{percent}<extra></extra>"
        ))
        
        fig.update_layout(
            title=dict(text="⚡ Activity Distribution", font=dict(size=20, color=PremiumColors.PRIMARY)),
            height=450,
            showlegend=True,
            legend=dict(orientation="v", yanchor="middle", y=0.5, xanchor="left", x=1.05),
            **PREMIUM_CHART_TEMPLATE['layout']
        )
        
        st.plotly_chart(fig, width='stretch')

    def _render_resource_summary(self):
        """Render resource summary treemap."""
        resources = []
        counts = []
        
        resource_types = [
            ("Pipelines", get_count_with_fallback("Pipelines", ["PipelineAnalysis"])),
            ("DataFlows", get_count_with_fallback("DataFlows", ["DataFlowLineage"])),
            ("Datasets", get_count_with_fallback("Datasets", ["Datasets"])),
            ("Triggers", get_count_with_fallback("Triggers", ["TriggerDetails"])),
            ("LinkedServices", get_count_with_fallback("LinkedServices", ["LinkedServices"])),
        ]
        
        for label, count in resource_types:
            if count > 0:
                resources.append(label)
                counts.append(count)
        
        if not resources:
            st.info("📦 No resource data")
            return
        
        fig = go.Figure(go.Treemap(
            labels=resources,
            parents=[""] * len(resources),
            values=counts,
            textinfo="label+value+percent root",
            marker=dict(colorscale="Viridis", line=dict(width=2, color='white')),
            hovertemplate="<b>%{label}</b><br>Count: %{value}<br>%{percentRoot}<extra></extra>"
        ))
        
        fig.update_layout(
            title=dict(text="📦 Resources Overview", font=dict(size=20, color=PremiumColors.PRIMARY)),
            height=400,
            **PREMIUM_CHART_TEMPLATE['layout']
        )
        
        st.plotly_chart(fig, width='stretch')

    # ═══════════════════════════════════════════════════════════════════════
    # NETWORK TAB - Enhanced with Better Filtering
    # ═══════════════════════════════════════════════════════════════════════

    def render_network_tab(self):
        """Render network visualization with enhanced filtering."""
        st.markdown("### 🌐 Dependency Network")
        st.markdown("*Interactive visualization of factory dependencies*")
        
        if not HAS_NETWORKX or st.session_state.dependency_graph is None:
            st.warning("⚠️ Network graph not available")
            return
        
        G = st.session_state.dependency_graph
        
        if G.number_of_nodes() == 0:
            st.warning("⚠️ No nodes in graph")
            return
        
        # Enhanced controls
        col1, col2, col3 = st.columns(3)
        
        with col1:
            show_types = st.multiselect(
                "🎨 Node Types",
                ["Triggers", "Pipelines", "DataFlows", "Datasets"],
                default=["Triggers", "Pipelines", "DataFlows"],
                key="net_types"
            )
        
        with col2:
            layout = st.selectbox(
                "📐 Layout",
                ["Spring", "Circular", "Hierarchical"],
                key="net_layout"
            )
        
        with col3:
            labels = st.checkbox("Show Labels", value=True, key="net_labels")
        
        # Filter nodes
        filtered = self._filter_graph_nodes(G, show_types)
        
        if not filtered:
            st.warning("⚠️ No nodes match filters")
            return
        
        # Create subgraph
        H = G.subgraph(filtered)
        
        # Calculate layout
        pos = self._calculate_graph_layout(H, layout)
        
        # Render graph
        self._render_2d_network(H, pos, labels)
        
        # Stats
        st.markdown("---")
        self._render_network_stats(H)

    def _filter_graph_nodes(self, G: nx.DiGraph, show_types: List[str]) -> List[str]:
        """Filter graph nodes by type."""
        filtered = []
        
        for node, data in G.nodes(data=True):
            node_type = data.get('type', 'unknown')
            
            if (
                (node_type == 'trigger' and 'Triggers' in show_types) or
                (node_type == 'pipeline' and 'Pipelines' in show_types) or
                (node_type == 'dataflow' and 'DataFlows' in show_types) or
                (node_type == 'dataset' and 'Datasets' in show_types)
            ):
                filtered.append(node)
        
        return filtered

    def _calculate_graph_layout(self, G: nx.DiGraph, layout_type: str) -> dict:
        """Calculate graph layout positions."""
        n = G.number_of_nodes()
        if n == 0:
            return {}
            
        try:
            if layout_type == "Spring":
                k = 1.0 / np.sqrt(max(n, 1))
                return nx.spring_layout(G, k=k, iterations=50, seed=42)
            elif layout_type == "Circular":
                return nx.circular_layout(G)
            else:  # Hierarchical
                try:
                    return nx.kamada_kawai_layout(G)
                except Exception:
                    return nx.spring_layout(G, seed=42)
        except Exception:
            return nx.spring_layout(G, seed=42)

    def _render_2d_network(self, G: nx.DiGraph, pos: dict, show_labels: bool):
        """Render 2D network graph."""
        # Edge trace
        edge_x, edge_y = [], []
        for edge in G.edges():
            x0, y0 = pos[edge[0]]
            x1, y1 = pos[edge[1]]
            edge_x.extend([x0, x1, None])
            edge_y.extend([y0, y1, None])
        
        edge_trace = go.Scatter(
            x=edge_x, y=edge_y,
            mode='lines',
            line=dict(width=2, color='rgba(102,126,234,0.4)'),
            hoverinfo='none',
            showlegend=False
        )
        
        # Node trace
        node_x, node_y, node_colors, node_text, node_sizes = [], [], [], [], []
        
        # Decide if labels should be shown (hide on large graphs)
        show_labels = show_labels and G.number_of_nodes() < 50
        
        for node in G.nodes():
            x, y = pos[node]
            node_x.append(x)
            node_y.append(y)
            
            data = G.nodes[node]
            node_type = data.get('type', 'unknown')
            
            # Assign colors using theme
            if node_type == 'trigger':
                color = PremiumColors.TRIGGER
                icon = "🔔"
                size = 30
            elif node_type == 'pipeline':
                color = PremiumColors.PIPELINE
                icon = "📦"
                size = 25
            elif node_type == 'dataflow':
                color = PremiumColors.DATAFLOW
                icon = "🌊"
                size = 25
            else:
                color = PremiumColors.DATASET
                icon = "📊"
                size = 20
            
            node_colors.append(color)
            node_text.append(f"{icon} {node}")
            node_sizes.append(size + G.degree(node) * 2)
        
        node_trace = go.Scatter(
            x=node_x, y=node_y,
            mode='markers+text' if show_labels else 'markers',
            marker=dict(
                size=node_sizes,
                color=node_colors,
                line=dict(color='rgba(255,255,255,0.8)', width=2),
                opacity=0.9
            ),
            text=node_text if show_labels else None,
            textposition='top center',
            hovertext=node_text,
            hoverinfo='text',
            showlegend=False
        )
        
        # Create figure
        fig = go.Figure(data=[edge_trace, node_trace])
        
        fig.update_layout(
            title=dict(
                text=f"🌐 Network ({G.number_of_nodes()} nodes, {G.number_of_edges()} edges)",
                font=dict(size=20, color=PremiumColors.PRIMARY)
            ),
            showlegend=False,
            hovermode='closest',
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            height=650,
            **PREMIUM_CHART_TEMPLATE['layout']
        )
        
        st.plotly_chart(fig, width='stretch')

    def _render_network_stats(self, G: nx.DiGraph):
        """Render network statistics."""
        st.markdown("### 📊 Network Statistics")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Nodes", G.number_of_nodes())
        with col2:
            st.metric("Edges", G.number_of_edges())
        with col3:
            density = nx.density(G) if G.number_of_nodes() > 0 else 0
            st.metric("Density", f"{density:.3f}")
        with col4:
            node_types = Counter(d.get('type', 'unknown') for _, d in G.nodes(data=True))
            st.metric("Node Types", len(node_types))

    # ═══════════════════════════════════════════════════════════════════════
    # IMPACT ANALYSIS TAB - Enhanced
    # ═══════════════════════════════════════════════════════════════════════

    def render_impact_analysis_tab(self):
        """Render impact analysis with enhanced visuals."""
        st.markdown("### 💥 Impact Analysis")
        st.markdown("*Understand change impact before making modifications*")
        
        impact_df = safe_get_dataframe("ImpactAnalysis", "PipelineAnalysis")
        
        if impact_df.empty or 'Pipeline' not in impact_df.columns:
            st.warning("⚠️ No impact data available")
            return
        
        if 'Impact' not in impact_df.columns:
            impact_df['Impact'] = 'LOW'
        
        # Impact distribution
        col1, col2 = st.columns([1, 2])
        
        with col1:
            self._render_impact_pie(impact_df)
        
        with col2:
            self._render_impact_metrics(impact_df)
        
        st.markdown("---")
        
        # Filters
        impact_filter = st.multiselect(
            "🎯 Impact Level",
            ["CRITICAL", "HIGH", "MEDIUM", "LOW"],
            default=["CRITICAL", "HIGH"],
            key="impact_filter"
        )
        
        orphan_filter = st.selectbox(
            "🗑️ Orphaned Filter",
            ["All", "Only Orphaned", "Exclude Orphaned"],
            key="impact_orphan_filter"
        )
        
        sort_by = st.selectbox(
            "📊 Sort By",
            ["Impact", "BlastRadius", "Pipeline"],
            key="impact_sort"
        )
        
        # Apply filters
        filtered = impact_df.copy()
        
        if impact_filter:
            filtered = filtered[filtered['Impact'].isin(impact_filter)]
        
        if orphan_filter == "Only Orphaned" and 'IsOrphaned' in filtered.columns:
            filtered = filtered[filtered['IsOrphaned'].isin(['Yes', True, 1])]
        elif orphan_filter == "Exclude Orphaned" and 'IsOrphaned' in filtered.columns:
            filtered = filtered[~filtered['IsOrphaned'].isin(['Yes', True, 1])]
        
        # Sort
        sort_col_map = {"Impact": "Impact", "BlastRadius": "BlastRadius", "Pipeline": "Pipeline"}
        sort_col = sort_col_map.get(sort_by, "Impact")
        if sort_col in filtered.columns:
            filtered = filtered.sort_values(sort_col, ascending=(sort_by == "Pipeline"))
        
        # Display
        st.markdown(f"**Showing {len(filtered)} of {len(impact_df)} pipelines**")
        self._safe_dataframe(filtered, height=500)
        
        # Pipeline detail selector
        if not filtered.empty:
            selected_pipeline = st.selectbox(
                "🔍 Select pipeline for dependency view",
                ["-- Select Pipeline --"] + filtered['Pipeline'].tolist()
            )
            if selected_pipeline and selected_pipeline != "-- Select Pipeline --":
                pipeline_data = filtered[filtered['Pipeline'] == selected_pipeline].iloc[0]
                self.render_pipeline_dependency_sankey(pipeline_data)

    def _render_impact_pie(self, impact_df: pd.DataFrame):
        """Render impact distribution pie chart."""
        impact_counts = impact_df['Impact'].value_counts()
        
        labels, values, colors = [], [], []
        for level in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
            count = impact_counts.get(level, 0)
            if count > 0:
                labels.append(level)
                values.append(count)
                colors.append(PremiumColors.get_status_color(level))
        
        if not labels:
            st.info("📊 No impact data")
            return
        
        fig = go.Figure(go.Pie(
            labels=labels,
            values=values,
            hole=0.5,
            marker=dict(colors=colors),
            textinfo='label+percent',
            hovertemplate="<b>%{label}</b><br>Count: %{value}<br>%{percent}<extra></extra>"
        ))
        
        fig.update_layout(
            title="Impact Distribution",
            height=300,
            showlegend=True,
            **PREMIUM_CHART_TEMPLATE['layout']
        )
        
        st.plotly_chart(fig, width='stretch')

    def _render_impact_metrics(self, impact_df: pd.DataFrame):
        """Render impact level metrics."""
        st.markdown("#### 💥 Impact Summary")
        
        impact_counts = impact_df['Impact'].value_counts()
        
        col1, col2, col3, col4 = st.columns(4)
        
        metrics = [
            (col1, "CRITICAL", impact_counts.get('CRITICAL', 0), "fire"),
            (col2, "HIGH", impact_counts.get('HIGH', 0), "orange"),
            (col3, "MEDIUM", impact_counts.get('MEDIUM', 0), "blue"),
            (col4, "LOW", impact_counts.get('LOW', 0), "green"),
        ]
        
        # Per-severity icons: universally rendered, semantically meaningful
        severity_icons = {
            "CRITICAL": "🔴",
            "HIGH": "🟠",
            "MEDIUM": "🟡",
            "LOW": "🟢",
        }
        
        for col, label, count, variant in metrics:
            with col:
                render_premium_tile(
                    icon=severity_icons.get(label, "⚪"),
                    value=count,
                    label=label,
                    variant=variant
                )

    def _render_impact_filters(self):
        """Render impact analysis filters."""
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.multiselect(
                "🎯 Impact Level",
                ["CRITICAL", "HIGH", "MEDIUM", "LOW"],
                default=["CRITICAL", "HIGH"],
                key="impact_filter"
            )
        
        with col2:
            st.selectbox(
                "🗑️ Orphaned",
                ["All", "Only Orphaned", "Exclude Orphaned"],
                key="impact_orphan_filter"
            )
        
        with col3:
            st.selectbox(
                "📊 Sort By",
                ["Impact", "Blast Radius", "Name"],
                key="impact_sort"
            )


# ═══════════════════════════════════════════════════════════════════════════
# END OF PART 3
# ═══════════════════════════════════════════════════════════════════════════
    # ═══════════════════════════════════════════════════════════════════════
    # IMPACT ANALYSIS - Sankey Diagram (COMPLETED)
    # ═══════════════════════════════════════════════════════════════════════

    def render_pipeline_dependency_sankey(self, pipeline_data: pd.Series):
        """
        Render Sankey diagram for pipeline dependencies.
        
        ✅ FIXED: Completed function that was cut off in original code.
        
        Args:
            pipeline_data: Row from ImpactAnalysis DataFrame
        """
        pipeline_name = pipeline_data.get('Pipeline', 'Unknown')
        
        def safe_split(value):
            """Split string safely, return empty list if None/empty."""
            if pd.isna(value) or not value:
                return []
            value_str = str(value).strip()
            if not value_str or value_str in ['None', 'nan', 'NaN', '']:
                return []
            return [x.strip() for x in value_str.split(',') if x.strip()]
        
        # Extract dependencies
        triggers = safe_split(pipeline_data.get('DirectUpstreamTriggers', ''))
        upstream = safe_split(pipeline_data.get('DirectUpstreamPipelines', ''))
        downstream = safe_split(pipeline_data.get('DirectDownstreamPipelines', ''))
        dataflows = safe_split(pipeline_data.get('UsedDataFlows', ''))
        
        total_deps = len(triggers) + len(upstream) + len(downstream) + len(dataflows)
        
        if total_deps == 0:
            st.info(f"📭 No dependencies for pipeline: {pipeline_name}")
            return
        
        # Build Sankey data
        labels = [pipeline_name]
        sources, targets, values, colors = [], [], [], []
        node_index = {pipeline_name: 0}
        current_idx = 1
        
        # Add triggers → pipeline
        for trigger in triggers[:5]:
            if trigger not in node_index:
                labels.append(trigger)
                node_index[trigger] = current_idx
                current_idx += 1
            sources.append(node_index[trigger])
            targets.append(node_index[pipeline_name])
            values.append(3)
            colors.append('rgba(255, 215, 0, 0.5)')
        
        # Add upstream pipelines → pipeline
        for pipe in upstream[:5]:
            if pipe not in node_index:
                labels.append(pipe)
                node_index[pipe] = current_idx
                current_idx += 1
            sources.append(node_index[pipe])
            targets.append(node_index[pipeline_name])
            values.append(2)
            colors.append('rgba(135, 206, 235, 0.5)')
        
        # Add pipeline → downstream pipelines
        for pipe in downstream[:5]:
            if pipe not in node_index:
                labels.append(pipe)
                node_index[pipe] = current_idx
                current_idx += 1
            sources.append(node_index[pipeline_name])
            targets.append(node_index[pipe])
            values.append(2)
            colors.append('rgba(144, 238, 144, 0.5)')
        
        # Add pipeline → dataflows
        for df in dataflows[:5]:
            if df not in node_index:
                labels.append(df)
                node_index[df] = current_idx
                current_idx += 1
            sources.append(node_index[pipeline_name])
            targets.append(node_index[df])
            values.append(1)
            colors.append('rgba(221, 160, 221, 0.5)')
        
        if not sources:
            st.warning("⚠️ Could not build dependency graph")
            return
        
        # Create Sankey
        fig = go.Figure(go.Sankey(
            node=dict(
                pad=15,
                thickness=20,
                line=dict(color='white', width=2),
                label=labels,
                color=[
                    PremiumColors.PIPELINE if l == pipeline_name else
                    PremiumColors.TRIGGER if l in triggers else
                    PremiumColors.DATAFLOW if l in dataflows else
                    PremiumColors.INFO
                    for l in labels
                ],
            ),
            link=dict(source=sources, target=targets, value=values, color=colors)
        ))
        
        fig.update_layout(
            title=dict(text=f"Dependencies: {pipeline_name}", font=dict(size=16, color=PremiumColors.PRIMARY)),
            height=400,
            **PREMIUM_CHART_TEMPLATE['layout']
        )
        
        st.plotly_chart(fig, width='stretch')

    # ═══════════════════════════════════════════════════════════════════════
    # ORPHANED RESOURCES TAB - Enhanced
    # ═══════════════════════════════════════════════════════════════════════

    def render_orphaned_resources_tab(self):
        """Render orphaned resources analysis with enhanced UI."""
        st.markdown("### 🗑️ Orphaned Resources")
        st.markdown("*Identify unused resources for cleanup*")
        
        # Get orphaned data
        orphaned_data = {
            'Pipelines': safe_get_dataframe("OrphanedPipelines"),
            'Datasets': safe_get_dataframe("OrphanedDatasets"),
            'LinkedServices': safe_get_dataframe("OrphanedLinkedServices"),
            'Triggers': safe_get_dataframe("OrphanedTriggers"),
        }
        
        # Summary cards
        col1, col2, col3, col4 = st.columns(4)
        
        tiles = [
            (col1, "📦", len(orphaned_data['Pipelines']), "Pipelines", "fire"),
            (col2, "📋", len(orphaned_data['Datasets']), "Datasets", "orange"),
            (col3, "🔗", len(orphaned_data['LinkedServices']), "Services", "pink"),
            (col4, "⏰", len(orphaned_data['Triggers']), "Triggers", "teal"),
        ]
        
        for col, icon, count, label, variant in tiles:
            with col:
                render_premium_tile(icon=icon, value=count, label=f"Orphaned {label}", variant=variant)
        
        st.markdown("---")
        
        # Tabs for each resource type
        tabs = st.tabs(["📦 Pipelines", "📋 Datasets", "🔗 Services", "⏰ Triggers"])
        
        for tab, (resource_type, df) in zip(tabs, orphaned_data.items()):
            with tab:
                if df.empty:
                    st.success(f"✅ No orphaned {resource_type.lower()} found!")
                else:
                    st.markdown(f"#### Found {len(df)} orphaned {resource_type.lower()}")
                    self._safe_dataframe(df, height=400)
                    
                    # Export button
                    csv_bytes = to_csv_bytes(df)
                    st.download_button(
                        label=f"📥 Download {resource_type} CSV",
                        data=csv_bytes,
                        file_name=f"orphaned_{resource_type.lower()}.csv",
                        mime="text/csv",
                        key=f"download_orphaned_{resource_type}"
                    )
        
        # Cleanup recommendations
        st.markdown("---")
        self._render_cleanup_recommendations(orphaned_data)

    def _render_cleanup_recommendations(self, orphaned_data: Dict[str, pd.DataFrame]):
        """Render cleanup recommendations."""
        st.markdown("### 🧹 Cleanup Recommendations")
        
        total = sum(len(df) for df in orphaned_data.values())
        
        if total == 0:
            st.success("🎉 Excellent! No orphaned resources. Your factory is well-maintained!")
        else:
            st.warning(f"⚠️ Found **{total}** orphaned resources")
            
            st.markdown("""
            **Recommended Steps:**
            1. ✅ Review orphaned pipelines - Verify they're truly unused
            2. 🔧 Fix broken trigger references
            3. 🗑️ Clean up unused datasets
            4. 📦 Archive or remove obsolete linked services
            5. 📝 Document before deletion
            
            💡 **Tip:** Start with LOW impact resources first.
            """)

    # ═══════════════════════════════════════════════════════════════════════
    # STATISTICS TAB - Enhanced with Caching
    # ═══════════════════════════════════════════════════════════════════════

    def render_statistics_tab(self):
        """Render statistics dashboard with cached charts."""
        st.markdown("### 📊 Statistics & Analytics")
        
        activity_df = safe_get_dataframe("ActivityCount")
        
        if not activity_df.empty:
            st.markdown("#### ⚡ Activity Distribution")
            
            # Clean data
            activity_df = activity_df[~activity_df['ActivityType'].astype(str).str.contains('TOTAL', na=False)]
            activity_df['Count'] = pd.to_numeric(activity_df['Count'], errors='coerce').fillna(0).astype(int)
            
            col1, col2 = st.columns(2)
            
            with col1:
                self._render_activity_bar_chart(activity_df)
            
            with col2:
                self._render_activity_pie_chart(activity_df)
        
        st.markdown("---")
        
        # Dataset usage
        dataset_usage = safe_get_dataframe("DatasetUsage")
        if not dataset_usage.empty:
            self._render_dataset_usage(dataset_usage)

    def _render_activity_bar_chart(self, activity_df: pd.DataFrame):
        """Render activity bar chart (cached)."""
        top10 = activity_df.nlargest(10, 'Count')
        
        fig = go.Figure(go.Bar(
            y=top10['ActivityType'],
            x=top10['Count'],
            orientation='h',
            marker=dict(
                color=[PremiumColors.get_gradient(f'gradient_{i%8+1}', 0) for i in range(len(top10))],
                line=dict(color='rgba(255,255,255,0.8)', width=1.5)
            ),
            text=top10['Count'],
            textposition='auto',
            hovertemplate="<b>%{y}</b><br>Count: <b>%{x:,}</b><extra></extra>"
        ))
        
        fig.update_layout(
            title=dict(text="🚀 Top 10 Activities", font=dict(size=18, color=PremiumColors.PRIMARY)),
            xaxis_title="Count",
            height=450,
            **PREMIUM_CHART_TEMPLATE['layout']
        )
        
        st.plotly_chart(fig, width='stretch')

    def _render_activity_pie_chart(self, activity_df: pd.DataFrame):
        """Render activity pie chart (cached)."""
        top8 = activity_df.nlargest(8, 'Count')
        
        fig = go.Figure(go.Pie(
            labels=top8['ActivityType'],
            values=top8['Count'],
            hole=0.4,
            marker=dict(
                colors=[PremiumColors.get_gradient(f'gradient_{i%8+1}', 0) for i in range(len(top8))],
                line=dict(color='rgba(255,255,255,0.8)', width=2)
            ),
            textinfo='label+percent',
            hovertemplate="<b>%{label}</b><br>%{value:,} (%{percent})<extra></extra>"
        ))
        
        fig.update_layout(
            title=dict(text="📊 Activity Breakdown", font=dict(size=18, color=PremiumColors.PRIMARY)),
            height=450,
            **PREMIUM_CHART_TEMPLATE['layout']
        )
        
        st.plotly_chart(fig, width='stretch')

    def _render_dataset_usage(self, dataset_df: pd.DataFrame):
        """Render dataset usage statistics."""
        st.markdown("#### 📋 Dataset Usage")
        
        if 'UsageCount' in dataset_df.columns:
            top10 = dataset_df.nlargest(10, 'UsageCount')
            
            fig = go.Figure(go.Bar(
                x=top10['Dataset'],
                y=top10['UsageCount'],
                marker=dict(
                    color=[PremiumColors.get_gradient('gradient_4', i) for i in range(len(top10))],
                    line=dict(color='rgba(255,255,255,0.8)', width=1.5)
                ),
                hovertemplate="<b>%{x}</b><br>Usage: <b>%{y:,}</b><extra></extra>"
            ))
            
            fig.update_layout(
                title=dict(text="📈 Top 10 Used Datasets", font=dict(size=18, color=PremiumColors.PRIMARY)),
                xaxis_title="Dataset",
                yaxis_title="Usage Count",
                height=400,
                xaxis={'tickangle': -45},
                **PREMIUM_CHART_TEMPLATE['layout']
            )
            
            st.plotly_chart(fig, width='stretch')

    # ═══════════════════════════════════════════════════════════════════════
    # DATAFLOW TAB - Enhanced
    # ═══════════════════════════════════════════════════════════════════════

    def render_dataflow_tab(self):
        """Render DataFlow analysis."""
        st.markdown("### 🌊 DataFlow Analysis")
        
        dataflow_df = safe_get_dataframe("DataFlows")
        
        if dataflow_df.empty:
            st.info("📊 No DataFlow data available")
            return
        
        # Overview metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total DataFlows", len(dataflow_df))
        with col2:
            if 'Sources' in dataflow_df.columns:
                if dataflow_df['Sources'].dtype == 'object':
                    all_sources = dataflow_df['Sources'].dropna().str.split(',').explode().str.strip()
                    total_sources = all_sources.nunique()
                else:
                    total_sources = int(dataflow_df['Sources'].sum())
            else:
                total_sources = 0
            st.metric("Total Sources", total_sources)
        
        with col3:
            if 'Sinks' in dataflow_df.columns:
                if dataflow_df['Sinks'].dtype == 'object':
                    all_sinks = dataflow_df['Sinks'].dropna().str.split(',').explode().str.strip()
                    total_sinks = all_sinks.nunique()
                else:
                    total_sinks = int(dataflow_df['Sinks'].sum())
            else:
                total_sinks = 0
            st.metric("Total Sinks", total_sinks)
        
        st.markdown("---")
        
        # DataFlow selector
        if 'DataFlow' in dataflow_df.columns:
            selected = st.selectbox("🌊 Select DataFlow", dataflow_df['DataFlow'].tolist())
            
            if selected:
                df_data = dataflow_df[dataflow_df['DataFlow'] == selected].iloc[0]
                
                st.markdown(f"#### Details: {selected}")
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Type", df_data.get('Type', 'MappingDataFlow'))
                with col2:
                    st.metric("Sources", df_data.get('Sources', 0))
                with col3:
                    st.metric("Transformations", df_data.get('Transformations', 0))

    # ═══════════════════════════════════════════════════════════════════════
    # LINEAGE TAB - Enhanced
    # ═══════════════════════════════════════════════════════════════════════

    def render_lineage_tab(self):
        """Render data lineage visualization."""
        st.markdown("### 🔄 Data Lineage")
        st.markdown("*Track data flow from source to sink*")
        
        lineage_df = safe_get_dataframe("DataLineage")
        
        if lineage_df.empty:
            st.info("📊 No lineage data available")
            return
        
        # Metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Records", len(lineage_df))
        with col2:
            st.metric("Sources", lineage_df.get('Source', pd.Series()).nunique())
        with col3:
            st.metric("Sinks", lineage_df.get('Sink', pd.Series()).nunique())
        with col4:
            st.metric("Pipelines", lineage_df.get('Pipeline', pd.Series()).nunique())
        
        st.markdown("---")
        
        # Filters
        pipeline_filter = "All"
        col1, col2 = st.columns(2)
        
        with col1:
            if 'Pipeline' in lineage_df.columns:
                pipelines = ["All"] + sorted(lineage_df['Pipeline'].unique().tolist())
                pipeline_filter = st.selectbox("🔍 Filter by Pipeline", pipelines)
        
        with col2:
            search = st.text_input("🔎 Search", placeholder="Search source/sink...")
        
        # Apply filters
        filtered = lineage_df.copy()
        
        if pipeline_filter != "All" and 'Pipeline' in filtered.columns:
            filtered = filtered[filtered['Pipeline'] == pipeline_filter]
        
        if search:
            search_lower = search.lower()
            mask = pd.Series(False, index=filtered.index)
            for col in filtered.columns:
                mask |= filtered[col].astype(str).str.lower().str.contains(search_lower, na=False, regex=False)
            filtered = filtered[mask]
        
        # Display
        self._safe_dataframe(filtered, height=400)
        
        # Export
        csv_bytes = to_csv_bytes(filtered)
        st.download_button(
            label="📥 Download Lineage CSV",
            data=csv_bytes,
            file_name="data_lineage.csv",
            mime="text/csv"
        )

    # ═══════════════════════════════════════════════════════════════════════
    # EXPLORER TAB - Enhanced
    # ═══════════════════════════════════════════════════════════════════════

    def render_explorer_tab(self):
        """Render data explorer."""
        st.markdown("### 📁 Data Explorer")
        st.markdown("*Browse and export raw data*")
        
        if not st.session_state.excel_data:
            st.warning("⚠️ No data loaded")
            return
        
        sheet_names = list(st.session_state.excel_data.keys())
        
        col1, col2 = st.columns([1, 3])
        
        with col1:
            st.markdown("#### 📚 Sheets")
            selected = st.selectbox("Select Sheet", sheet_names, label_visibility="collapsed")
        
        with col2:
            if selected:
                df = st.session_state.excel_data.get(selected)
                
                if not isinstance(df, pd.DataFrame):
                    st.warning(f"⚠️ '{selected}' is not a DataFrame")
                    return
                
                st.markdown(f"#### 📄 {selected}")
                
                # Info
                col_info1, col_info2, col_info3 = st.columns(3)
                with col_info1:
                    st.metric("Rows", len(df))
                with col_info2:
                    st.metric("Columns", len(df.columns))
                with col_info3:
                    memory = df.memory_usage(deep=True).sum() / 1024 / 1024
                    st.metric("Memory", f"{memory:.2f} MB")
                
                # Data
                self._safe_dataframe(df, height=400)
                
                # Export
                col_exp1, col_exp2 = st.columns(2)
                
                with col_exp1:
                    csv_bytes = to_csv_bytes(df)
                    st.download_button(
                        label="📥 Download CSV",
                        data=csv_bytes,
                        file_name=f"{selected}.csv",
                        mime="text/csv"
                    )
                
                with col_exp2:
                    json_bytes = to_json_bytes(df.to_dict(orient='records'))
                    st.download_button(
                        label="📥 Download JSON",
                        data=json_bytes,
                        file_name=f"{selected}.json",
                        mime="application/json"
                    )

    # ═══════════════════════════════════════════════════════════════════════
    # EXPORT TAB - Streamlined
    # ═══════════════════════════════════════════════════════════════════════

    def render_export_tab(self):
        """Render export options."""
        st.markdown("### 📤 Export Dashboard")
        st.markdown("*Download analysis data in multiple formats*")
        
        if not st.session_state.excel_data:
            st.warning("⚠️ No data loaded")
            return
        
        sheet_names = list(st.session_state.excel_data.keys())
        
        # Sheet selection
        st.markdown("#### 📋 Select Data")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("✅ Select All", width='stretch'):
                st.session_state['export_multiselect'] = sheet_names
                st.rerun()
        with col2:
            if st.button("❌ Clear All", width='stretch'):
                st.session_state['export_multiselect'] = []
                st.rerun()
        
        selected = st.multiselect(
            "Sheets to Export",
            sheet_names,
            default=sheet_names[:5],
            key='export_multiselect'
        )
        
        if not selected:
            st.info("👆 Select at least one sheet to proceed")
            return
            
        st.markdown(f"**Selected: {len(selected)} sheets**")
        st.markdown("---")
        
        # Export formats
        st.markdown("#### 📦 Export Format")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("##### 📄 CSV Bundle")
            if st.button("📥 Download ZIP", type="primary", width='stretch'):
                self._export_csv_zip(selected)
        
        with col2:
            st.markdown("##### 📗 Excel File")
            if st.button("📥 Download XLSX", type="primary", width='stretch'):
                self._export_excel(selected)
        
        with col3:
            st.markdown("##### 📋 JSON Bundle")
            if st.button("📥 Download JSON", type="primary", width='stretch'):
                self._export_json(selected)

    def _export_csv_zip(self, sheets: List[str]):
        """Export sheets as CSV zip."""
        try:
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
                for sheet in sheets:
                    df = st.session_state.excel_data.get(sheet)
                    if isinstance(df, pd.DataFrame):
                        csv_bytes = to_csv_bytes(df)
                        zf.writestr(f"{sheet}.csv", csv_bytes)
            
            st.download_button(
                label="📥 Click to Download ZIP",
                data=buffer.getvalue(),
                file_name="adf_export.zip",
                mime="application/zip",
                width='stretch'
            )
            st.success(f"✅ Created ZIP with {len(sheets)} files")
        except Exception as e:
            st.error(f"❌ Export failed: {e}")

    def _export_excel(self, sheets: List[str]):
        """Export sheets as Excel workbook."""
        try:
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                for sheet in sheets:
                    df = st.session_state.excel_data.get(sheet)
                    if isinstance(df, pd.DataFrame):
                        df.to_excel(writer, sheet_name=sheet[:31], index=False)
            
            st.download_button(
                label="📥 Click to Download XLSX",
                data=buffer.getvalue(),
                file_name="adf_export.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                width='stretch'
            )
            st.success(f"✅ Created Excel with {len(sheets)} sheets")
        except Exception as e:
            st.error(f"❌ Export failed: {e}")

    def _export_json(self, sheets: List[str]):
        """Export sheets as JSON."""
        try:
            export_data = {}
            for sheet in sheets:
                df = st.session_state.excel_data.get(sheet)
                if isinstance(df, pd.DataFrame):
                    export_data[sheet] = df.to_dict(orient='records')
            
            json_bytes = to_json_bytes(export_data)
            
            st.download_button(
                label="📥 Click to Download JSON",
                data=json_bytes,
                file_name="adf_export.json",
                mime="application/json",
                width='stretch'
            )
            st.success(f"✅ Created JSON with {len(sheets)} sheets")
        except Exception as e:
            st.error(f"❌ Export failed: {e}")

    # ═══════════════════════════════════════════════════════════════════════
    # DOCUMENTATION
    # ═══════════════════════════════════════════════════════════════════════

    def render_comprehensive_documentation(self):
        """Render comprehensive documentation."""
        st.header("📚 Complete Documentation")
        st.markdown("Access all guides and technical references.")
        
        doc_tabs = st.tabs([
            "📊 Tile Reference",
            "🧠 Logic Guide",
            "🐍 Python Files",
            "📖 Project Guide",
            "⚙️ Configuration"
        ])
        
        with doc_tabs[0]:
            self._render_tile_reference()
        with doc_tabs[1]:
            self._render_logic_guide()
        with doc_tabs[2]:
            self._render_python_files_reference()
        with doc_tabs[3]:
            self._render_project_guide()
        with doc_tabs[4]:
            self._render_configuration_guide()

    @st.cache_data(ttl=3600)
    def _load_doc_file(_self, path: str) -> str:
        """Load and cache documentation file."""
        p = Path(path)
        if p.exists():
            return p.read_text(encoding='utf-8')
        return ""

    def _render_tile_reference(self):
        """Render tile reference documentation."""
        st.subheader("📊 Dashboard Tiles Reference")
        tiles_path = str(Path(__file__).parent / "docs" / "TILES.md")
        content = self._load_doc_file(tiles_path)
        if content:
            st.markdown(content)
        else:
            st.warning("📋 TILES.md not found")
            st.markdown("""
                4. **Triggers** - Trigger configurations
                5. **Dependencies** - Total relationships
                6. **Health** - Factory health score (0-100)
                7. **Orphaned** - Unused resources
                
                ### 🔗 Data Sources
                - Primary: Summary sheet
                - Fallback: Sheet row counts
                - Lineage: DataLineage analysis
            """)

    def _render_logic_guide(self):
        """Render logic documentation."""
        st.subheader("🧠 Technical Logic & Algorithms")
        st.markdown("""
        ### 🏥 Health Score Algorithm
        ```python
        if pipelines > 0:
            health_score = int((1 - orphaned / pipelines) * 100)
        else:
            health_score = 100
        ```
        **Status Thresholds:**
        - **90-100:** Excellent ✨
        - **75-89:** Good 🔵
        - **60-74:** Fair ⚠️
        - **<60:** Needs Attention ❌
        
        ### 📊 Quality Score (Excel)
        Starting from 100, deductions:
        1. **Circular Dependencies:** -10 per cycle (max -30)
        2. **Orphaned Resources:** Based on % (max -20)
        3. **Broken Triggers:** -5 per trigger (max -15)
        
        ### 🔄 Dependency Detection
        - **Algorithm:** DFS with back-edge detection
        - **Deduplication:** Canonical cycle representation
        - **Severity:** CRITICAL (production blocker)
        """)

    def _render_python_files_reference(self):
        """Render Python files reference."""
        st.subheader("🐍 Python Files Overview")
        st.markdown("""
        ### 🚀 Core Analysis Engine
        - **`adf_analyzer_v10_complete.py`** - Main analysis engine
        - **`adf_runner_wrapper.py`** - Production wrapper
        - **`adf_analyzer_v10_patched_runner.py`** - Orchestrator
        
        ### 🎨 Enhancement Layer
        - **`adf_analyzer_v10_excel_enhancements.py`** - Excel beautification
        - **`adf_analyzer_v10_patch.py`** - Functional patches
        
        ### 📊 Dashboard & UI
        - **`app.py`** - Main Streamlit dashboard (THIS FILE)
        
        ### 🔧 Utilities
        - **`scripts/setup_environment.py`** - Environment setup
        - **`scripts/run_analysis.py`** - Direct execution
        - **`test_metrics.py`** - Testing suite
        """)

    def _render_project_guide(self):
        """Render project guide."""
        st.subheader("📖 Complete Project Guide")
        st.markdown("""
        # 🚀 ADF Analyzer v10.1 - Enhanced Edition
        
        ## ⚡ Quick Start
        ```bash
        # Generate Excel (recommended)
        python adf_runner_wrapper.py your_template.json
        
        # Launch Dashboard
        streamlit run app.py
        ```
        
        ## 🎯 Key Features
        - **Comprehensive Analysis** - ARM template parsing
        - **Impact Analysis** - Health scoring, orphan detection
        - **Enhanced Reporting** - Professional Excel with charts
        - **Interactive Dashboard** - Real-time analytics
        """)

    def _render_configuration_guide(self):
        """Render configuration guide."""
        st.subheader("⚙️ Configuration Guide")
        st.markdown("""
        ### 🎨 Enhancement Configuration
        **Core Features:**
        - `core_formatting` - Column sizing, borders, colors
        - `conditional_formatting` - Data bars, color scales
        - `hyperlinks` - Navigation between sheets
        
        ### 💡 Configuration Methods
        **Via Dashboard (Recommended):**
        1. Go to Generate Excel tab
        2. Use Enhancement Configuration section
        3. Toggle features with checkboxes
        4. Click "Save Config"
        """)

    # ═══════════════════════════════════════════════════════════════════════
    # UPLOAD INTERFACE
    # ═══════════════════════════════════════════════════════════════════════

    def render_upload_interface(self):
        """Render upload interface for existing Excel files."""
        st.markdown("## 📊 Upload Excel for Analysis")
        st.markdown("Upload an existing ADF analysis Excel file to view dashboard.")
        
        uploaded_file = st.file_uploader(
            "Choose Excel File",
            type=["xlsx", "xls"],
            help="Upload adf_analysis_latest.xlsx",
            key="main_upload"
        )
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if uploaded_file and st.button("📤 Load Excel", type="primary", width='stretch'):
                self.load_excel_file(uploaded_file)
                st.rerun()
        
        with col2:
            if st.button("🎮 Load Sample", width='stretch'):
                self.load_sample_data()
                st.rerun()
        
        with col3:
            pass  # Spacing
        
        st.markdown("---")
        st.markdown("""
        ### 💡 Upload Tips
        
        **Supported Files:**
        - `adf_analysis_latest.xlsx` from ADF Analyzer v9.1+
        - Excel files with standard analysis sheets
        
        **After Upload:**
        - Enhanced metrics tiles appear
        - Interactive dashboard tabs activate
        - Network graphs, impact analysis available
        
        **Sample Data:**
        - Try "Load Sample" for demo experience
        - Perfect for testing features
        """)


# ═══════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT - Enhanced with Error Boundaries
# ═══════════════════════════════════════════════════════════════════════════

def main():
    """
    Main application entry point with error boundaries.
    
    ✅ ENHANCED: Proper error handling and recovery.
    """
    try:
        # Create and run dashboard
        dashboard = ADF_Dashboard()
        dashboard.run()
    
    except Exception as e:
        st.error(f"❌ Application Error: {e}")
        
        with st.expander("🔍 Debug Information"):
            st.code(traceback.format_exc())
        
        st.markdown("---")
        st.markdown("""
        ### 🔧 Troubleshooting
        
        **Common Issues:**
        1. **File Upload Error** - Ensure Excel is from ADF Analyzer v9.1+
        2. **Missing Sheets** - Check all required sheets exist
        3. **Memory Error** - Try smaller dataset
        4. **Display Issues** - Refresh page (F5)
        
        **Quick Fixes:**
        - Clear browser cache
        - Re-upload file
        - Try sample data
        
        **Dependencies:**
        ```bash
        pip install streamlit pandas plotly networkx openpyxl
        ```
        """)


if __name__ == "__main__":
    main()

