#!/usr/bin/env python3
"""
browse_app.py — Streamlit read-only table viewer for custom_table() exports.

Launch via hdb.custom_table(), or manually:
    streamlit run /Users/pickettj/Projects/database/browse_app.py
"""

import streamlit as st
import pandas as pd
import os
import glob
import json

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Eurasia DB Browser",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Session state defaults ────────────────────────────────────────────────────
if 'earth_tones' not in st.session_state:
    st.session_state.earth_tones = False

# ── CSS ───────────────────────────────────────────────────────────────────────
def inject_css(earth_tones: bool):
    if earth_tones:
        css = """
        <style>
            .stApp {
                background-color: #E1D0B3;
                color: #2a1f1f;
            }
            section[data-testid="stSidebar"] {
                background-color: #d4c09e;
                border-right: 1px solid #A18D6D;
            }
            h1, h2, h3 {
                color: #703B3B;
                font-weight: 600;
            }
            html, body, [class*="css"] { font-size: 15px; }

            div[data-testid="metric-container"] {
                background-color: #c8b08a;
                border: 1px solid #A18D6D;
                border-radius: 3px;
                padding: 6px 12px;
            }
            .stDownloadButton > button {
                background-color: #703B3B;
                color: #ffffff;
                border: none;
                border-radius: 3px;
            }
            .stDownloadButton > button:hover {
                background-color: #A18D6D;
                color: #ffffff;
            }
            span[data-baseweb="tag"] {
                background-color: #9BB4C0 !important;
                color: #2a1f1f !important;
            }
            span[data-baseweb="tag"] span { color: #2a1f1f !important; }
            div[data-baseweb="select"] > div {
                border-color: #A18D6D !important;
                background-color: #EDE0C8 !important;
            }
            .streamlit-expanderHeader {
                font-weight: 600;
                color: #703B3B;
            }
            section[data-testid="stSidebar"] * { color: #2a1f1f; }
            div[data-testid="stSidebar"] .stButton > button {
                background-color: #A18D6D;
                color: #ffffff;
                border: none;
                border-radius: 3px;
                width: 100%;
            }
            div[data-testid="stSidebar"] .stButton > button:hover {
                background-color: #703B3B;
                color: #ffffff;
            }
            div[data-testid="stDataFrame"] {
                border: 1px solid #A18D6D;
                border-radius: 3px;
            }
            /* Sage green panel for column summary expander */
            div[data-testid="stExpander"] {
                background-color: #D4E3C8;
                border: 1px solid #A18D6D;
                border-radius: 3px;
            }
        </style>
        """
    else:
        css = """
        <style>
            .stApp { background-color: #ffffff; color: #111111; }
            section[data-testid="stSidebar"] {
                background-color: #f4f4f4;
                border-right: 1px solid #cccccc;
            }
            h1, h2, h3 { color: #111111; font-weight: 600; }
            html, body, [class*="css"] { font-size: 15px; }
            div[data-testid="metric-container"] {
                background-color: #f8f8f8;
                border: 1px solid #dddddd;
                border-radius: 3px;
                padding: 6px 12px;
            }
            .stDownloadButton > button {
                background-color: #111111;
                color: #ffffff;
                border: none;
                border-radius: 3px;
            }
            .stDownloadButton > button:hover {
                background-color: #333333;
                color: #ffffff;
            }
            span[data-baseweb="tag"] {
                background-color: #444444 !important;
                color: #ffffff !important;
            }
            span[data-baseweb="tag"] span { color: #ffffff !important; }
            .streamlit-expanderHeader { font-weight: 600; color: #111; }
            div[data-testid="stSidebar"] .stButton > button {
                background-color: #444444;
                color: #ffffff;
                border: none;
                border-radius: 3px;
                width: 100%;
            }
            div[data-testid="stSidebar"] .stButton > button:hover {
                background-color: #222222;
                color: #ffffff;
            }
        </style>
        """
    st.markdown(css, unsafe_allow_html=True)


inject_css(st.session_state.earth_tones)

# ── Exports directory ─────────────────────────────────────────────────────────
hdir        = os.path.expanduser('~')
EXPORTS_DIR = os.path.join(
    hdir,
    'Dropbox/Active_Directories/Digital_Humanities/Datasets/custom_table_exports'
)

# ── Helpers ───────────────────────────────────────────────────────────────────
def get_export_files():
    if not os.path.exists(EXPORTS_DIR):
        return []
    files = glob.glob(os.path.join(EXPORTS_DIR, "*.parquet"))
    files.sort(key=os.path.getmtime, reverse=True)
    return files


def load_metadata(parquet_path):
    meta_path = parquet_path.replace('.parquet', '.json')
    if os.path.exists(meta_path):
        with open(meta_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def display_name(path):
    return os.path.basename(path).replace('.parquet', '')


# Columns likely to contain Arabic/Persian script → get wider display
ARABIC_PATTERNS = [
    '_Arabic', '_Emic', 'Term', 'Honorific',
    'Full_Name_Arabic', 'Title_Arabic', 'Author_Arabic',
    'Location_Name_Arabic', 'Role_Emic', 'Name_Emic'
]

# Columns that are short-value (Type, Language, dates) → medium
MEDIUM_PATTERNS = [
    'Type', 'Language', 'Status', 'Date_', 'Century',
    'Birthdate', 'Deathdate', 'Acronym', 'Tags'
]

# Columns that are always narrow (IDs, UID)
NARROW_PATTERNS = ['UID', '_ID']


def classify_column_width(col: str) -> str:
    """Return 'small', 'medium', or 'large' for a column name."""
    if col == 'UID' or any(col.endswith(p) for p in ['_ID']):
        return 'small'
    if any(p in col for p in ARABIC_PATTERNS):
        return 'large'
    if any(p in col for p in ['Title', 'Gloss', 'Notes', 'Definition',
                               'Description', 'Translation', 'Purpose']):
        return 'large'
    if any(p in col for p in MEDIUM_PATTERNS):
        return 'medium'
    return 'medium'


def build_column_config(columns):
    """Build st.column_config dict with smart width defaults."""
    config = {}
    for col in columns:
        width = classify_column_width(col)
        if col == 'UID' or col.endswith('_ID'):
            config[col] = st.column_config.NumberColumn(
                col, width='small', format="%d"
            )
        else:
            config[col] = st.column_config.TextColumn(col, width=width)
    return config


def style_rows(df: pd.DataFrame, earth_tones: bool) -> pd.DataFrame:
    """
    Apply alternating row background colors for earth tones theme.
    Returns a pandas Styler. Two very close near-white sand tones
    so the alternation is subtle but readable.

    For B&W theme, returns the plain DataFrame (no styling overhead).
    """
    if not earth_tones:
        return df

    row_colors = ['#F8F2E8', '#EDE4D4']  # near-white sand pair

    def _alternate(row):
        color = row_colors[row.name % 2]
        return [f'background-color: {color}'] * len(row)

    return df.style.apply(_alternate, axis=1)


# ── Main ──────────────────────────────────────────────────────────────────────
st.title("Eurasia Database Browser")

available = get_export_files()

if not available:
    st.warning(f"No exports found in:\n`{EXPORTS_DIR}`")
    st.info("Run `hdb.custom_table()` in your Python session to generate an export.")
    st.stop()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:

    # Theme toggle — top of sidebar
    theme_label = "Switch to Earth Tones" if not st.session_state.earth_tones \
                  else "Switch to Black & White"
    if st.button(theme_label):
        st.session_state.earth_tones = not st.session_state.earth_tones
        st.rerun()

    st.markdown("---")
    st.header("Export File")

    labels       = [display_name(f) for f in available]
    chosen_label = st.selectbox(
        "Select export",
        labels,
        index=0,
        help="Sorted newest first."
    )
    chosen_path = available[labels.index(chosen_label)]

    try:
        df   = pd.read_parquet(chosen_path)
        meta = load_metadata(chosen_path)
    except Exception as e:
        st.error(f"Failed to load file: {e}")
        st.stop()

    # Apply display labels from metadata
    col_labels = meta.get('column_labels', {})
    if col_labels:
        df = df.rename(columns=col_labels)

    st.markdown("---")
    st.markdown(f"**Table:** `{meta.get('table', '—')}`")
    st.markdown(f"**Generated:** {meta.get('timestamp', '—')}")
    st.markdown(f"**Rows:** {len(df):,}")
    st.markdown(f"**Columns:** {len(df.columns)}")

    if meta.get('filters'):
        st.markdown("**Filters applied:**")
        for filt in meta['filters']:
            st.caption(f"• {filt}")

    # ── Column visibility ─────────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("Columns")

    all_cols = list(df.columns)

    visible_cols = st.multiselect(
        "Show / hide columns",
        all_cols,
        default=all_cols,
        help="Deselect to hide. Re-add in any order to reorder."
    )

    # ── Column reorder ────────────────────────────────────────────────────────
    if visible_cols and len(visible_cols) > 1:
        with st.expander("Reorder columns", expanded=False):
            for i, col in enumerate(visible_cols, 1):
                st.caption(f"{i}. {col}")

            new_order_input = st.text_input(
                "Promote columns to front (space-separated numbers)",
                value="",
                placeholder="e.g. 3 1  →  puts col 3 first, col 1 second, rest follow",
                help="Only list the columns you want moved. The rest stay in original order after them."
            )

            if new_order_input.strip():
                try:
                    promoted_indices = []
                    seen = set()
                    valid = True
                    for x in new_order_input.split():
                        idx = int(x) - 1
                        if not (0 <= idx < len(visible_cols)):
                            st.caption(f"⚠️ {x} is out of range (1–{len(visible_cols)})")
                            valid = False
                            break
                        if idx in seen:
                            st.caption(f"⚠️ Column {x} listed more than once")
                            valid = False
                            break
                        promoted_indices.append(idx)
                        seen.add(idx)

                    if valid and promoted_indices:
                        # Remaining columns in original order, excluding promoted ones
                        remaining = [i for i in range(len(visible_cols))
                                     if i not in seen]
                        final_indices = promoted_indices + remaining
                        visible_cols  = [visible_cols[i] for i in final_indices]
                        st.caption("✅ " + " → ".join(
                            str(i + 1) for i in final_indices
                        ))
                except ValueError:
                    st.caption("⚠️ Numbers only, space-separated")

                    
# ── Display ───────────────────────────────────────────────────────────────────
df_view = df[visible_cols] if visible_cols else df

m1, m2, m3 = st.columns([2, 2, 3])
with m1:
    st.metric("Rows", f"{len(df_view):,}")
with m2:
    st.metric("Columns shown", len(df_view.columns))
with m3:
    csv_bytes = df_view.to_csv(index=False).encode('utf-8-sig')
    st.download_button(
        label="Download as CSV",
        data=csv_bytes,
        file_name=f"{chosen_label}.csv",
        mime="text/csv"
    )

st.markdown("---")

# Build column config with smart widths
col_config = build_column_config(list(df_view.columns))

# Apply row styling for earth tones, plain df for B&W
display_data = style_rows(df_view, st.session_state.earth_tones)

st.dataframe(
    display_data,
    use_container_width=True,
    height=620,
    column_config=col_config,
    hide_index=True
)

# ── Column summary (sage green panel in earth tones via CSS) ──────────────────
with st.expander("Column summary"):
    n_cols    = len(df_view.columns)
    grid_cols = st.columns(min(n_cols, 3))

    for i, col in enumerate(df_view.columns):
        with grid_cols[i % 3]:
            n_unique = df_view[col].nunique()
            n_null   = df_view[col].isna().sum()
            st.markdown(f"**{col}**")
            st.caption(f"{n_unique} unique · {n_null} null")

            if df_view[col].dtype == object and n_unique <= 25:
                top = df_view[col].value_counts().head(5)
                for val, count in top.items():
                    if val is not None:
                        label = str(val)
                        if len(label) > 35:
                            label = label[:35] + "…"
                        st.caption(f"  {label}: {count}")