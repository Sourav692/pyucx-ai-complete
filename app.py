import streamlit as st
import pandas as pd
import os
import tempfile
import zipfile
import re
import hashlib
from typing import List, Dict
try:
    from pyvis.network import Network
except Exception:  # fallback if render deps missing
    Network = None  # type: ignore


def _extract_lineage_from_sql(sql: str) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    op_re = re.compile(r"\b(INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+([`\[\]\"\w\.]+)", re.IGNORECASE)
    for m in op_re.finditer(sql):
        op = m.group(1).upper()
        table = m.group(2).strip().strip('`"[]')
        results.append({"operation": op, "table": table})
    return results


def extract_lineage_from_py(py_path: str) -> List[Dict[str, str]]:
    try:
        with open(py_path, "r", encoding="utf-8", errors="ignore") as f:
            code = f.read()
    except Exception:
        return []

    # Find cursor.execute(<string>) patterns and inspect SQL
    lineage: List[Dict[str, str]] = []
    exec_re = re.compile(
        r"execute\s*\(\s*([frubFRUB]*)\s*(\"\"\"|'''|\"|')([\s\S]*?)(\2)",
        re.IGNORECASE,
    )
    for m in exec_re.finditer(code):
        sql = m.group(3)
        for item in _extract_lineage_from_sql(sql):
            lineage.append({"file": py_path, **item, "preview": sql[:120].replace("\n", " ")})
    return lineage


def gather_py_files(root_dir: str) -> List[str]:
    py_files: List[str] = []
    for r, _, files in os.walk(root_dir):
        for f in files:
            if f.endswith('.py'):
                py_files.append(os.path.join(r, f))
    return py_files


def render_lineage(title: str, lineage_rows: List[Dict[str, str]]):
    st.write(f"Found {len(lineage_rows)} write statements")
    if lineage_rows:
        st.subheader(title)
        try:
            df = pd.DataFrame(lineage_rows)[["file", "operation", "table", "preview"]]
            df = df.sort_values(["file", "table", "operation"]).reset_index(drop=True)
            st.dataframe(df, use_container_width=True)
        except Exception as e:
            st.error(f"Dataframe render failed: {e}")
            st.write(lineage_rows)
        with st.expander("Show raw JSON"):
            st.json(lineage_rows)
        with st.expander("Show as list"):
            for row in lineage_rows:
                st.markdown(f"- `{row.get('file','')}` → **{row.get('operation','')}** `{row.get('table','')}`")
    else:
        st.info("No INSERT/UPDATE/DELETE statements found via cursor.execute().")


def extract_lineage_from_text(code: str, source_name: str) -> List[Dict[str, str]]:
    lineage: List[Dict[str, str]] = []
    exec_re = re.compile(
        r"execute\s*\(\s*([frubFRUB]*)\s*(\"\"\"|'''|\"|')([\s\S]*?)(\2)",
        re.IGNORECASE,
    )
    for m in exec_re.finditer(code):
        sql = m.group(3)
        for item in _extract_lineage_from_sql(sql):
            lineage.append({"file": source_name, **item, "preview": sql[:120].replace("\n", " ")})
    return lineage


def render_lineage_graph(lineage_rows: List[Dict[str, str]]):
    if not lineage_rows or Network is None:
        return
    net = Network(height="500px", width="100%", directed=True, notebook=False)
    net.barnes_hut()

    files = sorted({row['file'] for row in lineage_rows})
    tables = sorted({row['table'] for row in lineage_rows})

    for f in files:
        net.add_node(f, label=f, shape='box', color='#e5e7eb')
    for t in tables:
        net.add_node(t, label=t, shape='ellipse', color='#ffedd5')

    op_colors = {
        'INSERT INTO': '#10b981',
        'UPDATE': '#f59e0b',
        'DELETE FROM': '#ef4444',
    }

    for row in lineage_rows:
        f = row['file']
        t = row['table']
        op = row['operation']
        color = op_colors.get(op, '#6366f1')
        net.add_edge(f, t, label=op, color=color)

    try:
        with tempfile.TemporaryDirectory() as tmp:
            html_path = os.path.join(tmp, 'lineage.html')
            net.write_html(html_path, notebook=False)
            with open(html_path, 'r', encoding='utf-8') as fh:
                html = fh.read()
            # Append a content hash comment to hint rerender on data change
            h = hashlib.md5(html.encode('utf-8')).hexdigest()
            html += f"\n<!-- hash:{h} -->\n"
            st.components.v1.html(html, height=520, scrolling=False)
    except Exception as e:
        st.warning(f"Graph render unavailable: {e}")


def render_filters_and_outputs(lineage_rows: List[Dict[str, str]]):
    if not lineage_rows:
        st.info("No lineage to display.")
        return
    files = sorted({row['file'] for row in lineage_rows})
    tables = sorted({row['table'] for row in lineage_rows})
    c1, c2 = st.columns(2)
    with c1:
        file_opt = ["All"] + files
        sel_file = st.selectbox("Filename", file_opt, index=0, key="filter_filename")
    with c2:
        table_opt = ["All"] + tables
        sel_table = st.selectbox("Table name", table_opt, index=0, key="filter_tablename")

    def _match(val: str, sel: str) -> bool:
        return sel == "All" or val == sel

    filtered = [r for r in lineage_rows if _match(r['file'], sel_file) and _match(r['table'], sel_table)]
    render_lineage("Lineage (write operations)", filtered)
    render_lineage_graph(filtered)

st.set_page_config(layout="wide")

st.header("UC-FY")

st.subheader("Choose input")
mode = st.radio(
    "Input mode",
    options=["Folder path", "Upload folder (.zip)", "Upload files"],
    horizontal=True,
)

folder_path: str = ""
zip_file = None
uploaded_files = None

if mode == "Folder path":
    folder_path = st.text_input(
        "Folder path (local or DBFS)",
        value="",
        placeholder="e.g., /Users/you/project or /dbfs/path",
        help="Type a local folder path or DBFS path."
    )
elif mode == "Upload folder (.zip)":
    zip_file = st.file_uploader("Upload a zipped folder", type=["zip"], accept_multiple_files=False)
else:
    uploaded_files = st.file_uploader("Upload files (multiple)", accept_multiple_files=True)

if mode == "Folder path" and folder_path:
    try:
        entries = os.listdir(folder_path)
        st.success(f"Found {len(entries)} items in {folder_path}")
        st.write(entries[:50])
    except Exception as e:
        st.error(f"Cannot read folder: {e}")
elif mode == "Upload folder (.zip)" and zip_file is not None:
    st.info(f"Selected archive: {zip_file.name} ({zip_file.size} bytes)")
elif mode == "Upload files" and uploaded_files:
    st.success(f"{len(uploaded_files)} files uploaded")
    st.write([f.name for f in uploaded_files][:50])
else:
    st.info("Provide a folder path, a zipped folder, or upload files to begin.")

st.divider()

col_a, col_b = st.columns(2)
with col_a:
    submit = st.button("Submit")
with col_b:
    explore = st.button("Explore lineage")

trigger = submit or explore
if trigger:
    if mode == "Folder path" and folder_path:
        try:
            py_files = gather_py_files(folder_path)
            lineage_rows: List[Dict[str, str]] = []
            for p in py_files:
                lineage_rows.extend(extract_lineage_from_py(p))

            st.success(f"Scanned {len(py_files)} Python files in {folder_path}")
            st.session_state['lineage_rows'] = lineage_rows
        except Exception as e:
            st.error(f"Cannot scan folder: {e}")
    elif mode == "Upload folder (.zip)" and zip_file is not None:
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                zpath = os.path.join(tmpdir, zip_file.name)
                with open(zpath, 'wb') as f:
                    f.write(zip_file.getbuffer())
                with zipfile.ZipFile(zpath, 'r') as zf:
                    zf.extractall(tmpdir)

                py_files = gather_py_files(tmpdir)
                lineage_rows: List[Dict[str, str]] = []
                for p in py_files:
                    lineage_rows.extend(extract_lineage_from_py(p))

                st.success(f"Scanned {len(py_files)} Python files in uploaded folder")
                st.session_state['lineage_rows'] = lineage_rows
        except Exception as e:
            st.error(f"Cannot process uploaded archive: {e}")
    elif mode == "Upload files" and uploaded_files:
        try:
            lineage_rows: List[Dict[str, str]] = []
            for f in uploaded_files:
                try:
                    code = f.getvalue().decode('utf-8', errors='ignore')
                except Exception:
                    code = ""
                lineage_rows.extend(extract_lineage_from_text(code, f.name))
            st.success(f"{len(uploaded_files)} files selected")
            st.session_state['lineage_rows'] = lineage_rows
        except Exception as e:
            st.error(f"Cannot process uploaded files: {e}")
    else:
        st.warning("Please provide input before submitting.")

# Always show filters and outputs if we have results from a previous run
if 'lineage_rows' in st.session_state and st.session_state['lineage_rows']:
    st.divider()
    render_filters_and_outputs(st.session_state['lineage_rows'])

