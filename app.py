import streamlit as st
from pathlib import Path
from src.ingestion.ingest import ingest_files
from src.utils.config_loader import Config

# Load config
config = Config.load("config/settings.yaml")
INBOX = config["paths"]["inbox"]
PROCESSED = config["paths"]["processed"]
FAILED = config["paths"]["failed"]

st.title("ETL Pipeline - File Ingestion Dashboard")

def list_files_with_status(folder_path: str, status: str):
    files = []
    for f in Path(folder_path).glob("*.*"):
        files.append({"file": f.name, "status": status, "path": str(f)})
    return files

# Show files in inbox
st.subheader("Files in Inbox")
inbox_files = list_files_with_status(INBOX, "new")
if inbox_files:
    st.table([f["file"] for f in inbox_files])
else:
    st.info("Inbox is empty.")

# Show processed/failed files
st.subheader("Processed & Failed Files")
processed_files = list_files_with_status(PROCESSED, "processed")
failed_files = list_files_with_status(FAILED, "failed")

all_status_files = processed_files + failed_files
for f in all_status_files:
    status_color = "green" if f["status"] == "processed" else "red"
    st.markdown(f"**{f['file']}** → <span style='color:{status_color}'>{f['status']}</span>", unsafe_allow_html=True)
    if f["status"] == "failed":
        st.button(f"Reprocess {f['file']}", key=f['file'], on_click=lambda path=f['path']: reprocess_file(path))

# Button to process all inbox files
if st.button("Process Inbox"):
    statuses = ingest_files(INBOX)
    st.success("Ingestion completed!")
    for s in statuses:
        if s.get("errors"):
            st.error(f"{s['file']} → {s['status']} | Errors: {s['errors']}")
        else:
            st.success(f"{s['file']} → {s['status']} | Rows: {s['rows']}")

# Reprocessing logic
def reprocess_file(file_path: str):
    """
    Moves file back to inbox and triggers ingestion
    """
    import shutil
    import os
    dest = os.path.join(INBOX, os.path.basename(file_path))
    shutil.move(file_path, dest)
    st.info(f"Moved {os.path.basename(file_path)} back to inbox for reprocessing.")
    # Optional: trigger ingestion immediately
    # statuses = ingest_files(INBOX)
