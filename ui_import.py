"""
Streamlit UI components for guided CSV imports with account inference.
"""

from __future__ import annotations

import logging
from io import BytesIO
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st

from account_management import AccountManager
from config_manager import get_app_connection_string, load_config
from data_ingestion import preview_csv
from database_ops import Account, AccountType, DatabaseManager
from enhanced_import import EnhancedImporter
from utils import IngestionError

logger = logging.getLogger(__name__)

ACCOUNT_TYPE_OPTIONS: List[str] = [acct_type.value for acct_type in AccountType]


def _format_account_type(value: str) -> str:
    """Return a human-friendly label for an account type value."""
    try:
        acct_type = AccountType(value)
        return acct_type.name.title()
    except ValueError:
        return value.title()


def _build_account_select_options(accounts: List[Account]) -> List[str]:
    """Convert account objects into selectbox-ready options."""
    return [account.name for account in accounts]


def _initialize_session_defaults(spec_key: str, defaults: Dict[str, object]) -> None:
    """Ensure session state contains default values for interactive widgets."""
    for suffix, default_value in defaults.items():
        state_key = f"{spec_key}_{suffix}"
        if state_key not in st.session_state:
            st.session_state[state_key] = default_value


def launch_import_tab() -> None:
    """Render the guided Import Data workflow inside the analytics dashboard."""
    st.header("📥 Import Data")
    st.write(
        "Upload one or more CSV files, verify the inferred account details, "
        "and import transactions in bulk with duplicate detection and transfer handling."
    )
    
    debug_default = st.session_state.get("import_debug_enabled", False)
    debug_enabled = st.checkbox(
        "Show debug details (inferences, raw previews, and status messages)",
        value=debug_default,
        help="Useful when a file will not import as expected.",
    )
    st.session_state["import_debug_enabled"] = debug_enabled
    
    if "import_result" in st.session_state:
        result = st.session_state["import_result"]
        status_icon = "✅" if result.get("success") else "⚠️"
        st.markdown(f"### {status_icon} Latest Import Summary")
        totals = result.get("totals", {})
        st.write(
            f"- Imported: **{totals.get('imported', 0)}** "
            f"- Duplicates: **{totals.get('duplicates', 0)}** "
            f"- Skipped: **{totals.get('skipped', 0)}** "
            f"- Errors: **{totals.get('errors', 0)}**"
        )
        with st.expander("View detailed results"):
            for detail in result.get("details", []):
                prefix = "✅" if detail.get("success") else "⚠️"
                st.write(f"{prefix} **{detail.get('filename')}** – {detail.get('message', 'No message')}")
                if detail.get("warnings"):
                    for warning in detail["warnings"]:
                        st.warning(warning)
        if st.button("Clear Summary"):
            st.session_state.pop("import_result")
            st.experimental_rerun()
    
    uploaded_files = st.file_uploader(
        "Select CSV files to import",
        type=["csv"],
        accept_multiple_files=True,
        help="Each file should represent a single financial account."
    )
    
    if not uploaded_files:
        st.info("Upload CSV files to begin the guided import process.")
        st.session_state.pop("import_specs", None)
        return
    
    config = load_config()
    
    try:
        connection_string = get_app_connection_string()
        db_manager = DatabaseManager(connection_string)
        account_manager = AccountManager(db_manager)
        importer = EnhancedImporter(db_manager, account_manager)
    except Exception as exc:  # pragma: no cover - defensive
        st.error(f"Unable to initialize database connection: {exc}")
        logger.exception("Failed to initialize import dependencies")
        return
    
    try:
        existing_accounts = account_manager.list_accounts()
        existing_names = _build_account_select_options(existing_accounts)
        name_to_id = {acc.name: acc.id for acc in existing_accounts}
        
        specs: Dict[str, Dict[str, object]] = st.session_state.setdefault("import_specs", {})
        current_keys = set()
        
        for uploaded in uploaded_files:
            file_key = f"{uploaded.name}_{uploaded.size}"
            current_keys.add(file_key)
            if file_key in specs:
                continue
            
            file_bytes = uploaded.getvalue()
            buffer = BytesIO(file_bytes)
            preview = pd.DataFrame()
            error_message: Optional[str] = None
            spec_debug: Dict[str, object] = {}
            try:
                preview = preview_csv(buffer)
                spec_debug["preview_rows"] = len(preview)
                spec_debug["preview_columns"] = preview.columns.tolist()
            except IngestionError as exc:
                logger.error("Preview failed for %s: %s", uploaded.name, exc)
                error_message = str(exc)
                spec_debug["preview_error"] = error_message
            finally:
                buffer.seek(0)
            
            headers = preview.columns.tolist() if not preview.empty else None
            sample_rows = preview.to_dict("records") if not preview.empty else None
            suggestions = account_manager.get_account_suggestions(
                uploaded.name,
                sample_headers=headers,
                sample_rows=sample_rows
            )
            default_selection = suggestions[0] if suggestions else "Create New Account"
            spec_debug["suggestions"] = suggestions
            
            specs[file_key] = {
                "filename": uploaded.name,
                "bytes": file_bytes,
                "preview": preview,
                "error": error_message,
                "suggestions": suggestions,
                "debug": spec_debug,
            }
            _initialize_session_defaults(file_key, {
                "skip": False,
                "account_choice": default_selection,
                "new_account_name": default_selection if default_selection not in existing_names else uploaded.name.replace(".csv", ""),
                "account_type": AccountType.BANK.value,
                "initial_balance": 0.0,
            })
        
        stale_keys = [key for key in specs.keys() if key not in current_keys]
        for stale_key in stale_keys:
            specs.pop(stale_key, None)
        
        st.markdown("### Review & Configure Imports")
        for spec_key, spec in specs.items():
            filename = spec["filename"]
            preview_df: pd.DataFrame = spec["preview"]
            error_message = spec.get("error")
            suggestions: List[str] = spec.get("suggestions", [])  # type: ignore[assignment]
            
            st.markdown(f"#### 📄 {filename}")
            columns = st.columns([1, 2, 1])
            with columns[0]:
                skip = st.checkbox(
                    "Skip file",
                    key=f"{spec_key}_skip",
                    help="Skip importing this file during the batch operation."
                )
            
            account_options = ["Create New Account"] + existing_names
            default_option = st.session_state.get(f"{spec_key}_account_choice", account_options[0])
            if default_option not in account_options:
                account_options.insert(1, default_option)
            
            with columns[1]:
                selected_account = st.selectbox(
                    "Attach to account",
                    options=account_options,
                    index=account_options.index(default_option) if default_option in account_options else 0,
                    key=f"{spec_key}_account_choice",
                    help="Select an existing account or create a new one."
                )
            
            with columns[2]:
                st.caption("Inferred suggestions:")
                for suggestion in suggestions:
                    st.write(f"- {suggestion}")
                if debug_enabled:
                    st.caption("Raw debug info:")
                    st.json(spec.get("debug", {}))
            
            if selected_account == "Create New Account":
                with st.expander("Create New Account Details", expanded=True):
                    st.text_input(
                        "Account name",
                        key=f"{spec_key}_new_account_name",
                        help="Provide a descriptive name for this account."
                    )
                    st.selectbox(
                        "Account type",
                        options=ACCOUNT_TYPE_OPTIONS,
                        key=f"{spec_key}_account_type",
                        format_func=_format_account_type,
                        help="Select the account type to classify these transactions."
                    )
                    st.number_input(
                        "Initial balance (optional)",
                        min_value=-1_000_000.0,
                        max_value=1_000_000.0,
                        value=st.session_state.get(f"{spec_key}_initial_balance", 0.0),
                        key=f"{spec_key}_initial_balance",
                        help="Optional starting balance applied at the earliest transaction date."
                    )
            else:
                st.info(f"Transactions will be linked to **{selected_account}**.")
            
            if error_message:
                st.error(f"Unable to preview this file: {error_message}")
            elif preview_df.empty:
                st.warning("No rows detected in preview. Ensure the CSV contains data before importing.")
            else:
                st.caption("Preview (first 5 rows)")
                if debug_enabled:
                    st.data_editor(
                        preview_df.head(5),
                        use_container_width=True,
                        disabled=True,
                        key=f"{spec_key}_preview_editor",
                    )
                else:
                    st.dataframe(preview_df.head(5), use_container_width=True)
    
        if st.button("Import Selected Files", type="primary"):
            files_to_import: List[Dict[str, object]] = []
            validation_errors: List[str] = []
            
            for spec_key, spec in specs.items():
                if st.session_state.get(f"{spec_key}_skip", False):
                    continue
                
                selected_account = st.session_state.get(f"{spec_key}_account_choice", "Create New Account")
                file_bytes = spec["bytes"]
                entry: Dict[str, object] = {
                    "file_obj": BytesIO(file_bytes),
                    "filename": spec["filename"],
                    "debug": spec.get("debug", {}),
                }
                
                if selected_account == "Create New Account":
                    new_name = st.session_state.get(f"{spec_key}_new_account_name", "").strip()
                    if not new_name:
                        validation_errors.append(f"{spec['filename']}: Account name is required for new accounts.")
                        continue
                    account_type_value = st.session_state.get(f"{spec_key}_account_type", AccountType.BANK.value)
                    entry["new_account"] = {
                        "name": new_name,
                        "type": account_type_value,
                        "initial_balance": st.session_state.get(f"{spec_key}_initial_balance", 0.0),
                    }
                else:
                    account_id = name_to_id.get(selected_account)
                    if account_id is None:
                        validation_errors.append(f"{spec['filename']}: Selected account '{selected_account}' no longer exists.")
                        continue
                    entry["account_id"] = account_id
                
                files_to_import.append(entry)
            
            if validation_errors:
                for message in validation_errors:
                    st.error(message)
            elif not files_to_import:
                st.warning("No files selected for import. Adjust selections or upload new files.")
            else:
                progress_bar = st.progress(0.0)
                
                def _progress_callback(done: int, total: int) -> None:
                    total = max(total, 1)
                    progress_bar.progress(done / total)
                    if debug_enabled:
                        st.info(f"Processed {done}/{total} files...", icon="ℹ️")
                
                try:
                    with st.spinner("Importing transactions..."):
                        result = importer.batch_import(
                            files_to_import,
                            config=config,
                            apply_categorization=True,
                            progress_callback=_progress_callback
                        )
                except Exception as exc:  # pragma: no cover - defensive
                    logger.exception("Unexpected import failure")
                    st.error(f"Batch import failed: {exc}")
                    progress_bar.progress(0.0)
                else:
                    st.session_state["import_result"] = result
                    st.session_state.pop("import_specs", None)
                    progress_bar.progress(1.0)
                    if debug_enabled:
                        st.write("Import result (debug):", result)
                    st.success("Import completed. Dashboard will refresh automatically.")
                    st.experimental_rerun()
    finally:
        db_manager.close()

