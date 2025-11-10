"""
Streamlit-based web UI viewer for financial transactions.

This module provides an interactive web interface for viewing and filtering
transactions using Streamlit.
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime, date
import streamlit as st
import pandas as pd

from data_viewer import DataViewer
from database_ops import DatabaseManager

# Configure logging
logger = logging.getLogger(__name__)


def format_amount(amount: float) -> str:
    """
    Format amount as currency string.
    
    Args:
        amount: Transaction amount
    
    Returns:
        Formatted string (e.g., "$1,234.56")
    """
    return f"${amount:,.2f}"


def get_connection_string() -> str:
    """
    Get database connection string from environment or config.
    
    Returns:
        Database connection string
    """
    import os
    import yaml
    from pathlib import Path
    
    # Try environment variable first
    conn_str = os.environ.get("DB_CONNECTION_STRING")
    if conn_str:
        return conn_str
    
    # Fall back to config file
    config_path = Path("config.yaml")
    if config_path.exists():
        with open(config_path) as f:
            config = yaml.safe_load(f)
        db_config = config.get("database", {})
        if "connection_string" in db_config:
            return db_config["connection_string"]
        db_type = db_config.get("type", "sqlite")
        db_path = db_config.get("path", "transactions.db")
        if db_type == "sqlite":
            return f"sqlite:///{db_path}"
    
    # Default fallback
    return "sqlite:///transactions.db"


def main_ui_viewer(connection_string: Optional[str] = None) -> None:
    """
    Main entry point for Streamlit UI viewer.
    
    Args:
        connection_string: Optional database connection string (if None, will try to get from env/config)
    """
    if connection_string is None:
        connection_string = get_connection_string()
    # Page configuration
    st.set_page_config(
        page_title="Financial Transaction Viewer",
        page_icon="💰",
        layout="wide"
    )
    
    # Initialize database and viewer (with caching)
    @st.cache_resource
    def get_viewer():
        """Get cached DataViewer instance."""
        db_manager = DatabaseManager(connection_string)
        return DataViewer(db_manager), db_manager
    
    try:
        viewer, db_manager = get_viewer()
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        st.stop()
    
    # Title and header
    st.title("💰 Financial Transaction Viewer")
    st.markdown("View and filter your financial transactions")
    
    # Sidebar for filters
    with st.sidebar:
        st.header("🔍 Filters")
        
        # Date range filter
        st.subheader("Date Range")
        col1, col2 = st.columns(2)
        with col1:
            date_start = st.date_input(
                "Start Date",
                value=None,
                help="Filter transactions from this date onwards"
            )
        with col2:
            date_end = st.date_input(
                "End Date",
                value=None,
                help="Filter transactions up to this date"
            )
        
        # Amount range filter
        st.subheader("Amount Range")
        col1, col2 = st.columns(2)
        with col1:
            amount_min = st.number_input(
                "Min Amount",
                value=None,
                step=0.01,
                help="Minimum transaction amount"
            )
        with col2:
            amount_max = st.number_input(
                "Max Amount",
                value=None,
                step=0.01,
                help="Maximum transaction amount"
            )
        
        # Description search
        st.subheader("Description")
        description_keywords = st.text_input(
            "Search Keywords",
            value="",
            help="Search in transaction descriptions (case-insensitive)"
        )
        
        # Category filter
        st.subheader("Category")
        category = st.text_input(
            "Category",
            value="",
            help="Filter by category (case-insensitive partial match)"
        )
        
        # Source file filter
        st.subheader("Source File")
        source_file = st.text_input(
            "Source File",
            value="",
            help="Filter by source file name (case-insensitive partial match)"
        )
        
        # Display options
        st.header("⚙️ Display Options")
        limit = st.number_input(
            "Limit Results",
            min_value=1,
            max_value=10000,
            value=1000,
            step=100,
            help="Maximum number of transactions to display"
        )
        
        sort_by = st.selectbox(
            "Sort By",
            options=["date", "amount", "description", "category", "source_file"],
            index=0
        )
        
        sort_asc = st.checkbox("Sort Ascending", value=False)
        
        # Clear filters button
        if st.button("🔄 Clear All Filters"):
            st.rerun()
    
    # Build filters dictionary
    filters: Dict[str, Any] = {}
    if date_start:
        filters["date_start"] = date_start.isoformat()
    if date_end:
        filters["date_end"] = date_end.isoformat()
    if amount_min is not None:
        filters["amount_min"] = amount_min
    if amount_max is not None:
        filters["amount_max"] = amount_max
    if description_keywords:
        filters["description_keywords"] = description_keywords
    if category:
        filters["category"] = category
    if source_file:
        filters["source_file"] = source_file
    
    # Main content area
    try:
        # Get transactions
        df = viewer.get_transactions_df(
            filters=filters if filters else None,
            limit=limit,
            order_by=sort_by,
            order_desc=not sort_asc
        )
        
        if df.empty:
            st.info("No transactions found matching the criteria.")
            
            # Show summary stats even if no results
            with st.expander("📊 Summary Statistics", expanded=False):
                stats = viewer.get_summary_stats(filters=filters if filters else None)
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Transactions", stats["total_count"])
                with col2:
                    st.metric("Total Amount", format_amount(stats["total_amount"]))
                with col3:
                    st.metric("Average Amount", format_amount(stats["average_amount"]))
                with col4:
                    st.metric("Credits", stats["positive_count"])
        else:
            # Format for display
            formatted_df = viewer.format_transactions_df(df)
            
            # Display summary statistics
            st.header("📊 Summary Statistics")
            stats = viewer.get_summary_stats(filters=filters if filters else None)
            
            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                st.metric("Total Transactions", f"{stats['total_count']:,}")
            with col2:
                st.metric("Total Amount", format_amount(stats["total_amount"]))
            with col3:
                st.metric("Average Amount", format_amount(stats["average_amount"]))
            with col4:
                st.metric("Credits", f"{stats['positive_count']:,}")
            with col5:
                st.metric("Debits", f"{stats['negative_count']:,}")
            
            # Display detailed breakdown
            with st.expander("💰 Amount Breakdown", expanded=False):
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Total Credits", format_amount(stats["positive_total"]))
                with col2:
                    st.metric("Total Debits", format_amount(stats["negative_total"]))
                st.metric("Net Amount", format_amount(stats["total_amount"]))
            
            # Display transactions table
            st.header(f"📋 Transactions ({len(df):,} shown)")
            
            # Prepare display DataFrame
            display_df = formatted_df[[
                "date", "description", "amount", "category", "source_file"
            ]].copy()
            
            # Format amount column
            display_df["amount"] = display_df["amount"].apply(format_amount)
            
            # Rename columns for display
            display_df.columns = ["Date", "Description", "Amount", "Category", "Source File"]
            
            # Display interactive table
            st.dataframe(
                display_df,
                use_container_width=True,
                height=600,
                hide_index=True
            )
            
            # Export button
            st.download_button(
                label="📥 Export to CSV",
                data=df.to_csv(index=False).encode('utf-8'),
                file_name=f"transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                help="Download the filtered transactions as a CSV file"
            )
            
            # Show raw data option
            with st.expander("🔍 View Raw Data", expanded=False):
                st.dataframe(df, use_container_width=True, height=400)
    
    except ValueError as e:
        st.error(f"Invalid filter: {e}")
    except Exception as e:
        logger.error(f"Error in UI viewer: {e}", exc_info=True)
        st.error(f"Error: {e}")
        st.exception(e)
    
    # Footer
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: gray;'>"
        "Financial Transaction Viewer | Built with Streamlit"
        "</div>",
        unsafe_allow_html=True
    )


# Streamlit entry point
if __name__ == "__main__" or "streamlit" in __name__:
    main_ui_viewer()

