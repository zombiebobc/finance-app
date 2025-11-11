"""
Streamlit UI components for YNAB-style budgeting.

This module provides interactive budget management interface with
editable tables, category assignments, and spending tracking.
"""

import logging
from datetime import date, datetime, timedelta
from typing import Dict, Any
import pandas as pd
import streamlit as st

from database_ops import DatabaseManager
from budgeting import BudgetManager
from analytics import AnalyticsEngine

logger = logging.getLogger(__name__)


def format_currency(amount: float) -> str:
    """Format amount as currency."""
    return f"${amount:,.2f}"


def get_month_options() -> Dict[str, date]:
    """
    Generate month options for selector.
    
    Returns:
        Dictionary mapping month labels to date objects
    """
    today = date.today()
    months = {}
    
    for i in range(12):  # Show last 12 months + current
        month_date = today.replace(day=1) - timedelta(days=30 * i)
        month_date = month_date.replace(day=1)  # Normalize to first of month
        label = month_date.strftime("%B %Y")
        months[label] = month_date
    
    return months


def render_budget_dashboard(db_manager: DatabaseManager):
    """
    Render the main budgeting dashboard.
    
    Args:
        db_manager: Database manager instance
    """
    st.header("💰 Monthly Budget (YNAB-Style)")
    st.markdown("*Assign every dollar a job!*")
    
    # Initialize managers
    budget_manager = BudgetManager(db_manager)
    
    # Session state for add/edit flows
    if "budget_add_mode" not in st.session_state:
        st.session_state.budget_add_mode = False
    if "budget_add_category" not in st.session_state:
        st.session_state.budget_add_category = None
    if "budget_add_amount" not in st.session_state:
        st.session_state.budget_add_amount = 0.0
    
    # Month selector
    months = get_month_options()
    month_labels = list(months.keys())
    selected_month_label = st.selectbox(
        "Select Month",
        month_labels,
        index=0,
        help="Choose the month to view/edit budgets"
    )
    selected_month = months[selected_month_label]
    period_start, period_end = BudgetManager.get_month_period(selected_month)
    
    st.caption(f"Budget period: {period_start.strftime('%b %d, %Y')} — {period_end.strftime('%b %d, %Y')}")
    
    # Fetch existing budgets and category lists
    all_categories = budget_manager.get_budget_categories()
    budget_overview = budget_manager.get_budget_overview(selected_month)
    available_categories = budget_manager.get_available_categories_for_month(
        selected_month,
        categories=all_categories
    )
    
    # Handle add budget flow
    add_container = st.container()
    with add_container:
        add_disabled = not available_categories
        add_button_clicked = st.button(
            "+ Add Budget Category",
            type="primary",
            disabled=add_disabled,
            help="Start a new budget envelope" if not add_disabled else "No additional categories available to budget right now."
        )
        
        if add_button_clicked:
            st.session_state.budget_add_mode = True
            st.session_state.budget_add_category = None
            st.session_state.budget_add_amount = 0.0
        
        if add_disabled:
            if not all_categories:
                st.info("No categories found. Import transactions or add `budget_categories` to `config.yaml` to get started.")
            elif available_categories is not None and not available_categories and not budget_overview:
                st.info("No categories available to budget. Add categories to your transactions or configuration.")
            else:
                st.caption("All available categories for this month already have budgets.")
    
    if st.session_state.budget_add_mode:
        st.markdown("---")
        st.subheader("Add Budget Category")
        with st.container():
            category_selection = st.selectbox(
                "Category",
                options=available_categories,
                key="budget_add_category",
                help="Select the category you want to budget."
            )
            assigned_amount = st.number_input(
                "Assigned Amount",
                min_value=0.0,
                value=st.session_state.get("budget_add_amount", 0.0),
                format="%.2f",
                step=10.0,
                help="Enter the amount you want to assign to this category."
            )
            st.session_state.budget_add_amount = assigned_amount
            
            add_actions = st.columns([1, 1, 3])
            with add_actions[0]:
                if st.button("Save Budget", type="primary", key="save_new_budget"):
                    if not category_selection:
                        st.error("Please select a category.")
                    elif assigned_amount < 0:
                        st.error("Assigned amount must be zero or greater.")
                    else:
                        result = budget_manager.upsert_monthly_budget(
                            category=category_selection,
                            month=selected_month,
                            allocated_amount=float(assigned_amount)
                        )
                        if result:
                            st.success(f"Budget for **{category_selection}** saved.")
                            st.session_state.budget_add_mode = False
                            st.session_state.budget_add_category = None
                            st.session_state.budget_add_amount = 0.0
                            st.rerun()
                        else:
                            st.error("Failed to save budget. Please try again.")
            with add_actions[1]:
                if st.button("Cancel", key="cancel_new_budget"):
                    st.session_state.budget_add_mode = False
                    st.session_state.budget_add_category = None
                    st.session_state.budget_add_amount = 0.0
                    st.rerun()
    
    st.markdown("---")
    
    if not budget_overview:
        st.info("No budget categories added yet. Click '+ Add Budget Category' to start.")
        return
    
    budget_df = pd.DataFrame(budget_overview)
    
    # Summary metrics
    total_assigned = float(budget_df["assigned"].sum())
    total_activity = float(budget_df["activity"].sum())
    total_available = float(budget_df["available"].sum())
    total_pct_used = (total_activity / total_assigned * 100) if total_assigned > 0 else 0.0
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Assigned", format_currency(total_assigned))
    with col2:
        st.metric("Total Activity", format_currency(total_activity))
    with col3:
        availability_color = "normal" if total_available >= 0 else "inverse"
        st.metric(
            "Total Available",
            format_currency(total_available),
            delta=None,
            delta_color=availability_color
        )
    with col4:
        st.metric("Budget Used", f"{total_pct_used:.1f}%")
    
    st.markdown("---")
    st.subheader("Category Budgets")
    
    def _availability_color(value: float) -> str:
        if value > 0:
            return "#2ecc71"  # green
        if value < 0:
            return "#e74c3c"  # red
        return "#f1c40f"      # yellow
    
    def _usage_color(percentage: float) -> str:
        if percentage > 100:
            return "#e74c3c"  # red
        if percentage >= 90:
            return "#f1c40f"  # yellow
        return "#2ecc71"      # green
    
    for budget in budget_overview:
        st.markdown(
            f"#### {budget['category']} "
            f"<span style='font-size:0.9rem; color:#7f8c8d;'>({format_currency(budget['assigned'])} assigned)</span>",
            unsafe_allow_html=True
        )
        row_cols = st.columns([2, 2, 2, 2, 1])
        
        assigned_label = f"Assigned for {budget['category']}"
        with row_cols[0]:
            assigned_amount = st.number_input(
                assigned_label,
                min_value=0.0,
                value=float(budget["assigned"]),
                format="%.2f",
                step=10.0,
                label_visibility="collapsed",
            )
            st.caption("Assigned")
        
        with row_cols[1]:
            st.markdown(f"<div style='font-weight:600;'>{format_currency(budget['activity'])}</div>", unsafe_allow_html=True)
            st.caption("Activity (Spent)")
        
        available_value = float(budget["available"])
        with row_cols[2]:
            available_color = _availability_color(available_value)
            st.markdown(
                f"<div style='font-weight:600; color:{available_color};'>{format_currency(available_value)}</div>",
                unsafe_allow_html=True
            )
            st.caption("Available")
        
        used_pct = float(budget["budget_used_pct"])
        with row_cols[3]:
            usage_color = _usage_color(used_pct)
            st.markdown(
                f"<div style='font-weight:600; color:{usage_color};'>{used_pct:.1f}%</div>",
                unsafe_allow_html=True
            )
            st.caption("Budget Used")
        
        with row_cols[4]:
            if st.button(
                "Save",
                key=f"save_budget_{budget['id']}",
                help="Persist any changes to the assigned amount for this category."
            ):
                if assigned_amount < 0:
                    st.error("Assigned amount must be zero or greater.")
                elif abs(assigned_amount - float(budget["assigned"])) < 0.01:
                    st.info("No changes detected for this category.")
                else:
                    updated = budget_manager.update_budget(
                        budget_id=budget["id"],
                        allocated_amount=float(assigned_amount)
                    )
                    if updated:
                        st.success(f"Updated budget for **{budget['category']}**.")
                        # Reset widget state so the new value is shown after rerun
                        assigned_key = f"Assigned for {budget['category']}"
                        if assigned_key in st.session_state:
                            del st.session_state[assigned_key]
                        st.rerun()
                    else:
                        st.error("Failed to update budget. Please try again.")
        
        st.markdown(
            "<hr style='margin-top:0.5rem; margin-bottom:1.5rem; opacity:0.2;'>",
            unsafe_allow_html=True
        )
    
    with st.expander("View Budget Table"):
        table_df = budget_df.copy()
        table_df["Assigned"] = table_df["assigned"].apply(format_currency)
        table_df["Activity"] = table_df["activity"].apply(format_currency)
        table_df["Available"] = table_df["available"].apply(format_currency)
        table_df["Budget Used %"] = table_df["budget_used_pct"].map(lambda v: f"{v:.1f}%")
        table_df = table_df[["category", "Assigned", "Activity", "Available", "Budget Used %"]]
        st.dataframe(
            table_df,
            use_container_width=True,
            hide_index=True,
        )


def render_quick_budget_setup(db_manager: DatabaseManager):
    """
    Render a quick budget setup wizard for new users.
    
    Args:
        db_manager: Database manager instance
    """
    st.subheader("🚀 Quick Budget Setup")
    st.markdown("Set up budgets based on your average spending")
    
    budget_manager = BudgetManager(db_manager)
    analytics_engine = AnalyticsEngine(db_manager)
    
    # Get categories
    categories = budget_manager.get_all_categories_from_transactions()
    
    if not categories:
        st.warning("Import transactions first to set up budgets!")
        return
    
    # Time frame for average calculation
    time_frame = st.selectbox(
        "Base budgets on average spending from:",
        ["Last 3 months", "Last 6 months", "Last 12 months"],
        help="Calculate initial budgets based on historical spending"
    )
    
    # Map to time frame string
    time_frame_map = {
        "Last 3 months": "3m",
        "Last 6 months": "6m",
        "Last 12 months": "12m"
    }
    
    if st.button("Generate Budget Suggestions", type="primary"):
        st.info("Analyzing your spending patterns...")
        
        # Get category breakdown
        category_breakdown = analytics_engine.get_category_breakdown(
            time_frame=time_frame_map[time_frame],
            expense_only=True
        )
        
        if category_breakdown.empty:
            st.warning("No spending data found for the selected period.")
            return
        
        # Calculate monthly average
        months_map = {"3m": 3, "6m": 6, "12m": 12}
        months = months_map[time_frame_map[time_frame]]
        
        st.success("💡 Suggested monthly budgets based on your average spending:")
        
        for _, row in category_breakdown.iterrows():
            monthly_avg = row['total'] / months
            st.markdown(f"- **{row['category']}**: {format_currency(monthly_avg)}")
        
        st.info("Go to the main Budget tab to review and adjust these suggestions!")

