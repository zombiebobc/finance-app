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
    analytics_engine = AnalyticsEngine(db_manager)
    
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
    
    # Get all categories from transactions
    categories = budget_manager.get_all_categories_from_transactions()
    
    if not categories:
        st.warning("No transaction categories found. Import transactions first!")
        return
    
    # Get or create budgets for all categories
    budget_data = []
    
    for category in categories:
        # Get or create budget for this month
        budget = budget_manager.get_or_create_monthly_budget(
            category=category,
            month=selected_month,
            allocated_amount=0.0
        )
        
        if budget:
            # Calculate spending for this category
            period_start = budget.period_start.date()
            period_end = budget.period_end.date()
            activity = budget_manager.calculate_category_spending(
                category=category,
                start_date=period_start,
                end_date=period_end
            )
            
            # Calculate available (assigned - spent)
            available = budget.allocated_amount - activity
            
            budget_data.append({
                'id': budget.id,
                'Category': category,
                'Assigned': budget.allocated_amount,
                'Activity': activity,
                'Available': available
            })
    
    if not budget_data:
        st.info("No budgets found. Start by assigning amounts to categories!")
        return
    
    # Create DataFrame
    df = pd.DataFrame(budget_data)
    
    # Display summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    total_assigned = df['Assigned'].sum()
    total_activity = df['Activity'].sum()
    total_available = df['Available'].sum()
    
    with col1:
        st.metric("Total Assigned", format_currency(total_assigned))
    
    with col2:
        st.metric("Total Spent", format_currency(total_activity), 
                  delta=format_currency(total_activity), delta_color="inverse")
    
    with col3:
        st.metric("Total Available", format_currency(total_available),
                  delta=format_currency(total_available), 
                  delta_color="normal" if total_available >= 0 else "inverse")
    
    with col4:
        # Calculate percentage used
        pct_used = (total_activity / total_assigned * 100) if total_assigned > 0 else 0
        st.metric("Budget Used", f"{pct_used:.1f}%")
    
    st.markdown("---")
    
    # Editable budget table
    st.subheader("Category Budgets")
    st.markdown("*Edit the 'Assigned' column to set your budgets*")
    
    # Prepare display dataframe
    display_df = df[['Category', 'Assigned', 'Activity', 'Available']].copy()
    
    # Format currency columns for display
    display_df['Assigned_Display'] = display_df['Assigned'].apply(format_currency)
    display_df['Activity_Display'] = display_df['Activity'].apply(format_currency)
    display_df['Available_Display'] = display_df['Available'].apply(format_currency)
    
    # Use st.data_editor for editable table
    edited_df = st.data_editor(
        display_df[['Category', 'Assigned', 'Activity', 'Available']],
        column_config={
            "Category": st.column_config.TextColumn("Category", disabled=True),
            "Assigned": st.column_config.NumberColumn(
                "Assigned",
                min_value=0.0,
                format="$%.2f",
                help="Amount assigned to this category"
            ),
            "Activity": st.column_config.NumberColumn(
                "Activity (Spent)",
                disabled=True,
                format="$%.2f",
                help="Amount spent in this category"
            ),
            "Available": st.column_config.NumberColumn(
                "Available",
                disabled=True,
                format="$%.2f",
                help="Assigned - Activity"
            )
        },
        hide_index=True,
        use_container_width=True,
        key=f"budget_editor_{selected_month.strftime('%Y%m')}"
    )
    
    # Check if any changes were made
    if not edited_df.equals(display_df[['Category', 'Assigned', 'Activity', 'Available']]):
        if st.button("💾 Save Budget Changes", type="primary"):
            # Update budgets in database
            changes_made = 0
            for idx, row in edited_df.iterrows():
                original_assigned = df.loc[df['Category'] == row['Category'], 'Assigned'].values[0]
                new_assigned = row['Assigned']
                
                if original_assigned != new_assigned:
                    budget_id = df.loc[df['Category'] == row['Category'], 'id'].values[0]
                    success = budget_manager.update_budget(
                        budget_id=budget_id,
                        allocated_amount=new_assigned
                    )
                    if success:
                        changes_made += 1
            
            if changes_made > 0:
                st.success(f"✅ Updated {changes_made} budget(s)!")
                st.rerun()
            else:
                st.info("No changes detected.")
    
    # Color-coded budget status
    st.markdown("---")
    st.subheader("Budget Status")
    
    # Show categories with issues
    over_budget = df[df['Available'] < 0].sort_values('Available')
    under_budget = df[(df['Available'] > 0) & (df['Assigned'] > 0)].sort_values('Available', ascending=False)
    
    if not over_budget.empty:
        st.error(f"⚠️ {len(over_budget)} categor{'y is' if len(over_budget) == 1 else 'ies are'} over budget!")
        for _, row in over_budget.iterrows():
            st.markdown(f"- **{row['Category']}**: {format_currency(row['Available'])} (overspent by {format_currency(abs(row['Available']))})")
    
    if not under_budget.empty:
        st.success(f"✅ {len(under_budget)} categor{'y has' if len(under_budget) == 1 else 'ies have'} budget remaining")
        with st.expander("Show categories with remaining budget"):
            for _, row in under_budget.iterrows():
                st.markdown(f"- **{row['Category']}**: {format_currency(row['Available'])} remaining")
    
    # Budget tips
    with st.expander("💡 Budget Tips (YNAB Method)"):
        st.markdown("""
        **Rule 1: Give Every Dollar a Job**
        - Assign all your income to specific categories
        - Don't leave money unallocated
        
        **Rule 2: Embrace Your True Expenses**
        - Plan for irregular expenses (insurance, car repairs, etc.)
        - Break them down into monthly amounts
        
        **Rule 3: Roll With the Punches**
        - Move money between categories as needed
        - Life happens - adjust your budget!
        
        **Rule 4: Age Your Money**
        - Try to use last month's income for this month's expenses
        - Build a buffer for financial security
        """)


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

