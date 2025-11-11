"""
Streamlit UI for financial analytics and reporting.

This module provides an interactive web-based dashboard for
exploring financial data with charts, filters, and drill-down.
"""

import logging
from pathlib import Path
from typing import Optional, Dict
from datetime import date
import pandas as pd
import streamlit as st
import altair as alt

from database_ops import DatabaseManager, AccountType
from analytics import AnalyticsEngine
from report_generator import ReportGenerator
from data_fetch import (
    fetch_account_summaries,
    fetch_balance_history,
    fetch_net_worth_history,
    get_time_frame_dates
)
from viz_components import (
    kpi_metric,
    account_card,
    net_worth_progress,
    create_asset_liability_pie,
    create_net_worth_trend_chart,
    ACCOUNT_ICONS,
    COLORS
)
from config_manager import (
    get_net_worth_goal,
    set_net_worth_goal,
    get_dashboard_preference,
    initialize_session_state
)
from utils import ensure_data_dir, resolve_connection_string

logger = logging.getLogger(__name__)


def load_connection_string() -> str:
    """Resolve the database connection string with data directory initialization."""
    import yaml
    
    config_path = Path('config.yaml')
    config = {}
    if config_path.exists():
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f) or {}
    
    ensure_data_dir(config)
    return resolve_connection_string(config)


def display_comparison_summary(
    changes: Dict[str, Dict[str, float]],
    comparison_type: str
) -> None:
    """
    Display comparison summary metrics with percentage changes.
    
    Args:
        changes: Dictionary from calculate_percentage_changes
        comparison_type: Type of comparison ('previous_month' or 'previous_year')
    """
    comp_label = "Previous Month" if comparison_type == 'previous_month' else "Previous Year"
    
    st.markdown("### Comparison Summary")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        income_chg = changes['income']['percent_change']
        income_color = "normal" if income_chg >= 0 else "inverse"
        st.metric(
            "Income Change",
            f"{income_chg:+.1f}%",
            delta=format_currency(changes['income']['change']),
            delta_color=income_color,
            help=f"Current: {format_currency(changes['income']['current'])}, {comp_label}: {format_currency(changes['income']['comparison'])}"
        )
    
    with col2:
        expenses_chg = changes['expenses']['percent_change']
        # For expenses, positive change is bad (spending more), negative is good (spending less)
        expenses_color = "inverse" if expenses_chg >= 0 else "normal"
        st.metric(
            "Expenses Change",
            f"{expenses_chg:+.1f}%",
            delta=format_currency(changes['expenses']['change']),
            delta_color=expenses_color,
            help=f"Current: {format_currency(changes['expenses']['current'])}, {comp_label}: {format_currency(changes['expenses']['comparison'])}"
        )
    
    with col3:
        net_chg = changes['net']['percent_change']
        net_color = "normal" if net_chg >= 0 else "inverse"
        st.metric(
            "Net Change",
            f"{net_chg:+.1f}%",
            delta=format_currency(changes['net']['change']),
            delta_color=net_color,
            help=f"Current: {format_currency(changes['net']['current'])}, {comp_label}: {format_currency(changes['net']['comparison'])}"
        )


def format_currency(amount: float) -> str:
    """Format amount as currency."""
    return f"${amount:,.2f}"


def create_category_pie_chart(df: pd.DataFrame, title: str = "Spending by Category") -> alt.Chart:
    """
    Create interactive pie chart for category breakdown using Altair.
    
    Args:
        df: Category breakdown DataFrame
        title: Chart title
    
    Returns:
        Altair chart object
    """
    if df.empty:
        return alt.Chart(pd.DataFrame({'message': ['No data']})).mark_text(size=20).encode(
            text='message:N'
        )
    
    # Take top 10, group rest as Other
    df_plot = df.head(10).copy()
    if len(df) > 10:
        other_total = df.iloc[10:]['total'].sum()
        other_row = pd.DataFrame([{
            'category': 'Other',
            'total': other_total,
            'percentage': df.iloc[10:]['percentage'].sum()
        }])
        df_plot = pd.concat([df_plot, other_row], ignore_index=True)
    
    chart = alt.Chart(df_plot).mark_arc(innerRadius=50).encode(
        theta=alt.Theta(field='total', type='quantitative'),
        color=alt.Color(field='category', type='nominal', legend=alt.Legend(title='Category')),
        tooltip=[
            alt.Tooltip('category:N', title='Category'),
            alt.Tooltip('total:Q', title='Amount', format='$,.2f'),
            alt.Tooltip('percentage:Q', title='Percentage', format='.1f')
        ]
    ).properties(
        title=title,
        width=400,
        height=400
    )
    
    return chart


def create_monthly_trend_chart(
    df: pd.DataFrame, 
    title: str = "Monthly Trends",
    comparison_df: Optional[pd.DataFrame] = None,
    comparison_type: Optional[str] = None
) -> alt.Chart:
    """
    Create interactive line/bar chart for monthly trends using Altair with optional comparison overlay.
    
    Args:
        df: Monthly trends DataFrame (current period)
        title: Chart title
        comparison_df: Optional DataFrame with comparison period data
        comparison_type: Type of comparison ('previous_month' or 'previous_year') for labeling
    
    Returns:
        Altair chart object with layered visualization
    """
    if df.empty:
        return alt.Chart(pd.DataFrame({'message': ['No data']})).mark_text(size=20).encode(
            text='message:N'
        )
    
    # Reshape current period data for Altair
    df_melted = df.melt(
        id_vars=['period'],
        value_vars=['income', 'expenses', 'net'],
        var_name='type',
        value_name='amount'
    )
    df_melted['period_type'] = 'current'
    
    # Create base chart encoding
    base = alt.Chart(df_melted).encode(
        x=alt.X('period:N', title='Period', axis=alt.Axis(labelAngle=-45))
    )
    
    # Bars for current period income and expenses (solid colors)
    bars_current = base.transform_filter(
        alt.FieldOneOfPredicate(field='type', oneOf=['income', 'expenses'])
    ).mark_bar(opacity=0.8).encode(
        y=alt.Y('amount:Q', title='Amount ($)'),
        color=alt.Color('type:N', scale=alt.Scale(domain=['income', 'expenses'], range=['#2ecc71', '#e74c3c'])),
        xOffset='type:N',
        tooltip=[
            alt.Tooltip('period:N', title='Period'),
            alt.Tooltip('type:N', title='Type'),
            alt.Tooltip('amount:Q', title='Amount', format='$,.2f'),
            alt.Tooltip('period_type:N', title='Period Type')
        ]
    )
    
    # Line for current period net (solid line)
    line_current = base.transform_filter(
        alt.FieldEqualPredicate(field='type', equal='net')
    ).mark_line(point=True, color='#3498db', strokeWidth=3, opacity=0.9).encode(
        y=alt.Y('amount:Q'),
        tooltip=[
            alt.Tooltip('period:N', title='Period'),
            alt.Tooltip('amount:Q', title='Net', format='$,.2f'),
            alt.Tooltip('period_type:N', title='Period Type')
        ]
    )
    
    # Combine current period layers
    chart_layers = [bars_current, line_current]
    
    # Add comparison period if provided
    if comparison_df is not None and not comparison_df.empty:
        # Reshape comparison data
        comp_melted = comparison_df.melt(
            id_vars=['period'],
            value_vars=['income', 'expenses', 'net'],
            var_name='type',
            value_name='amount'
        )
        comp_melted['period_type'] = 'comparison'
        
        # Create comparison base (need to align periods with current)
        # Merge comparison data with current periods for alignment
        comp_aligned = []
        for _, row in df.iterrows():
            period = row['period']
            # Find matching comparison period
            if comparison_type == 'previous_month':
                # Find previous month
                year, month = map(int, period.split('-'))
                if month == 1:
                    comp_year = year - 1
                    comp_month = 12
                else:
                    comp_year = year
                    comp_month = month - 1
                comp_period = f"{comp_year}-{comp_month:02d}"
            elif comparison_type == 'previous_year':
                # Find previous year same month
                year, month = map(int, period.split('-'))
                comp_year = year - 1
                comp_period = f"{comp_year}-{month:02d}"
            else:
                comp_period = period
            
            # Find matching row in comparison_df
            comp_row = comparison_df[comparison_df['period'] == comp_period]
            if not comp_row.empty:
                comp_aligned.append({
                    'period': period,  # Use current period for alignment
                    'income': comp_row.iloc[0]['income'],
                    'expenses': comp_row.iloc[0]['expenses'],
                    'net': comp_row.iloc[0]['net']
                })
        
        if comp_aligned:
            comp_df = pd.DataFrame(comp_aligned)
            comp_melted = comp_df.melt(
                id_vars=['period'],
                value_vars=['income', 'expenses', 'net'],
                var_name='type',
                value_name='amount'
            )
            comp_melted['period_type'] = 'comparison'
            
            comp_base = alt.Chart(comp_melted).encode(
                x=alt.X('period:N', title='Period', axis=alt.Axis(labelAngle=-45))
            )
            
            # Bars for comparison period (semi-transparent fill + strong stroke for visibility)
            # Use low fill opacity with thick dashed stroke to ensure visibility regardless of z-order
            bars_comp = comp_base.transform_filter(
                alt.FieldOneOfPredicate(field='type', oneOf=['income', 'expenses'])
            ).mark_bar(
                fillOpacity=0.15,  # Very low fill opacity - just enough to show shape
                strokeWidth=3,     # Thick stroke for strong visibility
                strokeDash=[8, 4],  # Dashed pattern to distinguish from current
                strokeJoin='round',
                strokeCap='round',
                cornerRadius=2  # Slightly rounded corners
            ).encode(
                y=alt.Y('amount:Q', title='Amount ($)'),
                color=alt.Color(
                    'type:N', 
                    scale=alt.Scale(domain=['income', 'expenses'], range=['#2ecc71', '#e74c3c']),
                    legend=None  # Hide legend to avoid clutter
                ),
                stroke=alt.Stroke(
                    'type:N', 
                    scale=alt.Scale(domain=['income', 'expenses'], range=['#27ae60', '#c0392b']),
                    legend=None
                ),
                xOffset='type:N',
                tooltip=[
                    alt.Tooltip('period:N', title='Period'),
                    alt.Tooltip('type:N', title='Type'),
                    alt.Tooltip('amount:Q', title='Amount', format='$,.2f'),
                    alt.Tooltip('period_type:N', title='Period Type')
                ]
            )
            
            # Line for comparison period net (thicker dashed line for visibility)
            line_comp = comp_base.transform_filter(
                alt.FieldEqualPredicate(field='type', equal='net')
            ).mark_line(
                point=True, 
                strokeWidth=3,  # Thicker for visibility
                strokeDash=[8, 4],  # Dashed pattern
                color='#2980b9',  # Slightly darker blue for contrast
                opacity=0.8  # Higher opacity
            ).encode(
                y=alt.Y('amount:Q'),
                tooltip=[
                    alt.Tooltip('period:N', title='Period'),
                    alt.Tooltip('amount:Q', title='Net', format='$,.2f'),
                    alt.Tooltip('period_type:N', title='Period Type')
                ]
            )
            
            # Add comparison layers LAST so they're drawn on top and always visible
            # This ensures comparison bars/lines are visible even when current values are higher
            chart_layers.extend([bars_comp, line_comp])
    
    # Combine all layers (comparison will be on top due to being added last)
    chart = alt.layer(*chart_layers).properties(
        title=title,
        width=700,
        height=400
    ).configure_axis(
        labelFontSize=11,
        titleFontSize=12
    )
    
    return chart


def create_account_comparison_chart(df: pd.DataFrame, title: str = "Account Comparison") -> alt.Chart:
    """
    Create horizontal bar chart for account comparison using Altair.
    
    Args:
        df: Account summary DataFrame
        title: Chart title
    
    Returns:
        Altair chart object
    """
    if df.empty:
        return alt.Chart(pd.DataFrame({'message': ['No data']})).mark_text(size=20).encode(
            text='message:N'
        )
    
    # Reshape data
    df_melted = df.melt(
        id_vars=['account_name', 'type'],
        value_vars=['income', 'expenses'],
        var_name='category',
        value_name='amount'
    )
    
    chart = alt.Chart(df_melted).mark_bar().encode(
        y=alt.Y('account_name:N', title='Account', sort='-x'),
        x=alt.X('amount:Q', title='Amount ($)'),
        color=alt.Color('category:N', scale=alt.Scale(domain=['income', 'expenses'], range=['#2ecc71', '#e74c3c'])),
        tooltip=[
            alt.Tooltip('account_name:N', title='Account'),
            alt.Tooltip('type:N', title='Type'),
            alt.Tooltip('category:N', title='Category'),
            alt.Tooltip('amount:Q', title='Amount', format='$,.2f')
        ]
    ).properties(
        title=title,
        width=700,
        height=400
    )
    
    return chart


def main_ui_analytics():
    """Main Streamlit UI for analytics dashboard."""
    st.set_page_config(
        page_title="Financial Analytics Dashboard",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Custom CSS for better viewing at smaller zoom levels
    st.markdown("""
        <style>
        /* Optimize for 75% zoom - slightly larger fonts and better spacing */
        .stMetric {
            padding: 0.5rem 0;
        }
        .stMetric > div {
            font-size: 1.1rem;
        }
        .stMetric label {
            font-size: 1rem !important;
        }
        .stMetric [data-testid="stMetricValue"] {
            font-size: 1.75rem !important;
        }
        h1 {
            font-size: 2.5rem !important;
            padding-bottom: 1rem;
        }
        h2 {
            font-size: 2rem !important;
            padding-top: 0.5rem;
        }
        h3 {
            font-size: 1.5rem !important;
            padding-top: 0.5rem;
        }
        /* Better spacing for expanders */
        .streamlit-expanderHeader {
            font-size: 1.1rem !important;
            font-weight: 600;
        }
        /* Improve chart legends */
        .vega-embed {
            padding: 0.5rem 0;
        }
        </style>
    """, unsafe_allow_html=True)
    
    st.title("📊 Financial Analytics Dashboard")
    
    # Initialize session state
    initialize_session_state()
    
    # Load database
    connection_string = load_connection_string()
    
    try:
        db_manager = DatabaseManager(connection_string)
        analytics = AnalyticsEngine(db_manager)
        report_gen = ReportGenerator()
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        return
    
    # Sidebar filters
    st.sidebar.header("Filters")
    
    # Time frame selection
    time_frame_options = {
        'Last Month': '1m',
        'Last 3 Months': '3m',
        'Last 6 Months': '6m',
        'Last 12 Months': '12m',
        'All Time': 'all',
        'Custom Range': 'custom'
    }
    
    time_frame_label = st.sidebar.selectbox(
        "Time Frame",
        options=list(time_frame_options.keys()),
        index=2  # Default to 6 months
    )
    
    time_frame = time_frame_options[time_frame_label]
    
    # Custom date range if selected
    if time_frame == 'custom':
        col1, col2 = st.sidebar.columns(2)
        with col1:
            start_date = st.date_input("Start Date")
        with col2:
            end_date = st.date_input("End Date")
        time_frame = f"{start_date}:{end_date}"
    
    # Account filter
    try:
        from account_management import AccountManager
        account_manager = AccountManager(db_manager)
        accounts = account_manager.list_accounts()
        
        account_options = {'All Accounts': None}
        for acc in accounts:
            account_options[f"{acc['name']} ({acc['type']})"] = acc['id']
        
        selected_account = st.sidebar.selectbox(
            "Account",
            options=list(account_options.keys())
        )
        account_id = account_options[selected_account]
    except:
        account_id = None
    
    # Transfer filter
    st.sidebar.markdown("---")
    include_transfers = st.sidebar.checkbox(
        "Include Transfers",
        value=False,
        help="Show internal transfers between accounts (e.g., credit card payments)"
    )
    
    # Report type selection
    report_type = st.sidebar.radio(
        "Report Type",
        ["Overview", "Budget", "Spending Categories", "Income Categories", "Trends", "Accounts", "Comparison"]
    )
    
    # Net Worth Goal Setting
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🎯 Net Worth Goal")
    current_goal = get_net_worth_goal()
    new_goal = st.sidebar.number_input(
        "Target Net Worth ($)",
        min_value=0.0,
        value=current_goal,
        step=10000.0,
        format="%.2f",
        help="Set your net worth goal to track progress"
    )
    if new_goal != current_goal:
        if st.sidebar.button("💾 Save Goal"):
            if set_net_worth_goal(new_goal, save_to_file=True):
                st.sidebar.success("✅ Goal saved!")
                st.rerun()
            else:
                st.sidebar.error("❌ Failed to save goal")
    
    # Main content area
    try:
        if report_type == "Overview":
            render_overview(analytics, report_gen, time_frame, account_id, time_frame_label, include_transfers)
        
        elif report_type == "Budget":
            render_budget_tab(db_manager)
        
        elif report_type == "Spending Categories":
            render_categories(analytics, report_gen, time_frame, account_id, time_frame_label, include_transfers)
        
        elif report_type == "Income Categories":
            render_income_categories(analytics, report_gen, time_frame, account_id, time_frame_label, include_transfers)
        
        elif report_type == "Trends":
            render_trends(analytics, report_gen, time_frame, account_id, time_frame_label)
        
        elif report_type == "Accounts":
            render_accounts(analytics, report_gen, time_frame, time_frame_label)
        
        elif report_type == "Comparison":
            render_comparison(analytics, report_gen)
    
    except Exception as e:
        st.error(f"Error generating report: {e}")
        logger.error(f"UI error: {e}", exc_info=True)
    
    finally:
        db_manager.close()


def display_improved_account_section(db_manager: DatabaseManager, show_export: bool = True) -> None:
    """
    Display enhanced account balances section with interactive visualizations.
    
    Features:
    - Large KPI cards for assets, liabilities, and net worth
    - Interactive pie charts for asset/liability distributions
    - Collapsible account cards with balance history sparklines
    - Net worth progress bar with configurable goal
    - Time frame filter for historical snapshots
    - Export functionality
    
    Args:
        db_manager: Database manager instance
        show_export: Whether to show export button
    """
    st.subheader("Account Balances")
    
    try:
        # Time frame filter
        col1, col2 = st.columns([3, 1])
        with col1:
            time_frame = st.selectbox(
                "View as of:",
                options=['Current', 'Last Month', 'Last Quarter'],
                key='account_time_frame'
            )
        with col2:
            if st.button("📅 Custom Date", key='custom_date_btn'):
                st.session_state.show_custom_date = True
        
        # Custom date picker (if requested)
        as_of_date = date.today()
        if st.session_state.get('show_custom_date', False):
            as_of_date = st.date_input(
                "Select date:",
                value=date.today(),
                key='custom_account_date'
            )
            time_frame = as_of_date.strftime('%Y-%m-%d')
        else:
            _, as_of_date = get_time_frame_dates(time_frame)
        
        # Fetch account data
        with st.spinner('Loading account data...'):
            summary = fetch_account_summaries(db_manager, as_of_date)
        
        assets_df = summary['assets']
        liabilities_df = summary['liabilities']
        net_worth = summary['net_worth']
        assets_total = summary['assets_total']
        liabilities_total = summary['liabilities_total']
        
        # Check if we have any accounts
        if assets_df.empty and liabilities_df.empty:
            st.warning("📊 No accounts found. Create accounts to start tracking your finances!")
            return
        
        # KPI Metrics Row
        col1, col2, col3 = st.columns(3)
        
        with col1:
            kpi_metric(
                "Total Assets",
                assets_total,
                color_logic=lambda v: COLORS['positive'],
                help_text="Sum of all asset account balances"
            )
        
        with col2:
            kpi_metric(
                "Total Liabilities",
                abs(liabilities_total),
                color_logic=lambda v: COLORS['negative'],
                help_text="Sum of all liability account balances"
            )
        
        with col3:
            kpi_metric(
                "Net Worth",
                net_worth,
                help_text="Total Assets - Total Liabilities"
            )
        
        st.markdown("---")
        
        # Net Worth Progress Bar
        net_worth_goal = get_net_worth_goal()
        net_worth_progress(net_worth, net_worth_goal, show_details=True)
        
        st.markdown("---")
        
        # Pie Charts and Account Details
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### Assets")
            if not assets_df.empty:
                # Pie chart
                asset_pie = create_asset_liability_pie(assets_df, liabilities_df, 'assets')
                st.altair_chart(asset_pie, use_container_width=True)
                
                # Define color palette matching pie chart
                asset_colors = [
                    '#00b894',  # Bright green
                    '#00cec9',  # Cyan
                    '#0984e3',  # Blue
                    '#6c5ce7',  # Purple
                    '#fdcb6e',  # Yellow
                    '#e17055',  # Orange
                    '#74b9ff',  # Light blue
                    '#a29bfe',  # Light purple
                ]
                
                # Collapsible account cards - EXPANDED BY DEFAULT
                with st.expander(f"📊 View {len(assets_df)} Asset Account(s)", expanded=True):
                    show_sparklines = get_dashboard_preference('show_sparklines', True)
                    balance_history_days = get_dashboard_preference('balance_history_days', 30)
                    
                    for idx, row in assets_df.iterrows():
                        with st.container():
                            # Fetch balance history if sparklines enabled
                            history_df = None
                            if show_sparklines:
                                history_df = fetch_balance_history(
                                    db_manager,
                                    row['id'],
                                    days=balance_history_days
                                )
                            
                            # Get matching color from palette
                            card_color = asset_colors[idx % len(asset_colors)]
                            account_card(row, history_df, show_sparkline=show_sparklines, card_color=card_color)
                            
                            if idx < len(assets_df) - 1:
                                st.markdown("<hr style='margin: 0.5rem 0; opacity: 0.3;'>", unsafe_allow_html=True)
            else:
                st.info("No asset accounts found")
        
        with col2:
            st.markdown("### Liabilities")
            if not liabilities_df.empty:
                # Pie chart
                liability_pie = create_asset_liability_pie(assets_df, liabilities_df, 'liabilities')
                st.altair_chart(liability_pie, use_container_width=True)
                
                # Define color palette matching pie chart
                liability_colors = [
                    '#ff7675',  # Bright red
                    '#fd79a8',  # Pink
                    '#fdcb6e',  # Yellow
                    '#e17055',  # Orange
                    '#d63031',  # Dark red
                    '#e84393',  # Magenta
                    '#fab1a0',  # Light orange
                    '#ff7675',  # Coral
                ]
                
                # Collapsible account cards - EXPANDED BY DEFAULT
                with st.expander(f"💳 View {len(liabilities_df)} Liability Account(s)", expanded=True):
                    show_sparklines = get_dashboard_preference('show_sparklines', True)
                    balance_history_days = get_dashboard_preference('balance_history_days', 30)
                    
                    for idx, row in liabilities_df.iterrows():
                        with st.container():
                            # Fetch balance history if sparklines enabled
                            history_df = None
                            if show_sparklines:
                                history_df = fetch_balance_history(
                                    db_manager,
                                    row['id'],
                                    days=balance_history_days
                                )
                            
                            # Get matching color from palette
                            card_color = liability_colors[idx % len(liability_colors)]
                            account_card(row, history_df, show_sparkline=show_sparklines, card_color=card_color)
                            
                            if idx < len(liabilities_df) - 1:
                                st.markdown("<hr style='margin: 0.5rem 0; opacity: 0.3;'>", unsafe_allow_html=True)
            else:
                st.info("No liability accounts found")
        
        # Net Worth Trend Chart
        st.markdown("---")
        st.markdown("### Net Worth Trend")
        
        net_worth_history_days = get_dashboard_preference('net_worth_history_days', 90)
        with st.spinner('Loading net worth history...'):
            net_worth_df = fetch_net_worth_history(db_manager, days=net_worth_history_days)
        
        if not net_worth_df.empty:
            trend_chart = create_net_worth_trend_chart(net_worth_df)
            st.altair_chart(trend_chart, use_container_width=True)
        else:
            st.info("No net worth history available")
        
        # Export button
        if show_export:
            st.markdown("---")
            combined_df = pd.concat([assets_df, liabilities_df], ignore_index=True)
            if not combined_df.empty:
                csv = combined_df.to_csv(index=False)
                st.download_button(
                    label="📥 Export Account Summary CSV",
                    data=csv,
                    file_name=f"account_summary_{as_of_date.strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
    
    except Exception as e:
        st.error(f"Error loading account information: {e}")
        logger.error(f"Enhanced account section error: {e}", exc_info=True)


def render_overview(analytics, report_gen, time_frame, account_id, time_frame_label, include_transfers=False):
    """Render overview dashboard."""
    st.header(f"Financial Overview ({time_frame_label})")
    
    # Show transfer filter status
    if not include_transfers:
        st.info("ℹ️ Transfers are excluded from this analysis. Toggle 'Include Transfers' in the sidebar to include them.")
    
    # Two column layout for pie charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Spending by Category")
        df_spending = analytics.get_category_breakdown(
            time_frame=time_frame,
            account_id=account_id,
            expense_only=True,
            include_transfers=include_transfers
        )
        if not df_spending.empty:
            # Summary metrics ABOVE the pie chart
            metrics_col1, metrics_col2 = st.columns(2)
            with metrics_col1:
                total_spending = df_spending['total'].sum()
                st.metric("Total Spending", format_currency(total_spending))
            with metrics_col2:
                st.metric("Categories", len(df_spending))
            
            # Pie chart (without redundant title)
            chart = create_category_pie_chart(df_spending, "")
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No spending data available")
    
    with col2:
        st.subheader("Income by Category")
        df_income = analytics.get_income_breakdown(
            time_frame=time_frame,
            account_id=account_id,
            include_transfers=include_transfers
        )
        if not df_income.empty:
            # Summary metrics ABOVE the pie chart
            metrics_col1, metrics_col2 = st.columns(2)
            with metrics_col1:
                total_income = df_income['total'].sum()
                st.metric("Total Income", format_currency(total_income))
            with metrics_col2:
                st.metric("Categories", len(df_income))
            
            # Pie chart (without redundant title)
            chart = create_category_pie_chart(df_income, "")
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No income data available")
    
    # Enhanced Account Balances Section
    st.markdown("---")
    display_improved_account_section(analytics.db_manager, show_export=True)
    
    # Monthly Trends
    st.markdown("---")
    st.subheader("Monthly Trends")
    
    # Comparison dropdown
    comparison_options = ["None", "Previous Month", "Previous Year"]
    comparison_selection = st.selectbox(
        "Compare with:",
        comparison_options,
        key="overview_comparison",
        help="Select a comparison period to overlay on the chart"
    )
    
    df_trends = analytics.get_monthly_trends(
        time_frame=time_frame,
        account_id=account_id
    )
    
    if not df_trends.empty:
        # Determine comparison type and fetch data if needed
        comparison_df = None
        comparison_type = None
        changes = None
        
        if comparison_selection == "Previous Month":
            # Check if we have enough data (need at least 2 months for previous month comparison)
            if len(df_trends) < 2:
                st.warning("⚠️ Previous Month comparison requires at least 2 months of data. Showing current period only.")
            else:
                try:
                    comparison_type = 'previous_month'
                    comparison_df = analytics.get_comparison_data(
                        current_df=df_trends,
                        comparison_type=comparison_type,
                        account_id=account_id
                    )
                    if comparison_df.empty:
                        st.warning("⚠️ No data available for previous month comparison.")
                    else:
                        changes = analytics.calculate_percentage_changes(df_trends, comparison_df)
                except Exception as e:
                    logger.error(f"Error fetching comparison data: {e}", exc_info=True)
                    st.error(f"Error loading comparison data: {e}")
        
        elif comparison_selection == "Previous Year":
            try:
                comparison_type = 'previous_year'
                comparison_df = analytics.get_comparison_data(
                    current_df=df_trends,
                    comparison_type=comparison_type,
                    account_id=account_id
                )
                if comparison_df.empty:
                    st.warning("⚠️ No data available for previous year comparison.")
                else:
                    changes = analytics.calculate_percentage_changes(df_trends, comparison_df)
            except Exception as e:
                logger.error(f"Error fetching comparison data: {e}", exc_info=True)
                st.error(f"Error loading comparison data: {e}")
        
        # Create and display chart
        chart_title = "Last 6 Months"
        if comparison_selection != "None" and comparison_df is not None and not comparison_df.empty:
            comp_label = "Previous Month" if comparison_type == 'previous_month' else "Previous Year"
            chart_title = f"Last 6 Months (vs {comp_label})"
        
        chart = create_monthly_trend_chart(
            df_trends.tail(6),
            chart_title,
            comparison_df=comparison_df,
            comparison_type=comparison_type
        )
        st.altair_chart(chart, use_container_width=True)
        
        # Display comparison summary if available
        if changes is not None:
            display_comparison_summary(changes, comparison_type)
    else:
        st.info("No trend data available")
    
    # Top transactions
    st.markdown("---")
    st.subheader("Top Expenses")
    df_top = analytics.get_top_transactions(
        time_frame=time_frame,
        limit=10,
        transaction_type='expenses',
        account_id=account_id
    )
    if not df_top.empty:
        df_top['amount'] = df_top['amount'].apply(lambda x: format_currency(abs(x)))
        st.dataframe(df_top, use_container_width=True, hide_index=True)
    else:
        st.info("No transactions found")


def render_categories(analytics, report_gen, time_frame, account_id, time_frame_label, include_transfers=False):
    """Render category breakdown report with drill-down to transactions."""
    st.header(f"Category Breakdown ({time_frame_label})")
    
    # Show transfer filter status
    if not include_transfers:
        st.info("ℹ️ Transfers are excluded from spending analysis. Toggle 'Include Transfers' in the sidebar to include them.")
    else:
        st.warning("⚠️ Transfers are included in this analysis (may inflate spending totals)")
    
    # Initialize session state for selected category
    if 'selected_category' not in st.session_state:
        st.session_state.selected_category = None
    
    # Check if we're in drill-down mode
    if st.session_state.selected_category:
        render_category_transactions(
            analytics, 
            st.session_state.selected_category, 
            time_frame, 
            account_id, 
            time_frame_label,
            include_transfers
        )
        return
    
    # Get category data
    df = analytics.get_category_breakdown(
        time_frame=time_frame,
        account_id=account_id,
        expense_only=True,
        include_transfers=include_transfers
    )
    
    if df.empty:
        st.warning("No spending data available for the selected time frame.")
        return
    
    # Display chart
    col1, col2 = st.columns([2, 1])
    
    with col1:
        chart = create_category_pie_chart(df, f"Spending by Category ({time_frame_label})")
        st.altair_chart(chart, use_container_width=True)
    
    with col2:
        st.subheader("Summary")
        total = df['total'].sum()
        st.metric("Total Spending", format_currency(total))
        st.metric("Categories", len(df))
        top_category = df.iloc[0]
        st.metric("Top Category", top_category['category'], f"{top_category['percentage']:.1f}%")
    
    # Display interactive table with drill-down
    st.subheader("Detailed Breakdown")
    st.markdown("*Click a category name to see individual transactions*")
    
    # Create clickable category buttons in a table-like format
    for idx, row in df.iterrows():
        col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
        
        with col1:
            if st.button(
                f"📂 {row['category']}", 
                key=f"cat_{idx}",
                help=f"Click to see transactions in {row['category']}"
            ):
                st.session_state.selected_category = row['category']
                st.rerun()
        
        with col2:
            st.write(format_currency(row['total']))
        
        with col3:
            st.write(f"{row['percentage']:.1f}%")
        
        with col4:
            st.write(f"{row['count']} txn")
    
    # Export button
    st.markdown("---")
    csv = df.to_csv(index=False)
    st.download_button(
        label="📥 Download Category Breakdown CSV",
        data=csv,
        file_name=f"category_breakdown_{time_frame}.csv",
        mime="text/csv"
    )


def render_category_transactions(analytics, category, time_frame, account_id, time_frame_label, include_transfers=False):
    """Render transaction details for a specific category."""
    from data_viewer import DataViewer
    from database_ops import DatabaseManager
    
    # Back button
    col1, col2 = st.columns([1, 5])
    with col1:
        if st.button("⬅️ Back to Categories"):
            st.session_state.selected_category = None
            st.rerun()
    
    with col2:
        st.subheader(f"Transactions in: {category}")
    
    # Check if this is the Transfer category
    is_transfer_category = (category == "Transfer")
    
    if is_transfer_category:
        st.info("ℹ️ Viewing internal transfers (excluded from spending totals by default)")
    elif not include_transfers:
        st.info("ℹ️ Transfers are excluded from this view")
    
    st.markdown(f"*Showing transactions for {time_frame_label}*")
    
    # Get transactions for this category
    try:
        # Parse time frame to get date range
        start_date, end_date = analytics.parse_time_frame(time_frame)
        
        # Get all transactions in this category
        from database_ops import Transaction
        from sqlalchemy import and_
        
        session = analytics.db_manager.get_session()
        
        try:
            # Build query
            query = session.query(Transaction).filter(
                and_(
                    Transaction.date >= start_date,
                    Transaction.date <= end_date,
                    Transaction.category == category
                )
            )
            
            # Filter by account if specified
            if account_id:
                query = query.filter(Transaction.account_id == account_id)
            
            # EXCLUDE transfers unless explicitly included
            if not include_transfers:
                query = query.filter(Transaction.is_transfer == 0)
            
            # EXCLUDE income/deposits (only show expenses - negative amounts)
            query = query.filter(Transaction.amount < 0)
            
            # Order by date descending (most recent first)
            query = query.order_by(Transaction.date.desc())
            
            # Execute query
            transactions = query.all()
            
            if not transactions:
                st.warning(f"No transactions found in category '{category}' for the selected time frame.")
                session.close()
                return
            
            # Convert to DataFrame
            data = []
            for trans in transactions:
                data.append({
                    'id': trans.id,
                    'date': trans.date,
                    'description': trans.description,
                    'amount': trans.amount,
                    'category': trans.category,
                    'account': trans.account or 'Unknown',
                    'source_file': trans.source_file
                })
            
            df = pd.DataFrame(data)
            
        finally:
            session.close()
        
        # Display metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total = df['amount'].sum()
            st.metric("Total Spent", format_currency(abs(total)))
        
        with col2:
            avg = df['amount'].mean()
            st.metric("Average Transaction", format_currency(abs(avg)))
        
        with col3:
            st.metric("Transaction Count", len(df))
        
        # Display transactions table
        st.markdown("---")
        st.subheader("Transaction Details")
        
        # Format for display
        df_display = df.copy()
        df_display['date'] = pd.to_datetime(df_display['date']).dt.strftime('%Y-%m-%d')
        df_display['amount'] = df_display['amount'].apply(lambda x: format_currency(abs(x)))
        
        # Select columns to display
        display_columns = ['date', 'description', 'amount', 'account', 'source_file']
        available_columns = [col for col in display_columns if col in df_display.columns]
        
        # Display table
        st.dataframe(
            df_display[available_columns],
            use_container_width=True,
            hide_index=True,
            column_config={
                "date": "Date",
                "description": "Description",
                "amount": "Amount",
                "account": "Account",
                "source_file": "Source"
            }
        )
        
        # Export transactions
        st.markdown("---")
        csv = df[available_columns].to_csv(index=False)
        st.download_button(
            label=f"📥 Download {category} Transactions CSV",
            data=csv,
            file_name=f"{category.lower().replace(' ', '_')}_transactions_{time_frame}.csv",
            mime="text/csv"
        )
        
    except Exception as e:
        st.error(f"Error loading transactions: {e}")
        logger.error(f"Category drill-down error: {e}", exc_info=True)


def render_income_categories(analytics, report_gen, time_frame, account_id, time_frame_label, include_transfers=False):
    """Render income category breakdown report with drill-down to transactions."""
    st.header(f"Income Category Breakdown ({time_frame_label})")
    
    # Show transfer filter status
    if not include_transfers:
        st.info("ℹ️ Transfers are excluded from income analysis. Toggle 'Include Transfers' in the sidebar to include them.")
    else:
        st.warning("⚠️ Transfers are included in this analysis (may inflate income totals)")
    
    # Initialize session state for selected category
    if 'selected_income_category' not in st.session_state:
        st.session_state.selected_income_category = None
    
    # Check if we're in drill-down mode
    if st.session_state.selected_income_category:
        render_income_category_transactions(
            analytics, 
            st.session_state.selected_income_category, 
            time_frame, 
            account_id, 
            time_frame_label,
            include_transfers
        )
        return
    
    # Get income category data
    df = analytics.get_income_breakdown(
        time_frame=time_frame,
        account_id=account_id,
        include_transfers=include_transfers
    )
    
    if df.empty:
        st.warning("No income data available for the selected time frame.")
        return
    
    # Display chart
    col1, col2 = st.columns([2, 1])
    
    with col1:
        chart = create_category_pie_chart(df, f"Income by Category ({time_frame_label})")
        st.altair_chart(chart, use_container_width=True)
    
    with col2:
        st.subheader("Summary")
        total = df['total'].sum()
        st.metric("Total Income", format_currency(total))
        st.metric("Categories", len(df))
        top_category = df.iloc[0]
        st.metric("Top Category", top_category['category'], f"{top_category['percentage']:.1f}%")
    
    # Display interactive table with drill-down
    st.subheader("Detailed Breakdown")
    st.markdown("*Click a category name to see individual transactions*")
    
    # Create clickable category buttons in a table-like format
    for idx, row in df.iterrows():
        col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
        
        with col1:
            if st.button(
                f"💰 {row['category']}", 
                key=f"income_cat_{idx}",
                help=f"Click to see transactions in {row['category']}"
            ):
                st.session_state.selected_income_category = row['category']
                st.rerun()
        
        with col2:
            st.write(format_currency(row['total']))
        
        with col3:
            st.write(f"{row['percentage']:.1f}%")
        
        with col4:
            st.write(f"{row['count']} txn")
    
    # Export button
    st.markdown("---")
    csv = df.to_csv(index=False)
    st.download_button(
        label="📥 Download Income Category Breakdown CSV",
        data=csv,
        file_name=f"income_category_breakdown_{time_frame}.csv",
        mime="text/csv"
    )


def render_income_category_transactions(analytics, category, time_frame, account_id, time_frame_label, include_transfers=False):
    """Render transaction details for a specific income category."""
    from data_viewer import DataViewer
    from database_ops import DatabaseManager
    
    # Back button
    col1, col2 = st.columns([1, 5])
    with col1:
        if st.button("⬅️ Back to Income Categories"):
            st.session_state.selected_income_category = None
            st.rerun()
    
    with col2:
        st.subheader(f"Income Transactions in: {category}")
    
    # Check if this is the Transfer category
    is_transfer_category = (category == "Transfer")
    
    if is_transfer_category:
        st.info("ℹ️ Viewing internal transfers (excluded from income totals by default)")
    elif not include_transfers:
        st.info("ℹ️ Transfers are excluded from this view")
    
    st.markdown(f"*Showing transactions for {time_frame_label}*")
    
    # Get transactions for this category
    try:
        # Parse time frame to get date range
        start_date, end_date = analytics.parse_time_frame(time_frame)
        
        # Get all transactions in this category
        from database_ops import Transaction
        from sqlalchemy import and_
        
        session = analytics.db_manager.get_session()
        
        try:
            # Build query
            query = session.query(Transaction).filter(
                and_(
                    Transaction.date >= start_date,
                    Transaction.date <= end_date,
                    Transaction.category == category
                )
            )
            
            # Filter by account if specified
            if account_id:
                query = query.filter(Transaction.account_id == account_id)
            
            # EXCLUDE transfers unless explicitly included
            if not include_transfers:
                query = query.filter(Transaction.is_transfer == 0)
            
            # ONLY income/deposits (positive amounts)
            query = query.filter(Transaction.amount > 0)
            
            # Order by date descending (most recent first)
            query = query.order_by(Transaction.date.desc())
            
            # Execute query
            transactions = query.all()
            
            if not transactions:
                st.warning(f"No income transactions found in category '{category}' for the selected time frame.")
                session.close()
                return
            
            # Convert to DataFrame
            data = []
            for trans in transactions:
                data.append({
                    'id': trans.id,
                    'date': trans.date,
                    'description': trans.description,
                    'amount': trans.amount,
                    'category': trans.category,
                    'account': trans.account or 'Unknown',
                    'source_file': trans.source_file
                })
            
            df = pd.DataFrame(data)
            
        finally:
            session.close()
        
        # Display metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total = df['amount'].sum()
            st.metric("Total Received", format_currency(total))
        
        with col2:
            avg = df['amount'].mean()
            st.metric("Average Transaction", format_currency(avg))
        
        with col3:
            st.metric("Transaction Count", len(df))
        
        # Display transactions table
        st.markdown("---")
        st.subheader("Transaction Details")
        
        # Format for display
        df_display = df.copy()
        df_display['date'] = pd.to_datetime(df_display['date']).dt.strftime('%Y-%m-%d')
        df_display['amount'] = df_display['amount'].apply(lambda x: format_currency(x))
        
        # Select columns to display
        display_columns = ['date', 'description', 'amount', 'account', 'source_file']
        available_columns = [col for col in display_columns if col in df_display.columns]
        
        # Display table
        st.dataframe(
            df_display[available_columns],
            use_container_width=True,
            hide_index=True,
            column_config={
                "date": "Date",
                "description": "Description",
                "amount": "Amount",
                "account": "Account",
                "source_file": "Source"
            }
        )
        
        # Export transactions
        st.markdown("---")
        csv = df[available_columns].to_csv(index=False)
        st.download_button(
            label=f"📥 Download {category} Income Transactions CSV",
            data=csv,
            file_name=f"{category.lower().replace(' ', '_')}_income_transactions_{time_frame}.csv",
            mime="text/csv"
        )
        
    except Exception as e:
        st.error(f"Error loading transactions: {e}")
        logger.error(f"Income category drill-down error: {e}", exc_info=True)


def render_trends(analytics, report_gen, time_frame, account_id, time_frame_label):
    """Render monthly trends report."""
    st.header(f"Monthly Trends ({time_frame_label})")
    
    # Comparison dropdown
    comparison_options = ["None", "Previous Month", "Previous Year"]
    comparison_selection = st.selectbox(
        "Compare with:",
        comparison_options,
        key="trends_comparison",
        help="Select a comparison period to overlay on the chart"
    )
    
    # Get trends data
    df = analytics.get_monthly_trends(
        time_frame=time_frame,
        account_id=account_id
    )
    
    if df.empty:
        st.warning("No trend data available for the selected time frame.")
        return
    
    # Determine comparison type and fetch data if needed
    comparison_df = None
    comparison_type = None
    changes = None
    
    if comparison_selection == "Previous Month":
        # Check if we have enough data (need at least 2 months for previous month comparison)
        if len(df) < 2:
            st.warning("⚠️ Previous Month comparison requires at least 2 months of data. Showing current period only.")
        else:
            try:
                comparison_type = 'previous_month'
                comparison_df = analytics.get_comparison_data(
                    current_df=df,
                    comparison_type=comparison_type,
                    account_id=account_id
                )
                if comparison_df.empty:
                    st.warning("⚠️ No data available for previous month comparison.")
                else:
                    changes = analytics.calculate_percentage_changes(df, comparison_df)
            except Exception as e:
                logger.error(f"Error fetching comparison data: {e}", exc_info=True)
                st.error(f"Error loading comparison data: {e}")
    
    elif comparison_selection == "Previous Year":
        try:
            comparison_type = 'previous_year'
            comparison_df = analytics.get_comparison_data(
                current_df=df,
                comparison_type=comparison_type,
                account_id=account_id
            )
            if comparison_df.empty:
                st.warning("⚠️ No data available for previous year comparison.")
            else:
                changes = analytics.calculate_percentage_changes(df, comparison_df)
        except Exception as e:
            logger.error(f"Error fetching comparison data: {e}", exc_info=True)
            st.error(f"Error loading comparison data: {e}")
    
    # Create chart title
    chart_title = f"Monthly Income & Expenses ({time_frame_label})"
    if comparison_selection != "None" and comparison_df is not None and not comparison_df.empty:
        comp_label = "Previous Month" if comparison_type == 'previous_month' else "Previous Year"
        chart_title = f"Monthly Income & Expenses ({time_frame_label}) vs {comp_label}"
    
    # Display chart
    chart = create_monthly_trend_chart(
        df,
        chart_title,
        comparison_df=comparison_df,
        comparison_type=comparison_type
    )
    st.altair_chart(chart, use_container_width=True)
    
    # Display comparison summary if available
    if changes is not None:
        display_comparison_summary(changes, comparison_type)
        st.markdown("---")
    
    # Display summary metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        avg_income = df['income'].mean()
        st.metric("Avg Monthly Income", format_currency(avg_income))
    
    with col2:
        avg_expenses = df['expenses'].mean()
        st.metric("Avg Monthly Expenses", format_currency(avg_expenses))
    
    with col3:
        avg_net = df['net'].mean()
        st.metric("Avg Monthly Net", format_currency(avg_net))
    
    # Display table
    st.subheader("Monthly Data")
    df_display = df.copy()
    df_display['income'] = df_display['income'].apply(format_currency)
    df_display['expenses'] = df_display['expenses'].apply(format_currency)
    df_display['net'] = df_display['net'].apply(format_currency)
    st.dataframe(df_display[['period', 'income', 'expenses', 'net']], use_container_width=True, hide_index=True)
    
    # Export button
    csv = df.to_csv(index=False)
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name=f"monthly_trends_{time_frame}.csv",
        mime="text/csv"
    )


def render_accounts(analytics, report_gen, time_frame, time_frame_label):
    """Render account balances with Assets/Liabilities split and drill-down details."""
    st.header(f"Account Balances")
    
    # Get refined account data with assets/liabilities split
    summary = analytics.get_account_summary_refined()
    
    assets_df = summary['assets']
    liabilities_df = summary['liabilities']
    net_worth = summary['net_worth']
    assets_total = summary['assets_total']
    liabilities_total = summary['liabilities_total']
    
    # Display net worth prominently
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Assets", format_currency(assets_total), delta=None)
    with col2:
        st.metric("Total Liabilities", format_currency(abs(liabilities_total)), delta=None, delta_color="inverse")
    with col3:
        st.metric("Net Worth", format_currency(net_worth), delta=None)
    
    st.markdown("---")
    
    # Assets Section
    if not assets_df.empty:
        st.subheader("Assets")
        
        # Create bar chart for assets
        assets_chart = alt.Chart(assets_df).mark_bar(color='#2ecc71').encode(
            y=alt.Y('name:N', title='Account', sort='-x'),
            x=alt.X('balance:Q', title='Balance ($)'),
            tooltip=[
                alt.Tooltip('name:N', title='Account'),
                alt.Tooltip('type:N', title='Type'),
                alt.Tooltip('balance:Q', title='Balance', format='$,.2f')
            ]
        ).properties(
            title="Assets by Account",
            width=700,
            height=max(200, len(assets_df) * 40)
        )
        st.altair_chart(assets_chart, use_container_width=True)
        
        # Display assets with drill-down
        for _, account in assets_df.iterrows():
            render_account_detail(
                account['id'], 
                account['name'], 
                account['type'], 
                account['balance'],
                is_liability=False,
                analytics=analytics
            )
    else:
        st.info("No asset accounts found.")
    
    st.markdown("---")
    
    # Liabilities Section
    if not liabilities_df.empty:
        st.subheader("Liabilities")
        
        # Create bar chart for liabilities (show as positive values for visualization)
        liabilities_display_chart = liabilities_df.copy()
        liabilities_display_chart['balance_abs'] = liabilities_display_chart['balance'].abs()
        
        liabilities_chart = alt.Chart(liabilities_display_chart).mark_bar(color='#e74c3c').encode(
            y=alt.Y('name:N', title='Account', sort='x'),
            x=alt.X('balance_abs:Q', title='Balance Owed ($)'),
            tooltip=[
                alt.Tooltip('name:N', title='Account'),
                alt.Tooltip('type:N', title='Type'),
                alt.Tooltip('balance_abs:Q', title='Balance Owed', format='$,.2f')
            ]
        ).properties(
            title="Liabilities by Account",
            width=700,
            height=max(200, len(liabilities_df) * 40)
        )
        st.altair_chart(liabilities_chart, use_container_width=True)
        
        # Display liabilities with drill-down
        for _, account in liabilities_df.iterrows():
            render_account_detail(
                account['id'], 
                account['name'], 
                account['type'], 
                account['balance'],
                is_liability=True,
                analytics=analytics
            )
    else:
        st.info("No liability accounts found.")
    
    # Export button for combined data
    st.markdown("---")
    combined_df = pd.concat([assets_df, liabilities_df], ignore_index=True)
    if not combined_df.empty:
        csv = combined_df.to_csv(index=False)
        st.download_button(
            label="Download All Accounts CSV",
            data=csv,
            file_name="account_balances.csv",
            mime="text/csv"
        )


def render_account_detail(account_id, account_name, account_type, balance, is_liability, analytics):
    """Render expandable account detail section with balance calculation breakdown."""
    from account_management import AccountManager
    from database_ops import Transaction
    from datetime import datetime
    
    # Format balance display
    if is_liability:
        balance_display = format_currency(abs(balance)) + " owed"
    else:
        balance_display = format_currency(balance)
    
    # Create expander for drill-down
    with st.expander(f"📊 {account_name} - {balance_display}"):
        account_manager = AccountManager(analytics.db_manager)
        
        # Get balance override info
        overrides = account_manager.get_balance_overrides(account_id)
        
        if overrides:
            st.markdown("#### Balance Calculation")
            latest_override = overrides[0]
            override_date = latest_override['override_date']
            override_balance = latest_override['override_balance']
            
            # Show override details
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Override Balance", format_currency(override_balance))
                st.caption(f"As of {override_date.strftime('%Y-%m-%d')}")
            with col2:
                transactions_after = balance - override_balance
                if is_liability:
                    transactions_after = -balance - override_balance
                st.metric("Transactions Since Override", format_currency(transactions_after))
                st.caption("Net change after override date")
            
            if latest_override['notes']:
                st.info(f"📝 Note: {latest_override['notes']}")
            
            st.markdown("---")
            
            # Get transactions after override date
            session = analytics.db_manager.get_session()
            try:
                recent_transactions = session.query(Transaction).filter(
                    Transaction.account_id == account_id,
                    Transaction.date > override_date
                ).order_by(Transaction.date.desc()).limit(50).all()
                
                if recent_transactions:
                    st.markdown(f"#### Recent Transactions (since {override_date.strftime('%Y-%m-%d')})")
                    
                    # Create DataFrame for transactions
                    trans_data = []
                    running_balance = override_balance
                    for trans in reversed(recent_transactions):
                        running_balance += trans.amount
                        trans_data.append({
                            'Date': trans.date.strftime('%Y-%m-%d'),
                            'Description': trans.description,
                            'Amount': trans.amount,
                            'Running Balance': running_balance,
                            'Category': trans.category or 'Uncategorized'
                        })
                    
                    trans_df = pd.DataFrame(reversed(trans_data))
                    
                    # Format for display
                    trans_display = trans_df.copy()
                    trans_display['Amount'] = trans_display['Amount'].apply(format_currency)
                    trans_display['Running Balance'] = trans_display['Running Balance'].apply(format_currency)
                    
                    st.dataframe(trans_display, use_container_width=True, hide_index=True, height=300)
                    
                    # Transaction summary
                    total_income = trans_df[trans_df['Amount'] > 0]['Amount'].sum()
                    total_expenses = abs(trans_df[trans_df['Amount'] < 0]['Amount'].sum())
                    net_change = total_income - total_expenses
                    
                    st.markdown("**Transaction Summary:**")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Income", format_currency(total_income))
                    with col2:
                        st.metric("Expenses", format_currency(total_expenses))
                    with col3:
                        st.metric("Net Change", format_currency(net_change))
                else:
                    st.info("No transactions since override date.")
            finally:
                session.close()
        else:
            # No override - show all transactions
            st.markdown("#### All Transactions")
            st.caption("This account has no balance override. Balance is calculated from all transactions.")
            
            session = analytics.db_manager.get_session()
            try:
                all_transactions = session.query(Transaction).filter(
                    Transaction.account_id == account_id
                ).order_by(Transaction.date.desc()).limit(100).all()
                
                if all_transactions:
                    # Create DataFrame for transactions
                    trans_data = []
                    for trans in all_transactions:
                        trans_data.append({
                            'Date': trans.date.strftime('%Y-%m-%d'),
                            'Description': trans.description,
                            'Amount': trans.amount,
                            'Category': trans.category or 'Uncategorized'
                        })
                    
                    trans_df = pd.DataFrame(trans_data)
                    
                    # Format for display
                    trans_display = trans_df.copy()
                    trans_display['Amount'] = trans_display['Amount'].apply(format_currency)
                    
                    st.dataframe(trans_display, use_container_width=True, hide_index=True, height=300)
                    
                    # Transaction summary
                    total_income = trans_df[trans_df['Amount'] > 0]['Amount'].sum()
                    total_expenses = abs(trans_df[trans_df['Amount'] < 0]['Amount'].sum())
                    net_change = total_income - total_expenses
                    
                    st.markdown("**Transaction Summary:**")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total Income", format_currency(total_income))
                    with col2:
                        st.metric("Total Expenses", format_currency(total_expenses))
                    with col3:
                        st.metric("Net Balance", format_currency(net_change))
                    
                    if len(all_transactions) == 100:
                        st.caption("⚠️ Showing most recent 100 transactions only")
                else:
                    st.warning("No transactions found for this account.")
            finally:
                session.close()


def render_comparison(analytics, report_gen):
    """Render period comparison report."""
    st.header("Period Comparison")
    
    # Get comparison data
    periods = ['1m', '3m', '6m', '12m']
    df = analytics.get_comparison_periods(periods)
    
    if df.empty:
        st.warning("No data available for comparison.")
        return
    
    # Reshape for visualization
    df_melted = df.melt(
        id_vars=['period'],
        value_vars=['income', 'expenses', 'net'],
        var_name='type',
        value_name='amount'
    )
    
    # Create chart
    chart = alt.Chart(df_melted).mark_bar().encode(
        x=alt.X('period:N', title='Period'),
        y=alt.Y('amount:Q', title='Amount ($)'),
        color=alt.Color('type:N', scale=alt.Scale(domain=['income', 'expenses', 'net'], range=['#2ecc71', '#e74c3c', '#3498db'])),
        xOffset='type:N',
        tooltip=[
            alt.Tooltip('period:N', title='Period'),
            alt.Tooltip('type:N', title='Type'),
            alt.Tooltip('amount:Q', title='Amount', format='$,.2f')
        ]
    ).properties(
        title="Period Comparison",
        width=700,
        height=400
    )
    
    st.altair_chart(chart, use_container_width=True)
    
    # Display table
    st.subheader("Comparison Data")
    df_display = df.copy()
    df_display['income'] = df_display['income'].apply(format_currency)
    df_display['expenses'] = df_display['expenses'].apply(format_currency)
    df_display['net'] = df_display['net'].apply(format_currency)
    st.dataframe(df_display, use_container_width=True, hide_index=True)


def render_budget_tab(db_manager):
    """Render YNAB-style budget dashboard."""
    from ui_budgeting import render_budget_dashboard
    
    try:
        render_budget_dashboard(db_manager)
    except Exception as e:
        st.error(f"Error loading budget dashboard: {e}")
        logger.error(f"Budget UI error: {e}", exc_info=True)
        st.info("💡 Tip: Make sure you have imported transactions with categories!")


if __name__ == "__main__":
    main_ui_analytics()

