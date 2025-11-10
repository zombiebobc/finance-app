"""
Visualization components for the financial dashboard.

This module provides reusable Streamlit and Altair components for
displaying financial data in an engaging and interactive way.
"""

import logging
from typing import Optional, Callable, Dict
import pandas as pd
import streamlit as st
import altair as alt

logger = logging.getLogger(__name__)

# Account type icons
ACCOUNT_ICONS = {
    'bank': '🏦',
    'credit': '💳',
    'investment': '📈',
    'savings': '💰',
    'cash': '💵',
    'other': '📊'
}

# Color scheme
COLORS = {
    'positive': '#2ecc71',  # Green
    'negative': '#e74c3c',  # Red
    'neutral': '#95a5a6',   # Gray
    'primary': '#3498db',   # Blue
    'warning': '#f39c12'    # Orange
}


def format_currency(amount: float) -> str:
    """Format amount as currency with proper sign."""
    return f"${amount:,.2f}"


def kpi_metric(
    label: str,
    value: float,
    color_logic: Optional[Callable[[float], str]] = None,
    help_text: Optional[str] = None
) -> None:
    """
    Display a large KPI metric with color coding.
    
    Args:
        label: Metric label
        value: Metric value
        color_logic: Optional function that returns color based on value
        help_text: Optional help text to display
    """
    # Default color logic: positive green, negative red, zero neutral
    if color_logic is None:
        def default_color(v):
            if v > 0:
                return COLORS['positive']
            elif v < 0:
                return COLORS['negative']
            else:
                return COLORS['neutral']
        color_logic = default_color
    
    color = color_logic(value)
    
    # Use Streamlit metric with custom CSS
    st.markdown(
        f"""
        <style>
        .kpi-container {{
            padding: 1rem;
            border-radius: 0.5rem;
            background-color: rgba(255, 255, 255, 0.05);
            border-left: 4px solid {color};
        }}
        .kpi-label {{
            font-size: 0.875rem;
            color: #aaaaaa;
            margin-bottom: 0.25rem;
        }}
        .kpi-value {{
            font-size: 2rem;
            font-weight: bold;
            color: {color};
        }}
        </style>
        """,
        unsafe_allow_html=True
    )
    
    st.metric(
        label=label,
        value=format_currency(value),
        help=help_text
    )


def account_card(
    account: pd.Series,
    balance_history: Optional[pd.DataFrame] = None,
    show_sparkline: bool = True,
    card_color: Optional[str] = None
) -> None:
    """
    Display an account card with balance and optional sparkline.
    
    Args:
        account: Series with account data (id, name, type, balance)
        balance_history: Optional DataFrame with date and balance columns
        show_sparkline: Whether to show sparkline chart
        card_color: Optional color for card accent (matches pie chart)
    """
    icon = ACCOUNT_ICONS.get(account['type'], ACCOUNT_ICONS['other'])
    balance = account['balance']
    
    # Determine color based on balance
    if balance >= 0:
        color = COLORS['positive']
        balance_str = format_currency(balance)
    else:
        color = COLORS['negative']
        balance_str = format_currency(abs(balance))
    
    # Use card color if provided, otherwise use default
    border_color = card_color if card_color else color
    
    # Create card with colored border matching pie chart
    st.markdown(
        f"""
        <div style='
            padding: 0.75rem;
            border-left: 5px solid {border_color};
            background-color: rgba(255, 255, 255, 0.02);
            border-radius: 0.25rem;
            margin-bottom: 0.5rem;
        '>
        </div>
        """,
        unsafe_allow_html=True
    )
    
    # Create card layout
    col1, col2, col3 = st.columns([1, 3, 2])
    
    with col1:
        st.markdown(f"<div style='font-size: 2rem;'>{icon}</div>", unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"**{account['name']}**")
        st.caption(f"{account['type'].title()}")
    
    with col3:
        st.markdown(
            f"<div style='text-align: right; font-size: 1.25rem; font-weight: bold; color: {color};'>{balance_str}</div>",
            unsafe_allow_html=True
        )
    
    # Show sparkline if history provided
    if show_sparkline and balance_history is not None and not balance_history.empty:
        try:
            sparkline = create_sparkline_chart(balance_history)
            st.altair_chart(sparkline, use_container_width=True)
        except Exception as e:
            logger.warning(f"Failed to create sparkline: {e}")


def create_sparkline_chart(df: pd.DataFrame) -> alt.Chart:
    """
    Create a small sparkline chart showing balance trend.
    
    Args:
        df: DataFrame with 'date' and 'balance' columns
    
    Returns:
        Altair chart object
    """
    if df.empty:
        return alt.Chart(pd.DataFrame()).mark_text()
    
    # Determine color based on trend
    if len(df) > 1:
        trend = df['balance'].iloc[-1] - df['balance'].iloc[0]
        color = COLORS['positive'] if trend >= 0 else COLORS['negative']
    else:
        color = COLORS['neutral']
    
    chart = alt.Chart(df).mark_line(
        strokeWidth=2,
        color=color,
        point=False
    ).encode(
        x=alt.X('date:T', axis=None),
        y=alt.Y('balance:Q', axis=None, scale=alt.Scale(zero=False)),
        tooltip=[
            alt.Tooltip('date:T', title='Date', format='%Y-%m-%d'),
            alt.Tooltip('balance:Q', title='Balance', format='$,.2f')
        ]
    ).properties(
        height=50
    ).configure_view(
        strokeWidth=0
    )
    
    return chart


def net_worth_progress(
    current: float,
    goal: float,
    show_details: bool = True
) -> None:
    """
    Display net worth progress towards goal with progress bar.
    
    Args:
        current: Current net worth
        goal: Target net worth goal
        show_details: Whether to show detailed metrics
    """
    if goal <= 0:
        st.warning("⚠️ Please set a positive net worth goal in the sidebar")
        return
    
    # Calculate progress
    progress = min(current / goal, 1.0) if goal > 0 else 0.0
    remaining = goal - current
    percentage = progress * 100
    
    # Display progress bar
    st.markdown("### 🎯 Net Worth Goal Progress")
    
    if show_details:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Current", format_currency(current))
        with col2:
            st.metric("Goal", format_currency(goal))
        with col3:
            st.metric("Remaining", format_currency(remaining))
    
    # Progress bar with custom styling
    st.progress(progress)
    
    # Progress text
    if current >= goal:
        st.success(f"🎉 Congratulations! You've reached {percentage:.1f}% of your goal!")
    elif progress >= 0.75:
        st.info(f"💪 Great progress! {percentage:.1f}% complete")
    elif progress >= 0.5:
        st.info(f"📈 Halfway there! {percentage:.1f}% complete")
    else:
        st.info(f"🚀 Keep going! {percentage:.1f}% complete")


def create_asset_liability_pie(
    assets_df: pd.DataFrame,
    liabilities_df: pd.DataFrame,
    chart_type: str = 'assets'
) -> alt.Chart:
    """
    Create interactive pie chart for asset or liability distribution.
    
    Args:
        assets_df: DataFrame with asset accounts
        liabilities_df: DataFrame with liability accounts
        chart_type: 'assets' or 'liabilities'
    
    Returns:
        Altair chart object
    """
    df = assets_df if chart_type == 'assets' else liabilities_df
    
    if df.empty:
        return alt.Chart(pd.DataFrame({'message': ['No data']})).mark_text(size=16).encode(
            text='message:N'
        )
    
    # For liabilities, use absolute values for visualization
    plot_df = df.copy()
    if chart_type == 'liabilities':
        plot_df['balance'] = plot_df['balance'].abs()
    
    # Calculate percentages
    total = plot_df['balance'].sum()
    plot_df['percentage'] = (plot_df['balance'] / total * 100) if total > 0 else 0
    
    # Create distinct color palette - more contrasting colors for better differentiation
    if chart_type == 'assets':
        # Green spectrum - distinct shades
        color_range = [
            '#00b894',  # Bright green
            '#00cec9',  # Cyan
            '#0984e3',  # Blue
            '#6c5ce7',  # Purple
            '#fdcb6e',  # Yellow
            '#e17055',  # Orange
            '#74b9ff',  # Light blue
            '#a29bfe',  # Light purple
        ]
    else:
        # Orange/Red spectrum - distinct warm shades
        color_range = [
            '#ff7675',  # Bright red
            '#fd79a8',  # Pink
            '#fdcb6e',  # Yellow
            '#e17055',  # Orange
            '#d63031',  # Dark red
            '#e84393',  # Magenta
            '#fab1a0',  # Light orange
            '#ff7675',  # Coral
        ]
    
    # Create color scale with distinct colors
    color_scale = alt.Scale(
        domain=list(plot_df['name']),
        range=color_range[:len(plot_df)]
    )
    
    chart = alt.Chart(plot_df).mark_arc(
        innerRadius=40,
        stroke='#2d3436',  # Dark border between slices for better separation
        strokeWidth=2
    ).encode(
        theta=alt.Theta(field='balance', type='quantitative'),
        color=alt.Color(
            field='name',
            type='nominal',
            scale=color_scale,
            legend=alt.Legend(
                title=f"{chart_type.title()}",
                orient='right',
                labelFontSize=12,
                titleFontSize=14,
                symbolSize=150,
                symbolStrokeWidth=2
            )
        ),
        tooltip=[
            alt.Tooltip('name:N', title='Account'),
            alt.Tooltip('type:N', title='Type'),
            alt.Tooltip('balance:Q', title='Balance', format='$,.2f'),
            alt.Tooltip('percentage:Q', title='Percentage', format='.1f')
        ]
    ).properties(
        width=250,
        height=250,
        title=alt.TitleParams(
            text=f"{chart_type.title()} Distribution",
            fontSize=16,
            fontWeight='bold'
        )
    )
    
    return chart


def create_net_worth_trend_chart(df: pd.DataFrame) -> alt.Chart:
    """
    Create area chart showing net worth trend over time.
    
    Args:
        df: DataFrame with 'date' and 'net_worth' columns
    
    Returns:
        Altair chart object
    """
    if df.empty:
        return alt.Chart(pd.DataFrame({'message': ['No data']})).mark_text(size=16).encode(
            text='message:N'
        )
    
    # Determine color based on overall trend
    if len(df) > 1:
        trend = df['net_worth'].iloc[-1] - df['net_worth'].iloc[0]
        color = COLORS['positive'] if trend >= 0 else COLORS['negative']
    else:
        color = COLORS['primary']
    
    chart = alt.Chart(df).mark_area(
        line={'color': color, 'strokeWidth': 2},
        color=alt.Gradient(
            gradient='linear',
            stops=[
                alt.GradientStop(color=color, offset=0),
                alt.GradientStop(color='rgba(255,255,255,0)', offset=1)
            ],
            x1=0, x2=0, y1=0, y2=1
        )
    ).encode(
        x=alt.X('date:T', title='Date', axis=alt.Axis(format='%b %d')),
        y=alt.Y('net_worth:Q', title='Net Worth ($)', axis=alt.Axis(format='$,.0f')),
        tooltip=[
            alt.Tooltip('date:T', title='Date', format='%Y-%m-%d'),
            alt.Tooltip('net_worth:Q', title='Net Worth', format='$,.2f')
        ]
    ).properties(
        height=200,
        title='Net Worth Trend'
    ).configure_axis(
        gridOpacity=0.3
    )
    
    return chart


def account_summary_table(df: pd.DataFrame, account_type: str) -> None:
    """
    Display account summary as formatted table.
    
    Args:
        df: DataFrame with account data
        account_type: 'assets' or 'liabilities'
    """
    if df.empty:
        st.info(f"No {account_type} accounts found")
        return
    
    # Format display
    display_df = df.copy()
    
    # Add icons
    display_df[''] = display_df['type'].apply(lambda t: ACCOUNT_ICONS.get(t, ACCOUNT_ICONS['other']))
    
    # Format balance
    if account_type == 'liabilities':
        display_df['Balance'] = display_df['balance'].abs().apply(format_currency)
    else:
        display_df['Balance'] = display_df['balance'].apply(format_currency)
    
    # Rename columns
    display_df = display_df[['', 'name', 'type', 'Balance']]
    display_df.columns = ['', 'Account', 'Type', 'Balance']
    
    # Display table
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            '': st.column_config.TextColumn(width="small"),
            'Account': st.column_config.TextColumn(width="large"),
            'Type': st.column_config.TextColumn(width="medium"),
            'Balance': st.column_config.TextColumn(width="medium")
        }
    )

