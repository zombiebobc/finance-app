"""
Report generator module for formatting analytics data.

This module provides functions to format analytics data into
various output formats including text tables, CSV, and visualizations.
"""

import logging
from typing import Dict, Optional
from pathlib import Path
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for CLI
import matplotlib.pyplot as plt
from io import BytesIO

logger = logging.getLogger(__name__)


class ReportGenerator:
    """
    Generate formatted reports from analytics data.
    
    Supports multiple output formats: text tables, CSV, and
    visualizations (matplotlib for CLI/export, designed to
    work alongside Altair for web UI).
    """
    
    def __init__(self):
        """Initialize the report generator."""
        logger.info("Report generator initialized")
    
    def format_currency(self, amount: float) -> str:
        """
        Format amount as currency string.
        
        Args:
            amount: Amount to format
        
        Returns:
            Formatted currency string
        """
        return f"${amount:,.2f}"
    
    def format_percentage(self, percentage: float) -> str:
        """
        Format percentage string.
        
        Args:
            percentage: Percentage value
        
        Returns:
            Formatted percentage string
        """
        return f"{percentage:.1f}%"
    
    def generate_income_expense_report(
        self,
        summary: Dict,
        time_frame: str = 'all'
    ) -> str:
        """
        Generate text report for income/expense summary.
        
        Args:
            summary: Summary dictionary from analytics
            time_frame: Time frame label
        
        Returns:
            Formatted text report
        """
        report_lines = [
            "=" * 80,
            f"INCOME & EXPENSE SUMMARY ({time_frame})",
            "=" * 80,
            "",
            f"Total Income:           {self.format_currency(summary['total_income']):>20}  ({summary['income_count']} transactions)",
            f"Total Expenses:         {self.format_currency(summary['total_expenses']):>20}  ({summary['expense_count']} transactions)",
            "-" * 80,
            f"Net Change:             {self.format_currency(summary['net_change']):>20}",
            "",
            f"Total Transactions:     {summary['total_count']:>20}",
            "=" * 80
        ]
        
        return "\n".join(report_lines)
    
    def generate_category_report(
        self,
        df: pd.DataFrame,
        time_frame: str = 'all',
        top_n: Optional[int] = None
    ) -> str:
        """
        Generate text report for category breakdown.
        
        Args:
            df: Category breakdown DataFrame
            time_frame: Time frame label
            top_n: Optional limit to top N categories
        
        Returns:
            Formatted text report
        """
        if df.empty:
            return f"\nNo spending data found for time frame: {time_frame}\n"
        
        if top_n:
            df = df.head(top_n)
        
        report_lines = [
            "=" * 100,
            f"CATEGORY BREAKDOWN ({time_frame})",
            "=" * 100,
            "",
            f"{'Category':<30} {'Total':>15} {'Count':>10} {'Percentage':>12}",
            "-" * 100
        ]
        
        for _, row in df.iterrows():
            report_lines.append(
                f"{row['category']:<30} "
                f"{self.format_currency(row['total']):>15} "
                f"{int(row['count']):>10} "
                f"{self.format_percentage(row['percentage']):>12}"
            )
        
        total = df['total'].sum()
        count = df['count'].sum()
        
        report_lines.extend([
            "-" * 100,
            f"{'TOTAL':<30} {self.format_currency(total):>15} {int(count):>10} {'100.0%':>12}",
            "=" * 100
        ])
        
        return "\n".join(report_lines)
    
    def generate_monthly_trends_report(
        self,
        df: pd.DataFrame,
        time_frame: str = 'all'
    ) -> str:
        """
        Generate text report for monthly trends.
        
        Args:
            df: Monthly trends DataFrame
            time_frame: Time frame label
        
        Returns:
            Formatted text report
        """
        if df.empty:
            return f"\nNo trend data found for time frame: {time_frame}\n"
        
        report_lines = [
            "=" * 100,
            f"MONTHLY TRENDS ({time_frame})",
            "=" * 100,
            "",
            f"{'Period':<12} {'Income':>15} {'Expenses':>15} {'Net':>15} {'Net %':>10}",
            "-" * 100
        ]
        
        for _, row in df.iterrows():
            net_pct = (row['net'] / row['income'] * 100) if row['income'] > 0 else 0
            report_lines.append(
                f"{row['period']:<12} "
                f"{self.format_currency(row['income']):>15} "
                f"{self.format_currency(row['expenses']):>15} "
                f"{self.format_currency(row['net']):>15} "
                f"{self.format_percentage(net_pct):>10}"
            )
        
        # Calculate averages
        avg_income = df['income'].mean()
        avg_expenses = df['expenses'].mean()
        avg_net = df['net'].mean()
        
        report_lines.extend([
            "-" * 100,
            f"{'AVERAGE':<12} "
            f"{self.format_currency(avg_income):>15} "
            f"{self.format_currency(avg_expenses):>15} "
            f"{self.format_currency(avg_net):>15}",
            "=" * 100
        ])
        
        return "\n".join(report_lines)
    
    def generate_account_summary_report(
        self,
        df: pd.DataFrame,
        time_frame: str = 'all'
    ) -> str:
        """
        Generate text report for account summary.
        
        Args:
            df: Account summary DataFrame
            time_frame: Time frame label
        
        Returns:
            Formatted text report
        """
        if df.empty:
            return f"\nNo account data found for time frame: {time_frame}\n"
        
        report_lines = [
            "=" * 110,
            f"ACCOUNT SUMMARY ({time_frame})",
            "=" * 110,
            "",
            f"{'Account':<35} {'Type':<12} {'Income':>15} {'Expenses':>15} {'Net':>15} {'Count':>8}",
            "-" * 110
        ]
        
        for _, row in df.iterrows():
            report_lines.append(
                f"{row['account_name']:<35} "
                f"{row['type']:<12} "
                f"{self.format_currency(row['income']):>15} "
                f"{self.format_currency(row['expenses']):>15} "
                f"{self.format_currency(row['net']):>15} "
                f"{int(row['count']):>8}"
            )
        
        total_income = df['income'].sum()
        total_expenses = df['expenses'].sum()
        total_net = df['net'].sum()
        total_count = df['count'].sum()
        
        report_lines.extend([
            "-" * 110,
            f"{'TOTAL':<35} {'':<12} "
            f"{self.format_currency(total_income):>15} "
            f"{self.format_currency(total_expenses):>15} "
            f"{self.format_currency(total_net):>15} "
            f"{int(total_count):>8}",
            "=" * 110
        ])
        
        return "\n".join(report_lines)
    
    def export_to_csv(
        self,
        df: pd.DataFrame,
        output_path: Path,
        report_name: str = "report"
    ) -> None:
        """
        Export DataFrame to CSV file.
        
        Args:
            df: DataFrame to export
            output_path: Output file path
            report_name: Name of the report for logging
        """
        try:
            df.to_csv(output_path, index=False)
            logger.info(f"Exported {report_name} to {output_path}")
        except Exception as e:
            logger.error(f"Failed to export {report_name}: {e}")
            raise
    
    def create_category_pie_chart(
        self,
        df: pd.DataFrame,
        output_path: Optional[Path] = None,
        title: str = "Spending by Category",
        top_n: int = 10
    ) -> Optional[BytesIO]:
        """
        Create pie chart for category breakdown.
        
        Args:
            df: Category breakdown DataFrame
            output_path: Optional file path to save chart
            title: Chart title
            top_n: Number of top categories to show
        
        Returns:
            BytesIO object if output_path is None, otherwise None
        """
        if df.empty:
            logger.warning("No data to plot pie chart")
            return None
        
        # Take top N categories, group rest as "Other"
        df_plot = df.head(top_n).copy()
        if len(df) > top_n:
            other_total = df.iloc[top_n:]['total'].sum()
            other_row = pd.DataFrame([{
                'category': 'Other',
                'total': other_total,
                'count': df.iloc[top_n:]['count'].sum(),
                'percentage': df.iloc[top_n:]['percentage'].sum()
            }])
            df_plot = pd.concat([df_plot, other_row], ignore_index=True)
        
        # Create figure
        fig, ax = plt.subplots(figsize=(10, 8))
        
        # Create pie chart
        wedges, texts, autotexts = ax.pie(
            df_plot['total'],
            labels=df_plot['category'],
            autopct='%1.1f%%',
            startangle=90,
            textprops={'fontsize': 10}
        )
        
        ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
        
        # Equal aspect ratio ensures pie is circular
        ax.axis('equal')
        
        plt.tight_layout()
        
        # Save or return
        if output_path:
            plt.savefig(output_path, dpi=150, bbox_inches='tight')
            logger.info(f"Saved pie chart to {output_path}")
            plt.close()
            return None
        else:
            buf = BytesIO()
            plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
            buf.seek(0)
            plt.close()
            return buf
    
    def create_monthly_trend_chart(
        self,
        df: pd.DataFrame,
        output_path: Optional[Path] = None,
        title: str = "Monthly Income & Expenses"
    ) -> Optional[BytesIO]:
        """
        Create bar chart for monthly trends.
        
        Args:
            df: Monthly trends DataFrame
            output_path: Optional file path to save chart
            title: Chart title
        
        Returns:
            BytesIO object if output_path is None, otherwise None
        """
        if df.empty:
            logger.warning("No data to plot trend chart")
            return None
        
        # Create figure
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Set up x-axis
        x = range(len(df))
        width = 0.35
        
        # Create bars
        bars1 = ax.bar([i - width/2 for i in x], df['income'], width, label='Income', color='#2ecc71')
        bars2 = ax.bar([i + width/2 for i in x], df['expenses'], width, label='Expenses', color='#e74c3c')
        
        # Add net line
        ax2 = ax.twinx()
        line = ax2.plot(x, df['net'], color='#3498db', marker='o', linewidth=2, label='Net')
        ax2.set_ylabel('Net ($)', fontsize=11)
        ax2.axhline(y=0, color='gray', linestyle='--', linewidth=0.5)
        ax2.grid(True, alpha=0.3)
        
        # Labels and formatting
        ax.set_xlabel('Period', fontsize=11)
        ax.set_ylabel('Amount ($)', fontsize=11)
        ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
        ax.set_xticks(x)
        ax.set_xticklabels(df['period'], rotation=45, ha='right')
        
        # Legends
        ax.legend(loc='upper left')
        ax2.legend(loc='upper right')
        
        plt.tight_layout()
        
        # Save or return
        if output_path:
            plt.savefig(output_path, dpi=150, bbox_inches='tight')
            logger.info(f"Saved trend chart to {output_path}")
            plt.close()
            return None
        else:
            buf = BytesIO()
            plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
            buf.seek(0)
            plt.close()
            return buf
    
    def create_comparison_chart(
        self,
        df: pd.DataFrame,
        output_path: Optional[Path] = None,
        title: str = "Period Comparison"
    ) -> Optional[BytesIO]:
        """
        Create comparison bar chart for multiple periods.
        
        Args:
            df: Comparison DataFrame
            output_path: Optional file path to save chart
            title: Chart title
        
        Returns:
            BytesIO object if output_path is None, otherwise None
        """
        if df.empty:
            logger.warning("No data to plot comparison chart")
            return None
        
        # Create figure
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Set up x-axis
        x = range(len(df))
        width = 0.25
        
        # Create bars
        bars1 = ax.bar([i - width for i in x], df['income'], width, label='Income', color='#2ecc71')
        bars2 = ax.bar(x, df['expenses'], width, label='Expenses', color='#e74c3c')
        bars3 = ax.bar([i + width for i in x], df['net'], width, label='Net', color='#3498db')
        
        # Labels and formatting
        ax.set_xlabel('Period', fontsize=11)
        ax.set_ylabel('Amount ($)', fontsize=11)
        ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
        ax.set_xticks(x)
        ax.set_xticklabels(df['period'])
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
        ax.axhline(y=0, color='black', linewidth=0.5)
        
        plt.tight_layout()
        
        # Save or return
        if output_path:
            plt.savefig(output_path, dpi=150, bbox_inches='tight')
            logger.info(f"Saved comparison chart to {output_path}")
            plt.close()
            return None
        else:
            buf = BytesIO()
            plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
            buf.seek(0)
            plt.close()
            return buf

