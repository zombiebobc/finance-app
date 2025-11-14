"""
CLI interface for financial analytics and reporting.

This module provides command-line interface functions for
generating and displaying analytical reports.
"""

import logging
import sys
from pathlib import Path
from typing import Optional
import argparse

from database_ops import DatabaseManager, AccountType
from analytics import AnalyticsEngine
from report_generator import ReportGenerator

logger = logging.getLogger(__name__)


class CLIAnalytics:
    """
    Command-line interface for analytics.
    
    Provides functions to generate and display various analytical
    reports in the terminal, with optional export capabilities.
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize CLI analytics interface.
        
        Args:
            db_manager: Database manager instance
        """
        self.db_manager = db_manager
        self.analytics = AnalyticsEngine(db_manager)
        self.report_gen = ReportGenerator()
        logger.info("CLI analytics initialized")
    
    def run_summary_report(
        self,
        time_frame: str = 'all',
        account_id: Optional[int] = None,
        account_type: Optional[str] = None,
        export_path: Optional[Path] = None
    ) -> None:
        """
        Generate and display income/expense summary report.
        
        Args:
            time_frame: Time frame for analysis
            account_id: Optional account ID filter
            account_type: Optional account type filter
            export_path: Optional path to export report
        """
        try:
            # Parse account type
            acc_type = None
            if account_type:
                acc_type = AccountType[account_type.upper()]
            
            # Get summary data
            summary = self.analytics.get_income_expense_summary(
                time_frame=time_frame,
                account_id=account_id,
                account_type=acc_type
            )
            
            # Generate report
            report = self.report_gen.generate_income_expense_report(summary, time_frame)
            
            # Display
            print("\n" + report + "\n")
            
            # Export if requested
            if export_path:
                with open(export_path, 'w') as f:
                    f.write(report)
                print(f"Report exported to: {export_path}\n")
            
        except Exception as e:
            logger.error(f"Failed to generate summary report: {e}", exc_info=True)
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
    
    def run_category_report(
        self,
        time_frame: str = 'all',
        account_id: Optional[int] = None,
        account_type: Optional[str] = None,
        top_n: Optional[int] = None,
        export_csv: Optional[Path] = None,
        export_chart: Optional[Path] = None
    ) -> None:
        """
        Generate and display category breakdown report.
        
        Args:
            time_frame: Time frame for analysis
            account_id: Optional account ID filter
            account_type: Optional account type filter
            top_n: Optional limit to top N categories
            export_csv: Optional path to export CSV
            export_chart: Optional path to export chart
        """
        try:
            # Parse account type
            acc_type = None
            if account_type:
                acc_type = AccountType[account_type.upper()]
            
            # Get category data
            df = self.analytics.get_category_breakdown(
                time_frame=time_frame,
                account_id=account_id,
                account_type=acc_type,
                expense_only=True
            )
            
            # Generate report
            report = self.report_gen.generate_category_report(df, time_frame, top_n)
            
            # Display
            print("\n" + report + "\n")
            
            # Export CSV if requested
            if export_csv:
                self.report_gen.export_to_csv(df, export_csv, "category_breakdown")
                print(f"CSV exported to: {export_csv}\n")
            
            # Export chart if requested
            if export_chart:
                self.report_gen.create_category_pie_chart(
                    df,
                    output_path=export_chart,
                    title=f"Spending by Category ({time_frame})",
                    top_n=10
                )
                print(f"Chart exported to: {export_chart}\n")
            
        except Exception as e:
            logger.error(f"Failed to generate category report: {e}", exc_info=True)
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
    
    def run_trends_report(
        self,
        time_frame: str = '12m',
        account_id: Optional[int] = None,
        account_type: Optional[str] = None,
        export_csv: Optional[Path] = None,
        export_chart: Optional[Path] = None
    ) -> None:
        """
        Generate and display monthly trends report.
        
        Args:
            time_frame: Time frame for analysis
            account_id: Optional account ID filter
            account_type: Optional account type filter
            export_csv: Optional path to export CSV
            export_chart: Optional path to export chart
        """
        try:
            # Parse account type
            acc_type = None
            if account_type:
                acc_type = AccountType[account_type.upper()]
            
            # Get trends data
            df = self.analytics.get_monthly_trends(
                time_frame=time_frame,
                account_id=account_id,
                account_type=acc_type
            )
            
            # Generate report
            report = self.report_gen.generate_monthly_trends_report(df, time_frame)
            
            # Display
            print("\n" + report + "\n")
            
            # Export CSV if requested
            if export_csv:
                self.report_gen.export_to_csv(df, export_csv, "monthly_trends")
                print(f"CSV exported to: {export_csv}\n")
            
            # Export chart if requested
            if export_chart:
                self.report_gen.create_monthly_trend_chart(
                    df,
                    output_path=export_chart,
                    title=f"Monthly Trends ({time_frame})"
                )
                print(f"Chart exported to: {export_chart}\n")
            
        except Exception as e:
            logger.error(f"Failed to generate trends report: {e}", exc_info=True)
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
    
    def run_accounts_report(
        self,
        time_frame: str = 'all',
        export_csv: Optional[Path] = None
    ) -> None:
        """
        Generate and display account summary report.
        
        Args:
            time_frame: Time frame for analysis
            export_csv: Optional path to export CSV
        """
        try:
            # Get account data
            df = self.analytics.get_account_summary(time_frame=time_frame)
            
            # Generate report
            report = self.report_gen.generate_account_summary_report(df, time_frame)
            
            # Display
            print("\n" + report + "\n")
            
            # Export CSV if requested
            if export_csv:
                self.report_gen.export_to_csv(df, export_csv, "account_summary")
                print(f"CSV exported to: {export_csv}\n")
            
        except Exception as e:
            logger.error(f"Failed to generate accounts report: {e}", exc_info=True)
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
    
    def run_comparison_report(
        self,
        periods: list,
        export_chart: Optional[Path] = None
    ) -> None:
        """
        Generate and display period comparison report.
        
        Args:
            periods: List of time frame strings
            export_chart: Optional path to export chart
        """
        try:
            # Get comparison data
            df = self.analytics.get_comparison_periods(periods)
            
            # Display table
            print("\n" + "=" * 80)
            print(f"PERIOD COMPARISON")
            print("=" * 80)
            print()
            print(f"{'Period':<12} {'Income':>15} {'Expenses':>15} {'Net':>15} {'Transactions':>12}")
            print("-" * 80)
            
            for _, row in df.iterrows():
                print(
                    f"{row['period']:<12} "
                    f"{self.report_gen.format_currency(row['income']):>15} "
                    f"{self.report_gen.format_currency(row['expenses']):>15} "
                    f"{self.report_gen.format_currency(row['net']):>15} "
                    f"{int(row['transactions']):>12}"
                )
            
            print("=" * 80)
            print()
            
            # Export chart if requested
            if export_chart:
                self.report_gen.create_comparison_chart(
                    df,
                    output_path=export_chart,
                    title="Period Comparison"
                )
                print(f"Chart exported to: {export_chart}\n")
            
        except Exception as e:
            logger.error(f"Failed to generate comparison report: {e}", exc_info=True)
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
    
    def run_full_report(
        self,
        time_frame: str = 'all',
        output_dir: Optional[Path] = None
    ) -> None:
        """
        Generate comprehensive report with all analytics.
        
        Args:
            time_frame: Time frame for analysis
            output_dir: Optional directory to export all reports
        """
        print("\n" + "=" * 80)
        print(f"COMPREHENSIVE FINANCIAL REPORT ({time_frame})")
        print("=" * 80)
        print()
        
        # Prepare export paths if output_dir provided
        csv_summary = None
        csv_categories = None
        csv_trends = None
        csv_accounts = None
        chart_categories = None
        chart_trends = None
        
        if output_dir:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            csv_categories = output_dir / f"categories_{time_frame}.csv"
            csv_trends = output_dir / f"trends_{time_frame}.csv"
            csv_accounts = output_dir / f"accounts_{time_frame}.csv"
            chart_categories = output_dir / f"categories_{time_frame}.png"
            chart_trends = output_dir / f"trends_{time_frame}.png"
        
        # Run all reports
        self.run_summary_report(time_frame=time_frame)
        self.run_category_report(
            time_frame=time_frame,
            export_csv=csv_categories,
            export_chart=chart_categories
        )
        self.run_trends_report(
            time_frame=time_frame,
            export_csv=csv_trends,
            export_chart=chart_trends
        )
        self.run_accounts_report(
            time_frame=time_frame,
            export_csv=csv_accounts
        )
        
        if output_dir:
            print(f"\nAll reports exported to: {output_dir}")
        
        print("\n" + "=" * 80)
        print("REPORT COMPLETE")
        print("=" * 80 + "\n")


def main_cli_analytics(connection_string: str, args: argparse.Namespace) -> None:
    """
    Main entry point for CLI analytics with top-level error handling.
    
    Args:
        connection_string: Database connection string
        args: Parsed command-line arguments
    """
    from exceptions import FinanceAppError, DatabaseError, ConfigError, AnalyticsError, ReportError
    
    # Initialize database and analytics with error handling
    try:
        db_manager = DatabaseManager(connection_string)
        cli_analytics = CLIAnalytics(db_manager)
    except DatabaseError as e:
        # User-friendly error message for CLI
        error_msg = e.message if hasattr(e, 'message') else str(e)
        logger.error(f"Database error: {error_msg}", exc_info=True)
        print(f"Database connection error: {error_msg}", file=sys.stderr)
        if e.details:
            for key, value in e.details.items():
                logger.error(f"  {key}: {value}")
        sys.exit(1)
    except (ConfigError, FinanceAppError) as e:
        error_msg = e.message if hasattr(e, 'message') else str(e)
        logger.error(f"Configuration error: {error_msg}", exc_info=True)
        print(f"Configuration error: {error_msg}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Unexpected error connecting to database: {e}")
        print(f"Unexpected error connecting to database: {e}", file=sys.stderr)
        sys.exit(1)
    
    try:
        # Route to appropriate report based on args
        if args.report_type == 'summary':
            cli_analytics.run_summary_report(
                time_frame=args.time_frame,
                account_id=args.account_id,
                account_type=args.account_type,
                export_path=Path(args.export) if args.export else None
            )
        
        elif args.report_type == 'categories':
            cli_analytics.run_category_report(
                time_frame=args.time_frame,
                account_id=args.account_id,
                account_type=args.account_type,
                top_n=args.top_n,
                export_csv=Path(args.export_csv) if args.export_csv else None,
                export_chart=Path(args.export_chart) if args.export_chart else None
            )
        
        elif args.report_type == 'trends':
            cli_analytics.run_trends_report(
                time_frame=args.time_frame,
                account_id=args.account_id,
                account_type=args.account_type,
                export_csv=Path(args.export_csv) if args.export_csv else None,
                export_chart=Path(args.export_chart) if args.export_chart else None
            )
        
        elif args.report_type == 'accounts':
            cli_analytics.run_accounts_report(
                time_frame=args.time_frame,
                export_csv=Path(args.export_csv) if args.export_csv else None
            )
        
        elif args.report_type == 'comparison':
            periods = args.periods.split(',') if hasattr(args, 'periods') else ['1m', '3m', '6m', '12m']
            cli_analytics.run_comparison_report(
                periods=periods,
                export_chart=Path(args.export_chart) if args.export_chart else None
            )
        
        elif args.report_type == 'full':
            cli_analytics.run_full_report(
                time_frame=args.time_frame,
                output_dir=Path(args.output_dir) if args.output_dir else None
            )
        
        else:
            print(f"Unknown report type: {args.report_type}", file=sys.stderr)
            sys.exit(1)
    
    except (AnalyticsError, ReportError, DatabaseError) as e:
        # Normalize error messages for CLI
        error_msg = e.message if hasattr(e, 'message') else str(e)
        logger.error(f"Analytics error: {error_msg}", exc_info=True)
        if e.details:
            for key, value in e.details.items():
                logger.error(f"  {key}: {value}")
        print(f"Error generating report: {error_msg}", file=sys.stderr)
        sys.exit(1)
    except FinanceAppError as e:
        error_msg = e.message if hasattr(e, 'message') else str(e)
        logger.error(f"Application error: {error_msg}", exc_info=True)
        print(f"Error: {error_msg}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("CLI analytics interrupted by user")
        print("\nAnalytics interrupted by user.", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        # Unexpected error - log full details, show user-friendly message
        logger.exception(f"Unexpected error in CLI analytics: {e}")
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        print("Check the logs for details.", file=sys.stderr)
        sys.exit(1)
    finally:
        db_manager.close()

