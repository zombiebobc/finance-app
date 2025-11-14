"""
Performance utilities for query profiling and optimization.

This module provides tools for profiling SQL queries using EXPLAIN ANALYZE
to identify performance bottlenecks and optimize database queries.
"""

import logging
import os
from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


def explain_query(
    session: Session,
    query: Any,
    analyze: bool = True,
    verbose: bool = False
) -> Dict[str, Any]:
    """
    Run EXPLAIN (ANALYZE) on a SQLAlchemy query to profile performance.
    
    This function executes EXPLAIN ANALYZE on the generated SQL query to provide
    detailed execution plan information including actual execution times, row counts,
    and index usage.
    
    Args:
        session: SQLAlchemy session for database connection
        query: SQLAlchemy query object to profile
        analyze: If True, run EXPLAIN ANALYZE (executes query); if False, run EXPLAIN only
        verbose: If True, include verbose output (PostgreSQL-specific)
    
    Returns:
        Dictionary containing:
        - sql: The actual SQL query string
        - explain_result: List of explain plan rows (text format)
        - formatted_plan: Formatted explain plan as string
        - query_time: Query execution time in ms (if analyze=True, PostgreSQL only)
        - planning_time: Query planning time in ms (if analyze=True, PostgreSQL only)
    
    Raises:
        SQLAlchemyError: If query execution fails
    
    Example:
        >>> from analytics import AnalyticsEngine
        >>> engine = AnalyticsEngine(db_manager)
        >>> session = db_manager.get_session()
        >>> query = session.query(Transaction).filter(Transaction.date >= start_date)
        >>> profile = explain_query(session, query, analyze=True)
        >>> print(profile['formatted_plan'])
        Seq Scan on transactions ...
    """
    try:
        # Get the SQL query string
        sql_query = str(query.statement.compile(compile_kwargs={"literal_binds": True}))
        
        # Build EXPLAIN command
        explain_cmd = "EXPLAIN"
        if analyze:
            explain_cmd += " ANALYZE"
        if verbose:
            explain_cmd += " VERBOSE"
        
        explain_sql = f"{explain_cmd} {sql_query}"
        
        # Execute EXPLAIN
        result = session.execute(text(explain_sql))
        explain_rows = [row[0] for row in result.fetchall()]
        
        # Parse timing information from explain output (PostgreSQL format)
        query_time = None
        planning_time = None
        formatted_plan = "\n".join(explain_rows)
        
        if analyze:
            # Try to extract timing from PostgreSQL EXPLAIN ANALYZE output
            for row in explain_rows:
                if "Planning Time:" in row:
                    try:
                        planning_time = float(row.split("Planning Time:")[1].split("ms")[0].strip())
                    except (IndexError, ValueError):
                        pass
                if "Execution Time:" in row:
                    try:
                        query_time = float(row.split("Execution Time:")[1].split("ms")[0].strip())
                    except (IndexError, ValueError):
                        pass
        
        return {
            "sql": sql_query,
            "explain_result": explain_rows,
            "formatted_plan": formatted_plan,
            "query_time": query_time,
            "planning_time": planning_time
        }
    
    except SQLAlchemyError as e:
        logger.error(f"Failed to execute EXPLAIN query: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error in explain_query: {e}", exc_info=True)
        raise SQLAlchemyError(f"Query profiling failed: {e}") from e


def profile_query_with_timing(
    session: Session,
    query: Any,
    iterations: int = 1
) -> Dict[str, Any]:
    """
    Profile a query's execution time and EXPLAIN plan.
    
    This function combines timing measurements with EXPLAIN analysis to provide
    comprehensive performance metrics.
    
    Args:
        session: SQLAlchemy session for database connection
        query: SQLAlchemy query object to profile
        iterations: Number of times to execute the query for timing (default: 1)
    
    Returns:
        Dictionary containing:
        - explain_info: Result from explain_query()
        - execution_times: List of execution times in seconds
        - avg_execution_time: Average execution time in seconds
        - min_execution_time: Minimum execution time in seconds
        - max_execution_time: Maximum execution time in seconds
    
    Example:
        >>> profile = profile_query_with_timing(session, query, iterations=5)
        >>> print(f"Avg time: {profile['avg_execution_time']:.3f}s")
    """
    import time
    
    try:
        # Get EXPLAIN plan first (without ANALYZE for timing measurement)
        explain_info = explain_query(session, query, analyze=False)
        
        # Measure actual execution time
        execution_times = []
        sql_query = str(query.statement.compile(compile_kwargs={"literal_binds": True}))
        
        for i in range(iterations):
            start_time = time.perf_counter()
            result = session.execute(text(sql_query))
            # Consume results to get accurate timing
            list(result.fetchall())
            end_time = time.perf_counter()
            execution_times.append(end_time - start_time)
        
        avg_time = sum(execution_times) / len(execution_times) if execution_times else 0.0
        
        return {
            "explain_info": explain_info,
            "execution_times": execution_times,
            "avg_execution_time": avg_time,
            "min_execution_time": min(execution_times) if execution_times else 0.0,
            "max_execution_time": max(execution_times) if execution_times else 0.0,
            "iterations": iterations
        }
    
    except Exception as e:
        logger.error(f"Failed to profile query with timing: {e}", exc_info=True)
        raise


def log_query_performance(
    query_name: str,
    profile_result: Dict[str, Any],
    log_level: int = logging.INFO
) -> None:
    """
    Log query performance metrics in a structured format.
    
    Args:
        query_name: Name/identifier for the query being profiled
        profile_result: Result dictionary from explain_query() or profile_query_with_timing()
        log_level: Logging level to use (default: INFO)
    
    Example:
        >>> profile = explain_query(session, query)
        >>> log_query_performance("get_income_expense_summary", profile)
    """
    logger.log(log_level, f"=== Query Performance: {query_name} ===")
    
    if "formatted_plan" in profile_result:
        logger.log(log_level, f"Execution Plan:\n{profile_result['formatted_plan']}")
    
    if "query_time" in profile_result and profile_result["query_time"]:
        logger.log(log_level, f"Query Time: {profile_result['query_time']:.2f} ms")
    
    if "planning_time" in profile_result and profile_result["planning_time"]:
        logger.log(log_level, f"Planning Time: {profile_result['planning_time']:.2f} ms")
    
    if "avg_execution_time" in profile_result:
        logger.log(log_level, f"Avg Execution Time: {profile_result['avg_execution_time']*1000:.2f} ms")
        logger.log(log_level, f"Min: {profile_result.get('min_execution_time', 0)*1000:.2f} ms")
        logger.log(log_level, f"Max: {profile_result.get('max_execution_time', 0)*1000:.2f} ms")
        logger.log(log_level, f"Iterations: {profile_result.get('iterations', 1)}")
    
    logger.log(log_level, "=" * 50)


def is_profiling_enabled() -> bool:
    """
    Check if query profiling is enabled via environment variable.
    
    Returns:
        True if QUERY_PROFILING_ENABLED environment variable is set to 'true' or '1'
    
    Example:
        >>> if is_profiling_enabled():
        >>>     profile = explain_query(session, query)
    """
    profiling_env = os.getenv("QUERY_PROFILING_ENABLED", "false").lower()
    return profiling_env in ("true", "1", "yes", "on")


def profile_analytics_method(
    session: Session,
    method_name: str,
    query_func: Any,
    *args: Any,
    **kwargs: Any
) -> Optional[Dict[str, Any]]:
    """
    Convenience wrapper to profile analytics methods.
    
    This function checks if profiling is enabled, builds the query using the
    provided function, and profiles it. Useful for integrating profiling into
    analytics methods.
    
    Args:
        session: SQLAlchemy session for database connection
        method_name: Name of the method being profiled (for logging)
        query_func: Callable that returns a SQLAlchemy query object
        *args: Positional arguments to pass to query_func
        **kwargs: Keyword arguments to pass to query_func
    
    Returns:
        Profile result dictionary if profiling is enabled, None otherwise
    
    Example:
        >>> def build_summary_query(session, start_date, end_date):
        >>>     return session.query(...).filter(...)
        >>> profile = profile_analytics_method(
        >>>     session, "get_income_expense_summary",
        >>>     build_summary_query, start_date, end_date
        >>> )
    """
    if not is_profiling_enabled():
        return None
    
    try:
        query = query_func(session, *args, **kwargs)
        profile_result = explain_query(session, query, analyze=True)
        log_query_performance(method_name, profile_result)
        return profile_result
    except Exception as e:
        logger.warning(f"Failed to profile {method_name}: {e}")
        return None

