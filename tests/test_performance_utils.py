"""
Tests for performance utilities module.

Tests query profiling, EXPLAIN functionality, and performance measurement tools.
"""

import pytest
import os
from unittest.mock import Mock, MagicMock, patch, call
from sqlalchemy.orm import Session
from sqlalchemy import text, func, case

from performance_utils import (
    explain_query,
    profile_query_with_timing,
    log_query_performance,
    is_profiling_enabled,
    profile_analytics_method
)
from database_ops import Transaction
from analytics import AnalyticsEngine
from database_ops import DatabaseManager


@pytest.fixture
def mock_session():
    """Create a mock SQLAlchemy session."""
    session = Mock(spec=Session)
    return session


@pytest.fixture
def mock_query():
    """Create a mock SQLAlchemy query."""
    query = Mock()
    return query


class TestExplainQuery:
    """Test EXPLAIN query functionality."""
    
    def test_explain_query_without_analyze(self, mock_session, mock_query):
        """Test EXPLAIN query without ANALYZE."""
        # Setup mock
        mock_query.statement.compile.return_value = "SELECT * FROM transactions"
        mock_query.filter.return_value = mock_query
        
        # Mock EXPLAIN result
        mock_result = Mock()
        mock_result.fetchall.return_value = [
            ("Seq Scan on transactions",),
            ("Filter: (date >= '2023-01-01')",),
        ]
        mock_session.execute.return_value = mock_result
        
        # Execute
        result = explain_query(mock_session, mock_query, analyze=False)
        
        # Verify
        assert 'sql' in result
        assert 'explain_result' in result
        assert 'formatted_plan' in result
        assert 'SELECT * FROM transactions' in result['sql']
        assert len(result['explain_result']) == 2
        assert 'Seq Scan' in result['formatted_plan']
        assert result['query_time'] is None  # No timing without ANALYZE
    
    def test_explain_query_with_analyze(self, mock_session, mock_query):
        """Test EXPLAIN ANALYZE query."""
        # Setup mock
        mock_query.statement.compile.return_value = "SELECT * FROM transactions"
        
        # Mock EXPLAIN ANALYZE result with timing
        mock_result = Mock()
        mock_result.fetchall.return_value = [
            ("Seq Scan on transactions (actual time=0.123..45.678 rows=1000 loops=1)",),
            ("Planning Time: 2.345 ms",),
            ("Execution Time: 48.012 ms",),
        ]
        mock_session.execute.return_value = mock_result
        
        # Execute
        result = explain_query(mock_session, mock_query, analyze=True)
        
        # Verify
        assert result['query_time'] is not None
        assert abs(result['query_time'] - 48.012) < 0.01
        assert result['planning_time'] is not None
        assert abs(result['planning_time'] - 2.345) < 0.01
        assert 'Execution Time' in result['formatted_plan']
    
    def test_explain_query_with_verbose(self, mock_session, mock_query):
        """Test EXPLAIN VERBOSE query."""
        mock_query.statement.compile.return_value = "SELECT * FROM transactions"
        
        mock_result = Mock()
        mock_result.fetchall.return_value = [("Output: id, date, amount",)]
        mock_session.execute.return_value = mock_result
        
        result = explain_query(mock_session, mock_query, analyze=False, verbose=True)
        
        # Verify VERBOSE was included in SQL
        assert 'VERBOSE' in str(mock_session.execute.call_args)
    
    def test_explain_query_handles_sql_error(self, mock_session, mock_query):
        """Test EXPLAIN query handles SQL errors gracefully."""
        from sqlalchemy.exc import SQLAlchemyError
        
        mock_query.statement.compile.return_value = "SELECT * FROM transactions"
        mock_session.execute.side_effect = SQLAlchemyError("Connection failed")
        
        with pytest.raises(SQLAlchemyError):
            explain_query(mock_session, mock_query, analyze=True)
    
    def test_explain_query_handles_parsing_error(self, mock_session, mock_query):
        """Test EXPLAIN query handles timing parsing errors."""
        mock_query.statement.compile.return_value = "SELECT * FROM transactions"
        
        # Mock result with invalid timing format
        mock_result = Mock()
        mock_result.fetchall.return_value = [
            ("Planning Time: invalid",),
            ("Execution Time: also invalid",),
        ]
        mock_session.execute.return_value = mock_result
        
        # Should not raise, just return None for timing
        result = explain_query(mock_session, mock_query, analyze=True)
        
        assert result['query_time'] is None
        assert result['planning_time'] is None


class TestProfileQueryWithTiming:
    """Test query profiling with timing."""
    
    def test_profile_query_with_timing(self, mock_session, mock_query):
        """Test profiling query with execution timing."""
        import time
        
        mock_query.statement.compile.return_value = "SELECT * FROM transactions"
        mock_query.filter.return_value = mock_query
        
        # Mock EXPLAIN result
        mock_explain_result = Mock()
        mock_explain_result.fetchall.return_value = [("Seq Scan",)]
        mock_session.execute.return_value = mock_explain_result
        
        # Mock query execution result
        mock_exec_result = Mock()
        mock_exec_result.fetchall.return_value = [(1,), (2,), (3,)]
        
        # First call for EXPLAIN, subsequent for timing
        mock_session.execute.side_effect = [mock_explain_result, mock_exec_result, mock_exec_result, mock_exec_result]
        
        result = profile_query_with_timing(mock_session, mock_query, iterations=3)
        
        # Verify
        assert 'explain_info' in result
        assert 'execution_times' in result
        assert 'avg_execution_time' in result
        assert 'min_execution_time' in result
        assert 'max_execution_time' in result
        assert len(result['execution_times']) == 3
        assert result['iterations'] == 3
        assert result['avg_execution_time'] > 0
    
    def test_profile_query_single_iteration(self, mock_session, mock_query):
        """Test profiling with single iteration."""
        mock_query.statement.compile.return_value = "SELECT * FROM transactions"
        
        mock_explain_result = Mock()
        mock_explain_result.fetchall.return_value = [("Index Scan",)]
        mock_exec_result = Mock()
        mock_exec_result.fetchall.return_value = [(1,)]
        
        mock_session.execute.side_effect = [mock_explain_result, mock_exec_result]
        
        result = profile_query_with_timing(mock_session, mock_query, iterations=1)
        
        assert len(result['execution_times']) == 1
        assert result['min_execution_time'] == result['max_execution_time']
        assert result['min_execution_time'] == result['avg_execution_time']


class TestLogQueryPerformance:
    """Test query performance logging."""
    
    @patch('performance_utils.logger')
    def test_log_query_performance(self, mock_logger, mock_session, mock_query):
        """Test logging query performance."""
        profile_result = {
            'formatted_plan': 'Seq Scan on transactions',
            'query_time': 45.67,
            'planning_time': 2.34,
            'avg_execution_time': 0.045,
            'min_execution_time': 0.040,
            'max_execution_time': 0.050,
            'iterations': 5
        }
        
        log_query_performance("test_query", profile_result)
        
        # Verify logging was called
        assert mock_logger.log.called
        # Check that query name was logged
        calls = [str(call) for call in mock_logger.log.call_args_list]
        assert any('test_query' in str(c) for c in calls)
    
    @patch('performance_utils.logger')
    def test_log_query_performance_minimal(self, mock_logger):
        """Test logging with minimal profile data."""
        profile_result = {
            'formatted_plan': 'Simple query'
        }
        
        log_query_performance("simple_query", profile_result)
        
        # Should not raise, just log available info
        assert mock_logger.log.called


class TestIsProfilingEnabled:
    """Test profiling enable/disable functionality."""
    
    def test_profiling_enabled_true(self):
        """Test profiling enabled with 'true'."""
        with patch.dict(os.environ, {'QUERY_PROFILING_ENABLED': 'true'}):
            assert is_profiling_enabled() is True
    
    def test_profiling_enabled_1(self):
        """Test profiling enabled with '1'."""
        with patch.dict(os.environ, {'QUERY_PROFILING_ENABLED': '1'}):
            assert is_profiling_enabled() is True
    
    def test_profiling_enabled_yes(self):
        """Test profiling enabled with 'yes'."""
        with patch.dict(os.environ, {'QUERY_PROFILING_ENABLED': 'yes'}):
            assert is_profiling_enabled() is True
    
    def test_profiling_disabled_false(self):
        """Test profiling disabled with 'false'."""
        with patch.dict(os.environ, {'QUERY_PROFILING_ENABLED': 'false'}):
            assert is_profiling_enabled() is False
    
    def test_profiling_disabled_default(self):
        """Test profiling disabled by default."""
        with patch.dict(os.environ, {}, clear=True):
            # Remove the key if it exists
            if 'QUERY_PROFILING_ENABLED' in os.environ:
                del os.environ['QUERY_PROFILING_ENABLED']
            assert is_profiling_enabled() is False


class TestProfileAnalyticsMethod:
    """Test profiling integration with analytics methods."""
    
    def test_profile_analytics_method_when_enabled(self, mock_session, mock_query):
        """Test profiling analytics method when enabled."""
        with patch.dict(os.environ, {'QUERY_PROFILING_ENABLED': 'true'}):
            with patch('performance_utils.explain_query') as mock_explain:
                with patch('performance_utils.log_query_performance') as mock_log:
                    mock_explain.return_value = {'formatted_plan': 'Test plan'}
                    
                    def build_query(session, *args, **kwargs):
                        return mock_query
                    
                    result = profile_analytics_method(
                        mock_session, "test_method", build_query, arg1=1
                    )
                    
                    assert result is not None
                    mock_explain.assert_called_once()
                    mock_log.assert_called_once()
    
    def test_profile_analytics_method_when_disabled(self, mock_session):
        """Test profiling analytics method when disabled."""
        with patch.dict(os.environ, {'QUERY_PROFILING_ENABLED': 'false'}):
            def build_query(session, *args, **kwargs):
                return Mock()
            
            result = profile_analytics_method(
                mock_session, "test_method", build_query
            )
            
            assert result is None
    
    def test_profile_analytics_method_handles_errors(self, mock_session):
        """Test profiling method handles errors gracefully."""
        with patch.dict(os.environ, {'QUERY_PROFILING_ENABLED': 'true'}):
            with patch('performance_utils.logger') as mock_logger:
                def build_query(session, *args, **kwargs):
                    raise Exception("Query build failed")
                
                # Should not raise, just log warning
                result = profile_analytics_method(
                    mock_session, "test_method", build_query
                )
                
                assert result is None
                assert mock_logger.warning.called


class TestIntegrationWithRealQueries:
    """Integration tests with real SQLAlchemy queries (using in-memory SQLite)."""
    
    @pytest.fixture
    def test_db(self, tmp_path):
        """Create a temporary test database."""
        db_path = tmp_path / "test_perf.db"
        db_manager = DatabaseManager(f"sqlite:///{db_path}")
        db_manager.create_tables()
        return db_manager
    
    def test_explain_query_with_real_database(self, test_db):
        """Test EXPLAIN with a real database connection."""
        session = test_db.get_session()
        try:
            # Create a simple query
            from database_ops import Transaction
            query = session.query(Transaction).filter(Transaction.id > 0)
            
            # Run EXPLAIN (should work even with empty table)
            result = explain_query(session, query, analyze=False)
            
            assert 'sql' in result
            assert 'explain_result' in result
            assert 'formatted_plan' in result
            assert 'transactions' in result['sql'].lower() or 'transaction' in result['sql'].lower()
        finally:
            session.close()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

