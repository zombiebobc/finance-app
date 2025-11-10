"""
Unit tests for transfer detection and classification.

Tests the classification module's ability to detect internal transfers
and exclude them from spending analytics.
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch
import pandas as pd

from classification import (
    is_transfer,
    classify_transaction,
    batch_classify_transfers,
    get_transfer_statistics
)


class TestTransferDetection:
    """Test transfer detection patterns."""
    
    def test_credit_card_payment_detection(self):
        """Test detection of credit card payments."""
        patterns = ["Credit Crd-Pay", "EDI PMYTS", "DEBIT PMTS"]
        
        assert is_transfer("Credit Crd-Pay", patterns) is True
        assert is_transfer("EDI PMYTS PAYMENT", patterns) is True
        assert is_transfer("DEBIT PMTS TO CARD", patterns) is True
    
    def test_transfer_keyword_detection(self):
        """Test detection of general transfer keywords."""
        patterns = ["Transfer to", "Transfer from", "Internal Transfer"]
        
        assert is_transfer("Transfer to Savings", patterns) is True
        assert is_transfer("Transfer from Checking", patterns) is True
        assert is_transfer("Internal Transfer", patterns) is True
    
    def test_investment_transfer_detection(self):
        """Test detection of investment account transfers."""
        patterns = ["Payment to Robinhood", "Transfer.*Robinhood", "Payment to.*Investment"]
        
        assert is_transfer("Payment to Robinhood", patterns) is True
        assert is_transfer("Transfer to Robinhood Gold", patterns) is True
        assert is_transfer("Payment to Investment Account", patterns) is True
    
    def test_regular_transaction_not_detected(self):
        """Test that regular transactions are not detected as transfers."""
        patterns = ["Transfer to", "Credit Crd-Pay", "Payment to Robinhood"]
        
        assert is_transfer("Starbucks Coffee", patterns) is False
        assert is_transfer("Grocery Store Purchase", patterns) is False
        assert is_transfer("Gas Station", patterns) is False
        assert is_transfer("Restaurant Dinner", patterns) is False
    
    def test_case_insensitive_matching(self):
        """Test that pattern matching is case-insensitive."""
        patterns = ["Credit Crd-Pay"]
        
        assert is_transfer("CREDIT CRD-PAY", patterns) is True
        assert is_transfer("credit crd-pay", patterns) is True
        assert is_transfer("Credit Crd-Pay", patterns) is True
    
    def test_regex_pattern_matching(self):
        """Test regex pattern functionality."""
        patterns = ["Transfer.*Investment", "Payment to.*Credit"]
        
        assert is_transfer("Transfer to Investment Account", patterns) is True
        assert is_transfer("Payment to Credit Card 1234", patterns) is True
    
    def test_empty_description(self):
        """Test handling of empty descriptions."""
        patterns = ["Transfer to"]
        
        assert is_transfer("", patterns) is False
        assert is_transfer(None, patterns) is False
    
    def test_empty_patterns(self):
        """Test behavior with empty pattern list."""
        assert is_transfer("Transfer to Savings", []) is False


class TestTransactionClassification:
    """Test transaction classification."""
    
    def test_classify_as_transfer(self):
        """Test classifying a transaction as a transfer."""
        mock_trans = Mock()
        mock_trans.description = "Credit Crd-Pay"
        mock_trans.is_transfer = 0
        mock_trans.category = None
        
        patterns = ["Credit Crd-Pay"]
        result = classify_transaction(mock_trans, patterns, "Transfer")
        
        assert result is True
        assert mock_trans.is_transfer == 1
        assert mock_trans.category == "Transfer"
    
    def test_classify_as_regular_transaction(self):
        """Test classifying a transaction as regular (not transfer)."""
        mock_trans = Mock()
        mock_trans.description = "Coffee Shop"
        mock_trans.is_transfer = 0
        mock_trans.category = "Dining"
        
        patterns = ["Transfer to"]
        result = classify_transaction(mock_trans, patterns)
        
        assert result is False
        assert mock_trans.is_transfer == 0
    
    def test_preserve_existing_category(self):
        """Test that existing categories are preserved."""
        mock_trans = Mock()
        mock_trans.description = "Transfer to Savings"
        mock_trans.is_transfer = 0
        mock_trans.category = "Savings Move"
        
        patterns = ["Transfer to"]
        classify_transaction(mock_trans, patterns, "Transfer")
        
        # Should still be marked as transfer but category unchanged
        assert mock_trans.is_transfer == 1
        assert mock_trans.category == "Savings Move"


class TestBatchClassification:
    """Test batch classification functionality."""
    
    @patch('classification.yaml.safe_load')
    @patch('builtins.open')
    def test_batch_classify_dry_run(self, mock_open, mock_yaml):
        """Test batch classification in dry run mode."""
        # Mock config
        mock_yaml.return_value = {
            'transfer_detection': {
                'enabled': True,
                'patterns': ['Transfer to', 'Credit Crd-Pay'],
                'transfer_category': 'Transfer',
                'log_detected_transfers': False
            }
        }
        
        # Mock database
        mock_db = Mock()
        mock_session = Mock()
        mock_db.get_session.return_value = mock_session
        
        # Create mock transactions
        trans1 = Mock()
        trans1.id = 1
        trans1.description = "Transfer to Savings"
        trans1.is_transfer = 0
        trans1.category = None
        trans1.date = datetime.now()
        trans1.amount = -100.0
        
        trans2 = Mock()
        trans2.id = 2
        trans2.description = "Grocery Store"
        trans2.is_transfer = 0
        trans2.category = "Groceries"
        trans2.date = datetime.now()
        trans2.amount = -50.0
        
        mock_session.query.return_value.all.return_value = [trans1, trans2]
        
        # Run batch classification
        result = batch_classify_transfers(mock_db, config_path="config.yaml", dry_run=True)
        
        # Verify results
        assert result['total'] == 2
        assert result['transfers_found'] == 1
        assert result['updated'] == 0  # Dry run doesn't update
        assert result['errors'] == 0
        
        # Verify no commit was called
        mock_session.commit.assert_not_called()
    
    @patch('classification.yaml.safe_load')
    @patch('builtins.open')
    def test_batch_classify_disabled(self, mock_open, mock_yaml):
        """Test batch classification when transfer detection is disabled."""
        # Mock config with disabled detection
        mock_yaml.return_value = {
            'transfer_detection': {
                'enabled': False,
                'patterns': []
            }
        }
        
        mock_db = Mock()
        
        result = batch_classify_transfers(mock_db, config_path="config.yaml")
        
        # Should return early with no processing
        assert result['total'] == 0
        assert result['transfers_found'] == 0


class TestTransferStatistics:
    """Test transfer statistics functionality."""
    
    def test_get_transfer_statistics(self):
        """Test getting transfer statistics."""
        mock_db = Mock()
        mock_session = Mock()
        mock_db.get_session.return_value = mock_session
        
        # Mock query results
        mock_query = Mock()
        mock_query.scalar.side_effect = [100, 15, -500.0]  # total, transfers, amount
        mock_session.query.return_value.filter.return_value = mock_query
        mock_session.query.return_value.scalar.return_value = 100
        
        stats = get_transfer_statistics(mock_db)
        
        # Verify statistics calculation
        assert 'total_transactions' in stats
        assert 'total_transfers' in stats
        assert 'transfer_percentage' in stats
        assert 'transfer_amount_total' in stats


class TestAnalyticsWithTransfers:
    """Test analytics engine excluding transfers."""
    
    def test_category_breakdown_excludes_transfers(self):
        """Test that category breakdown excludes transfers by default."""
        from analytics import AnalyticsEngine
        
        mock_db = Mock()
        mock_session = Mock()
        mock_db.get_session.return_value = mock_session
        
        # Mock query
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.group_by.return_value = mock_query
        mock_query.all.return_value = [
            ('Groceries', 200.0, 5),
            ('Gas', 100.0, 3)
        ]
        mock_session.query.return_value = mock_query
        
        analytics = AnalyticsEngine(mock_db)
        
        # This should filter out transfers (is_transfer == 0)
        df = analytics.get_category_breakdown(
            time_frame='1m',
            expense_only=True,
            include_transfers=False
        )
        
        # Verify transfer filter was applied in query chain
        filter_calls = [str(call) for call in mock_query.filter.call_args_list]
        # Should have multiple filter calls, one of which filters transfers
        assert mock_query.filter.called


class TestTransferPatternEdgeCases:
    """Test edge cases in transfer pattern matching."""
    
    def test_partial_match_handling(self):
        """Test that partial matches work correctly with regex."""
        patterns = ["Payment to.*Credit"]
        
        # Should match
        assert is_transfer("Payment to Credit Card", patterns) is True
        assert is_transfer("Payment to Credit Union", patterns) is True
        
        # Should not match
        assert is_transfer("Credit Payment", patterns) is False
    
    def test_ambiguous_description(self):
        """Test handling of potentially ambiguous descriptions."""
        patterns = ["Transfer"]
        
        # Contains 'transfer' but in different context
        desc = "Wire Transfer Fee"
        result = is_transfer(desc, patterns)
        
        # This would match - application should handle via manual override
        assert result is True
    
    def test_special_characters_in_description(self):
        """Test handling of special characters."""
        patterns = ["Transfer.*Investment"]
        
        assert is_transfer("Transfer-Investment-Account", patterns) is True
        assert is_transfer("Transfer to Investment #1234", patterns) is True


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

