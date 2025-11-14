"""
Unit tests for logging configuration and setup.

Tests setup_logging function, file handlers, email alerts, and edge cases.
"""

import logging
import logging.handlers
import sys
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock, Mock
import pytest
import yaml

from main import setup_logging, load_config
from exceptions import ConfigError


class TestSetupLogging:
    """Test setup_logging function."""
    
    def test_basic_logging_config(self):
        """Test basic logging configuration with defaults."""
        config = {
            "logging": {
                "level": "INFO",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            }
        }
        
        # Clear existing handlers
        logging.getLogger().handlers = []
        
        setup_logging(config)
        
        # Verify logging is configured
        assert logging.getLogger().level == logging.INFO
        assert len(logging.getLogger().handlers) >= 1
        
        # Check for StreamHandler
        stream_handlers = [h for h in logging.getLogger().handlers if isinstance(h, logging.StreamHandler)]
        assert len(stream_handlers) >= 1
    
    def test_file_logging_enabled(self):
        """Test file logging is enabled when log_file is specified."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, "test.log")
            config = {
                "logging": {
                    "level": "INFO",
                    "file": log_file
                }
            }
            
            # Clear existing handlers
            logging.getLogger().handlers = []
            
            setup_logging(config)
            
            # Check for FileHandler
            file_handlers = [h for h in logging.getLogger().handlers if isinstance(h, logging.FileHandler)]
            assert len(file_handlers) >= 1
            
            # Verify log file was created
            assert os.path.exists(log_file)
    
    def test_invalid_log_level_defaults_to_info(self):
        """Test that invalid log level defaults to INFO."""
        config = {
            "logging": {
                "level": "INVALID_LEVEL"
            }
        }
        
        # Clear existing handlers
        logging.getLogger().handlers = []
        
        with patch('main.logger') as mock_logger:
            setup_logging(config)
            # Should warn about invalid log level
            mock_logger.warning.assert_called()
        
        # Should default to INFO
        assert logging.getLogger().level == logging.INFO
    
    def test_log_format_includes_timestamp(self):
        """Test that log format includes timestamp."""
        config = {
            "logging": {
                "level": "INFO",
                "format": "%(levelname)s - %(message)s"  # Missing timestamp
            }
        }
        
        # Clear existing handlers
        logging.getLogger().handlers = []
        
        setup_logging(config)
        
        # Format should have been modified to include timestamp
        handler = logging.getLogger().handlers[0]
        formatter = handler.formatter
        if formatter:
            assert "%(asctime)s" in formatter._fmt
    
    def test_file_logging_permission_error_non_fatal(self):
        """Test that file permission errors don't crash the app."""
        config = {
            "logging": {
                "level": "INFO",
                "file": "/root/forbidden.log"  # Path that requires root
            }
        }
        
        # Clear existing handlers
        logging.getLogger().handlers = []
        
        # Should not raise exception, just log warning
        with patch('main.logger') as mock_logger:
            setup_logging(config)
            # Should warn about permission error but continue
            mock_logger.warning.assert_called()
        
        # Should still have console logging
        stream_handlers = [h for h in logging.getLogger().handlers if isinstance(h, logging.StreamHandler)]
        assert len(stream_handlers) >= 1
    
    def test_missing_logging_config_uses_defaults(self):
        """Test that missing logging config uses defaults."""
        config = {}  # No logging section
        
        # Clear existing handlers
        logging.getLogger().handlers = []
        
        setup_logging(config)
        
        # Should use defaults (INFO level)
        assert logging.getLogger().level == logging.INFO
    
    def test_email_alerts_enabled(self):
        """Test email alerts configuration."""
        config = {
            "logging": {
                "level": "INFO"
            },
            "email_alerts": {
                "enabled": True,
                "smtp_host": "smtp.gmail.com",
                "smtp_port": 587,
                "from_address": "test@example.com",
                "to_addresses": ["admin@example.com"],
                "level": "CRITICAL",
                "use_tls": True
            }
        }
        
        # Clear existing handlers
        logging.getLogger().handlers = []
        
        setup_logging(config)
        
        # Check for SMTPHandler
        smtp_handlers = [h for h in logging.getLogger().handlers if isinstance(h, logging.handlers.SMTPHandler)]
        assert len(smtp_handlers) >= 1
        
        # Verify handler level
        smtp_handler = smtp_handlers[0]
        assert smtp_handler.level == logging.CRITICAL
    
    def test_email_alerts_missing_config_non_fatal(self):
        """Test that missing email config doesn't crash."""
        config = {
            "logging": {
                "level": "INFO"
            },
            "email_alerts": {
                "enabled": True
                # Missing required fields
            }
        }
        
        # Clear existing handlers
        logging.getLogger().handlers = []
        
        with patch('main.logger') as mock_logger:
            setup_logging(config)
            # Should warn about missing config
            mock_logger.warning.assert_called()
        
        # Should still have basic logging
        assert len(logging.getLogger().handlers) >= 1
    
    def test_email_alerts_setup_failure_non_fatal(self):
        """Test that email setup failures don't crash."""
        config = {
            "logging": {
                "level": "INFO"
            },
            "email_alerts": {
                "enabled": True,
                "smtp_host": "invalid-host",
                "smtp_port": 99999,
                "from_address": "test@example.com",
                "to_addresses": ["admin@example.com"]
            }
        }
        
        # Clear existing handlers
        logging.getLogger().handlers = []
        
        with patch('main.logger') as mock_logger:
            setup_logging(config)
            # Should warn about setup failure but continue
            mock_logger.warning.assert_called()
        
        # Should still have basic logging
        assert len(logging.getLogger().handlers) >= 1
    
    def test_email_alerts_disabled(self):
        """Test that email alerts are not added when disabled."""
        config = {
            "logging": {
                "level": "INFO"
            },
            "email_alerts": {
                "enabled": False
            }
        }
        
        # Clear existing handlers
        logging.getLogger().handlers = []
        
        setup_logging(config)
        
        # Should not have SMTPHandler
        smtp_handlers = [h for h in logging.getLogger().handlers if isinstance(h, logging.handlers.SMTPHandler)]
        assert len(smtp_handlers) == 0
    
    def test_log_file_directory_creation(self):
        """Test that log file directory is created if it doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, "subdir", "test.log")
            config = {
                "logging": {
                    "level": "INFO",
                    "file": log_file
                }
            }
            
            # Clear existing handlers
            logging.getLogger().handlers = []
            
            setup_logging(config)
            
            # Directory should be created
            assert os.path.exists(os.path.dirname(log_file))
            assert os.path.exists(log_file)
    
    def test_multiple_handlers(self):
        """Test that multiple handlers can be configured."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, "test.log")
            config = {
                "logging": {
                    "level": "INFO",
                    "file": log_file
                }
            }
            
            # Clear existing handlers
            logging.getLogger().handlers = []
            
            setup_logging(config)
            
            # Should have both StreamHandler and FileHandler
            handlers = logging.getLogger().handlers
            assert len(handlers) >= 2
            
            stream_handlers = [h for h in handlers if isinstance(h, logging.StreamHandler)]
            file_handlers = [h for h in handlers if isinstance(h, logging.FileHandler)]
            
            assert len(stream_handlers) >= 1
            assert len(file_handlers) >= 1


class TestLoadConfig:
    """Test load_config function error handling."""
    
    def test_load_config_success(self):
        """Test successful config loading."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump({"test": "value"}, f)
            config_path = Path(f.name)
        
        try:
            config = load_config(config_path)
            assert config["test"] == "value"
        finally:
            os.unlink(config_path)
    
    def test_load_config_file_not_found(self):
        """Test ConfigError raised when file not found."""
        config_path = Path("/nonexistent/config.yaml")
        
        with pytest.raises(ConfigError) as exc_info:
            load_config(config_path)
        
        assert "not found" in exc_info.value.message.lower()
        assert "config_path" in exc_info.value.details
    
    def test_load_config_invalid_yaml(self):
        """Test ConfigError raised for invalid YAML."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("invalid: yaml: [")
            config_path = Path(f.name)
        
        try:
            with pytest.raises(ConfigError) as exc_info:
                load_config(config_path)
            
            assert "invalid yaml" in exc_info.value.message.lower()
            assert exc_info.value.original_error is not None
        finally:
            os.unlink(config_path)
    
    def test_load_config_empty_file(self):
        """Test ConfigError raised for empty config file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("")
            config_path = Path(f.name)
        
        try:
            with pytest.raises(ConfigError) as exc_info:
                load_config(config_path)
            
            assert "empty" in exc_info.value.message.lower()
        finally:
            if os.path.exists(config_path):
                os.unlink(config_path)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

