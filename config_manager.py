"""
Configuration management module for the financial dashboard.

This module handles loading and saving configuration values,
including user preferences like net worth goals.
"""

import logging
from pathlib import Path
from typing import Any, Dict, Optional
import yaml
import streamlit as st

logger = logging.getLogger(__name__)

# Default configuration values
DEFAULT_CONFIG = {
    'net_worth_goal': 100000.0,
    'default_time_frame': 'Current',
    'show_sparklines': True,
    'balance_history_days': 30,
    'net_worth_history_days': 90
}

CONFIG_FILE = 'config.yaml'


def load_config() -> Dict[str, Any]:
    """
    Load configuration from config.yaml file.
    
    Returns:
        Configuration dictionary with defaults for missing values
    """
    try:
        config_path = Path(CONFIG_FILE)
        if config_path.exists():
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f) or {}
        else:
            config = {}
        
        # Merge with defaults
        for key, value in DEFAULT_CONFIG.items():
            if key not in config:
                config[key] = value
        
        logger.info("Configuration loaded successfully")
        return config
    
    except Exception as e:
        logger.error(f"Error loading configuration: {e}", exc_info=True)
        return DEFAULT_CONFIG.copy()


def save_config(config: Dict[str, Any]) -> bool:
    """
    Save configuration to config.yaml file.
    
    Args:
        config: Configuration dictionary to save
    
    Returns:
        True if successful, False otherwise
    """
    try:
        config_path = Path(CONFIG_FILE)
        
        # Read existing config to preserve other settings
        existing_config = {}
        if config_path.exists():
            with open(config_path, 'r') as f:
                existing_config = yaml.safe_load(f) or {}
        
        # Update with new values
        existing_config.update(config)
        
        # Write back
        with open(config_path, 'w') as f:
            yaml.dump(existing_config, f, default_flow_style=False)
        
        logger.info("Configuration saved successfully")
        return True
    
    except Exception as e:
        logger.error(f"Error saving configuration: {e}", exc_info=True)
        return False


def get_net_worth_goal() -> float:
    """
    Get net worth goal from session state or config file.
    
    Returns:
        Net worth goal value
    """
    # Check session state first
    if 'net_worth_goal' in st.session_state:
        return st.session_state.net_worth_goal
    
    # Load from config file
    config = load_config()
    goal = config.get('net_worth_goal', DEFAULT_CONFIG['net_worth_goal'])
    
    # Store in session state
    st.session_state.net_worth_goal = goal
    
    return goal


def set_net_worth_goal(goal: float, save_to_file: bool = True) -> bool:
    """
    Set net worth goal in session state and optionally save to config file.
    
    Args:
        goal: Net worth goal value
        save_to_file: Whether to persist to config file
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Update session state
        st.session_state.net_worth_goal = goal
        
        # Optionally save to file
        if save_to_file:
            config = load_config()
            config['net_worth_goal'] = goal
            return save_config(config)
        
        return True
    
    except Exception as e:
        logger.error(f"Error setting net worth goal: {e}", exc_info=True)
        return False


def get_dashboard_preference(key: str, default: Any = None) -> Any:
    """
    Get dashboard preference from session state or config file.
    
    Args:
        key: Preference key
        default: Default value if not found
    
    Returns:
        Preference value
    """
    # Check session state first
    session_key = f'pref_{key}'
    if session_key in st.session_state:
        return st.session_state[session_key]
    
    # Load from config file
    config = load_config()
    value = config.get(key, default)
    
    # Store in session state
    st.session_state[session_key] = value
    
    return value


def set_dashboard_preference(key: str, value: Any, save_to_file: bool = False) -> bool:
    """
    Set dashboard preference in session state and optionally save to config file.
    
    Args:
        key: Preference key
        value: Preference value
        save_to_file: Whether to persist to config file
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Update session state
        session_key = f'pref_{key}'
        st.session_state[session_key] = value
        
        # Optionally save to file
        if save_to_file:
            config = load_config()
            config[key] = value
            return save_config(config)
        
        return True
    
    except Exception as e:
        logger.error(f"Error setting dashboard preference: {e}", exc_info=True)
        return False


def initialize_session_state() -> None:
    """
    Initialize session state with default values from config.
    
    Should be called once at app startup.
    """
    config = load_config()
    
    # Initialize preferences if not already set
    for key, value in config.items():
        session_key = f'pref_{key}'
        if session_key not in st.session_state:
            st.session_state[session_key] = value
    
    # Initialize net worth goal specifically
    if 'net_worth_goal' not in st.session_state:
        st.session_state.net_worth_goal = config.get('net_worth_goal', DEFAULT_CONFIG['net_worth_goal'])
    
    logger.info("Session state initialized with configuration")

