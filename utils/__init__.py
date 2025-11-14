"""
Utility modules for the finance-app.
"""

# Import functions from the root utils.py module to maintain backward compatibility
import importlib.util
from pathlib import Path

# Import directly from utils.py file to avoid circular imports
_parent_dir = Path(__file__).parent.parent
utils_file = _parent_dir / 'utils.py'

if utils_file.exists():
    spec = importlib.util.spec_from_file_location("utils_module", utils_file)
    utils_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(utils_module)
    
    # Export functions from utils.py
    prompt_user_choice = utils_module.prompt_user_choice
    get_project_root = utils_module.get_project_root
    get_data_dir = utils_module.get_data_dir
    ensure_data_dir = utils_module.ensure_data_dir
    resolve_connection_string = utils_module.resolve_connection_string
    resolve_log_path = utils_module.resolve_log_path
    
    # Export exceptions that are imported in utils.py (for backward compatibility)
    IngestionError = utils_module.IngestionError
    StandardizationError = utils_module.StandardizationError
    
    __all__ = [
        'prompt_user_choice',
        'get_project_root',
        'get_data_dir',
        'ensure_data_dir',
        'resolve_connection_string',
        'resolve_log_path',
        'IngestionError',
        'StandardizationError',
    ]
else:
    raise ImportError(f"Cannot find utils.py at {utils_file}")

