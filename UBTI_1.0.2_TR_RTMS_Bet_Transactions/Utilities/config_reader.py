# Local configuration reader
import configparser
import os

def read_local_config():
    """Read configuration file from the Config directory"""
    # Get the directory where this script is located and go up one level to find Config folder
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)  # Go up one level
    config_file = os.path.join(parent_dir, 'Config', 'config.cfg')
    
    if not os.path.exists(config_file):
        print(f"Configuration file not found at: {config_file}")
        return None
    
    cfg = configparser.ConfigParser()
    cfg.read(config_file)
    print(f"Configuration loaded successfully from: {config_file}")
    return cfg