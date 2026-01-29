# Local configuration reader
import configparser
import os

def read_local_config():
    """Read configuration file from the ETL directory"""
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(script_dir, 'config.cfg')
    
    if not os.path.exists(config_file):
        print(f"Configuration file not found at: {config_file}")
        return None
    
    cfg = configparser.ConfigParser()
    cfg.read(config_file)
    print(f"Configuration loaded successfully from: {config_file}")
    return cfg