import snowflake.connector
import cryptography.hazmat.primitives.serialization as serialization
import sys, os, configparser
current_dir = os.getcwd()
parent_dir = os.path.dirname(current_dir)
python_project_dir = os.path.dirname(parent_dir)  # Go up one more level to Python-Project
sys.path.insert(0, python_project_dir)

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

# Load configuration at module level
cfg = read_local_config()

def snowflake_connection():
    # Đọc private key có passphrase
    with open(cfg['SNOWFLAKE_CONFIG']['SNOWFLAKE_PRIVATE_KEY_PATH'], 'rb') as key_file:
        pkey = serialization.load_pem_private_key(
            key_file.read(),
            password=cfg['SNOWFLAKE_CONFIG']['SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'].encode()  # Passphrase khi tạo key
        )
        # Lấy raw bytes của private key
    private_key = pkey.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    if cfg['MODE_CONFIG']['MODE'] == 'LOCAL':
        conn = snowflake.connector.connect(
            user=cfg['SNOWFLAKE_CONFIG']['SNOWFLAKE_USER'],
            account=cfg['SNOWFLAKE_CONFIG']['SNOWFLAKE_ACCOUNT'],
            private_key=private_key,
        )
    else:
        conn = snowflake.connector.connect(
            user=cfg['SNOWFLAKE_CONFIG']['SNOWFLAKE_USER'],
            account=cfg['SNOWFLAKE_CONFIG']['SNOWFLAKE_ACCOUNT'],
            private_key=private_key,
            # Thêm cấu hình proxy ở đây
            proxy_host=cfg['SNOWFLAKE_CONFIG']['SNOWFLAKE_PROXY_HOST'],
            proxy_port=int(cfg['SNOWFLAKE_CONFIG']['SNOWFLAKE_PROXY_PORT']),
            proxy_protocol=cfg['SNOWFLAKE_CONFIG']['SNOWFLAKE_PROTOCOL']
        )
    return conn