import snowflake.connector
import cryptography.hazmat.primitives.serialization as serialization
from config_reader import read_local_config

# Load configuration
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