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

    conn = snowflake.connector.connect(
        user=cfg['SNOWFLAKE_CONFIG']['SNOWFLAKE_USER'],
        account=cfg['SNOWFLAKE_CONFIG']['SNOWFLAKE_ACCOUNT'],
        private_key=private_key,
        # Thêm cấu hình proxy ở đây
        proxy_host='10.208.100.10',
        proxy_port='8080',
        proxy_protocol='http' 
    )
    return conn

def snowflake_connection_using_connection_string(conn_str):
    from urllib.parse import urlparse, parse_qs

    # Parse JDBC connection string
    # Remove 'jdbc:snowflake://' prefix
    if conn_str.startswith('jdbc:snowflake://'):
        conn_str = conn_str[len('jdbc:snowflake://'):]

    # Parse URL
    parsed = urlparse(f'https://{conn_str}')
    host = parsed.hostname
    params = parse_qs(parsed.query)

    # Extract connection parameters, use config values as fallback
    connection_params = {
        'account': host.replace('.snowflakecomputing.com', '') if host else cfg['SNOWFLAKE_CONFIG']['SNOWFLAKE_ACCOUNT'],
        'user': params.get('user', [None])[0] or cfg['SNOWFLAKE_CONFIG']['SNOWFLAKE_USER'],
        'private_key_file': params.get('private_key_file', [None])[0] or cfg['SNOWFLAKE_CONFIG']['SNOWFLAKE_PRIVATE_KEY_PATH'],
        'private_key_file_pwd': params.get('private_key_file_pwd', [None])[0] or cfg['SNOWFLAKE_CONFIG']['SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'],
    }

    # Handle proxy settings
    if 'useProxy' in params and params['useProxy'][0].lower() == 'true':
        connection_params['proxy_host'] = params.get('proxyHost', [None])[0]
        connection_params['proxy_port'] = int(params.get('proxyPort', [0])[0]) if params.get('proxyPort', [None])[0] else None

    # Handle case insensitive columns
    if 'CLIENT_RESULT_COLUMN_CASE_INSENSITIVE' in params:
        connection_params['client_result_column_case_insensitive'] = params['CLIENT_RESULT_COLUMN_CASE_INSENSITIVE'][0].lower() == 'true'

    # Filter out None values
    connection_params = {k: v for k, v in connection_params.items() if v is not None}

    conn = snowflake.connector.connect(**connection_params)
    return conn