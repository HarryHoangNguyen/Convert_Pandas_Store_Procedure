import logging, os, time
from datetime import datetime

logger = logging.getLogger(__name__)

def init_logfiles():

    # Tạo thư mục logs nếu chưa tồn tại
    today = datetime.now()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    logs_dir = os.path.join(current_dir, 'logs', today.strftime("%Y%m%d%H%M"))
    os.makedirs(logs_dir, exist_ok=True)

    # Tạo tên file log với timestamp
    log_filename = f"Main_SP_UBT_GETOPERATINGHOURS{today.strftime('%Y%m%d%H%M')}.log"
    log_filepath = os.path.join(logs_dir, log_filename)

    level_logging = 'INFO'  # Chỉnh mức logging ở đây: DEBUG, INFO, WARNING, ERROR, CRITICAL

    # Cấu hình logging: ghi ra file trong thư mục logs
    logging.basicConfig(
        filename=log_filepath,               # File log trong thư mục logs với timestamp
        level=getattr(logging, level_logging),                 # Ghi từ mức INFO trở lên
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        encoding='utf-8'                     # Quan trọng để ghi tiếng Việt không bị lỗi font
    )
    ETL_name = "Main_SP_UBT_GETOPERATINGHOURS"
    logger = logging.getLogger(ETL_name)
    return True