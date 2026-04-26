import uuid
from logging.handlers import TimedRotatingFileHandler
from settings import log_root
import logging

# 在模块级别生成全局唯一ID
RUN_UUID = str(uuid.uuid4())

def create_log(name):
    # 确保日志目录存在
    log_root.mkdir(parents=True, exist_ok=True)

    # 创建logger对象
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # 清除已有的处理器
    if logger.handlers:
        for handler in logger.handlers:
            handler.close()
        logger.handlers.clear()

    # 创建格式化器
    formatter = logging.Formatter(
        f'%(asctime)s - {RUN_UUID} - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 创建文件处理器 - 按日期轮转，保留7天日志
    log_file = log_root / f'{name}.log'
    file_handler = TimedRotatingFileHandler(
        filename=log_file,
        when='midnight',  # 在每天午夜轮转
        interval=1,  # 每天一个文件
        backupCount=7,  # 保留7天的日志
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger