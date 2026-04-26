from threading import Lock
from datetime import datetime

lock = Lock()

def get_current_time():
    with lock:  # 加锁保证线程安全
        return datetime.now().strftime("%Y%m%d_%H%M%S")