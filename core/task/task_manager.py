# 保存为 core/task/task_manager.py
import json
import os
from datetime import datetime
from common.logger import create_log
import random

logger = create_log('task_manager')


class TaskManager:
    """
    任务管理器类，用于处理任务实体的增删改查操作
    数据存储在JSON文件中
    """

    def __init__(self, file_path=None):
        """
        初始化任务管理器

        Args:
            file_path: JSON文件路径，如果不提供则使用默认路径
        """
        if file_path is None:
            # 使用默认配置文件路径
            self.file_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                '../config',
                'scheduled_tasks.json'
            )
        else:
            self.file_path = file_path

        # 确保文件存在
        self._ensure_file_exists()

        # 默认配置
        self.default_target_stocks = [
            {"market": "us", "data_source": "akshare", "stock_code": "US.IVV", "adjust_type": "qfq"},
            {"market": "cn", "data_source": "baostock", "stock_code": "SH.600519", "adjust_type": "qfq"}
        ]

        self.default_backtest_config = {
            "strategy": "EnhancedVolumeStrategy",
            "init_cash": 5000000
        }

    def _ensure_file_exists(self):
        """
        确保配置文件和目录存在
        """
        config_dir = os.path.dirname(self.file_path)
        if not os.path.exists(config_dir):
            os.makedirs(config_dir)
            logger.info(f"创建配置目录: {config_dir}")

        if not os.path.exists(self.file_path):
            with open(self.file_path, 'w', encoding='utf-8') as f:
                json.dump([], f, ensure_ascii=False, indent=2)
            logger.info(f"创建空配置文件: {self.file_path}")

    def _read_tasks(self):
        """
        从JSON文件中读取任务列表

        Returns:
            list: 任务列表
        """
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"读取任务文件失败: {e}")
            return []

    def _write_tasks(self, tasks):
        """
        将任务列表写入JSON文件

        Args:
            tasks: 任务列表

        Returns:
            bool: 写入是否成功
        """
        try:
            # 确保每个任务都有必要的字段
            for task in tasks:
                self._ensure_task_fields(task)

            with open(self.file_path, 'w', encoding='utf-8') as f:
                json.dump(tasks, f, ensure_ascii=False, indent=2)
            logger.info(f"保存了 {len(tasks)} 个任务")
            return True
        except Exception as e:
            logger.error(f"写入任务文件失败: {e}")
            return False

    def _ensure_task_fields(self, task):
        """
        确保任务对象包含所有必要的字段

        Args:
            task: 任务对象
        """
        # 更新修改时间
        task['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 确保必要字段存在
        if 'enabled' not in task:
            task['enabled'] = True
        if 'target_stocks' not in task:
            task['target_stocks'] = self.default_target_stocks
        if 'backtest_config' not in task:
            task['backtest_config'] = self.default_backtest_config

    def create(self, task_data):
        """
        创建新任务

        Args:
            task_data: 任务数据字典

        Returns:
            dict: 创建的任务（包含id和时间戳），失败返回None
        """
        # 读取现有任务
        tasks = self._read_tasks()

        # 为新任务添加必要字段
        task = task_data.copy()

        # id，生成一个
        task['id'] = f"task_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        # 检查ID是否已存在
        if any(t['id'] == task['id'] for t in tasks):
            logger.warning(f"任务ID已存在: {task['id']}，重新生成ID")
            task['id'] = f"task_{datetime.now().strftime('%Y%m%d%H%M%S')}_{random.randint(1000, 9999)}"

        # 添加时间戳
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        task['create_time'] = current_time
        task['update_time'] = current_time

        # 确保必要字段存在
        self._ensure_task_fields(task)

        # 添加到任务列表
        tasks.append(task)

        # 写入文件
        if self._write_tasks(tasks):
            logger.info(f"创建任务成功: {task['name']} (ID: {task['id']})")
            return task
        else:
            logger.error(f"创建任务失败: {task['name']}")
            return None

    def read(self, task_id):
        """
        根据id获取任务

        Args:
            task_id: 任务id

        Returns:
            dict: 任务信息，如果不存在返回None
        """
        tasks = self._read_tasks()
        for task in tasks:
            if task.get('id') == task_id:
                return task
        logger.warning(f"未找到任务: {task_id}")
        return None

    def read_all(self):
        """
        获取所有任务

        Returns:
            list: 所有任务的列表
        """
        tasks = self._read_tasks()
        logger.info(f"获取到 {len(tasks)} 个任务")
        return tasks

    def update(self, task_id, update_data):
        """
        更新任务

        Args:
            task_id: 任务id
            update_data: 要更新的数据

        Returns:
            dict: 更新后的任务，如果不存在返回None
        """
        tasks = self._read_tasks()
        updated = False

        for i, task in enumerate(tasks):
            if task.get('id') == task_id:
                # 保留不变的字段
                original_create_time = task.get('create_time')

                # 更新任务数据
                task.update(update_data)

                # 确保ID不变
                task['id'] = task_id
                # 保留创建时间
                task['create_time'] = original_create_time
                # 更新修改时间
                task['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                updated = True
                break

        if updated and self._write_tasks(tasks):
            logger.info(f"更新任务成功: {task_id}")
            return task
        else:
            logger.error(f"更新任务失败: {task_id}")
            return None

    def delete(self, task_id):
        """
        删除任务

        Args:
            task_id: 任务id

        Returns:
            bool: 删除是否成功
        """
        tasks = self._read_tasks()
        original_length = len(tasks)

        # 过滤掉要删除的任务
        tasks = [task for task in tasks if task.get('id') != task_id]

        # 检查是否删除了任务
        if len(tasks) < original_length:
            success = self._write_tasks(tasks)
            if success:
                logger.info(f"删除任务成功: {task_id}")
            return success
        else:
            logger.error(f"未找到任务: {task_id}")
            return False

    def query(self, filters=None):
        """
        根据条件查询任务

        Args:
            filters: 查询条件字典，例如 {"type": "backtest", "enabled": True}

        Returns:
            list: 符合条件的任务列表
        """
        tasks = self._read_tasks()

        if filters is None or not filters:
            return tasks

        filtered_tasks = []
        for task in tasks:
            match = True
            for key, value in filters.items():
                # 支持嵌套查询，例如 "backtest_config.strategy": "EnhancedVolumeStrategy"
                if '.' in key:
                    nested_keys = key.split('.')
                    nested_value = task
                    try:
                        for nested_key in nested_keys:
                            nested_value = nested_value.get(nested_key)
                        if nested_value != value:
                            match = False
                            break
                    except (TypeError, AttributeError):
                        match = False
                        break
                else:
                    if task.get(key) != value:
                        match = False
                        break

            if match:
                filtered_tasks.append(task)

        logger.info(f"查询到 {len(filtered_tasks)} 个符合条件的任务")
        return filtered_tasks

    def enable(self, task_id):
        """
        启用任务

        Args:
            task_id: 任务id

        Returns:
            dict: 更新后的任务，如果不存在返回None
        """
        return self.update(task_id, {'enabled': True})

    def disable(self, task_id):
        """
        禁用任务

        Args:
            task_id: 任务id

        Returns:
            dict: 更新后的任务，如果不存在返回None
        """
        return self.update(task_id, {'enabled': False})

    def count(self):
        """
        获取任务总数

        Returns:
            int: 任务数量
        """
        tasks = self._read_tasks()
        return len(tasks)

    def exists(self, task_id):
        """
        检查任务是否存在

        Args:
            task_id: 任务id

        Returns:
            bool: 任务是否存在
        """
        return self.read(task_id) is not None


if __name__ == '__main__':
    # 初始化任务管理器
    task_manager = TaskManager()

    # 示例任务数据
    new_task = {
        "id": "1",
        "name": "每日股票分析任务",
        "type": "backtest",
        "description": "123",
        "schedule_time": "0 8 * * 1-5",
        "create_time": "2025-11-17T22:09:45.353382",
        "update_time": "2025-11-17T22:09:45.353430",
        "target_stocks": [
            {
                "market": "us",
                "data_source": "akshare",
                "stock_code": "US.IVV",
                "adjust_type": "qfq"
            },
            {
                "market": "cn",
                "data_source": "baostock",
                "stock_code": "SH.600519",
                "adjust_type": "qfq"
            }
        ],
        "backtest_config": {
            "strategy": "EnhancedVolumeStrategy",
            "init_cash": 5000000,
            "during": 1460
        }
    }

    # 创建任务
    created_task = task_manager.create(new_task)
    logger.info(created_task)

    # 获取任务
    task_id = "task_20251119163324"
    task = task_manager.read(task_id)
    logger.info(task)

    # 更新任务
    updated_task = task_manager.update(task_id, {"name": "更新后的每日股票分析任务", "type": "backtest12fghjk"})
    #
    # 查询任务
    backtest_tasks = task_manager.query({"type": "backtest"})
    logger.info(backtest_tasks)
    strategy_tasks = task_manager.query({"backtest_config.strategy": "EnhancedVolumeStrategy"})
    logger.info(strategy_tasks)
    #
    # 删除任务
    task_manager.delete("task_20251119162230")

    # 启用任务
    enabled_task = task_manager.enable(task_id)
    logger.info(enabled_task)

    # 禁用任务
    disabled_task = task_manager.disable(task_id)
    logger.info(disabled_task)

    # 读取所有任务
    all_tasks = task_manager.query()
    logger.info(all_tasks)
