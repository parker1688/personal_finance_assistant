"""
采集编排器 - scheduler/collection_director.py
负责管理数据采集任务的调度、去重、冲突检测和优化
"""

import threading
from datetime import datetime
from typing import Dict, List, Optional, Callable, Any
from enum import Enum
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    """任务状态"""
    PENDING = 'pending'
    REGISTERED = 'registered'
    RUNNING = 'running'
    SUCCESS = 'success'
    FAILED = 'failed'
    SKIPPED = 'skipped'


@dataclass
class CollectionTask:
    """数据采集任务"""
    task_id: str
    task_type: str
    target: str
    priority: int = 1
    name: str = ""
    dependencies: List[str] = field(default_factory=list)
    status: str = 'pending'


class CollectionDirector:
    """中央采集编排器 - 统一管理所有数据源采集"""
    
    def __init__(self, max_workers: int = 5, dedup_window_seconds: int = 300):
        """初始化采集编排器"""
        self.max_workers = max_workers
        self.max_concurrent_tasks = max_workers  # 兼容性别名
        self.dedup_window_seconds = dedup_window_seconds
        self.task_registry: Dict[str, CollectionTask] = {}
        self.task_funcs: Dict[str, Callable] = {}
        self.last_executed_at: Dict[str, datetime] = {}
        self.lock = threading.Lock()
        logger.info(f"✅ CollectionDirector 初始化成功 (并发: {max_workers})")
    
    def register_task(self, task: CollectionTask, collector_func: Optional[Callable] = None) -> bool:
        """注册采集任务"""
        with self.lock:
            if task.task_id in self.task_registry:
                existing = self.task_registry[task.task_id]
                recent = self.last_executed_at.get(task.task_id)

                # 去重窗口：近期执行过且仍在窗口内，则跳过重复注册。
                if recent is not None:
                    age = (datetime.now() - recent).total_seconds()
                    if age < self.dedup_window_seconds:
                        logger.info(
                            f"⏭️ 跳过重复任务: {task.task_id} (age={age:.1f}s < dedup={self.dedup_window_seconds}s)"
                        )
                        return False

                # 已注册但未执行完成，保持原任务状态。
                if existing.status in [TaskStatus.PENDING.value, TaskStatus.REGISTERED.value, TaskStatus.RUNNING.value]:
                    logger.info(f"⏭️ 任务仍在队列或执行中: {task.task_id}")
                    return False

                self.task_registry[task.task_id] = task
                if collector_func is not None:
                    self.task_funcs[task.task_id] = collector_func
                task.status = TaskStatus.REGISTERED.value
                logger.info(f"✅ 任务已重新注册: {task.task_id}")
                return True
            
            self.task_registry[task.task_id] = task
            if collector_func is not None:
                self.task_funcs[task.task_id] = collector_func
            task.status = TaskStatus.REGISTERED.value
            logger.info(f"✅ 任务已注册: {task.task_id}")
            return True
    
    def execute_task(self, task_id: str) -> Dict[str, Any]:
        """执行单个采集任务"""
        if task_id not in self.task_registry:
            return {'success': False, 'error': 'Task not found'}
        
        task = self.task_registry[task_id]
        start_time = datetime.now()
        
        try:
            task.status = TaskStatus.RUNNING.value
            self.last_executed_at[task_id] = start_time
            logger.info(f"▶️  执行任务: {task_id}")

            collector_func = self.task_funcs.get(task_id)
            if collector_func is None:
                raise RuntimeError(f"任务未绑定执行函数: {task_id}")

            collector_func()
            
            duration = (datetime.now() - start_time).total_seconds()
            task.status = TaskStatus.SUCCESS.value
            logger.info(f"✅ 采集成功: {task_id}")
            
            return {
                'success': True,
                'task_id': task_id,
                'duration': duration
            }
        except Exception as e:
            logger.error(f"❌ 采集失败: {str(e)}")
            task.status = TaskStatus.FAILED.value
            return {
                'success': False,
                'task_id': task_id,
                'error': str(e)
            }
    
    def get_execution_plan(self) -> List[str]:
        """生成执行计划"""
        valid_tasks = {
            task_id: task for task_id, task in self.task_registry.items()
            if task.status in [TaskStatus.PENDING.value, TaskStatus.REGISTERED.value]
        }
        
        if not valid_tasks:
            return []
        
        conflicts = self._detect_conflicts()
        if conflicts:
            logger.info(f"⚠️ 检测到潜在冲突组: {len(conflicts)}")

        sorted_tasks = sorted(valid_tasks.items(), key=lambda x: x[1].priority)
        
        plan = [task_id for task_id, _ in sorted_tasks[:self.max_workers]]
        logger.info(f"📋 生成执行计划: {len(plan)} 个任务")
        return plan
    
    def _detect_conflicts(self) -> Dict[str, set]:
        """检测任务冲突"""
        conflicts: Dict[str, set] = {}
        by_type: Dict[str, List[str]] = {}

        for task_id, task in self.task_registry.items():
            if task.status == TaskStatus.RUNNING.value:
                by_type.setdefault(task.task_type, []).append(task_id)

        for task_type, task_ids in by_type.items():
            if len(task_ids) > 1:
                conflicts[task_type] = set(task_ids)

        return conflicts
    
    def get_task_status(self, task_id: str) -> Optional[Dict]:
        """获取任务状态"""
        if task_id not in self.task_registry:
            return None
        
        task = self.task_registry[task_id]
        return {
            'task_id': task_id,
            'task_type': task.task_type,
            'target': task.target,
            'status': task.status,
            'priority': task.priority
        }
