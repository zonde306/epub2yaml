"""智能数组合并模块，基于相似度比较实现YAML数组的智能合并。"""

from __future__ import annotations

from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Any


@dataclass(slots=True)
class MergeConfig:
    """合并配置"""
    similarity_threshold: float = 0.6
    field_weights: dict[str, float] = field(default_factory=dict)


class SmartArrayMerger:
    """
    智能数组合并器
    
    基于相似度比较实现数组的智能合并：
    - 字符串元素：使用difflib进行相似度比较
    - 字典元素：递归计算字段加权相似度
    - 列表元素：使用无序匹配算法
    - 基本类型：直接比较相等性
    """
    
    def __init__(self, config: MergeConfig):
        self.config = config
        self.similarity_threshold = config.similarity_threshold
        self.field_weights = config.field_weights
    
    def merge_arrays(self, current: list, incoming: list) -> list:
        """
        智能合并两个数组
        
        Args:
            current: 当前数组
            incoming: 新增数组
            
        Returns:
            合并后的数组
        """
        result = current.copy()
        used_indices: set[int] = set()  # 防止一个元素被多次替换
        
        for incoming_item in incoming:
            match_info = self._find_best_match(result, incoming_item, used_indices)
            
            if match_info is not None:
                index, _ = match_info
                result[index] = self._clone_value(incoming_item)
                used_indices.add(index)
            else:
                result.append(self._clone_value(incoming_item))
        
        return result
    
    def _find_best_match(
        self,
        array: list,
        item: Any,
        used_indices: set[int]
    ) -> tuple[int, float] | None:
        """
        在数组中寻找最佳匹配
        
        Args:
            array: 目标数组
            item: 待匹配元素
            used_indices: 已使用的索引集合
            
        Returns:
            (索引, 相似度) 或 None
        """
        best_index = -1
        best_similarity = 0.0
        
        for i, existing in enumerate(array):
            if i in used_indices:
                continue
            
            similarity = self._calculate_similarity(existing, item)
            if similarity >= self.similarity_threshold:
                if similarity > best_similarity:
                    best_similarity = similarity
                    best_index = i
        
        if best_index >= 0:
            return best_index, best_similarity
        return None
    
    def _calculate_similarity(
        self,
        value1: Any,
        value2: Any,
        current_path: str = ""
    ) -> float:
        """
        计算两个值的相似度
        
        Args:
            value1: 第一个值
            value2: 第二个值
            current_path: 当前递归路径，用于嵌套字段权重匹配
            
        Returns:
            相似度值 (0.0 - 1.0)
        """
        # 类型不同则完全不相似
        if type(value1) != type(value2):
            return 0.0
        
        # 字符串：使用difflib
        if isinstance(value1, str):
            return SequenceMatcher(None, value1, value2).ratio()
        
        # 字典：递归计算字段加权平均值
        if isinstance(value1, dict):
            return self._calculate_dict_similarity(value1, value2, current_path)
        
        # 列表：使用无序匹配算法
        if isinstance(value1, list):
            return self._calculate_list_similarity_unordered(value1, value2, current_path)
        
        # 基本类型：相等返回1，不等返回0
        return 1.0 if value1 == value2 else 0.0
    
    def _calculate_dict_similarity(
        self,
        dict1: dict,
        dict2: dict,
        current_path: str = ""
    ) -> float:
        """
        计算字典相似度（加权平均），支持嵌套路径
        
        Args:
            dict1: 第一个字典
            dict2: 第二个字典
            current_path: 当前递归路径
            
        Returns:
            相似度值 (0.0 - 1.0)
        """
        all_keys = set(dict1.keys()) | set(dict2.keys())
        if not all_keys:
            return 1.0
        
        weighted_sum = 0.0
        total_weight = 0.0
        
        for key in all_keys:
            # 构建完整路径
            full_path = f"{current_path}.{key}" if current_path else key
            
            # 查找权重：优先精确匹配完整路径，其次匹配顶层字段名
            weight = self.field_weights.get(full_path, self.field_weights.get(key, 1.0))
            total_weight += weight
            
            if key not in dict1 or key not in dict2:
                # 缺失字段相似度为0
                weighted_sum += 0.0 * weight
            else:
                similarity = self._calculate_similarity(dict1[key], dict2[key], full_path)
                weighted_sum += similarity * weight
        
        return weighted_sum / total_weight if total_weight > 0 else 1.0
    
    def _calculate_list_similarity_unordered(
        self,
        list1: list,
        list2: list,
        current_path: str = ""
    ) -> float:
        """
        使用无序匹配算法计算列表相似度
        为list1的每个元素在list2中找最佳匹配
        
        Args:
            list1: 第一个列表
            list2: 第二个列表
            current_path: 当前递归路径
            
        Returns:
            相似度值 (0.0 - 1.0)
        """
        if not list1 and not list2:
            return 1.0
        if not list1 or not list2:
            return 0.0
        
        # 贪心算法找最佳匹配
        matched_indices: set[int] = set()
        similarities: list[float] = []
        
        for item1 in list1:
            best_similarity = 0.0
            best_index = -1
            
            for i, item2 in enumerate(list2):
                if i in matched_indices:
                    continue
                
                similarity = self._calculate_similarity(item1, item2, current_path)
                if similarity > best_similarity:
                    best_similarity = similarity
                    best_index = i
            
            if best_index >= 0:
                matched_indices.add(best_index)
                similarities.append(best_similarity)
            else:
                similarities.append(0.0)
        
        # 长度差异惩罚
        max_len = max(len(list1), len(list2))
        min_len = min(len(list1), len(list2))
        length_penalty = min_len / max_len
        
        return (sum(similarities) / len(similarities)) * length_penalty
    
    def _clone_value(self, value: Any) -> Any:
        """深拷贝值"""
        if isinstance(value, dict):
            return {k: self._clone_value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._clone_value(item) for item in value]
        return value
