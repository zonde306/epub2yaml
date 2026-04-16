"""智能数组合并器单元测试"""

import unittest
import sys
from pathlib import Path

# 添加src目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from smart_array_merger import MergeConfig, SmartArrayMerger


class TestSmartArrayMerger(unittest.TestCase):
    """SmartArrayMerger测试类"""
    
    def setUp(self):
        """测试初始化"""
        self.config = MergeConfig(similarity_threshold=0.6)
        self.merger = SmartArrayMerger(self.config)
    
    # ==================== 字符串相似度测试 ====================
    
    def test_string_similarity_identical(self):
        """测试完全相同的字符串"""
        similarity = self.merger._calculate_similarity("hello", "hello")
        self.assertEqual(similarity, 1.0)
    
    def test_string_similarity_similar(self):
        """测试相似的字符串"""
        similarity = self.merger._calculate_similarity("apple", "appl")
        self.assertGreater(similarity, 0.6)
    
    def test_string_similarity_different(self):
        """测试不同的字符串"""
        similarity = self.merger._calculate_similarity("apple", "orange")
        self.assertLess(similarity, 0.6)
    
    def test_string_similarity_empty(self):
        """测试空字符串"""
        similarity = self.merger._calculate_similarity("", "")
        self.assertEqual(similarity, 1.0)
    
    # ==================== 基本类型测试 ====================
    
    def test_primitive_equal(self):
        """测试相等的基本类型"""
        self.assertEqual(self.merger._calculate_similarity(1, 1), 1.0)
        self.assertEqual(self.merger._calculate_similarity(True, True), 1.0)
        self.assertEqual(self.merger._calculate_similarity(None, None), 1.0)
    
    def test_primitive_not_equal(self):
        """测试不相等的基本类型"""
        self.assertEqual(self.merger._calculate_similarity(1, 2), 0.0)
        self.assertEqual(self.merger._calculate_similarity(True, False), 0.0)
    
    def test_different_types(self):
        """测试不同类型"""
        self.assertEqual(self.merger._calculate_similarity(1, "1"), 0.0)
        self.assertEqual(self.merger._calculate_similarity([1], "1"), 0.0)
    
    # ==================== 字典相似度测试 ====================
    
    def test_dict_similarity_identical(self):
        """测试完全相同的字典"""
        dict1 = {"name": "Alice", "age": 30}
        dict2 = {"name": "Alice", "age": 30}
        similarity = self.merger._calculate_similarity(dict1, dict2)
        self.assertEqual(similarity, 1.0)
    
    def test_dict_similarity_partial(self):
        """测试部分相似的字典"""
        dict1 = {"name": "Alice", "age": 30}
        dict2 = {"name": "Alice", "age": 31}
        similarity = self.merger._calculate_similarity(dict1, dict2)
        # name完全匹配(1.0)，age不匹配(0.0)，平均相似度=0.5
        self.assertGreaterEqual(similarity, 0.5)
        self.assertLess(similarity, 1.0)
    
    def test_dict_similarity_empty(self):
        """测试空字典"""
        similarity = self.merger._calculate_similarity({}, {})
        self.assertEqual(similarity, 1.0)
    
    def test_dict_similarity_missing_key(self):
        """测试缺失字段的字典"""
        dict1 = {"name": "Alice", "age": 30}
        dict2 = {"name": "Alice"}
        similarity = self.merger._calculate_similarity(dict1, dict2)
        self.assertLess(similarity, 1.0)
    
    # ==================== 字段权重测试 ====================
    
    def test_field_weights_top_level(self):
        """测试顶层字段权重"""
        config = MergeConfig(
            similarity_threshold=0.6,
            field_weights={"name": 2.0, "age": 1.0}
        )
        merger = SmartArrayMerger(config)
        
        dict1 = {"name": "Alice", "age": 30}
        dict2 = {"name": "Alice", "age": 31}
        similarity = merger._calculate_similarity(dict1, dict2)
        
        # name完全匹配（权重2.0），age不匹配（权重1.0）
        # 相似度 = (1.0*2.0 + 0.0*1.0) / 3.0 = 0.667
        self.assertGreater(similarity, 0.6)
    
    def test_field_weights_nested_path(self):
        """测试嵌套字段路径权重"""
        config = MergeConfig(
            similarity_threshold=0.6,
            field_weights={"person.name": 3.0}
        )
        merger = SmartArrayMerger(config)
        
        dict1 = {"person": {"name": "Alice", "age": 30}}
        dict2 = {"person": {"name": "Alice", "age": 31}}
        similarity = merger._calculate_similarity(dict1, dict2)
        
        # person.name完全匹配（权重3.0），person.age不匹配（权重1.0）
        self.assertGreater(similarity, 0.6)
    
    # ==================== 列表无序匹配测试 ====================
    
    def test_list_similarity_unordered_identical(self):
        """测试完全相同的列表（无序）"""
        list1 = ["a", "b", "c"]
        list2 = ["a", "b", "c"]
        similarity = self.merger._calculate_similarity(list1, list2)
        self.assertEqual(similarity, 1.0)
    
    def test_list_similarity_unordered_reordered(self):
        """测试顺序不同但元素相同的列表"""
        list1 = ["a", "b", "c"]
        list2 = ["c", "b", "a"]
        similarity = self.merger._calculate_similarity(list1, list2)
        self.assertEqual(similarity, 1.0)
    
    def test_list_similarity_unordered_partial(self):
        """测试部分相似的列表"""
        list1 = ["a", "b"]
        list2 = ["a", "c"]
        similarity = self.merger._calculate_similarity(list1, list2)
        # 一个匹配，一个不匹配，加上长度惩罚
        self.assertGreater(similarity, 0.0)
        self.assertLess(similarity, 1.0)
    
    def test_list_similarity_unordered_different_length(self):
        """测试不同长度的列表"""
        list1 = ["a", "b"]
        list2 = ["a", "b", "c"]
        similarity = self.merger._calculate_similarity(list1, list2)
        # 长度惩罚 = 2/3
        self.assertLess(similarity, 1.0)
    
    def test_list_similarity_empty(self):
        """测试空列表"""
        similarity = self.merger._calculate_similarity([], [])
        self.assertEqual(similarity, 1.0)
    
    # ==================== 数组合并测试 ====================
    
    def test_merge_strings_replace(self):
        """测试字符串数组合并 - 替换相似元素"""
        current = ["apple", "banana", "cherry"]
        incoming = ["appl", "orange"]
        result = self.merger.merge_arrays(current, incoming)
        
        # "appl" 与 "apple" 相似度 > 0.6，应该替换
        self.assertIn("appl", result)
        self.assertNotIn("apple", result)
        # "orange" 无匹配，应该追加
        self.assertIn("orange", result)
    
    def test_merge_strings_append(self):
        """测试字符串数组合并 - 追加不相似元素"""
        current = ["apple", "banana"]
        incoming = ["xyz"]
        result = self.merger.merge_arrays(current, incoming)
        
        self.assertEqual(len(result), 3)
        self.assertIn("xyz", result)
    
    def test_merge_dicts_replace(self):
        """测试字典数组合并 - 替换相似元素"""
        # 使用字段权重来确保name字段更重要
        config = MergeConfig(
            similarity_threshold=0.6,
            field_weights={"name": 3.0}  # name权重更高
        )
        merger = SmartArrayMerger(config)
        
        current = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25}
        ]
        incoming = [
            {"name": "Alice", "age": 31}  # name完全匹配
        ]
        result = merger.merge_arrays(current, incoming)
        
        # name完全匹配(权重3.0)，age不同(权重1.0)
        # 相似度 = (1.0*3.0 + 0.0*1.0) / 4.0 = 0.75 > 0.6，应该替换
        self.assertEqual(len(result), 2)
        # 检查Alice被替换
        alice_items = [item for item in result if item.get("name") == "Alice"]
        self.assertEqual(len(alice_items), 1)
        self.assertEqual(alice_items[0]["age"], 31)
    
    def test_merge_dicts_append(self):
        """测试字典数组合并 - 追加新元素"""
        current = [
            {"name": "Alice", "age": 30}
        ]
        incoming = [
            {"name": "Charlie", "age": 35}  # 新元素
        ]
        result = self.merger.merge_arrays(current, incoming)
        
        self.assertEqual(len(result), 2)
    
    def test_merge_primitives_replace(self):
        """测试基本类型数组合并 - 替换相等元素"""
        current = [1, 2, 3]
        incoming = [1, 4]
        result = self.merger.merge_arrays(current, incoming)
        
        # 1相等替换，4替换2（因为都不相等，4会替换第一个不相等的）
        self.assertIn(1, result)
        self.assertIn(4, result)
    
    def test_merge_mixed_types(self):
        """测试混合类型数组合并"""
        current = [1, "hello", {"name": "Alice"}]
        incoming = [1, "helo", {"name": "Alic"}]
        result = self.merger.merge_arrays(current, incoming)
        
        # 类型不同不会匹配
        self.assertEqual(len(result), 3)
    
    def test_merge_empty_current(self):
        """测试当前数组为空的情况"""
        current = []
        incoming = ["a", "b"]
        result = self.merger.merge_arrays(current, incoming)
        
        self.assertEqual(result, ["a", "b"])
    
    def test_merge_empty_incoming(self):
        """测试新增数组为空的情况"""
        current = ["a", "b"]
        incoming = []
        result = self.merger.merge_arrays(current, incoming)
        
        self.assertEqual(result, ["a", "b"])
    
    # ==================== 嵌套结构测试 ====================
    
    def test_nested_dict_similarity(self):
        """测试嵌套字典相似度"""
        dict1 = {
            "person": {
                "name": "Alice",
                "age": 30
            },
            "city": "NYC"
        }
        dict2 = {
            "person": {
                "name": "Alice",
                "age": 31
            },
            "city": "Boston"
        }
        similarity = self.merger._calculate_similarity(dict1, dict2)
        
        # person.name匹配，person.age和city不匹配
        self.assertGreater(similarity, 0.0)
        self.assertLess(similarity, 1.0)
    
    def test_nested_list_in_dict(self):
        """测试字典中的列表"""
        dict1 = {"tags": ["a", "b"]}
        dict2 = {"tags": ["b", "a"]}  # 顺序不同
        similarity = self.merger._calculate_similarity(dict1, dict2)
        
        # 无序匹配应该识别为相同
        self.assertEqual(similarity, 1.0)
    
    # ==================== 边界情况测试 ====================
    
    def test_threshold_boundary(self):
        """测试阈值边界"""
        config = MergeConfig(similarity_threshold=0.8)
        merger = SmartArrayMerger(config)
        
        current = ["apple"]
        incoming = ["appl"]  # 相似度约0.89
        result = merger.merge_arrays(current, incoming)
        
        # 相似度 > 0.8，应该替换
        self.assertIn("appl", result)
    
    def test_no_match_below_threshold(self):
        """测试低于阈值不匹配"""
        config = MergeConfig(similarity_threshold=0.9)
        merger = SmartArrayMerger(config)
        
        current = ["apple"]
        incoming = ["appl"]  # 相似度约0.89 < 0.9
        result = merger.merge_arrays(current, incoming)
        
        # 相似度 < 0.9，应该追加
        self.assertEqual(len(result), 2)
    
    def test_multiple_incoming_match_same_current(self):
        """测试多个incoming元素匹配同一个current元素"""
        current = ["apple"]
        incoming = ["appl", "apple"]  # 都与apple相似
        result = self.merger.merge_arrays(current, incoming)
        
        # 第一个匹配替换，第二个应该追加（因为索引已被使用）
        self.assertEqual(len(result), 2)


class TestMergeConfig(unittest.TestCase):
    """MergeConfig测试类"""
    
    def test_default_values(self):
        """测试默认值"""
        config = MergeConfig()
        self.assertEqual(config.similarity_threshold, 0.6)
        self.assertEqual(config.field_weights, {})
    
    def test_custom_values(self):
        """测试自定义值"""
        config = MergeConfig(
            similarity_threshold=0.7,
            field_weights={"name": 2.0}
        )
        self.assertEqual(config.similarity_threshold, 0.7)
        self.assertEqual(config.field_weights, {"name": 2.0})


if __name__ == "__main__":
    unittest.main()
