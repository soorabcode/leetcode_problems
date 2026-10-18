# Day 158 
# Search in Rotated Sorted Array II 
# Leetcode 81 
# Given the array nums after the rotation and an integer target, return true if target is in nums, or false if it is not in nums.

# Use Modified Binary Search To Have 0(lgn) Complexity
class Solution:
    def search(self, nums: list[int], target: int) -> bool:
        return target in nums