# Day 170 
# Squares of a Sorted Array
# LeetCode 977
# Given an integer array nums sorted in non-decreasing order, return an array of the squares of each number sorted in non-decreasing order.
class Solution:
    def sortedSquares(self, nums: list[int]) -> list[int]:
        nums = [x * x for x in nums]
        nums.sort()
        return nums