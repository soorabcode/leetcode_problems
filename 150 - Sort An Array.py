# Day 150
# Sort an Array 
# Given Array of integers as num , return sorted version 

# Merge Sort

class Solution:
    def sortArray(self, nums: List[int]) -> List[int]:
        if len(nums) < 2: return nums
        m = len(nums) // 2
        l, r = self.sortArray(nums[:m]), self.sortArray(nums[m:])
        i = j = 0
        ans = []
        while i < len(l) and j < len(r):
            if l[i] < r[j]:
                ans.append(l[i]); i += 1
            else:
                ans.append(r[j]); j += 1
        return ans + l[i:] + r[j:]