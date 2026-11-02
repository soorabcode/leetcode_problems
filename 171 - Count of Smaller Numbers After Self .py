# Day 171 
# 315. Count of Smaller Numbers After Self
# Given an integer array nums, return an integer array counts where counts[i] is the number of smaller elements to the right of nums[i].


class Solution:
    def countSmaller(self, nums: list[int]) -> list[int]:
        n = len(nums)
        counts = [0] * n
        arr = list(enumerate(nums))

        def merge_sort(arr):
            if len(arr) <= 1:
                return arr

            mid = len(arr) // 2
            left = merge_sort(arr[:mid])
            right = merge_sort(arr[mid:])

            merged = []
            i = j = 0

            while i < len(left) and j < len(right):
                if left[i][1] <= right[j][1]:
                    counts[left[i][0]] += j
                    merged.append(left[i])
                    i += 1
                else:
                    merged.append(right[j])
                    j += 1

            while i < len(left):
                counts[left[i][0]] += j
                merged.append(left[i])
                i += 1

            merged.extend(right[j:])
            return merged

        merge_sort(arr)
        return counts