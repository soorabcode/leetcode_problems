# DAY 184 
# 378. Kth Smallest Element in a Sorted Matrix
# Given an n x n matrix where each of the rows and columns is sorted in ascending order, return the kth smallest element in the matrix.

import heapq

class Solution:
    def kthSmallest(self, matrix: list[list[int]], k: int) -> int:
        heap = []

        for row in matrix:
            for num in row:
                heapq.heappush(heap, num)

        for i in range(k):
            ans = heapq.heappop(heap)

        return ans 