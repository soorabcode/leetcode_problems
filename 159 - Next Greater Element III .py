# Day 159 
# Next Greater Element III 
# Given a positive integer n, find the smallest integer which has exactly the same digits existing in the integer n and is greater in value than n. If no such positive integer exists, return -1.

# Note that the returned integer should fit in 32-bit integer, if there is a valid answer but it does not fit in 32-bit integer, return -1.

class Solution:
    def nextGreaterElement(self, n: int) -> int:
        from itertools import permutations

        for p in sorted(set(permutations(str(n)))):
            x = int(''.join(p))
            if x > n:
                return x if x < 2**31 else -1
        return -1