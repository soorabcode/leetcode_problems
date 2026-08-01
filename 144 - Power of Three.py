# Day 144 
# Power of Three 
# Given an integer n, return true if it is a power of three. Otherwise, return false.
class Solution:
    def isPowerOfThree(self, n: int) -> bool:
        if n <= 0:
            return False

        while n % 3 == 0:
            n //= 3

        return n == 1