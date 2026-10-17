# Day 157 
# Lexicographical Numbers
# Given an integer n, return all the numbers in the range [1, n] sorted in lexicographical order.
# You must write an algorithm that runs in O(n) time and uses O(1) extra space. 

class Solution:
    def lexicalOrder(self, n: int) -> list[int]:
        res, num = [], 1
        for _ in range(n):
            res.append(num)
            if num * 10 <= n:
                num *= 10
            elif num % 10 != 9 and num + 1 <= n:
                num += 1
            else:
                while num % 10 == 9 or num + 1 > n:
                    num //= 10
                num += 1
        return res