# Day 165 
# Jewels and Stones
# LeetCode 771 
# You're given strings jewels representing the types of stones that are jewels, and stones representing the stones you have. Each character in stones is a type of stone you have. You want to know how many of the stones you have are also jewels.

class Solution:
    def numJewelsInStones(self, jewels: str, stones: str) -> int:
        sum = 0
        for s in stones :
            if s in jewels : 
                sum = sum + 1

        return sum