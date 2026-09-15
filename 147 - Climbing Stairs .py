# Day 147 
# Climbing Stairs 
# You are climbing a staircase. It takes n steps to reach the top.
# Each time you can either climb 1 or 2 steps. In how many distinct ways can you climb to the top?
# DP: dp[i] = dp[i − 1] + dp[i − 2]
# Python 3 
class Solution:
    def climbStairs(self, n: int) -> int:
        if n <= 2:
            return n

        first, second = 1, 2

        for _ in range(3, n + 1):
            first, second = second, first + second

        return second