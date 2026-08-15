# Day 153 
# Coin Change 
# You are given an integer array coins representing coins of different denominations and an integer amount representing a total amount of money.
# Return the fewest number of coins that you need to make up that amount. If that amount of money cannot be made up by any combination of the coins, return -1.

# Unbounded knapsack: dp[i] = minc(dp[i − c] + 1)
# bottom-up with initialization to ∞.
class Solution:
    def coinChange(self, coins: List[int], amount: int) -> int:
        dp = [amount + 1] * (amount + 1)
        dp[0] = 0

        for i in range(1, amount + 1):
            for c in coins:
                if c <= i:
                    dp[i] = min(dp[i], dp[i-c] + 1)

        return -1 if dp[amount] > amount else dp[amount]