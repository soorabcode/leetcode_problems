# Day 161
# Reverse Integer - LeetCode 7

class Solution:
  def reverse(self, x: int) -> int:
      r = int(str(abs(x))[::-1])
      r = -r if x < 0 else r
      return r if -2**31 <= r <= 2**31 - 1 else 0