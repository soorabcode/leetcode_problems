# Day 162 
# Pairs of Songs With Total Durations Divisible by 60
# Leetcode 1010 
# You are given a list of songs where the ith song has a duration of time[i] seconds.
# Return the number of pairs of songs for which their total duration in seconds is divisible by 60. Formally, we want the number of indices i, j such that i < j with (time[i] + time[j]) % 60 == 0.
class Solution:
  def numPairsDivisibleBy60(self, time: list[int]) -> int:
      count = [0] * 60
      pairs = 0

      for t in time:
          r = t % 60
          pairs += count[(60 - r) % 60]
          count[r] += 1

      return pairs