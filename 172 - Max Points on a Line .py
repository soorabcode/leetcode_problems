# Day 172 
# 149. Max Points on a Line
# Given an array of points where points[i] = [xi, yi] represents a point on the X-Y plane, return the maximum number of points that lie on the same straight line.

from collections import defaultdict
from math import gcd

class Solution:
    def maxPoints(self, points: list[list[int]]) -> int:
        result = 1

        for i, (x1, y1) in enumerate(points):
            slopes = defaultdict(int)
            same = 0
            best = 0

            for x2, y2 in points[i + 1:]:
                dx, dy = x2 - x1, y2 - y1

                if dx == 0 and dy == 0:
                    same += 1
                    continue

                g = gcd(dx, dy)
                dx, dy = dx // g, dy // g

                if dx < 0 or (dx == 0 and dy < 0):
                    dx, dy = -dx, -dy

                slopes[(dx, dy)] += 1
                best = max(best, slopes[(dx, dy)])

            result = max(result, best + same + 1)

        return result