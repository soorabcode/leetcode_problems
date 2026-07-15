# Day 140
# Pow(x,n)
# Implement pow(x, n), which calculates x raised to the power n (i.e., xn).
class Solution:
    def myPow(self, x: float, n: int) -> float:
        if n < 0:
            x = 1 / x
            n = -n

        ans = 1

        while n:
            if n % 2 == 1:      # If n is odd
                ans *= x
            x *= x              # Square the base
            n //= 2             # Divide exponent by 2

        return ans